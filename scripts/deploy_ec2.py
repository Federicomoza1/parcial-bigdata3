"""Automated EC2 t3.micro deployer for AWS Academy Learner Lab (us-east-1).

- Uses existing `LabInstanceProfile` (NEVER creates IAM roles).
- Reuses or creates Security Group `airline-p3-sg` with inbound TCP 22 and TCP 8000.
- Uses `vockey` key pair if present, or creates `airline-p3-key` (.pem saved locally).
- Packages `backend/`, `etl/`, `scripts/`, `requirements.txt`, and `.env` into S3
  (`s3://<S3_BUCKET>/_deploy/app.zip`) and launches an Amazon Linux 2023 `t3.micro`
  instance whose UserData downloads the bundle via `LabInstanceProfile` and runs
  `deploy_ec2_userdata.sh` to start `airline-backend.service` on port 8000.
- Waits until `GET http://<PUBLIC_IP>:8000/analytics/live` responds HTTP 200 and
  updates `parcial3-harness-estudiantes/config.yaml` automatically.

Usage:
    python scripts/deploy_ec2.py
"""
from __future__ import annotations

import io
import os
import sys
import time
import zipfile
from pathlib import Path

import boto3
import httpx
import yaml
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.config import get_settings  # noqa: E402

SG_NAME = "airline-p3-sg"
INSTANCE_TAG_NAME = "airline-p3-backend"
DEPLOY_S3_KEY = "_deploy/app.zip"


def build_code_zip() -> bytes:
    buf = io.BytesIO()
    include_dirs = ("backend", "etl", "scripts", "ref", "static")
    include_files = ("requirements.txt", ".env", ".env.example", "cost_projection.yaml")

    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fname in include_files:
            fpath = ROOT / fname
            if fpath.exists():
                zf.write(fpath, arcname=fname)
        for dname in include_dirs:
            dpath = ROOT / dname
            if not dpath.exists():
                continue
            for p in dpath.rglob("*"):
                if p.is_file() and "__pycache__" not in p.parts and not p.name.endswith((".pyc", ".pyo")):
                    zf.write(p, arcname=str(p.relative_to(ROOT)).replace("\\", "/"))
    return buf.getvalue()


def ensure_key_pair(ec2) -> str:
    keys = {k["KeyName"] for k in ec2.describe_key_pairs().get("KeyPairs", [])}
    if "vockey" in keys:
        print("[OK] Using existing AWS Academy key pair: vockey")
        return "vockey"

    custom_key = "airline-p3-key"
    pem_path = ROOT / f"{custom_key}.pem"
    if custom_key in keys:
        print(f"[OK] Using existing key pair: {custom_key}")
        return custom_key

    print(f"[..] 'vockey' not found; creating key pair '{custom_key}' -> {pem_path}")
    resp = ec2.create_key_pair(KeyName=custom_key, KeyType="rsa", KeyFormat="pem")
    pem_path.write_text(resp["KeyMaterial"], encoding="utf-8")
    try:
        os.chmod(pem_path, 0o400)
    except Exception:
        pass
    print(f"[OK] Saved private key to {pem_path}")
    return custom_key


def ensure_security_group(ec2) -> str:
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}]).get("Vpcs", [])
    if not vpcs:
        vpcs = ec2.describe_vpcs().get("Vpcs", [])
    vpc_id = vpcs[0]["VpcId"]

    existing = ec2.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [SG_NAME]}, {"Name": "vpc-id", "Values": [vpc_id]}]
    ).get("SecurityGroups", [])

    if existing:
        sg_id = existing[0]["GroupId"]
        print(f"[OK] Using existing Security Group: {SG_NAME} ({sg_id})")
    else:
        sg_id = ec2.create_security_group(
            GroupName=SG_NAME,
            Description="Airline Parcial 3 Backend (SSH 22 + FastAPI 8000)",
            VpcId=vpc_id,
        )["GroupId"]
        print(f"[OK] Created Security Group: {SG_NAME} ({sg_id})")

    for port in (22, 8000):
        try:
            ec2.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": port,
                        "ToPort": port,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": f"Allow TCP {port}"}],
                    }
                ],
            )
        except ClientError as exc:
            if "InvalidPermission.Duplicate" not in str(exc):
                raise
    return sg_id


def resolve_instance_profile_arn_or_name(iam) -> dict:
    """Finds LabInstanceProfile without ever creating new IAM roles."""
    try:
        prof = iam.get_instance_profile(InstanceProfileName="LabInstanceProfile")["InstanceProfile"]
        print(f"[OK] Found IAM Instance Profile: {prof['Arn']}")
        return {"Arn": prof["Arn"]}
    except Exception:
        print("[OK] Falling back to InstanceProfile Name='LabInstanceProfile'")
        return {"Name": "LabInstanceProfile"}


def latest_al2023_ami(ssm) -> str:
    param = ssm.get_parameter(
        Name="/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
    )
    ami_id = param["Parameter"]["Value"]
    print(f"[OK] Resolved latest Amazon Linux 2023 AMI: {ami_id}")
    return ami_id


def find_existing_instance(ec2):
    resp = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [INSTANCE_TAG_NAME]},
            {"Name": "instance-state-name", "Values": ["pending", "running"]},
        ]
    )
    for r in resp.get("Reservations", []):
        for inst in r.get("Instances", []):
            return inst
    return None


def update_harness_config(public_ip: str, bucket: str) -> Path:
    cfg_path = ROOT / "parcial3-harness-estudiantes" / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    cfg["backend"]["base_url"] = f"http://{public_ip}:8000"
    cfg["aws"]["raw_events_uri"] = f"s3://{bucket}/raw/events/"
    cfg["aws"]["athena_output"] = f"s3://{bucket}/athena-results/"
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh, sort_keys=False)
    return cfg_path


def main() -> None:
    s = get_settings()
    session = boto3.Session(region_name=s.aws_region)
    s3 = session.client("s3")
    ec2 = session.client("ec2")
    iam = session.client("iam")
    ssm = session.client("ssm")

    print(f"[1/6] Uploading application bundle to s3://{s.s3_bucket}/{DEPLOY_S3_KEY}...")
    bundle = build_code_zip()
    s3.put_object(Bucket=s.s3_bucket, Key=DEPLOY_S3_KEY, Body=bundle)
    print(f"[OK] Uploaded {len(bundle):,} bytes to s3://{s.s3_bucket}/{DEPLOY_S3_KEY}")

    key_name = ensure_key_pair(ec2)
    sg_id = ensure_security_group(ec2)
    iam_profile = resolve_instance_profile_arn_or_name(iam)
    ami_id = latest_al2023_ami(ssm)

    user_data = f"""#!/bin/bash
set -euxo pipefail
exec > /var/log/airline-bootstrap.log 2>&1

dnf install -y unzip python3.11 python3.11-pip
command -v aws >/dev/null 2>&1 || dnf install -y awscli
mkdir -p /opt/airline-p3
aws s3 cp s3://{s.s3_bucket}/{DEPLOY_S3_KEY} /tmp/app.zip --region {s.aws_region}
unzip -o /tmp/app.zip -d /opt/airline-p3
sed -i 's/\\r$//' /opt/airline-p3/.env /opt/airline-p3/scripts/deploy_ec2_userdata.sh
chown -R ec2-user:ec2-user /opt/airline-p3
chmod +x /opt/airline-p3/scripts/deploy_ec2_userdata.sh
SUDO_USER=ec2-user bash /opt/airline-p3/scripts/deploy_ec2_userdata.sh
"""

    existing = find_existing_instance(ec2)
    if existing:
        instance_id = existing["InstanceId"]
        print(f"[WARN] Terminating previous instance {instance_id} to deploy clean...")
        ec2.terminate_instances(InstanceIds=[instance_id])
        ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])

    print("[2/6] Launching EC2 t3.micro instance...")
    run_resp = ec2.run_instances(
        ImageId=ami_id,
        InstanceType="t3.micro",
        MinCount=1,
        MaxCount=1,
        KeyName=key_name,
        SecurityGroupIds=[sg_id],
        IamInstanceProfile=iam_profile,
        UserData=user_data,
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": INSTANCE_TAG_NAME}],
            }
        ],
    )
    instance_id = run_resp["Instances"][0]["InstanceId"]
    print(f"[OK] Launched EC2 instance: {instance_id}. Waiting for running state & Public IP...")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])

    inst_desc = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
    public_ip = inst_desc.get("PublicIpAddress")
    if not public_ip:
        raise RuntimeError(f"Instance {instance_id} has no PublicIpAddress!")
    print(f"[OK] Instance {instance_id} is RUNNING at Public IP: {public_ip}")

    cfg_path = update_harness_config(public_ip, s.s3_bucket)
    print(f"[OK] Updated {cfg_path} -> base_url = http://{public_ip}:8000")

    live_url = f"http://{public_ip}:8000/analytics/live"
    print(f"[3/6] Waiting for backend service to finish bootstrap (~60-120s): {live_url} ...")
    t0 = time.time()
    while time.time() - t0 < 240:
        try:
            r = httpx.get(live_url, timeout=5.0)
            if r.status_code == 200:
                print(f"\n[SUCCESS] Backend is LIVE at {live_url} -> {r.json()}")
                print("Ready to run `cd parcial3-harness-estudiantes && pytest`!")
                return
        except Exception:
            pass
        time.sleep(6)

    raise TimeoutError(
        f"Backend at {live_url} did not respond within 240s. Check /var/log/airline-bootstrap.log via SSH."
    )


if __name__ == "__main__":
    main()
