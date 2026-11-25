# Airline Reservation System — Tercer Parcial AWS (Events, Live View & Star Schema)

Implementación completa desde cero para el **Tercer Parcial de Big Data and Data Engineering**, diseñada bajo el principio rector del enunciado: **la arquitectura más simple y económica que cumple estrictamente R1–R12 (`$18.93 USD/mes` proyectados)**.

---

## Estructura del Repositorio

```text
.
├── backend/
│   ├── config.py              # Configuración centralizada (.env / variables de entorno)
│   ├── db.py                  # Almacén transaccional SQLite (WAL mode, control atómico de sillas)
│   ├── events.py              # Constructores de eventos Kinesis sin PII y UTC estricto (R1)
│   ├── publisher.py           # Cola asíncrona no bloqueante + batch put_records con reintentos (R2)
│   ├── consumer.py            # Consumidor Kinesis compartido -> Live View (R3) + S3 .jsonl.gz (R4)
│   └── main.py                # API FastAPI (endpoints Parcial 2 + Parcial 3)
├── etl/
│   ├── reference_data.py      # Carga/generación determinística de rutas, historial SCD2 y festivos CO
│   └── star_builder.py        # ETL idempotente PyArrow -> Parquet Snappy S3 + Glue Catalog + Particiones (R5-R10)
├── scripts/
│   ├── aws_up.py              # Crea S3, Kinesis (1 shard PROVISIONED, 24h), Glue DB y Athena cutoff <= 1GB
│   ├── aws_down.py            # Elimina el Kinesis Stream al pausar trabajo para cuidar los $50 del Learner Lab
│   ├── deploy_ec2_userdata.sh # Instalación automatizada con systemd en EC2 (Ubuntu / Amazon Linux 2023)
│   └── local_smoke_test.py    # Prueba local offline (API burst + Q1-Q8 contra el oráculo StarTruth)
├── docs/
│   ├── ENTREGA_DISENO_ARQUITECTURA.md  # Documento completo Sección 5, 6 y guía de Defensa Oral
│   └── PROMPT_LOG.md                   # Anexo obligatorio de registro de prompts de IA
├── cost_projection.yaml       # Proyección mensual de costos (R12)
└── parcial3-harness-estudiantes/       # Suite oficial de evaluación (pytest -> report.md / report.json)
```

---

## Paso a Paso para Desplegar y Sacar 70/70 en el Harness

### Paso 1: Prueba local rápida (sin gastar saldo de AWS)
En tu máquina local:
```bash
python -m venv .venv
# En Windows PowerShell:
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts/local_smoke_test.py --seed 417
```
Esto valida en segundos los endpoints, la ráfaga concurrente de 200 búsquedas, la ventana en vivo por `event_ts`, `dim_date` (1,826 días), `dim_route` (SCD Tipo 2) y las 8 consultas de negocio **Q1–Q8** contra el oráculo oficial.

---

### Paso 2: Configurar variables del equipo (`.env` y `config.yaml`)
1. Copia `.env.example` a `.env` y asigna tu semilla y un nombre de bucket S3 único:
   ```ini
   TEAM_ID=team01
   TEAM_SEED=417
   AWS_REGION=us-east-1
   KINESIS_STREAM=airline-events
   S3_BUCKET=airline-p3-team01-tu-codigo-unico
   ```
2. Genera los archivos de referencia oficiales con tu semilla:
   ```bash
   cd parcial3-harness-estudiantes
   python -m airline_data.generate_p3 --seed 417 --out ../ref
   cd ..
   ```

---

### Paso 3: Levantar la infraestructura mínima en AWS (`R10`, `R11`)
Exporta las credenciales temporales de tu **AWS Academy Learner Lab** (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) en tu terminal y ejecuta:
```bash
python scripts/aws_up.py
```
Este script deja listos:
* El bucket S3 (`raw/events/`, `star/`, `athena-results/`).
* El stream Kinesis `airline-events` en modo **`PROVISIONED`**, **1 shard**, retención **24 h** y **0 consumidores Enhanced Fan-Out** (**R11**).
* El Workgroup `primary` de Athena con **`BytesScannedCutoffPerQuery = 900 MB`** (`<= 1 GB`, **R10**).
* Las 5 tablas Parquet iniciales registradas en `airline_star` (Glue Data Catalog, **R5**).

---

### Paso 4: Desplegar el Backend en EC2 (Learner Lab)
1. En la consola EC2 del Learner Lab lanza una instancia **`t3.small`** (Amazon Linux 2023 o Ubuntu 22.04/24.04):
   * **IAM Instance Profile:** selecciona **`LabInstanceProfile`** (indispensable para que EC2 acceda a Kinesis, S3 y Glue sin credenciales estáticas).
   * **Security Group:** abre **TCP `22`** (SSH) y **TCP `8000`** (`0.0.0.0/0` o tu IP).
2. Sube o clona este repositorio en la instancia EC2, crea tu `.env` (con el mismo `TEAM_SEED` y `S3_BUCKET`) y ejecuta:
   ```bash
   chmod +x scripts/deploy_ec2_userdata.sh
   sudo ./scripts/deploy_ec2_userdata.sh
   curl -s http://127.0.0.1:8000/health
   ```

---

### Paso 5: Ejecutar el Harness Oficial (`pytest`)
1. Edita `parcial3-harness-estudiantes/config.yaml` colocando la **IP pública actual de tu EC2** y tu **bucket S3**:
   ```yaml
   team_id: "team01"
   seed: 417

   backend:
     base_url: "http://<IP_PUBLICA_EC2>:8000"

   aws:
     region: us-east-1
     kinesis_stream: airline-events
     raw_events_uri: "s3://airline-p3-team01-tu-codigo-unico/raw/events/"
     athena_workgroup: primary
     athena_database: airline_star
     athena_output: "s3://airline-p3-team01-tu-codigo-unico/athena-results/"

   cost_projection_file: ../cost_projection.yaml
   ```
2. Corre el harness:
   ```bash
   cd parcial3-harness-estudiantes
   pytest
   ```
   Al finalizar, revisa `parcial3-harness-estudiantes/report.md` y `report.json` (incluir `report.md` en la entrega final).

---

### Paso 6: Apagar recursos por hora al terminar de trabajar
Para no agotar los `$50 USD` del Learner Lab mientras no estés probando:
```bash
python scripts/aws_down.py
```
*(Elimina el stream de Kinesis en segundos; también detén la instancia EC2 desde la consola. Cuando vuelvas a trabajar, corre `python scripts/aws_up.py` y actualiza la IP en `config.yaml`).*
