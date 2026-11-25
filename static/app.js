/* Céfiro — booking site. Talks only to the existing backend API:
 *   GET  /flights/search        (publishes search_performed)
 *   POST /reservations          (publishes reservation_created)
 *   GET  /reservations/{id}
 *   POST /reservations/{id}/cancel (publishes reservation_cancelled)
 *   POST /admin/flights         (demo inventory only)
 * The backend has no per-seat model, so the chosen seat lives in the browser (localStorage).
 */
(() => {
  'use strict';

  const CITIES = {
    BOG: 'Bogotá', MDE: 'Medellín', CLO: 'Cali', CTG: 'Cartagena', BAQ: 'Barranquilla',
    SMR: 'Santa Marta', BGA: 'Bucaramanga', PEI: 'Pereira', CUC: 'Cúcuta',
    ADZ: 'San Andrés', LET: 'Leticia', PSO: 'Pasto',
  };
  const POPULAR = ['BOG-LET', 'BAQ-CTG', 'PSO-MDE', 'ADZ-BGA', 'SMR-CTG'];
  const BUSINESS_MULTIPLIER = 2.2; // same rule as backend/db.py
  const TRIPS_KEY = 'cefiro_trips_v1';

  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const money = (cents) => new Intl.NumberFormat('es-CO', { style: 'currency', currency: 'USD' }).format((Number(cents) || 0) / 100);
  const bizPrice = (base) => Math.round(base * BUSINESS_MULTIPLIER);
  const fmtDate = (iso) => {
    const [y, m, d] = String(iso).split('-').map(Number);
    if (!y) return iso;
    return new Date(y, m - 1, d).toLocaleDateString('es-CO', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' });
  };
  const todayIso = (offsetDays = 0) => {
    const d = new Date(); d.setDate(d.getDate() + offsetDays);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  };

  // One session id per browser tab → X-Session-Id (partition key of search events)
  const sessionId = (() => {
    let s = sessionStorage.getItem('cefiro_session');
    if (!s) { s = `web-${crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(16).slice(2)}`; sessionStorage.setItem('cefiro_session', s); }
    return s;
  })();

  function toast(msg, kind = 'info') {
    const colors = { info: 'border-slate-200', ok: 'border-accent', err: 'border-rose-400' };
    const el = document.createElement('div');
    el.className = `toast bg-white border-l-4 ${colors[kind] || colors.info} rounded-xl shadow-card px-4 py-3 text-sm text-slate-700`;
    el.textContent = msg;
    $('toasts').appendChild(el);
    setTimeout(() => el.remove(), 4200);
  }

  async function api(path, opts = {}) {
    const resp = await fetch(path, opts);
    let body = null;
    try { body = await resp.json(); } catch (_) { /* empty body */ }
    return { ok: resp.ok, status: resp.status, body };
  }

  // ---------------------------------------------------------------- trips store
  let trips = [];
  try { trips = JSON.parse(localStorage.getItem(TRIPS_KEY) || '[]'); } catch (_) { trips = []; }
  const saveTrips = () => { localStorage.setItem(TRIPS_KEY, JSON.stringify(trips)); renderTrips(); };

  // ---------------------------------------------------------------- search form
  const state = { query: null, flights: [], flight: null, layout: null, seat: null };

  function initSearchForm() {
    const opts = Object.entries(CITIES).map(([c, n]) => `<option value="${c}">${n} (${c})</option>`).join('');
    $('origin').innerHTML = opts; $('destination').innerHTML = opts;
    $('origin').value = 'BOG'; $('destination').value = 'LET';
    $('travel-date').min = todayIso(0);
    $('travel-date').value = todayIso(14);
    $('swap').addEventListener('click', () => {
      const o = $('origin').value; $('origin').value = $('destination').value; $('destination').value = o;
    });
    $('popular').innerHTML = '<span>Rutas frecuentes:</span>' + POPULAR.map((r) => {
      const [o, d] = r.split('-');
      return `<button type="button" class="chip" data-route="${r}">${CITIES[o]} → ${CITIES[d]}</button>`;
    }).join('');
    $('popular').addEventListener('click', (e) => {
      const b = e.target.closest('[data-route]'); if (!b) return;
      const [o, d] = b.dataset.route.split('-');
      $('origin').value = o; $('destination').value = d;
      search();
    });
    $('search-form').addEventListener('submit', (e) => { e.preventDefault(); search(); });
  }

  async function search() {
    const origin = $('origin').value, destination = $('destination').value, date = $('travel-date').value;
    if (origin === destination) { toast('El origen y el destino deben ser distintos.', 'err'); return; }
    if (!date) { toast('Selecciona una fecha de viaje.', 'err'); return; }
    state.query = { origin, destination, date };
    const r = await api(`/flights/search?origin=${origin}&destination=${destination}&date=${date}`, { headers: { 'X-Session-Id': sessionId } });
    if (!r.ok) { toast(`No pudimos buscar vuelos (HTTP ${r.status}).`, 'err'); return; }
    state.flights = r.body.flights || [];
    renderResults();
    $('seats').classList.add('hidden');
    $('results').classList.remove('hidden');
    $('results').scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function renderResults() {
    const { origin, destination, date } = state.query;
    $('results-title').textContent = `${CITIES[origin]} → ${CITIES[destination]}`;
    $('results-subtitle').textContent = `${fmtDate(date)} · ${state.flights.length} ${state.flights.length === 1 ? 'vuelo' : 'vuelos'}`;
    const list = $('results-list');
    if (!state.flights.length) {
      list.innerHTML = `
        <div class="fade-in bg-white rounded-2xl border border-dashed border-slate-300 p-8 text-center">
          <p class="text-ink font-medium">No hay vuelos programados para esta ruta y fecha.</p>
          <p class="mt-1 text-sm text-slate-500">Prueba otra fecha, o carga inventario de demostración para esta ruta.</p>
          <button type="button" id="btn-demo" class="btn-secondary mt-4 px-4 h-10">Programar vuelos de demostración</button>
        </div>`;
      $('btn-demo').addEventListener('click', createDemoFlights);
      return;
    }
    list.innerHTML = state.flights.map((f, i) => {
      const full = f.seats_available <= 0;
      const [o, d] = String(f.route).split('-');
      return `
        <article class="fade-in bg-white rounded-2xl border border-slate-200 shadow-card p-5 grid sm:grid-cols-[1fr_auto] gap-4 items-center" style="animation-delay:${i * 40}ms">
          <div class="flex items-center gap-6">
            <div>
              <div class="text-2xl font-semibold text-ink">${esc(o)}</div>
              <div class="text-xs text-slate-500">${esc(CITIES[o] || '')}</div>
            </div>
            <div class="flex-1 min-w-[80px] max-w-[180px] text-center">
              <div class="text-[11px] text-slate-400 font-mono">${esc(f.flight_id)}</div>
              <div class="my-1 h-px bg-slate-200 relative"><span class="absolute right-0 -top-[3px] w-[7px] h-[7px] rounded-full bg-accent"></span></div>
              <div class="text-[11px] text-slate-400">Directo</div>
            </div>
            <div>
              <div class="text-2xl font-semibold text-ink">${esc(d)}</div>
              <div class="text-xs text-slate-500">${esc(CITIES[d] || '')}</div>
            </div>
          </div>
          <div class="flex items-center gap-5 sm:justify-end">
            <div class="text-right">
              <div class="text-xs text-slate-500">Economy desde</div>
              <div class="text-xl font-semibold text-ink">${money(f.price_cents)}</div>
              <div class="text-xs text-slate-500">Business ${money(bizPrice(f.price_cents))}</div>
            </div>
            <div class="text-right">
              <div class="text-xs ${full ? 'text-rose-600' : f.seats_available <= 5 ? 'text-amber-600' : 'text-slate-500'} mb-2">
                ${full ? 'Vuelo lleno' : `${f.seats_available} de ${f.capacity} asientos libres`}
              </div>
              <button type="button" class="btn-primary px-5 h-10" data-pick="${esc(f.flight_id)}" ${full ? 'disabled' : ''}>Elegir asiento</button>
            </div>
          </div>
        </article>`;
    }).join('');
    list.querySelectorAll('[data-pick]').forEach((b) => b.addEventListener('click', () => openSeatMap(b.dataset.pick)));
  }

  async function createDemoFlights() {
    const { origin, destination, date } = state.query;
    const specs = [{ capacity: 60, price: 8900 }, { capacity: 96, price: 12900 }, { capacity: 42, price: 16500 }];
    const btn = $('btn-demo'); if (btn) btn.disabled = true;
    for (const s of specs) {
      const flight_id = `CF${Math.floor(1000 + Math.random() * 9000)}`;
      const r = await api('/admin/flights', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ flight_id, route: `${origin}-${destination}`, flight_date: date, capacity: s.capacity, price_cents: s.price + Math.floor(Math.random() * 40) * 100 }),
      });
      if (!r.ok) { toast(`No se pudo crear el vuelo de demostración (HTTP ${r.status}).`, 'err'); return; }
    }
    toast('Vuelos de demostración programados.', 'ok');
    search();
  }

  // ---------------------------------------------------------------- seat layout
  // Business cabin (2-2) at the front, Economy (3-3) behind. Total seats == flight capacity.
  function buildLayout(capacity) {
    let biz = capacity >= 20 ? Math.max(4, Math.round((capacity * 0.12) / 4) * 4) : 0;
    biz = Math.min(biz, capacity);
    const seats = [];
    let row = 1;
    const push = (n, letters, cabin) => {
      for (let i = 0; i < n; row++) {
        for (const L of letters) {
          if (i >= n) break;
          const pos = (L === 'A' || L === 'F') ? 'Ventana' : (L === 'C' || L === 'D') ? 'Pasillo' : 'Centro';
          seats.push({ id: `${row}${L}`, row, letter: L, cabin, pos });
          i++;
        }
      }
    };
    push(biz, ['A', 'C', 'D', 'F'], 'BUSINESS');
    push(capacity - biz, ['A', 'B', 'C', 'D', 'E', 'F'], 'ECONOMY');
    return seats;
  }

  // Deterministic hash so "occupied by others" seats stay stable between visits.
  function hash(str) {
    let h = 2166136261;
    for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619); }
    return h >>> 0;
  }

  function occupancy(flight, seats) {
    const mine = new Set(trips.filter((t) => t.flight_id === flight.flight_id && t.status !== 'CANCELLED' && t.seat).map((t) => t.seat));
    const othersCount = Math.max(0, Number(flight.seats_booked || 0) - mine.size);
    const candidates = seats.filter((s) => !mine.has(s.id))
      .sort((a, b) => hash(flight.flight_id + a.id) - hash(flight.flight_id + b.id));
    const occ = new Set(candidates.slice(0, othersCount).map((s) => s.id));
    return { mine, occ };
  }

  // ---------------------------------------------------------------- plane SVG
  const G = {
    R: 126,
    eco: { w: 26, h: 26, gap: 5, aisle: 30, pitch: 34 },
    biz: { w: 34, h: 34, gap: 8, aisle: 46, pitch: 50 },
    nose: 200, wing: 150, tail: 240,
  };

  function seatX(cabin, letter) {
    const g = cabin === 'BUSINESS' ? G.biz : G.eco;
    const letters = cabin === 'BUSINESS' ? ['A', 'C', 'D', 'F'] : ['A', 'B', 'C', 'D', 'E', 'F'];
    const perSide = letters.length / 2;
    const sideW = perSide * g.w + (perSide - 1) * g.gap;
    const idx = letters.indexOf(letter);
    const start = idx < perSide ? -(g.aisle / 2 + sideW) : g.aisle / 2;
    return start + (idx % perSide) * (g.w + g.gap);
  }

  function renderPlane() {
    const { flight, layout } = state;
    const { mine, occ } = occupancy(flight, layout);
    const R = G.R;
    const rowsBiz = [...new Set(layout.filter((s) => s.cabin === 'BUSINESS').map((s) => s.row))];
    const rowsEco = [...new Set(layout.filter((s) => s.cabin === 'ECONOMY').map((s) => s.row))];

    // vertical positions per row
    const rowY = {};
    $('seats-subtitle').textContent = `${flight.seats_available} asientos libres de ${flight.capacity} · pasa el cursor sobre un asiento para ver el detalle`;
    let y = G.nose + 34;
    const bizStart = y;
    rowsBiz.forEach((r) => { rowY[r] = y; y += G.biz.pitch; });
    if (rowsBiz.length) y += 30;
    const ecoStart = y;
    rowsEco.forEach((r) => { rowY[r] = y; y += G.eco.pitch; });
    const cabinEnd = y + 18;
    const total = cabinEnd + G.tail;
    const halfW = R + G.wing + 24;

    const wingY = G.nose + (cabinEnd - G.nose) * 0.36;
    const wing = (s) => `M ${s * (R - 4)} ${wingY} L ${s * (R + G.wing)} ${wingY + 120} L ${s * (R + G.wing)} ${wingY + 150} L ${s * (R - 4)} ${wingY + 140} Z`;
    const engX = R + 62, engY = wingY + 46;
    const tailY = total - 120;
    const stab = (s) => `M ${s * 18} ${tailY} L ${s * 150} ${tailY + 72} L ${s * 150} ${tailY + 94} L ${s * 18} ${tailY + 84} Z`;
    const body = `M ${-R} ${G.nose} C ${-R} 70 -62 0 0 0 C 62 0 ${R} 70 ${R} ${G.nose}
      L ${R} ${cabinEnd} C ${R} ${cabinEnd + 150} 30 ${total - 24} 12 ${total} L -12 ${total}
      C -30 ${total - 24} ${-R} ${cabinEnd + 150} ${-R} ${cabinEnd} Z`;

    const parts = [];
    parts.push(`<defs>
      <linearGradient id="gWing" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="#eef2f7"/><stop offset="1" stop-color="#d9e1eb"/></linearGradient>
      <linearGradient id="gBody" x1="0" x2="1" y1="0" y2="0"><stop offset="0" stop-color="#e9eef4"/><stop offset=".18" stop-color="#ffffff"/><stop offset=".82" stop-color="#ffffff"/><stop offset="1" stop-color="#e9eef4"/></linearGradient>
      <linearGradient id="gEng" x1="0" x2="1"><stop offset="0" stop-color="#cfd8e3"/><stop offset=".5" stop-color="#f4f7fa"/><stop offset="1" stop-color="#cfd8e3"/></linearGradient>
      <filter id="shadow" x="-20%" y="-10%" width="140%" height="120%"><feDropShadow dx="0" dy="10" stdDeviation="12" flood-color="#0f2a4a" flood-opacity=".14"/></filter>
    </defs>`);
    parts.push(`<g filter="url(#shadow)">
      <path d="${wing(1)}" fill="url(#gWing)" stroke="#c7d2de"/>
      <path d="${wing(-1)}" fill="url(#gWing)" stroke="#c7d2de"/>
      <path d="${stab(1)}" fill="url(#gWing)" stroke="#c7d2de"/>
      <path d="${stab(-1)}" fill="url(#gWing)" stroke="#c7d2de"/>
      <path d="${body}" fill="url(#gBody)" stroke="#c7d2de" stroke-width="1.2"/>
    </g>`);
    // engines (in front of wing leading edge)
    [1, -1].forEach((s) => parts.push(
      `<rect x="${s * engX - 15}" y="${engY - 40}" width="30" height="80" rx="14" fill="url(#gEng)" stroke="#b8c4d2"/>
       <rect x="${s * engX - 11}" y="${engY - 40}" width="22" height="6" rx="3" fill="#9aa9ba"/>`));
    // vertical fin (seen from above) in brand color
    parts.push(`<rect x="-5" y="${tailY - 60}" width="10" height="${total - tailY + 52}" rx="5" fill="#0f2a4a"/>`);
    // cockpit windshield
    parts.push(`<path d="M -42 92 Q 0 66 42 92 L 36 106 Q 0 86 -36 106 Z" fill="#1e3a5f" opacity=".85"/>`);
    // cabin floor (cut-away interior)
    parts.push(`<rect x="${-R + 12}" y="${bizStart - 34}" width="${2 * R - 24}" height="${cabinEnd - bizStart + 30}" rx="18" fill="#f7f9fc" stroke="#e5eaf1"/>`);
    // windows along fuselage
    layout.filter((s) => s.letter === 'A').forEach((s) => {
      const g = s.cabin === 'BUSINESS' ? G.biz : G.eco;
      const wy = rowY[s.row] + g.h / 2 - 6;
      parts.push(`<rect x="${-R + 3}" y="${wy}" width="4" height="12" rx="2" fill="#b9c6d6"/><rect x="${R - 7}" y="${wy}" width="4" height="12" rx="2" fill="#b9c6d6"/>`);
    });
    // cabin labels + column letters
    const letterRow = (cabin, yy) => {
      const g = cabin === 'BUSINESS' ? G.biz : G.eco;
      const letters = cabin === 'BUSINESS' ? ['A', 'C', 'D', 'F'] : ['A', 'B', 'C', 'D', 'E', 'F'];
      return letters.map((L) => `<text x="${seatX(cabin, L) + g.w / 2}" y="${yy}" text-anchor="middle" font-size="10" font-weight="600" fill="#94a3b8">${L}</text>`).join('');
    };
    if (rowsBiz.length) {
      parts.push(`<text x="0" y="${bizStart - 20}" text-anchor="middle" font-size="10" letter-spacing="2" font-weight="600" fill="#1e3a5f">BUSINESS</text>`);
      parts.push(letterRow('BUSINESS', bizStart - 6));
    }
    if (rowsEco.length) {
      parts.push(`<line x1="${-R + 24}" x2="${R - 24}" y1="${ecoStart - 26}" y2="${ecoStart - 26}" stroke="#e2e8f0"/>`);
      parts.push(`<text x="0" y="${ecoStart - 14}" text-anchor="middle" font-size="10" letter-spacing="2" font-weight="600" fill="#64748b">ECONOMY</text>`);
      parts.push(letterRow('ECONOMY', ecoStart - 3));
    }
    // row numbers in the aisle
    Object.entries(rowY).forEach(([r, yy]) => {
      const isBiz = rowsBiz.includes(Number(r));
      const g = isBiz ? G.biz : G.eco;
      parts.push(`<text x="0" y="${yy + g.h / 2 + 3.5}" text-anchor="middle" font-size="9.5" fill="#94a3b8">${r}</text>`);
    });
    // seats
    layout.forEach((s) => {
      const g = s.cabin === 'BUSINESS' ? G.biz : G.eco;
      const x = seatX(s.cabin, s.letter), yy = rowY[s.row];
      const st = mine.has(s.id) ? 'mine' : occ.has(s.id) ? 'occ' : state.seat === s.id ? 'sel' : 'free';
      const cls = `seat ${s.cabin === 'BUSINESS' ? 'biz' : 'eco'} ${st === 'sel' ? 'free sel' : st}`;
      const label = `Asiento ${s.id}, ${s.pos}, ${s.cabin === 'BUSINESS' ? 'Business' : 'Economy'}, ${st === 'free' || st === 'sel' ? 'disponible' : st === 'mine' ? 'tu asiento' : 'ocupado'}`;
      const focusable = st === 'free' || st === 'sel';
      parts.push(`<g class="${cls}" data-seat="${s.id}" transform="translate(${x} ${yy})" ${focusable ? 'tabindex="0" role="button"' : 'aria-disabled="true"'} aria-label="${label}">
        <rect class="base" width="${g.w}" height="${g.h}" rx="${s.cabin === 'BUSINESS' ? 8 : 6}"/>
        <rect class="back" x="3" y="${g.h - 6}" width="${g.w - 6}" height="4" rx="2"/>
        ${st === 'sel' ? `<path class="mark" d="M ${g.w / 2 - 5} ${g.h / 2 - 2} l 4 4 l 7 -7" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>` : ''}
        ${st === 'occ' ? `<path class="mark" d="M ${g.w / 2 - 4} ${g.h / 2 - 6} l 8 8 M ${g.w / 2 + 4} ${g.h / 2 - 6} l -8 8" stroke="#b4c0cf" stroke-width="1.5" stroke-linecap="round"/>` : ''}
      </g>`);
    });

    $('plane-tilt').innerHTML = `<svg viewBox="${-halfW} -10 ${halfW * 2} ${total + 20}" role="img" aria-label="Mapa de asientos del vuelo ${esc(flight.flight_id)}">${parts.join('')}</svg>`;
  }

  // ---------------------------------------------------------------- seat interaction
  function seatById(id) { return state.layout.find((s) => s.id === id); }
  function seatPrice(s) { return s.cabin === 'BUSINESS' ? bizPrice(state.flight.price_cents) : Number(state.flight.price_cents); }

  function selectSeat(id) {
    const s = seatById(id); if (!s) return;
    state.seat = state.seat === id ? null : id;
    renderPlane();
    updateSelection();
    if (state.seat) {
      const el = document.querySelector(`[data-seat="${id}"]`); el && el.focus({ preventScroll: true });
    }
  }

  function updateSelection() {
    const s = state.seat && seatById(state.seat);
    $('seat-empty').classList.toggle('hidden', !!s);
    $('seat-detail').classList.toggle('hidden', !s);
    if (s) {
      $('sel-seat').textContent = s.id;
      $('sel-price').textContent = money(seatPrice(s));
      $('sel-desc').textContent = `${s.pos} · ${s.cabin === 'BUSINESS' ? 'Business' : 'Economy'} · Fila ${s.row}`;
    }
    $('btn-book').disabled = !s;
  }

  function initSeatEvents() {
    const wrap = $('plane-tilt');
    const tip = $('seat-tooltip');
    const container = tip.parentElement;
    wrap.addEventListener('click', (e) => {
      const g = e.target.closest('.seat.free'); if (g) selectSeat(g.dataset.seat);
    });
    wrap.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      const g = e.target.closest('.seat.free'); if (!g) return;
      e.preventDefault(); selectSeat(g.dataset.seat);
    });
    wrap.addEventListener('mousemove', (e) => {
      const g = e.target.closest('.seat');
      if (!g) { tip.classList.add('hidden'); return; }
      const s = seatById(g.dataset.seat);
      const status = g.classList.contains('mine') ? 'Tu asiento' : g.classList.contains('occ') ? 'Ocupado' : money(seatPrice(s));
      tip.innerHTML = `<strong>${s.id}</strong> · ${s.pos} · ${s.cabin === 'BUSINESS' ? 'Business' : 'Economy'}<br><span style="opacity:.8">${esc(status)}</span>`;
      const box = container.getBoundingClientRect(), r = g.getBoundingClientRect();
      tip.style.left = `${r.left + r.width / 2 - box.left}px`;
      tip.style.top = `${r.top - box.top}px`;
      tip.classList.remove('hidden');
    });
    wrap.addEventListener('mouseleave', () => tip.classList.add('hidden'));
    $('plane-scroll').addEventListener('scroll', () => tip.classList.add('hidden'));

    document.querySelectorAll('.view-btn').forEach((b) => b.addEventListener('click', () => {
      document.querySelectorAll('.view-btn').forEach((x) => x.classList.toggle('is-active', x === b));
      $('plane-tilt').classList.toggle('is-tilted', b.dataset.view === 'tilt');
    }));
    $('back-to-results').addEventListener('click', () => {
      $('seats').classList.add('hidden');
      $('results').scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
    $('booking-form').addEventListener('submit', (e) => { e.preventDefault(); book(); });
  }

  function openSeatMap(flightId) {
    const f = state.flights.find((x) => x.flight_id === flightId); if (!f) return;
    state.flight = f;
    state.layout = buildLayout(Number(f.capacity));
    state.seat = null;
    const [o, d] = String(f.route).split('-');
    $('sum-flight').textContent = f.flight_id;
    $('sum-from').textContent = o; $('sum-from-city').textContent = CITIES[o] || '';
    $('sum-to').textContent = d; $('sum-to-city').textContent = CITIES[d] || '';
    $('sum-date').textContent = fmtDate(f.flight_date);
    renderPlane();
    updateSelection();
    $('seats').classList.remove('hidden');
    $('plane-scroll').scrollTop = 0;
    $('seats').scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  async function book() {
    const s = state.seat && seatById(state.seat);
    const name = $('passenger').value.trim();
    if (!s) return;
    if (!name) { $('passenger').focus(); toast('Escribe el nombre del pasajero.', 'err'); return; }
    $('btn-book').disabled = true;
    const r = await api('/reservations', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ flight_id: state.flight.flight_id, passenger_name: name, fare_class: s.cabin }),
    });
    if (r.status === 409) { toast('Este vuelo ya no tiene asientos disponibles.', 'err'); updateSelection(); return; }
    if (!r.ok) { toast(`No se pudo completar la reserva (HTTP ${r.status}).`, 'err'); updateSelection(); return; }
    const res = r.body;
    trips.unshift({ ...res, seat: s.id, seat_pos: s.pos, passenger_name: name });
    saveTrips();
    // reflect the new booking locally
    state.flight.seats_booked = Number(state.flight.seats_booked || 0) + 1;
    state.flight.seats_available = Math.max(0, Number(state.flight.seats_available) - 1);
    state.seat = null;
    renderPlane(); updateSelection(); renderResults();
    $('confirm-text').textContent = `Asiento ${s.id} (${s.cabin === 'BUSINESS' ? 'Business' : 'Economy'}) en el vuelo ${res.flight_id}, ${fmtDate(res.flight_date)}. Código de reserva: ${res.reservation_id}.`;
    $('confirm-dialog').showModal();
  }

  // ---------------------------------------------------------------- trips
  function renderTrips() {
    const active = trips.filter((t) => t.status !== 'CANCELLED').length;
    const badge = $('trips-count');
    badge.textContent = active; badge.classList.toggle('hidden', !active);
    const box = $('trips');
    if (!trips.length) {
      box.innerHTML = `<div class="md:col-span-2 bg-white rounded-2xl border border-dashed border-slate-300 p-8 text-center text-sm text-slate-500">Aún no tienes reservas. Cuando reserves un vuelo aparecerá aquí tu pase.</div>`;
      return;
    }
    box.innerHTML = trips.map((t) => {
      const [o, d] = String(t.route).split('-');
      const cx = t.status === 'CANCELLED';
      return `
        <article class="pass bg-white rounded-2xl border border-slate-200 shadow-card overflow-hidden flex ${cx ? 'opacity-70' : ''}">
          <div class="flex-1 p-5">
            <div class="flex items-center justify-between">
              <span class="font-mono text-xs text-slate-500">${esc(t.reservation_id)}</span>
              <span class="status ${cx ? 'cx' : 'ok'}">${cx ? 'Cancelada' : 'Confirmada'}</span>
            </div>
            <div class="mt-3 flex items-center gap-4">
              <div><div class="text-2xl font-semibold text-ink">${esc(o)}</div><div class="text-xs text-slate-500">${esc(CITIES[o] || '')}</div></div>
              <svg class="w-10 h-4 text-slate-300" viewBox="0 0 40 16" fill="none" aria-hidden="true"><path d="M2 8h32" stroke="currentColor" stroke-dasharray="3 3"/><path d="M31 4l5 4-5 4" stroke="currentColor" stroke-width="1.5"/></svg>
              <div><div class="text-2xl font-semibold text-ink">${esc(d)}</div><div class="text-xs text-slate-500">${esc(CITIES[d] || '')}</div></div>
            </div>
            <div class="mt-3 text-xs text-slate-500">${esc(fmtDate(t.flight_date))} · Vuelo ${esc(t.flight_id)}</div>
            <div class="text-xs text-slate-500">${esc(t.passenger_name || '')}</div>
          </div>
          <div class="perf w-36 p-5 flex flex-col justify-between bg-ink-50/60">
            <div>
              <div class="text-[11px] text-slate-500">Asiento</div>
              <div class="text-2xl font-semibold text-ink">${esc(t.seat || '—')}</div>
              <div class="text-[11px] text-slate-500">${t.fare_class === 'BUSINESS' ? 'Business' : 'Economy'} · ${money(t.price_cents)}</div>
            </div>
            ${cx ? '' : `<button type="button" class="mt-3 text-xs font-medium text-rose-600 hover:text-rose-700 text-left" data-cancel="${esc(t.reservation_id)}">Cancelar reserva</button>`}
          </div>
        </article>`;
    }).join('');
    box.querySelectorAll('[data-cancel]').forEach((b) => b.addEventListener('click', () => cancelTrip(b.dataset.cancel)));
  }

  async function cancelTrip(id) {
    const t = trips.find((x) => x.reservation_id === id); if (!t) return;
    if (!confirm(`¿Cancelar la reserva ${id}, asiento ${t.seat || ''}?`)) return;
    const r = await api(`/reservations/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
    if (r.ok || r.status === 409) {
      t.status = 'CANCELLED'; saveTrips();
      toast(r.ok ? 'Reserva cancelada. El asiento quedó libre.' : 'Esta reserva ya estaba cancelada.', 'ok');
      if (state.flight && state.flight.flight_id === t.flight_id && r.ok) {
        state.flight.seats_booked = Math.max(0, Number(state.flight.seats_booked) - 1);
        state.flight.seats_available = Number(state.flight.seats_available) + 1;
        renderPlane(); renderResults();
      }
    } else if (r.status === 404) {
      toast('La reserva no existe en el sistema.', 'err');
    } else {
      toast(`No se pudo cancelar (HTTP ${r.status}).`, 'err');
    }
  }

  // Sync stored trips with the backend (status may have changed, or DB may have been reset)
  async function syncTrips() {
    let changed = false;
    await Promise.all(trips.map(async (t) => {
      const r = await api(`/reservations/${encodeURIComponent(t.reservation_id)}`).catch(() => null);
      if (!r) return;
      if (r.status === 404) { t._gone = true; changed = true; }
      else if (r.ok && r.body.status !== t.status) { t.status = r.body.status; changed = true; }
    }));
    if (changed) { trips = trips.filter((t) => !t._gone); saveTrips(); }
  }

  // ---------------------------------------------------------------- boot
  document.querySelectorAll('#confirm-dialog [data-close]').forEach((b) => b.addEventListener('click', () => $('confirm-dialog').close()));
  initSearchForm();
  initSeatEvents();
  renderTrips();
  syncTrips();
})();
