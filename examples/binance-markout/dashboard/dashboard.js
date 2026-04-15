const HOST = (location.hostname && location.hostname !== '') ? location.hostname : '127.0.0.1';
const STREAMS = [
    { name: 'markouts', port: 9001, path: '/markouts' },
    { name: 'toxicity', port: 9002, path: '/toxicity' },
    { name: 'by_side',  port: 9003, path: '/by_side'  },
    { name: 'alerts',   port: 9004, path: '/alerts'   },
];

const msgCounts = { markouts: 0, toxicity: 0, by_side: 0, alerts: 0 };

// 5-minute rolling window for the scatter panel.
const SCATTER_WINDOW_MS = 5 * 60 * 1000;
const scatterPoints = [];

const canvas = document.getElementById('scatter');
const ctx = canvas.getContext('2d');

let lastCanvasW = 0;
let lastCanvasH = 0;
function fitCanvas() {
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const w = Math.max(1, Math.round(rect.width * dpr));
    const h = Math.max(1, Math.round(rect.height * dpr));
    if (w !== lastCanvasW || h !== lastCanvasH) {
        canvas.width = w;
        canvas.height = h;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        lastCanvasW = w;
        lastCanvasH = h;
    }
}
window.addEventListener('resize', fitCanvas);
fitCanvas();

// ------------------------------------------------------------------
// WebSocket wiring
// ------------------------------------------------------------------
STREAMS.forEach(s => connect(s));

function connect(s) {
    const url = `ws://${HOST}:${s.port}${s.path}`;
    const ws = new WebSocket(url);
    const tag = document.getElementById(`conn-${s.name}`);

    ws.onopen = () => {
        tag.classList.remove('off');
        tag.classList.add('on');
        updateGlobalStatus();
    };
    ws.onclose = () => {
        tag.classList.remove('on');
        tag.classList.add('off');
        updateGlobalStatus();
        setTimeout(() => connect(s), 2000);
    };
    ws.onerror = () => {};
    ws.onmessage = (ev) => {
        let payload;
        try { payload = JSON.parse(ev.data); } catch { return; }
        if (!payload || payload.type !== 'data' || !Array.isArray(payload.data)) return;
        msgCounts[s.name] += payload.data.length;
        for (const row of payload.data) handleRow(s.name, row);
    };
}

function updateGlobalStatus() {
    const ons = document.querySelectorAll('.conn.on').length;
    const el = document.getElementById('status');
    if (ons === 4) { el.textContent = 'live · 4/4 streams'; el.className = 'status live'; }
    else if (ons > 0) { el.textContent = `degraded · ${ons}/4 streams`; el.className = 'status degraded'; }
    else { el.textContent = 'no streams connected'; el.className = 'status'; }
}

// Refresh chip labels once a second rather than per-message.
setInterval(() => {
    for (const s of STREAMS) {
        const el = document.getElementById(`conn-${s.name}`);
        if (el && el.classList.contains('on')) {
            el.textContent = `${s.name} · ${msgCounts[s.name]}`;
        }
    }
}, 1000);

function handleRow(stream, row) {
    if (stream === 'markouts') return onMarkout(row);
    if (stream === 'toxicity') return onToxicity(row);
    if (stream === 'by_side')  return onBySide(row);
    if (stream === 'alerts')   return onAlert(row);
}

// ------------------------------------------------------------------
// Stream handlers
// ------------------------------------------------------------------
function onMarkout(row) {
    const offset = Number(row.offset_ms);
    if (offset !== 1000) return;  // scatter shows 1s horizon only
    const bps = Number(row.signed_markout_bps);
    if (!Number.isFinite(bps)) return;
    const ts = parseTs(row.T) || Date.now();
    scatterPoints.push({ ts, bps, side: row.side, qty: Number(row.quantity) || 0 });
    const cutoff = Date.now() - SCATTER_WINDOW_MS;
    while (scatterPoints.length && scatterPoints[0].ts < cutoff) scatterPoints.shift();
}

function onToxicity(row) {
    if (Number(row.offset_ms) !== 1000) return;
    const avg = Number(row.avg_markout_bps);
    const adv = Number(row.adverse_selection_rate);
    const cnt = Number(row.trade_count);

    setCard('m-avg', formatBps(avg), signClass(avg, 2, -2));
    setCard('m-adverse', formatPct(adv), adv > 0.6 ? 'neg' : (adv < 0.4 ? 'pos' : 'warn'));
    setCard('m-count', Number.isFinite(cnt) ? cnt.toString() : '—', null);
}

function onBySide(row) {
    const side = String(row.side || '').toUpperCase();
    const offset = String(Number(row.offset_ms));
    const tr = document.querySelector(`tr[data-side="${side}"][data-offset="${offset}"]`);
    if (!tr) return;
    const markout = Number(row.avg_markout_bps);
    const trades = Number(row.trade_count);
    const vol = Number(row.volume);
    const mc = tr.querySelector('.v-markout');
    mc.textContent = formatBps(markout);
    mc.classList.remove('pos', 'neg');
    if (markout > 0.5) mc.classList.add('pos');
    else if (markout < -0.5) mc.classList.add('neg');
    tr.querySelector('.v-trades').textContent = Number.isFinite(trades) ? trades : '—';
    tr.querySelector('.v-vol').textContent = Number.isFinite(vol) ? vol.toFixed(3) : '—';
}

function onAlert(row) {
    const feed = document.getElementById('alert-feed');
    const ph = feed.querySelector('.placeholder');
    if (ph) ph.remove();
    const li = document.createElement('li');
    const adv = Number(row.adverse_selection_rate);
    const avg = Number(row.avg_markout_bps);
    const cnt = Number(row.trade_count);
    const t = new Date().toTimeString().slice(0, 8);
    li.innerHTML = `
        <span class="t">${t}</span>
        <span class="b">HIGH_ADVERSE</span>
        <span class="m">adverse_rate=${formatPct(adv)} · avg_markout=${formatBps(avg)} bps · n=${cnt}</span>
    `;
    feed.prepend(li);
    while (feed.children.length > 50) feed.removeChild(feed.lastChild);
}

// ------------------------------------------------------------------
// Scatter rendering — redraws every animation frame
// ------------------------------------------------------------------
function drawScatter() {
    fitCanvas();
    const rect = canvas.getBoundingClientRect();
    const w = rect.width, h = rect.height;
    ctx.clearRect(0, 0, w, h);

    const pad = { l: 42, r: 10, t: 6, b: 20 };
    const plotW = w - pad.l - pad.r;
    const plotH = h - pad.t - pad.b;

    // Y range: auto, clamped to at least ±2 bps so low-vol periods still readable.
    let yMax = 2;
    for (const p of scatterPoints) { const a = Math.abs(p.bps); if (a > yMax) yMax = a; }
    yMax = Math.ceil(yMax * 1.1 * 2) / 2;

    const now = Date.now();
    const tMin = now - SCATTER_WINDOW_MS;

    const xOf = ts => pad.l + ((ts - tMin) / SCATTER_WINDOW_MS) * plotW;
    const yOf = bps => pad.t + plotH / 2 - (bps / yMax) * (plotH / 2);

    // Grid + zero line.
    ctx.strokeStyle = '#222a33';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
        const y = pad.t + (plotH * i / 4);
        ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(pad.l + plotW, y); ctx.stroke();
    }
    ctx.strokeStyle = '#4b5563';
    ctx.beginPath(); ctx.moveTo(pad.l, yOf(0)); ctx.lineTo(pad.l + plotW, yOf(0)); ctx.stroke();

    // Y labels.
    ctx.fillStyle = '#7d8794';
    ctx.font = '10px SFMono-Regular, Consolas, monospace';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= 4; i++) {
        const frac = 1 - i / 2; // from +1 to -1
        const v = frac * yMax;
        const y = pad.t + (plotH * i / 4);
        ctx.fillText(v.toFixed(1), pad.l - 6, y);
    }

    // X labels — ticks every 60s.
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    for (let m = 0; m <= 5; m++) {
        const ts = tMin + m * 60_000;
        const x = xOf(ts);
        ctx.fillText(`-${5 - m}m`, x, pad.t + plotH + 4);
    }

    for (const p of scatterPoints) {
        const x = xOf(p.ts);
        if (x < pad.l || x > pad.l + plotW) continue;
        const y = yOf(p.bps);
        const r = Math.min(9, 3 + (Math.log10(Math.max(1e-4, p.qty)) + 4) * 1.5);
        ctx.fillStyle = p.side === 'BUY' ? 'rgba(63,185,80,0.78)' : 'rgba(248,81,73,0.78)';
        ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2); ctx.fill();
    }

    requestAnimationFrame(drawScatter);
}
requestAnimationFrame(drawScatter);

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------
function parseTs(v) {
    if (v == null) return null;
    if (typeof v === 'number') return v;
    // Force UTC: LaminarDB's ISO strings carry no TZ suffix.
    const s = /[zZ]|[+-]\d{2}:?\d{2}$/.test(v) ? v : `${v}Z`;
    const d = Date.parse(s);
    return Number.isFinite(d) ? d : null;
}

function formatBps(v) {
    if (!Number.isFinite(v)) return '—';
    const sign = v > 0 ? '+' : '';
    return `${sign}${v.toFixed(2)}`;
}

function formatPct(v) {
    if (!Number.isFinite(v)) return '—';
    return `${(v * 100).toFixed(1)}%`;
}

function signClass(v, posThresh, negThresh) {
    if (!Number.isFinite(v)) return null;
    if (v > posThresh) return 'pos';
    if (v < negThresh) return 'neg';
    return null;
}

function setCard(id, text, cls) {
    const el = document.getElementById(id);
    el.textContent = text;
    el.classList.remove('pos', 'neg', 'warn');
    if (cls) el.classList.add(cls);
}
