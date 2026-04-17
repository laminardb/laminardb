const HOST = (location.hostname && location.hostname !== '') ? location.hostname : '127.0.0.1';
const STREAMS = [
    { name: 'markouts', port: 9001, path: '/markouts' },
    { name: 'curve',    port: 9002, path: '/curve'    },
    { name: 'toxicity', port: 9003, path: '/toxicity' },
    { name: 'alerts',   port: 9004, path: '/alerts'   },
    { name: 'regime',   port: 9005, path: '/regime'   },
    { name: 'signal',   port: 9006, path: '/signal'   },
];

const HORIZONS = [5000, 15000, 30000, 60000];
const HORIZON_LABELS = ['5s', '15s', '30s', '60s'];
const PRIMARY_OFFSET = 5000;

const SCATTER_WINDOW_MS = 5 * 60 * 1000;
const CURVE_HISTORY = 5;      // keep last 5 curve snapshots per side
const HEATMAP_ROWS = 12;       // 12 × 30s = 6 minutes

const msgCounts = { markouts: 0, curve: 0, toxicity: 0, alerts: 0, regime: 0, signal: 0 };
const scatterPoints = [];
const curveHistory = { BUY: [], SELL: [] };
const heatmapWindows = []; // [{ ts, cells: {5000: bps, 15000: bps, ...} }]

// ------------------------------------------------------------------
// Canvas setup
// ------------------------------------------------------------------
function bindCanvas(id) {
    const canvas = document.getElementById(id);
    const ctx = canvas.getContext('2d');
    let lastW = 0, lastH = 0;
    function fit() {
        const dpr = window.devicePixelRatio || 1;
        const r = canvas.getBoundingClientRect();
        const w = Math.max(1, Math.round(r.width * dpr));
        const h = Math.max(1, Math.round(r.height * dpr));
        if (w !== lastW || h !== lastH) {
            canvas.width = w; canvas.height = h;
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
            lastW = w; lastH = h;
        }
        return { w: r.width, h: r.height, ctx };
    }
    window.addEventListener('resize', fit);
    return fit;
}

const fitCurve   = bindCanvas('curve');
const fitScatter = bindCanvas('scatter');
const fitHeatmap = bindCanvas('heatmap');

// ------------------------------------------------------------------
// WebSocket wiring
// ------------------------------------------------------------------
STREAMS.forEach(connect);

function connect(s) {
    const url = `ws://${HOST}:${s.port}${s.path}`;
    const ws = new WebSocket(url);
    const tag = document.getElementById(`conn-${s.name}`);

    ws.onopen = () => {
        tag.classList.remove('off'); tag.classList.add('on');
        updateGlobalStatus();
    };
    ws.onclose = () => {
        tag.classList.remove('on'); tag.classList.add('off');
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
    const on = document.querySelectorAll('.conn.on').length;
    const el = document.getElementById('status');
    const txt = document.getElementById('status-text');
    el.classList.remove('live', 'degraded');
    if (on === STREAMS.length)      { el.classList.add('live');     txt.textContent = `LIVE · ${on}/${STREAMS.length}`; }
    else if (on > 0)                { el.classList.add('degraded'); txt.textContent = `degraded · ${on}/${STREAMS.length}`; }
    else                            { txt.textContent = 'no streams'; }
}

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
    if (stream === 'curve')    return onCurve(row);
    if (stream === 'toxicity') return onToxicity(row);
    if (stream === 'alerts')   return onAlert(row);
    if (stream === 'regime')   return onRegime(row);
    if (stream === 'signal')   return onSignal(row);
}

// ------------------------------------------------------------------
// Stream handlers
// ------------------------------------------------------------------
function onMarkout(row) {
    if (Number(row.offset_ms) !== PRIMARY_OFFSET) return;
    const bps = Number(row.signed_markout_bps);
    if (!Number.isFinite(bps)) return;
    const ts = parseTs(row.T) || Date.now();
    scatterPoints.push({ ts, bps, side: row.side, qty: Number(row.quantity) || 0 });
    const cutoff = Date.now() - SCATTER_WINDOW_MS;
    while (scatterPoints.length && scatterPoints[0].ts < cutoff) scatterPoints.shift();
}

function onCurve(row) {
    const side = String(row.side || '').toUpperCase();
    if (side !== 'BUY' && side !== 'SELL') return;
    const snap = {
        ts: parseTs(row.window_end) || Date.now(),
        values: HORIZONS.map((h, i) => {
            const key = i === 0 ? 'avg_5s' : i === 1 ? 'avg_15s' : i === 2 ? 'avg_30s' : 'avg_60s';
            return Number(row[key]);
        }),
    };
    const buf = curveHistory[side];
    buf.push(snap);
    while (buf.length > CURVE_HISTORY) buf.shift();
}

function onToxicity(row) {
    const offset = Number(row.offset_ms);
    const wend = parseTs(row.window_end) || Date.now();
    const avg = Number(row.avg_markout_bps);
    const cnt = Number(row.trade_count);
    const adv = Number(row.adverse_selection_rate);

    // Heatmap: one row per (window_end × offset). Group by window_end.
    let win = heatmapWindows.find(w => w.ts === wend);
    if (!win) {
        win = { ts: wend, cells: {}, counts: {} };
        heatmapWindows.push(win);
        heatmapWindows.sort((a, b) => b.ts - a.ts);
        while (heatmapWindows.length > HEATMAP_ROWS) heatmapWindows.pop();
    }
    win.cells[offset] = avg;
    win.counts[offset] = cnt;

    // Cards: 5s drives markout/adverse/count; 60s drives its own card.
    if (offset === 5000) {
        setCard('m-mk5', formatBps(avg), signClass(avg, 0.5, -0.5));
        setCard('m-adv', formatPct(adv), adv > 0.6 ? 'neg' : (adv < 0.4 ? 'pos' : 'warn'));
        setCard('m-cnt', Number.isFinite(cnt) ? cnt.toString() : '—', null);
    }
    if (offset === 60000) {
        setCard('m-mk60', formatBps(avg), signClass(avg, 0.5, -0.5));
    }
}

const REGIME_CLASS = {
    CLEAN:         'clean',
    INFORMED:      'informed',
    ADVERSE:       'adverse',
    TEMP_IMPACT:   'temp',
    SLOW_INFORMED: 'slow',
    MIXED:         'mixed',
    UNKNOWN:       'mixed',
};

function onRegime(row) {
    const side = String(row.side || '').toUpperCase();
    if (side !== 'BUY' && side !== 'SELL') return;
    const regime = String(row.regime || 'UNKNOWN');
    const mk5 = Number(row.avg_5s);
    const mk60 = Number(row.avg_60s);
    const cls = REGIME_CLASS[regime] || 'mixed';
    const idPrefix = side === 'BUY' ? 'buy' : 'sell';

    const el = document.getElementById(`${idPrefix}-regime`);
    el.textContent = regime;
    el.className = `signal-value regime ${cls}`;

    const sub = document.getElementById(`${idPrefix}-markout`);
    sub.textContent = `5s ${formatBps(mk5)} · 60s ${formatBps(mk60)}`;
}

function onSignal(row) {
    const skew = Number(row.skew_bps);
    const adv = Number(row.max_adverse_rate);

    const skewEl = document.getElementById('skew-bps');
    if (Number.isFinite(skew)) {
        const arrow = skew > 0.3 ? ' ↑' : skew < -0.3 ? ' ↓' : '';
        skewEl.textContent = `${formatBps(skew)}${arrow}`;
        skewEl.classList.remove('pos', 'neg', 'warn');
        if (skew > 0.5) skewEl.classList.add('pos');
        else if (skew < -0.5) skewEl.classList.add('neg');
    } else {
        skewEl.textContent = '—';
    }

    const advEl = document.getElementById('max-adverse');
    if (Number.isFinite(adv)) {
        advEl.textContent = formatPct(adv);
        advEl.classList.remove('pos', 'neg', 'warn');
        if (adv > 0.6) advEl.classList.add('neg');
        else if (adv > 0.5) advEl.classList.add('warn');
        else advEl.classList.add('pos');
    } else {
        advEl.textContent = '—';
    }
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
    li.innerHTML =
        `<span class="t">${t}</span>` +
        `<span class="b">HIGH_ADVERSE · 5s</span>` +
        `<span class="m">adverse=${formatPct(adv)} · avg=${formatBps(avg)} bps · n=${cnt}</span>`;
    feed.prepend(li);
    while (feed.children.length > 50) feed.removeChild(feed.lastChild);
}

// ------------------------------------------------------------------
// Curve chart
// ------------------------------------------------------------------
function drawCurve() {
    const { w, h, ctx } = fitCurve();
    ctx.clearRect(0, 0, w, h);

    const pad = { l: 50, r: 14, t: 10, b: 28 };
    const plotW = w - pad.l - pad.r;
    const plotH = h - pad.t - pad.b;

    // Y range: symmetric around zero, clamped to ≥1 bps.
    let yMax = 1;
    for (const side of ['BUY', 'SELL']) {
        for (const snap of curveHistory[side]) {
            for (const v of snap.values) {
                if (Number.isFinite(v) && Math.abs(v) > yMax) yMax = Math.abs(v);
            }
        }
    }
    yMax = Math.ceil(yMax * 1.1 * 2) / 2;

    const xOf = i => pad.l + (plotW * i) / (HORIZONS.length - 1);
    const yOf = v => pad.t + plotH / 2 - (v / yMax) * (plotH / 2);

    // Grid.
    ctx.strokeStyle = '#222a33'; ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
        const y = pad.t + (plotH * i) / 4;
        ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(pad.l + plotW, y); ctx.stroke();
    }
    ctx.strokeStyle = '#4b5563';
    ctx.beginPath(); ctx.moveTo(pad.l, yOf(0)); ctx.lineTo(pad.l + plotW, yOf(0)); ctx.stroke();

    // Y labels.
    ctx.fillStyle = '#7d8794';
    ctx.font = '11px SFMono-Regular, Consolas, monospace';
    ctx.textAlign = 'right'; ctx.textBaseline = 'middle';
    for (let i = 0; i <= 4; i++) {
        const v = (1 - i / 2) * yMax;
        ctx.fillText(`${v >= 0 ? '+' : ''}${v.toFixed(1)}`, pad.l - 8, pad.t + (plotH * i) / 4);
    }

    // X labels (horizon ticks).
    ctx.textAlign = 'center'; ctx.textBaseline = 'top';
    for (let i = 0; i < HORIZONS.length; i++) {
        const x = xOf(i);
        ctx.fillStyle = '#7d8794';
        ctx.fillText(HORIZON_LABELS[i], x, pad.t + plotH + 6);
        ctx.strokeStyle = '#30363d'; ctx.beginPath();
        ctx.moveTo(x, pad.t); ctx.lineTo(x, pad.t + plotH); ctx.stroke();
    }

    // Draw lines — oldest snapshot faintest, newest solid.
    for (const side of ['BUY', 'SELL']) {
        const colour = side === 'BUY' ? '63,185,80' : '248,81,73';
        const snaps = curveHistory[side];
        const n = snaps.length;
        for (let si = 0; si < n; si++) {
            const snap = snaps[si];
            const alpha = 0.2 + 0.8 * ((si + 1) / n);
            ctx.strokeStyle = `rgba(${colour},${alpha})`;
            ctx.lineWidth = si === n - 1 ? 2.2 : 1.2;
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < snap.values.length; i++) {
                const v = snap.values[i];
                if (!Number.isFinite(v)) continue;
                const x = xOf(i), y = yOf(v);
                if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); }
            }
            ctx.stroke();

            // Points (only latest snapshot, to avoid clutter).
            if (si === n - 1) {
                ctx.fillStyle = `rgba(${colour},1)`;
                for (let i = 0; i < snap.values.length; i++) {
                    const v = snap.values[i];
                    if (!Number.isFinite(v)) continue;
                    ctx.beginPath(); ctx.arc(xOf(i), yOf(v), 3.2, 0, Math.PI * 2); ctx.fill();
                }
            }
        }
    }

    requestAnimationFrame(drawCurve);
}
requestAnimationFrame(drawCurve);

// ------------------------------------------------------------------
// Scatter
// ------------------------------------------------------------------
function drawScatter() {
    const { w, h, ctx } = fitScatter();
    ctx.clearRect(0, 0, w, h);

    const pad = { l: 42, r: 10, t: 6, b: 20 };
    const plotW = w - pad.l - pad.r;
    const plotH = h - pad.t - pad.b;

    let yMax = 2;
    for (const p of scatterPoints) { const a = Math.abs(p.bps); if (a > yMax) yMax = a; }
    yMax = Math.ceil(yMax * 1.1 * 2) / 2;

    const now = Date.now();
    const tMin = now - SCATTER_WINDOW_MS;
    const xOf = ts => pad.l + ((ts - tMin) / SCATTER_WINDOW_MS) * plotW;
    const yOf = bps => pad.t + plotH / 2 - (bps / yMax) * (plotH / 2);

    ctx.strokeStyle = '#222a33'; ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
        const y = pad.t + (plotH * i) / 4;
        ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(pad.l + plotW, y); ctx.stroke();
    }
    ctx.strokeStyle = '#4b5563';
    ctx.beginPath(); ctx.moveTo(pad.l, yOf(0)); ctx.lineTo(pad.l + plotW, yOf(0)); ctx.stroke();

    ctx.fillStyle = '#7d8794';
    ctx.font = '10px SFMono-Regular, Consolas, monospace';
    ctx.textAlign = 'right'; ctx.textBaseline = 'middle';
    for (let i = 0; i <= 4; i++) {
        const v = (1 - i / 2) * yMax;
        ctx.fillText(v.toFixed(1), pad.l - 6, pad.t + (plotH * i) / 4);
    }
    ctx.textAlign = 'center'; ctx.textBaseline = 'top';
    for (let m = 0; m <= 5; m++) {
        ctx.fillText(`-${5 - m}m`, xOf(tMin + m * 60_000), pad.t + plotH + 4);
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
// Heatmap
// ------------------------------------------------------------------
function drawHeatmap() {
    const { w, h, ctx } = fitHeatmap();
    ctx.clearRect(0, 0, w, h);

    const pad = { l: 70, r: 14, t: 18, b: 6 };
    const cols = HORIZONS.length;
    const rows = Math.max(1, heatmapWindows.length);
    const plotW = w - pad.l - pad.r;
    const plotH = h - pad.t - pad.b;
    const cellW = plotW / cols;
    const cellH = plotH / Math.max(rows, HEATMAP_ROWS);

    // Colour scale: symmetric around zero, clamped to ±3 bps.
    const COLOUR_MAX = 3;
    const colourOf = (v) => {
        if (!Number.isFinite(v)) return '#1a1f25';
        const t = Math.max(-1, Math.min(1, v / COLOUR_MAX));
        if (t >= 0) {
            const a = t;
            return `rgba(63,185,80,${0.15 + 0.75 * a})`;
        }
        return `rgba(248,81,73,${0.15 + 0.75 * Math.abs(t)})`;
    };

    // Column headers.
    ctx.fillStyle = '#7d8794';
    ctx.font = '11px SFMono-Regular, Consolas, monospace';
    ctx.textAlign = 'center'; ctx.textBaseline = 'bottom';
    for (let c = 0; c < cols; c++) {
        ctx.fillText(HORIZON_LABELS[c], pad.l + cellW * c + cellW / 2, pad.t - 4);
    }

    // Row labels + cells.
    ctx.textAlign = 'right'; ctx.textBaseline = 'middle';
    for (let r = 0; r < heatmapWindows.length; r++) {
        const win = heatmapWindows[r];
        const y = pad.t + cellH * r;
        const label = new Date(win.ts).toTimeString().slice(0, 8);
        ctx.fillStyle = '#7d8794';
        ctx.fillText(label, pad.l - 8, y + cellH / 2);

        for (let c = 0; c < cols; c++) {
            const v = win.cells[HORIZONS[c]];
            ctx.fillStyle = colourOf(v);
            ctx.fillRect(pad.l + cellW * c + 1, y + 1, cellW - 2, cellH - 2);

            if (Number.isFinite(v) && cellH > 14) {
                ctx.fillStyle = Math.abs(v) > 1.5 ? '#0b0d10' : '#c9d1d9';
                ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
                ctx.font = '10px SFMono-Regular, Consolas, monospace';
                ctx.fillText(v.toFixed(2), pad.l + cellW * c + cellW / 2, y + cellH / 2);
                ctx.textAlign = 'right';
            }
        }
    }

    requestAnimationFrame(drawHeatmap);
}
requestAnimationFrame(drawHeatmap);

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------
function parseTs(v) {
    if (v == null) return null;
    if (typeof v === 'number') return v;
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
