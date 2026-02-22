/* ============================================
   LaminarDB -- Data Flow Playground
   Interactive streaming pipeline visualization
   with preset pipeline configurations
   ============================================ */

(function () {
  'use strict';

  // --- Configuration ---
  var MAX_PARTICLES = 120;
  var PARTICLE_POOL_SIZE = 150;
  var BASE_SPEED = 1.2;
  var SPAWN_INTERVAL = 400; // ms between spawns at 1x speed
  var PARTICLE_RADIUS = 4;
  var GLOW_RADIUS = 12;
  var CONNECTION_DOT_COUNT = 4;

  // Colors for different event types
  var PARTICLE_COLORS = [
    { r: 6, g: 182, b: 212 },    // cyan
    { r: 139, g: 92, b: 246 },   // violet
    { r: 74, g: 222, b: 128 },   // green
    { r: 251, g: 191, b: 36 },   // amber
    { r: 251, g: 113, b: 133 },  // rose
  ];

  // Blended color for merge operations
  function blendColors(c1, c2) {
    return {
      r: Math.round((c1.r + c2.r) / 2),
      g: Math.round((c1.g + c2.g) / 2),
      b: Math.round((c1.b + c2.b) / 2)
    };
  }

  // ===========================
  // Pipeline Preset Definitions
  // ===========================

  var PRESETS = {
    fraud: {
      operators: [
        {
          id: 'source', label: 'Source', sublabel: 'Kafka', icon: 'kafka',
          description: 'Ingests streaming events from Kafka topics. Particles spawn here with different colors representing event types.',
          enabled: true, behavior: 'spawn',
          // Layout: position hints as fractions [xFrac, yFrac]
          pos: [0.0, 0.5]
        },
        {
          id: 'filter', label: 'Filter', sublabel: 'WHERE price > 100', icon: 'filter',
          description: 'Filters events based on conditions. Some particles pass through, others dissolve when they do not match.',
          enabled: true, behavior: 'filter',
          pos: [0.25, 0.5]
        },
        {
          id: 'window', label: 'Window', sublabel: 'TUMBLE 1s', icon: 'window',
          description: 'Groups events into time-based windows. Particles accumulate, then release as a batch when the window closes.',
          enabled: true, behavior: 'window',
          pos: [0.5, 0.5]
        },
        {
          id: 'aggregate', label: 'Aggregate', sublabel: 'SUM / COUNT', icon: 'aggregate',
          description: 'Combines multiple events into summary values. Several particles merge into one larger particle.',
          enabled: true, behavior: 'aggregate',
          pos: [0.75, 0.5]
        },
        {
          id: 'sink', label: 'Sink', sublabel: 'Delta Lake', icon: 'sink',
          description: 'Writes processed results to storage. Particles arrive and stack up as persisted data.',
          enabled: true, behavior: 'sink',
          pos: [1.0, 0.5]
        }
      ],
      // Connections: [fromIndex, toIndex]
      connections: [[0, 1], [1, 2], [2, 3], [3, 4]],
      // Which operator indices are spawn sources
      sources: [0]
    },

    join: {
      operators: [
        {
          id: 'source1', label: 'Orders', sublabel: 'Kafka', icon: 'kafka',
          description: 'Ingests order events from a Kafka topic. Each order carries an order_id and account information.',
          enabled: true, behavior: 'spawn',
          pos: [0.0, 0.28]
        },
        {
          id: 'source2', label: 'Payments', sublabel: 'Kafka', icon: 'kafka',
          description: 'Ingests payment events from a Kafka topic. Payments are matched against orders via order_id.',
          enabled: true, behavior: 'spawn',
          pos: [0.0, 0.72]
        },
        {
          id: 'join', label: 'JOIN', sublabel: 'ON order_id', icon: 'join',
          description: 'Stream-stream join: merges Orders and Payments on order_id within a time bound. Two input particles combine into one.',
          enabled: true, behavior: 'join',
          pos: [0.4, 0.5]
        },
        {
          id: 'filter', label: 'Filter', sublabel: 'amount > 1000', icon: 'filter',
          description: 'Filters joined results, keeping only high-value matched orders.',
          enabled: true, behavior: 'filter',
          pos: [0.7, 0.5]
        },
        {
          id: 'sink', label: 'Sink', sublabel: 'Delta Lake', icon: 'sink',
          description: 'Writes filtered results to Delta Lake for analytics.',
          enabled: true, behavior: 'sink',
          pos: [1.0, 0.5]
        }
      ],
      connections: [[0, 2], [1, 2], [2, 3], [3, 4]],
      sources: [0, 1]
    },

    asof: {
      operators: [
        {
          id: 'source1', label: 'Trades', sublabel: 'Kafka', icon: 'kafka',
          description: 'Main driving stream of trade events. Each trade is enriched with the most recent quote.',
          enabled: true, behavior: 'spawn',
          pos: [0.0, 0.5]
        },
        {
          id: 'source2', label: 'Quotes', sublabel: 'Kafka', icon: 'kafka',
          description: 'Reference stream of quote updates. Provides bid/ask used for ASOF lookup against trades.',
          enabled: true, behavior: 'spawn',
          pos: [0.35, 0.15]
        },
        {
          id: 'asof_join', label: 'ASOF JOIN', sublabel: 't.ts >= q.ts', icon: 'asof',
          description: 'Temporal join: each trade is matched with the most recent preceding quote. The driving stream passes through enriched.',
          enabled: true, behavior: 'asof_join',
          pos: [0.4, 0.5]
        },
        {
          id: 'window', label: 'Window', sublabel: 'SLIDING 10s', icon: 'window',
          description: 'Sliding window that keeps multiple events in flight, releasing them one at a time after a delay.',
          enabled: true, behavior: 'sliding_window',
          pos: [0.7, 0.5]
        },
        {
          id: 'sink', label: 'Sink', sublabel: 'Delta Lake', icon: 'sink',
          description: 'Writes enriched trade data with quote spreads to Delta Lake.',
          enabled: true, behavior: 'sink',
          pos: [1.0, 0.5]
        }
      ],
      connections: [[0, 2], [1, 2], [2, 3], [3, 4]],
      sources: [0, 1]
    }
  };

  // --- State ---
  var canvas, ctx;
  var canvasWidth = 0, canvasHeight = 0;
  var dpr = 1;
  var isRunning = false;
  var lastTime = 0;
  var spawnTimers = {}; // per-source spawn timers
  var speedMultiplier = 1;
  var eventsPerSec = 0;
  var eventCount = 0;
  var eventCountTimer = 0;
  var lastEventCount = 0;

  // Current preset
  var currentPreset = 'fraud';
  var currentConfig = null;

  // Operator node layout
  var nodes = [];
  var connections = [];
  var draggingNode = null;
  var dragOffsetX = 0, dragOffsetY = 0;
  var dragStartX = 0, dragStartY = 0;
  var hoveredNode = null;
  var tooltip = null;
  var tooltipVisible = false;

  // Particle pool
  var particlePool = [];
  var activeParticles = [];

  // Transition state
  var transitioning = false;
  var transitionAlpha = 1; // 1 = fully visible, 0 = faded out

  // Window accumulator (tumble)
  var windowBuffer = [];
  var windowTimer = 0;
  var WINDOW_DURATION = 2000; // ms

  // Sliding window accumulator
  var slidingBuffer = [];
  var SLIDING_RELEASE_INTERVAL = 600; // ms
  var slidingReleaseTimer = 0;

  // Aggregate accumulator
  var aggregateBuffer = [];
  var AGGREGATE_THRESHOLD = 3;

  // Join buffers (dual-input)
  var joinBufferA = []; // from first input
  var joinBufferB = []; // from second input

  // ASOF Join state
  var asofMainBuffer = [];
  var asofLookupLatest = null; // most recent lookup particle color

  // Sink storage visual
  var sinkStack = [];
  var SINK_MAX = 8;

  // Connection flow dots
  var flowDots = [];

  // --- Particle Object ---
  function createParticle() {
    return {
      x: 0, y: 0,
      vx: 0, vy: 0,
      radius: PARTICLE_RADIUS,
      color: null,
      alpha: 1,
      alive: false,
      targetNode: 0,
      progress: 0,
      startX: 0, startY: 0,
      endX: 0, endY: 0,
      state: 'moving', // moving, filtering, windowed, aggregating, sinking, dying, join_waiting, sliding
      scale: 1,
      label: '',
      age: 0,
      sourceIndex: 0, // which source spawned this
      connectionIndex: -1, // which connection this particle is traveling along
      flashTimer: 0 // for merge/match flash effects
    };
  }

  // Initialize particle pool
  function initPool() {
    particlePool = [];
    activeParticles = [];
    for (var i = 0; i < PARTICLE_POOL_SIZE; i++) {
      particlePool.push(createParticle());
    }
  }

  function acquireParticle() {
    if (particlePool.length === 0) return null;
    if (activeParticles.length >= MAX_PARTICLES) return null;
    var p = particlePool.pop();
    p.alive = true;
    p.alpha = 0;
    p.scale = 1;
    p.state = 'moving';
    p.age = 0;
    p.label = '';
    p.progress = 0;
    p.sourceIndex = 0;
    p.connectionIndex = -1;
    p.flashTimer = 0;
    activeParticles.push(p);
    return p;
  }

  function releaseParticle(p) {
    p.alive = false;
    var idx = activeParticles.indexOf(p);
    if (idx > -1) activeParticles.splice(idx, 1);
    particlePool.push(p);
  }

  // --- Find connection index from node A to node B ---
  function findConnectionIndex(fromIdx, toIdx) {
    for (var i = 0; i < connections.length; i++) {
      if (connections[i][0] === fromIdx && connections[i][1] === toIdx) {
        return i;
      }
    }
    return -1;
  }

  // --- Find which connections lead TO a given node ---
  function getInputConnections(nodeIdx) {
    var result = [];
    for (var i = 0; i < connections.length; i++) {
      if (connections[i][1] === nodeIdx) {
        result.push({ connIdx: i, fromIdx: connections[i][0] });
      }
    }
    return result;
  }

  // --- Find which connections lead FROM a given node ---
  function getOutputConnections(nodeIdx) {
    var result = [];
    for (var i = 0; i < connections.length; i++) {
      if (connections[i][0] === nodeIdx) {
        result.push({ connIdx: i, toIdx: connections[i][1] });
      }
    }
    return result;
  }

  // --- Layout ---
  function layoutNodes() {
    var config = currentConfig;
    if (!config) return;

    var ops = config.operators;
    var totalWidth = canvasWidth / dpr;
    var totalHeight = canvasHeight / dpr;
    var nodeW = 100;
    var nodeH = 58;
    var paddingX = 40;
    var paddingY = 30;
    var availableWidth = totalWidth - paddingX * 2 - nodeW;
    var availableHeight = totalHeight - paddingY * 2 - nodeH;

    nodes = [];
    for (var i = 0; i < ops.length; i++) {
      var op = ops[i];
      var xFrac = op.pos[0];
      var yFrac = op.pos[1];
      var x = paddingX + xFrac * availableWidth;
      var y = paddingY + yFrac * availableHeight;

      nodes.push({
        op: op,
        x: x,
        y: y,
        w: nodeW,
        h: nodeH,
        centerX: x + nodeW / 2,
        centerY: y + nodeH / 2,
        originalIndex: i
      });
    }

    // Build connections from preset
    connections = config.connections.slice();

    // Init flow dots between connections
    flowDots = [];
    for (var j = 0; j < connections.length; j++) {
      for (var d = 0; d < CONNECTION_DOT_COUNT; d++) {
        flowDots.push({
          connIdx: j,
          progress: d / CONNECTION_DOT_COUNT,
          speed: 0.3 + Math.random() * 0.2
        });
      }
    }
  }

  // --- Drawing helpers ---
  function drawRoundedRect(x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
  }

  function drawIcon(type, cx, cy, size) {
    ctx.save();
    ctx.lineWidth = 1.5;
    ctx.lineCap = 'round';
    ctx.lineJoin = 'round';
    var s = size * 0.4;

    switch (type) {
      case 'kafka':
        ctx.beginPath();
        ctx.moveTo(cx - s, cy - s * 0.6);
        ctx.lineTo(cx + s, cy - s * 0.6);
        ctx.moveTo(cx - s * 0.7, cy);
        ctx.lineTo(cx + s * 0.7, cy);
        ctx.moveTo(cx - s, cy + s * 0.6);
        ctx.lineTo(cx + s, cy + s * 0.6);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(cx + s * 0.4, cy - s);
        ctx.lineTo(cx + s, cy - s * 0.6);
        ctx.lineTo(cx + s * 0.4, cy - s * 0.2);
        ctx.stroke();
        break;

      case 'filter':
        ctx.beginPath();
        ctx.moveTo(cx - s, cy - s * 0.7);
        ctx.lineTo(cx + s, cy - s * 0.7);
        ctx.lineTo(cx + s * 0.2, cy + s * 0.1);
        ctx.lineTo(cx + s * 0.2, cy + s * 0.8);
        ctx.lineTo(cx - s * 0.2, cy + s * 0.8);
        ctx.lineTo(cx - s * 0.2, cy + s * 0.1);
        ctx.closePath();
        ctx.stroke();
        break;

      case 'window':
        ctx.beginPath();
        ctx.arc(cx, cy, s * 0.8, 0, Math.PI * 2);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(cx, cy - s * 0.4);
        ctx.lineTo(cx, cy);
        ctx.lineTo(cx + s * 0.35, cy + s * 0.15);
        ctx.stroke();
        break;

      case 'aggregate':
        ctx.beginPath();
        ctx.moveTo(cx + s * 0.6, cy - s * 0.7);
        ctx.lineTo(cx - s * 0.5, cy - s * 0.7);
        ctx.lineTo(cx + s * 0.1, cy);
        ctx.lineTo(cx - s * 0.5, cy + s * 0.7);
        ctx.lineTo(cx + s * 0.6, cy + s * 0.7);
        ctx.stroke();
        break;

      case 'sink':
        ctx.beginPath();
        ctx.ellipse(cx, cy - s * 0.4, s * 0.65, s * 0.25, 0, 0, Math.PI * 2);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(cx - s * 0.65, cy - s * 0.4);
        ctx.lineTo(cx - s * 0.65, cy + s * 0.4);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(cx + s * 0.65, cy - s * 0.4);
        ctx.lineTo(cx + s * 0.65, cy + s * 0.4);
        ctx.stroke();
        ctx.beginPath();
        ctx.ellipse(cx, cy + s * 0.4, s * 0.65, s * 0.25, 0, 0, Math.PI);
        ctx.stroke();
        break;

      case 'join':
        // Two arrows merging into one
        ctx.beginPath();
        ctx.moveTo(cx - s, cy - s * 0.6);
        ctx.lineTo(cx, cy);
        ctx.lineTo(cx - s, cy + s * 0.6);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.lineTo(cx + s, cy);
        ctx.stroke();
        // Merge symbol
        ctx.beginPath();
        ctx.arc(cx, cy, s * 0.2, 0, Math.PI * 2);
        ctx.fillStyle = ctx.strokeStyle;
        ctx.fill();
        break;

      case 'asof':
        // Clock with arrow
        ctx.beginPath();
        ctx.arc(cx, cy, s * 0.7, 0, Math.PI * 2);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(cx, cy - s * 0.35);
        ctx.lineTo(cx, cy);
        ctx.lineTo(cx + s * 0.3, cy + s * 0.1);
        ctx.stroke();
        // Small lookup arrow from side
        ctx.beginPath();
        ctx.moveTo(cx - s * 0.9, cy - s * 0.4);
        ctx.lineTo(cx - s * 0.5, cy - s * 0.1);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(cx - s * 0.65, cy - s * 0.5);
        ctx.lineTo(cx - s * 0.9, cy - s * 0.4);
        ctx.lineTo(cx - s * 0.65, cy - s * 0.2);
        ctx.stroke();
        break;
    }
    ctx.restore();
  }

  // --- Draw grid background ---
  function drawGrid() {
    var w = canvasWidth / dpr;
    var h = canvasHeight / dpr;
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.04)';
    ctx.lineWidth = 0.5;
    var gridSize = 32;

    for (var x = 0; x < w; x += gridSize) {
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, h);
      ctx.stroke();
    }
    for (var y = 0; y < h; y += gridSize) {
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(w, y);
      ctx.stroke();
    }
  }

  // --- Draw connections ---
  function drawConnections(time) {
    for (var i = 0; i < connections.length; i++) {
      var conn = connections[i];
      var from = nodes[conn[0]];
      var to = nodes[conn[1]];
      if (!from || !to) continue;

      // Determine start/end points based on relative positions
      var x1, y1, x2, y2;
      var dx = to.centerX - from.centerX;
      var dy = to.centerY - from.centerY;

      if (Math.abs(dx) > Math.abs(dy)) {
        // Horizontal-dominant connection
        if (dx > 0) {
          x1 = from.x + from.w;
          y1 = from.centerY;
          x2 = to.x;
          y2 = to.centerY;
        } else {
          x1 = from.x;
          y1 = from.centerY;
          x2 = to.x + to.w;
          y2 = to.centerY;
        }
      } else {
        // Vertical-dominant connection
        if (dy > 0) {
          x1 = from.centerX;
          y1 = from.y + from.h;
          x2 = to.centerX;
          y2 = to.y;
        } else {
          x1 = from.centerX;
          y1 = from.y;
          x2 = to.centerX;
          y2 = to.y + to.h;
        }
      }

      // Connection line
      ctx.strokeStyle = from.op.enabled && to.op.enabled
        ? 'rgba(6, 182, 212, 0.15)'
        : 'rgba(148, 163, 184, 0.06)';
      ctx.lineWidth = 1.5;
      ctx.setLineDash([4, 4]);

      // Use curved path for non-straight connections
      var midX = (x1 + x2) / 2;
      var midY = (y1 + y2) / 2;
      ctx.beginPath();
      ctx.moveTo(x1, y1);
      if (Math.abs(x2 - x1) > 40 && Math.abs(y2 - y1) > 20) {
        // Curved bezier for angled connections
        ctx.bezierCurveTo(midX, y1, midX, y2, x2, y2);
      } else {
        ctx.lineTo(x2, y2);
      }
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // Flow dots along connections
    for (var d = 0; d < flowDots.length; d++) {
      var dot = flowDots[d];
      var connInfo = connections[dot.connIdx];
      if (!connInfo) continue;
      var fn = nodes[connInfo[0]];
      var tn = nodes[connInfo[1]];
      if (!fn || !tn) continue;
      if (!fn.op.enabled || !tn.op.enabled) continue;

      var pts = getConnectionEndpoints(connInfo[0], connInfo[1]);
      var sx = pts.x1, sy = pts.y1, ex = pts.x2, ey = pts.y2;

      // Interpolate along connection (for curved connections, approximate)
      var t = dot.progress;
      var px, py;
      if (Math.abs(ex - sx) > 40 && Math.abs(ey - sy) > 20) {
        // Approximate bezier interpolation
        var midXd = (sx + ex) / 2;
        var ct1x = midXd, ct1y = sy, ct2x = midXd, ct2y = ey;
        var mt = 1 - t;
        px = mt * mt * mt * sx + 3 * mt * mt * t * ct1x + 3 * mt * t * t * ct2x + t * t * t * ex;
        py = mt * mt * mt * sy + 3 * mt * mt * t * ct1y + 3 * mt * t * t * ct2y + t * t * t * ey;
      } else {
        px = sx + (ex - sx) * t;
        py = sy + (ey - sy) * t;
      }

      ctx.beginPath();
      ctx.arc(px, py, 1.5, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(6, 182, 212, ' + (0.3 + 0.3 * Math.sin(t * Math.PI)) + ')';
      ctx.fill();
    }
  }

  // Helper to get connection endpoints
  function getConnectionEndpoints(fromIdx, toIdx) {
    var from = nodes[fromIdx];
    var to = nodes[toIdx];
    var dx = to.centerX - from.centerX;
    var dy = to.centerY - from.centerY;
    var x1, y1, x2, y2;

    if (Math.abs(dx) > Math.abs(dy)) {
      if (dx > 0) {
        x1 = from.x + from.w; y1 = from.centerY;
        x2 = to.x; y2 = to.centerY;
      } else {
        x1 = from.x; y1 = from.centerY;
        x2 = to.x + to.w; y2 = to.centerY;
      }
    } else {
      if (dy > 0) {
        x1 = from.centerX; y1 = from.y + from.h;
        x2 = to.centerX; y2 = to.y;
      } else {
        x1 = from.centerX; y1 = from.y;
        x2 = to.centerX; y2 = to.y + to.h;
      }
    }
    return { x1: x1, y1: y1, x2: x2, y2: y2 };
  }

  // Interpolate along a connection path (handles curves)
  function interpolateConnection(fromIdx, toIdx, t) {
    var pts = getConnectionEndpoints(fromIdx, toIdx);
    var sx = pts.x1, sy = pts.y1, ex = pts.x2, ey = pts.y2;
    var px, py;
    if (Math.abs(ex - sx) > 40 && Math.abs(ey - sy) > 20) {
      var midX = (sx + ex) / 2;
      var ct1x = midX, ct1y = sy, ct2x = midX, ct2y = ey;
      var mt = 1 - t;
      px = mt * mt * mt * sx + 3 * mt * mt * t * ct1x + 3 * mt * t * t * ct2x + t * t * t * ex;
      py = mt * mt * mt * sy + 3 * mt * mt * t * ct1y + 3 * mt * t * t * ct2y + t * t * t * ey;
    } else {
      px = sx + (ex - sx) * t;
      py = sy + (ey - sy) * t;
    }
    return { x: px, y: py };
  }

  // --- Draw operator nodes ---
  function drawNodes(time) {
    for (var i = 0; i < nodes.length; i++) {
      var node = nodes[i];
      var op = node.op;
      var isHovered = hoveredNode === node;
      var enabled = op.enabled;

      ctx.save();

      // Glow on hover
      if (isHovered && enabled) {
        ctx.shadowColor = 'rgba(6, 182, 212, 0.3)';
        ctx.shadowBlur = 20;
      }

      // Glass background
      var bgAlpha = enabled ? 0.6 : 0.25;
      drawRoundedRect(node.x, node.y, node.w, node.h, 10);
      ctx.fillStyle = 'rgba(12, 18, 41, ' + bgAlpha + ')';
      ctx.fill();

      // Border
      var borderColor = enabled
        ? (isHovered ? 'rgba(6, 182, 212, 0.5)' : 'rgba(148, 163, 184, 0.12)')
        : 'rgba(148, 163, 184, 0.06)';

      if (enabled && isHovered) {
        ctx.strokeStyle = borderColor;
        ctx.lineWidth = 1.5;
      } else {
        ctx.strokeStyle = borderColor;
        ctx.lineWidth = 1;
      }
      drawRoundedRect(node.x, node.y, node.w, node.h, 10);
      ctx.stroke();

      ctx.shadowColor = 'transparent';
      ctx.shadowBlur = 0;

      // Disabled overlay
      if (!enabled) {
        drawRoundedRect(node.x, node.y, node.w, node.h, 10);
        ctx.fillStyle = 'rgba(5, 10, 24, 0.5)';
        ctx.fill();
      }

      // Icon
      var iconY = node.y + 17;
      ctx.strokeStyle = enabled ? 'rgba(6, 182, 212, 0.8)' : 'rgba(148, 163, 184, 0.3)';
      drawIcon(op.icon, node.x + node.w / 2, iconY, 28);

      // Label
      ctx.fillStyle = enabled ? '#f1f5f9' : 'rgba(148, 163, 184, 0.4)';
      ctx.font = '600 11px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText(op.label, node.x + node.w / 2, node.y + 38);

      // Sublabel
      ctx.fillStyle = enabled ? 'rgba(148, 163, 184, 0.6)' : 'rgba(148, 163, 184, 0.25)';
      ctx.font = '400 9px "JetBrains Mono", monospace';
      ctx.fillText(op.sublabel, node.x + node.w / 2, node.y + 50);

      // Disabled badge
      if (!enabled) {
        ctx.fillStyle = 'rgba(251, 191, 36, 0.15)';
        ctx.strokeStyle = 'rgba(251, 191, 36, 0.3)';
        ctx.lineWidth = 0.5;
        var badgeW = 32;
        var badgeH = 14;
        var bx = node.x + node.w / 2 - badgeW / 2;
        var by = node.y - 8;
        drawRoundedRect(bx, by, badgeW, badgeH, 4);
        ctx.fill();
        ctx.stroke();
        ctx.fillStyle = 'rgba(251, 191, 36, 0.8)';
        ctx.font = '600 7px Inter, sans-serif';
        ctx.fillText('OFF', node.x + node.w / 2, by + 10);
      }

      ctx.restore();
    }
  }

  // --- Draw particles ---
  function drawParticles() {
    for (var i = 0; i < activeParticles.length; i++) {
      var p = activeParticles[i];
      if (!p.alive) continue;

      var c = p.color;
      var alpha = p.alpha;
      var r = p.radius * p.scale;

      ctx.save();

      // Flash effect for merge/match
      if (p.flashTimer > 0) {
        ctx.shadowColor = 'rgba(255, 255, 255, ' + Math.min(p.flashTimer / 150, 0.8) + ')';
        ctx.shadowBlur = 20;
      } else {
        ctx.shadowColor = 'rgba(' + c.r + ',' + c.g + ',' + c.b + ', 0.6)';
        ctx.shadowBlur = GLOW_RADIUS * p.scale;
      }

      ctx.beginPath();
      ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(' + c.r + ',' + c.g + ',' + c.b + ',' + alpha + ')';
      ctx.fill();

      // Inner bright core
      ctx.shadowBlur = 0;
      ctx.shadowColor = 'transparent';
      ctx.beginPath();
      ctx.arc(p.x, p.y, r * 0.4, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(255, 255, 255, ' + (alpha * 0.7) + ')';
      ctx.fill();

      // Label for aggregate particles
      if (p.label) {
        ctx.fillStyle = 'rgba(255, 255, 255, ' + alpha + ')';
        ctx.font = 'bold ' + Math.max(8, r * 0.8) + 'px "JetBrains Mono", monospace';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(p.label, p.x, p.y);
      }

      ctx.restore();
    }
  }

  // --- Draw sink storage ---
  function drawSinkStorage() {
    // Find the sink node
    var sinkNode = null;
    for (var i = 0; i < nodes.length; i++) {
      if (nodes[i].op.behavior === 'sink') {
        sinkNode = nodes[i];
        break;
      }
    }
    if (!sinkNode) return;

    var baseX = sinkNode.x + sinkNode.w / 2;
    var baseY = sinkNode.y + sinkNode.h + 8;

    for (var j = 0; j < sinkStack.length; j++) {
      var item = sinkStack[j];
      var y = baseY + j * 6;
      var w = 40;
      var h = 4;

      ctx.fillStyle = 'rgba(' + item.color.r + ',' + item.color.g + ',' + item.color.b + ', ' + (item.alpha * 0.3) + ')';
      drawRoundedRect(baseX - w / 2, y, w, h, 2);
      ctx.fill();
    }
  }

  // --- Spawn a particle from a source node ---
  function spawnFromSource(sourceNodeIdx) {
    if (sourceNodeIdx >= nodes.length) return;
    var sourceNode = nodes[sourceNodeIdx];
    if (!sourceNode || !sourceNode.op.enabled) return;

    var p = acquireParticle();
    if (!p) return;

    // Pick color: different palette per source for visual distinction
    var colorBase = sourceNodeIdx * 2; // offset color index per source
    var colorIdx = (colorBase + Math.floor(Math.random() * 2)) % PARTICLE_COLORS.length;
    p.color = PARTICLE_COLORS[colorIdx];
    p.radius = PARTICLE_RADIUS;
    p.sourceIndex = sourceNodeIdx;

    // Find the first output connection from this source
    var outputs = getOutputConnections(sourceNodeIdx);
    if (outputs.length === 0) {
      releaseParticle(p);
      return;
    }

    // Start off-screen left of the source node
    var pts = getConnectionEndpoints(sourceNodeIdx, outputs[0].toIdx);
    p.x = sourceNode.centerX - 30;
    p.y = sourceNode.centerY + (Math.random() - 0.5) * 16;
    p.startX = p.x;
    p.startY = p.y;
    p.endX = sourceNode.centerX;
    p.endY = sourceNode.centerY;
    p.targetNode = sourceNodeIdx;
    p.connectionIndex = -1;
    p.progress = 0;
    p.alpha = 0;
    p.scale = 1;
    p.state = 'moving';

    eventCount++;
  }

  // --- Move particle to next node along a specific connection ---
  function advanceParticleAlongConnection(p, fromIdx, toIdx) {
    if (toIdx >= nodes.length || !nodes[toIdx]) {
      p.state = 'dying';
      return;
    }

    // Skip disabled operators (bypass to next)
    if (!nodes[toIdx].op.enabled) {
      var nextOutputs = getOutputConnections(toIdx);
      if (nextOutputs.length > 0) {
        advanceParticleAlongConnection(p, toIdx, nextOutputs[0].toIdx);
        return;
      }
      p.state = 'dying';
      return;
    }

    var connIdx = findConnectionIndex(fromIdx, toIdx);
    var pts = getConnectionEndpoints(fromIdx, toIdx);

    p.startX = p.x;
    p.startY = p.y;
    p.endX = nodes[toIdx].centerX;
    p.endY = nodes[toIdx].centerY;
    p.targetNode = toIdx;
    p.connectionIndex = connIdx;
    p.progress = 0;
    p.state = 'moving';
  }

  // --- Advance particle from its current node to the next one ---
  function advanceParticle(p) {
    var currentIdx = p.targetNode;
    var outputs = getOutputConnections(currentIdx);

    if (outputs.length === 0) {
      p.state = 'dying';
      return;
    }

    // Pick first enabled output
    for (var i = 0; i < outputs.length; i++) {
      var toIdx = outputs[i].toIdx;
      if (nodes[toIdx] && nodes[toIdx].op.enabled) {
        advanceParticleAlongConnection(p, currentIdx, toIdx);
        return;
      }
      // Try to skip disabled
      var deeper = getOutputConnections(toIdx);
      if (deeper.length > 0) {
        advanceParticleAlongConnection(p, currentIdx, deeper[0].toIdx);
        return;
      }
    }

    p.state = 'dying';
  }

  // --- Process particle arriving at an operator ---
  function processArrival(p) {
    if (p.targetNode >= nodes.length) {
      p.state = 'dying';
      return;
    }

    var node = nodes[p.targetNode];
    var behavior = node.op.behavior;

    switch (behavior) {
      case 'spawn':
        // Source node: just pass through
        advanceParticle(p);
        break;

      case 'filter':
        // 30% chance to be filtered out
        if (Math.random() < 0.3) {
          p.state = 'dying';
        } else {
          advanceParticle(p);
        }
        break;

      case 'window':
        // Accumulate in window buffer (tumble)
        p.state = 'windowed';
        windowBuffer.push(p);
        break;

      case 'sliding_window':
        // Sliding window: particles enter and slide out one at a time
        p.state = 'sliding';
        slidingBuffer.push({ particle: p, entryTime: performance.now() });
        break;

      case 'aggregate':
        // Accumulate for aggregation
        p.state = 'aggregating';
        aggregateBuffer.push(p);
        if (aggregateBuffer.length >= AGGREGATE_THRESHOLD) {
          var merged = aggregateBuffer[0];
          var count = aggregateBuffer.length;
          for (var i = 1; i < aggregateBuffer.length; i++) {
            releaseParticle(aggregateBuffer[i]);
          }
          aggregateBuffer = [];
          merged.scale = 1.5 + count * 0.2;
          merged.label = '' + count;
          merged.state = 'moving';
          merged.flashTimer = 200;
          advanceParticle(merged);
        }
        break;

      case 'join':
        // Dual-input join: determine which input this particle came from
        var inputs = getInputConnections(p.targetNode);
        var isFromFirstInput = false;
        if (inputs.length >= 2) {
          // The particle's source determines which buffer
          isFromFirstInput = (p.sourceIndex === inputs[0].fromIdx) ||
            isSourceDescendant(p.sourceIndex, inputs[0].fromIdx);
        } else {
          isFromFirstInput = true;
        }

        if (isFromFirstInput) {
          joinBufferA.push(p);
        } else {
          joinBufferB.push(p);
        }
        p.state = 'join_waiting';

        // Try to merge
        if (joinBufferA.length > 0 && joinBufferB.length > 0) {
          var pa = joinBufferA.shift();
          var pb = joinBufferB.shift();
          // Merge: keep pa, release pb, blend colors
          pa.color = blendColors(pa.color, pb.color);
          pa.scale = 1.3;
          pa.flashTimer = 250;
          pa.label = '';
          pa.state = 'moving';
          releaseParticle(pb);
          advanceParticle(pa);
        }
        break;

      case 'asof_join':
        // ASOF join: main stream passes through, lookup stream updates reference
        var asofInputs = getInputConnections(p.targetNode);
        var isMainStream = false;
        if (asofInputs.length >= 2) {
          isMainStream = (p.sourceIndex === asofInputs[0].fromIdx) ||
            isSourceDescendant(p.sourceIndex, asofInputs[0].fromIdx);
        } else {
          isMainStream = true;
        }

        if (!isMainStream) {
          // Lookup stream: update the latest reference color, consume particle
          asofLookupLatest = { r: p.color.r, g: p.color.g, b: p.color.b };
          p.state = 'dying';
        } else {
          // Main stream: pass through, enriched if lookup available
          if (asofLookupLatest) {
            p.color = blendColors(p.color, asofLookupLatest);
            p.flashTimer = 180;
          }
          p.state = 'moving';
          advanceParticle(p);
        }
        break;

      case 'sink':
        sinkStack.push({ color: { r: p.color.r, g: p.color.g, b: p.color.b }, alpha: 1 });
        if (sinkStack.length > SINK_MAX) sinkStack.shift();
        p.state = 'dying';
        break;

      default:
        advanceParticle(p);
    }
  }

  // Check if sourceIdx is or is a descendant of rootIdx (for determining which input branch)
  function isSourceDescendant(sourceIdx, rootIdx) {
    if (sourceIdx === rootIdx) return true;
    // Simple BFS
    var visited = {};
    var queue = [rootIdx];
    visited[rootIdx] = true;
    while (queue.length > 0) {
      var current = queue.shift();
      if (current === sourceIdx) return true;
      var inputs = getInputConnections(current);
      for (var i = 0; i < inputs.length; i++) {
        if (!visited[inputs[i].fromIdx]) {
          visited[inputs[i].fromIdx] = true;
          queue.push(inputs[i].fromIdx);
        }
      }
    }
    return false;
  }

  // --- Update loop ---
  function update(dt) {
    var speed = BASE_SPEED * speedMultiplier;

    // Handle transition fade
    if (transitioning) {
      transitionAlpha -= dt / 300; // 300ms fade out
      if (transitionAlpha <= 0) {
        transitionAlpha = 0;
        completeTransition();
        transitioning = false;
      }
      // During fade-out, don't spawn or update logic
      // Just fade particles
      for (var fi = activeParticles.length - 1; fi >= 0; fi--) {
        activeParticles[fi].alpha = transitionAlpha;
        if (transitionAlpha <= 0) {
          releaseParticle(activeParticles[fi]);
        }
      }
      return;
    }

    // Fade in after transition
    if (transitionAlpha < 1) {
      transitionAlpha = Math.min(1, transitionAlpha + dt / 300);
    }

    // Spawn timer per source
    var sources = currentConfig ? currentConfig.sources : [0];
    for (var si = 0; si < sources.length; si++) {
      var srcIdx = sources[si];
      if (!spawnTimers[srcIdx]) spawnTimers[srcIdx] = 0;
      spawnTimers[srcIdx] += dt;
      var interval = SPAWN_INTERVAL / speedMultiplier;
      // Stagger spawns between sources
      var staggerOffset = si * (interval / sources.length);
      while (spawnTimers[srcIdx] >= interval) {
        spawnTimers[srcIdx] -= interval;
        spawnFromSource(srcIdx);
      }
    }

    // Window timer (tumble)
    windowTimer += dt;
    var windowDur = WINDOW_DURATION / speedMultiplier;
    if (windowTimer >= windowDur && windowBuffer.length > 0) {
      windowTimer = 0;
      for (var w = 0; w < windowBuffer.length; w++) {
        var wp = windowBuffer[w];
        if (wp.alive) {
          wp.state = 'moving';
          advanceParticle(wp);
        }
      }
      windowBuffer = [];
    }

    // Sliding window release
    slidingReleaseTimer += dt;
    var slideInterval = SLIDING_RELEASE_INTERVAL / speedMultiplier;
    if (slidingReleaseTimer >= slideInterval && slidingBuffer.length > 0) {
      slidingReleaseTimer = 0;
      // Release the oldest particle
      var oldest = slidingBuffer.shift();
      if (oldest && oldest.particle.alive) {
        oldest.particle.state = 'moving';
        advanceParticle(oldest.particle);
      }
    }

    // Events/sec counter
    eventCountTimer += dt;
    if (eventCountTimer >= 1000) {
      eventsPerSec = eventCount - lastEventCount;
      lastEventCount = eventCount;
      eventCountTimer -= 1000;
      updateEventCounter();
    }

    // Update flow dots
    for (var d = 0; d < flowDots.length; d++) {
      var dot = flowDots[d];
      dot.progress += dot.speed * speed * dt / 1000;
      if (dot.progress > 1) dot.progress -= 1;
    }

    // Update particles
    for (var pi = activeParticles.length - 1; pi >= 0; pi--) {
      var p = activeParticles[pi];
      if (!p.alive) continue;

      p.age += dt;
      if (p.flashTimer > 0) p.flashTimer -= dt;

      switch (p.state) {
        case 'moving':
          p.progress += speed * dt / 800;
          if (p.alpha < 1) p.alpha = Math.min(1, p.alpha + dt / 200);

          var t = p.progress;
          var eased = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;

          // If traveling along a connection, use connection path interpolation
          if (p.connectionIndex >= 0 && p.connectionIndex < connections.length) {
            var conn = connections[p.connectionIndex];
            var pos = interpolateConnection(conn[0], conn[1], Math.min(eased, 1));
            // Blend from startX,startY to the interpolated path
            var blend = Math.min(eased * 3, 1); // quickly blend to path
            p.x = p.startX + (pos.x - p.startX) * blend;
            p.y = p.startY + (pos.y - p.startY) * blend;
            // As progress reaches 1, converge on target center
            if (eased > 0.8) {
              var endBlend = (eased - 0.8) / 0.2;
              p.x = p.x + (p.endX - p.x) * endBlend;
              p.y = p.y + (p.endY - p.y) * endBlend;
            }
          } else {
            p.x = p.startX + (p.endX - p.startX) * Math.min(eased, 1);
            p.y = p.startY + (p.endY - p.startY) * Math.min(eased, 1);
          }

          if (p.progress >= 1) {
            p.x = p.endX;
            p.y = p.endY;
            processArrival(p);
          }
          break;

        case 'windowed':
          if (p.targetNode < nodes.length) {
            var wNode = nodes[p.targetNode];
            var wIdx = windowBuffer.indexOf(p);
            var angle = (p.age * 0.002 * speed) + (wIdx * 0.8);
            var orbitR = 20 + wIdx * 3;
            p.x = wNode.centerX + Math.cos(angle) * orbitR;
            p.y = wNode.centerY + Math.sin(angle) * orbitR * 0.5;
            p.alpha = 0.6 + 0.3 * Math.sin(angle * 2);
          }
          break;

        case 'sliding':
          // Sliding: particles slowly orbit/drift, then get released one at a time
          if (p.targetNode < nodes.length) {
            var sNode = nodes[p.targetNode];
            var sIdx = slidingBuffer.findIndex(function (sb) { return sb.particle === p; });
            if (sIdx < 0) sIdx = 0;
            var sAngle = (p.age * 0.003 * speed) + (sIdx * 1.2);
            var sOrbitR = 16 + sIdx * 4;
            p.x = sNode.centerX + Math.cos(sAngle) * sOrbitR;
            p.y = sNode.centerY + Math.sin(sAngle) * sOrbitR * 0.4;
            p.alpha = 0.5 + 0.4 * Math.sin(sAngle * 1.5);
          }
          break;

        case 'join_waiting':
          // Drift toward center of join node
          if (p.targetNode < nodes.length) {
            var jNode = nodes[p.targetNode];
            p.x += (jNode.centerX - p.x) * 0.08;
            p.y += (jNode.centerY - p.y) * 0.08;
            p.alpha = 0.5 + 0.3 * Math.sin(p.age * 0.005);
          }
          break;

        case 'aggregating':
          if (p.targetNode < nodes.length) {
            var aNode = nodes[p.targetNode];
            p.x += (aNode.centerX - p.x) * 0.05;
            p.y += (aNode.centerY - p.y) * 0.05;
            p.scale *= 0.98;
          }
          break;

        case 'dying':
          p.alpha -= dt / 300;
          p.scale *= 0.97;
          if (p.alpha <= 0) {
            releaseParticle(p);
          }
          break;
      }

      // Kill particles that are too old
      if (p.age > 15000) {
        releaseParticle(p);
      }
    }

    // Fade sink stack
    for (var s = sinkStack.length - 1; s >= 0; s--) {
      sinkStack[s].alpha -= dt / 8000;
      if (sinkStack[s].alpha <= 0) sinkStack.splice(s, 1);
    }
  }

  // --- Render loop ---
  function render(time) {
    if (!isRunning) return;

    var dt = Math.min(time - lastTime, 50);
    lastTime = time;

    update(dt);

    ctx.clearRect(0, 0, canvasWidth, canvasHeight);

    ctx.save();
    ctx.scale(dpr, dpr);

    drawGrid();
    drawConnections(time);
    drawSinkStorage();
    drawParticles();
    drawNodes(time);

    ctx.restore();

    requestAnimationFrame(render);
  }

  // --- Event counter display ---
  function updateEventCounter() {
    var el = document.getElementById('playground-eps');
    if (el) {
      el.textContent = eventsPerSec + ' events/sec';
    }
  }

  // --- Hit testing ---
  function getNodeAt(mx, my) {
    for (var i = nodes.length - 1; i >= 0; i--) {
      var n = nodes[i];
      if (mx >= n.x && mx <= n.x + n.w && my >= n.y && my <= n.y + n.h) {
        return n;
      }
    }
    return null;
  }

  // --- Tooltip ---
  function showTooltip(node, x, y) {
    if (!tooltip) return;
    tooltip.querySelector('.playground-tooltip-title').textContent = node.op.label;
    tooltip.querySelector('.playground-tooltip-desc').textContent = node.op.description;
    tooltip.style.display = 'block';

    var rect = canvas.getBoundingClientRect();
    var tx = rect.left + x / dpr;
    var ty = rect.top + y / dpr - 10;

    var tw = tooltip.offsetWidth;
    var th = tooltip.offsetHeight;
    tooltip.style.left = Math.max(8, Math.min(tx - tw / 2, window.innerWidth - tw - 8)) + 'px';
    tooltip.style.top = Math.max(8, ty - th - 8) + 'px';
    tooltipVisible = true;
  }

  function hideTooltip() {
    if (tooltip) {
      tooltip.style.display = 'none';
    }
    tooltipVisible = false;
  }

  // --- Resize ---
  function resize() {
    var container = canvas.parentElement;
    var w = container.clientWidth;
    var h = Math.max(220, Math.min(320, w * 0.28));
    dpr = Math.min(window.devicePixelRatio || 1, 2);

    canvas.width = w * dpr;
    canvas.height = h * dpr;
    canvas.style.width = w + 'px';
    canvas.style.height = h + 'px';
    canvasWidth = w * dpr;
    canvasHeight = h * dpr;

    layoutNodes();
  }

  // --- Preset switching ---
  function switchPreset(presetName) {
    if (presetName === currentPreset && currentConfig) return;

    currentPreset = presetName;

    // Start fade-out transition
    transitioning = true;
    transitionAlpha = 1;
    pendingPreset = presetName;
  }

  var pendingPreset = null;

  function completeTransition() {
    // Clear all buffers
    windowBuffer = [];
    windowTimer = 0;
    slidingBuffer = [];
    slidingReleaseTimer = 0;
    aggregateBuffer = [];
    joinBufferA = [];
    joinBufferB = [];
    asofMainBuffer = [];
    asofLookupLatest = null;
    sinkStack = [];
    spawnTimers = {};
    eventCount = 0;
    lastEventCount = 0;
    eventsPerSec = 0;
    updateEventCounter();

    // Release any remaining particles
    for (var i = activeParticles.length - 1; i >= 0; i--) {
      releaseParticle(activeParticles[i]);
    }

    // Load new config
    var presetName = pendingPreset || currentPreset;
    currentConfig = JSON.parse(JSON.stringify(PRESETS[presetName]));
    layoutNodes();

    // Start fade in
    transitionAlpha = 0.01;
  }

  // --- Mouse / Touch handlers ---
  function getCanvasPos(e) {
    var rect = canvas.getBoundingClientRect();
    var clientX, clientY;
    if (e.touches && e.touches.length > 0) {
      clientX = e.touches[0].clientX;
      clientY = e.touches[0].clientY;
    } else {
      clientX = e.clientX;
      clientY = e.clientY;
    }
    return {
      x: (clientX - rect.left) * dpr,
      y: (clientY - rect.top) * dpr
    };
  }

  function onPointerDown(e) {
    var pos = getCanvasPos(e);
    var node = getNodeAt(pos.x / dpr, pos.y / dpr);
    if (node) {
      draggingNode = node;
      dragOffsetX = pos.x / dpr - node.x;
      dragOffsetY = pos.y / dpr - node.y;
      dragStartX = pos.x / dpr;
      dragStartY = pos.y / dpr;
      canvas.style.cursor = 'grabbing';
      e.preventDefault();
    }
  }

  function onPointerMove(e) {
    var pos = getCanvasPos(e);
    var mx = pos.x / dpr;
    var my = pos.y / dpr;

    if (draggingNode) {
      draggingNode.x = mx - dragOffsetX;
      draggingNode.y = my - dragOffsetY;
      draggingNode.centerX = draggingNode.x + draggingNode.w / 2;
      draggingNode.centerY = draggingNode.y + draggingNode.h / 2;
      hideTooltip();
      return;
    }

    var node = getNodeAt(mx, my);
    if (node !== hoveredNode) {
      hoveredNode = node;
      canvas.style.cursor = node ? 'pointer' : 'default';
      if (node) {
        showTooltip(node, pos.x, pos.y);
      } else {
        hideTooltip();
      }
    }
  }

  function onPointerUp(e) {
    if (draggingNode) {
      var pos = getCanvasPos(e.changedTouches ? e : e);
      var mx = pos.x / dpr;
      var my = pos.y / dpr;
      var dx = Math.abs(mx - dragStartX);
      var dy = Math.abs(my - dragStartY);
      if (dx < 5 && dy < 5) {
        // Treat as click: toggle
        draggingNode.op.enabled = !draggingNode.op.enabled;
      }
      canvas.style.cursor = 'default';
      draggingNode = null;
    }
  }

  // --- Initialize ---
  function init() {
    canvas = document.getElementById('playground-canvas');
    if (!canvas) return;

    ctx = canvas.getContext('2d');
    tooltip = document.getElementById('playground-tooltip');

    // Load default preset
    currentConfig = JSON.parse(JSON.stringify(PRESETS.fraud));

    initPool();
    resize();

    // Event listeners
    canvas.addEventListener('mousedown', onPointerDown);
    canvas.addEventListener('mousemove', onPointerMove);
    canvas.addEventListener('mouseup', onPointerUp);
    canvas.addEventListener('mouseleave', function () {
      hoveredNode = null;
      hideTooltip();
      if (draggingNode) {
        draggingNode = null;
        canvas.style.cursor = 'default';
      }
    });

    // Touch
    canvas.addEventListener('touchstart', function (e) { onPointerDown(e); }, { passive: false });
    canvas.addEventListener('touchmove', function (e) { onPointerMove(e); e.preventDefault(); }, { passive: false });
    canvas.addEventListener('touchend', function (e) { onPointerUp(e); });

    // Speed slider
    var slider = document.getElementById('playground-speed');
    if (slider) {
      slider.addEventListener('input', function () {
        speedMultiplier = parseFloat(slider.value);
        var label = document.getElementById('playground-speed-label');
        if (label) label.textContent = speedMultiplier.toFixed(1) + 'x';
      });
    }

    // Add event button
    var addBtn = document.getElementById('playground-add-event');
    if (addBtn) {
      addBtn.addEventListener('click', function () {
        var sources = currentConfig ? currentConfig.sources : [0];
        for (var i = 0; i < 5; i++) {
          (function (idx) {
            setTimeout(function () {
              spawnFromSource(sources[idx % sources.length]);
            }, idx * 60);
          })(i);
        }
      });
    }

    // Preset buttons
    var presetBtns = document.querySelectorAll('.preset-btn[data-preset]');
    presetBtns.forEach(function (btn) {
      btn.addEventListener('click', function () {
        var preset = btn.getAttribute('data-preset');
        if (!PRESETS[preset]) return;

        // Update active state
        presetBtns.forEach(function (b) { b.classList.remove('active'); });
        btn.classList.add('active');

        switchPreset(preset);
      });
    });

    // Resize observer
    window.addEventListener('resize', function () {
      resize();
    });

    // Intersection observer -- auto-play/pause
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          if (!isRunning) {
            isRunning = true;
            lastTime = performance.now();
            requestAnimationFrame(render);
          }
        } else {
          isRunning = false;
        }
      });
    }, { threshold: 0.1 });

    observer.observe(canvas);
  }

  // Run on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
