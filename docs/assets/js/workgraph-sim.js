/*
 * Workgraph simulator: the renderer.
 *
 * Draws the engine's state (workgraph-sim-engine.js, loaded first) as an SVG
 * for a spec written as a JS object literal inside a ```workgraph fenced
 * block (see docs/dev/reference/workgraph-sim.md), with the operator
 * controls, the card and lineage dialogs, and the help mode that discloses
 * each region of the picture beside the code that draws it.
 */
(function (root) {
  'use strict';

  const { Sim, IDENTITIES, T, LISTS, WG_STATES, PARCEL_STATES, ACTION_RESULTS, MODE_ROWS, OPERATOR_EDGES, PARCEL_TERMINAL, COUNT_KEY, CANCELLABLE, RUNNING, runningList } = root.WorkgraphSimEngine;

  /* A source identity's colour. The model counts identities and knows nothing of colour; these are the count it counts to,
     which the checks hold the two to. */
  const PALETTE = ['#5b5bd6', '#c58a1f', '#2f5fd6', '#8a6dd8', '#2f9e8f', '#d6485b', '#3d8f3d', '#a0522d'];

  /* Escaping and rounding, for the strings this file builds; the model builds none. */
  const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const fmt = (n) => (Math.round(n * 10) / 10).toFixed(1);

  /* Model seconds, for things only the drawing measures: how long a halo lingers behind a parcel that has just ended, how long
     the retry arc stays lit after a retry, and how long a finished list stays in view before its panel goes. */
  const FADE = { glow: 1.0, retry: 1.3, linger: 2.0 };

  const SPEEDS = [
    { key: '1', mult: 1, name: 'normal speed', chevrons: 1 },
    { key: '2', mult: 4, name: 'faster', chevrons: 2 },
    { key: '3', mult: 8, name: 'fastest', chevrons: 3 },
  ];
  /* One press of step. The transport ran the model or stopped it and offered nothing between, so taking a run a beat at a
     time meant pausing and un-pausing quickly. A press advances until something happens and holds it there.

     What counts as something is the model's own record of it: every transition the machines draw on their edges, inputs
     and parcels both, and the log besides. A fixed slice is worse in either direction — one 0.05 tick usually catches the
     model between events, the feeder sweeping on a period of 1s and the packer 0.5s, while a whole model second often
     brings three things at once and the reader cannot tell which caused which — and the transition counters are the
     finest grain the model has, so a press lands on one thing and the picture moves by exactly that.

     The log alone will not do the job, though it is the obvious mark and reads as the right one: the engine writes a line
     for a phase change, a hook's decision, an action's result, an input quarantined, and for nothing else. An input taken
     up, run and processed, a parcel reserved, assigned and done — the whole of an ordinary run — passes in silence. Over
     the models the documentation ships, two presses in three found the log where they left it and spent the cap instead,
     which is the fixed slice again and at twice the size. Against the counters no press in a hundred does.

     The cap is still there, for the stretch where nothing is in flight at all. The slices are the frame loop's own, so a
     stepped run and a played run of one seed follow one trajectory and end in one state. `held` is a dialog standing over
     the model, where the frame loop already stops: the press is inert then, since the picture a dialog covers has to be
     the picture it was opened on. Returns the model time the press took, zero where nothing moved, which is what tells the
     widget whether there was anything to pause. */
  const STEP = { slice: 0.05, cap: 2, over: new Set(['Archived', 'Cleaned']) };

  /* Where the model stands, as a number that moves when anything does: what the machines count, and what the log says. A
     number rather than the counters themselves, because the press asks this once a slice and only wants to know whether to
     stop; `stepSnap` is the same counters kept apart, which it asks twice a press and reports from. */
  function stepMark(sim) {
    let n = sim.logSeq;
    for (const node of Object.values(sim.nodes)) {
      for (const k in node.transitions) n += node.transitions[k];
      for (const k in node.parcelTransitions) n += node.parcelTransitions[k];
    }
    return n;
  }

  /* The same counters, one key each, so that the difference across a press says which edges were taken and not merely that
     some were. Inputs and parcels are kept apart because the two machines share state names — `Unassigned` and `Assigned`
     are boxes on both — and the noun is the only thing that tells a reader which machine a line is about. */
  function stepSnap(sim) {
    const m = {};
    for (const node of Object.values(sim.nodes)) {
      for (const k in node.transitions) m[`${node.id}|input|${k}`] = node.transitions[k];
      for (const k in node.parcelTransitions) m[`${node.id}|parcel|${k}`] = node.parcelTransitions[k];
    }
    return m;
  }

  /* What a press moved, in the machines' own grammar: the counters that rose, largest first, grouped under the member they
     belong to. `born` is the machine's word for an edge with no state at its tail, so a birth reads as the machine's title
     does rather than as a transition out of nowhere. Three at most and the rest counted, since a press that moved a dozen
     things at once is one the reader wanted the shape of, not the whole of; the tooltip carries every one with its tail. */
  const SAID = { rows: 3 };

  function stepSaid(sim, before, after, took) {
    const rows = [];
    for (const key in after) {
      const n = after[key] - (before[key] || 0);
      if (n <= 0) continue;
      const [id, noun, edge] = key.split('|');
      const [from, to] = edge.split('>');
      rows.push({ id, noun, from, to, n });
    }
    if (!rows.length) return { text: 'nothing moved', title: `the model ran ${fmt(took)}s and took no edge on any machine`, took };
    rows.sort((a, b) => b.n - a.n || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    const say = (r) => `${r.n} ${r.noun}${r.n === 1 ? '' : 's'} ${r.from === 'born' ? `born ${r.to}` : `\u2192 ${r.to}`}`;
    const full = (r) => `${sim.nodes[r.id] ? sim.nodes[r.id].spec.label : r.id}: ${r.n} ${r.noun}${r.n === 1 ? '' : 's'} ${r.from === 'born' ? `born ${r.to}` : `${r.from} \u2192 ${r.to}`}`;
    const shown = rows.slice(0, SAID.rows);
    /* the member is named once where its rows are together, which is what a press almost always moves */
    const parts = [];
    for (const r of shown) {
      const label = sim.nodes[r.id] ? sim.nodes[r.id].spec.label : r.id;
      if (parts.length && parts[parts.length - 1].id === r.id) parts[parts.length - 1].says.push(say(r));
      else parts.push({ id: r.id, label, says: [say(r)] });
    }
    const rest = rows.length - shown.length;
    return {
      text: parts.map((p) => `${p.label} ${p.says.join(', ')}`).join(' \u00b7 ') + (rest ? ` \u00b7 +${rest}` : ''),
      title: `${fmt(took)}s of model time: ${rows.map(full).join('; ')}`,
      took,
    };
  }

  function stepModel(sim, held) {
    if (held || STEP.over.has(sim.wg.status)) return 0;
    /* a model that has not run starts on a press, as it starts on a speed, and the press then takes it its one beat */
    if (sim.wg.status === 'New') sim.start();
    const mark = stepMark(sim);
    let t = 0;
    /* half a slice of room, so the cap counts whole slices rather than landing a hair short of the last one */
    while (t < STEP.cap - STEP.slice / 2 && stepMark(sim) === mark) {
      sim.step(STEP.slice);
      t += STEP.slice;
    }
    return t;
  }

  const CHAOS_RATE = 0.5;
  const NARROW = 720;
  /* The row of lists, in the px the stylesheet declares them in: `col` is the floor a column is never narrower than, which is
     also what the widget assumes for a column it has not drawn yet; `gap` the space between two; `more` the width the `+N`
     takes at the right end; `pad` the row's own side padding, which comes off the width it measures. A column above the floor
     is as wide as its own list needs, so how many fit is measured from what was drawn rather than divided out of these.
     `invariants.test.js` holds the two files to the same numbers. */
  const LIST_ROW = { col: 240, gap: 20, more: 56, pad: 14 };
  const TIGHT = 540;

  /* A pill is as wide as its label: a pad either side and an advance per character, each pair hand-fitted to the type size
     the stylesheet gives that pill. The advances differ because the sizes do, so they are named rather than unified — but
     named in one place, so a pill and whatever positions it can no longer disagree about its width. */
  const PILL = {
    op: { pad: 14, advance: 7.2 }, /* an operator's button on an edge: .wg-m-btn-label, 11.5px semibold */
    count: { pad: 8, advance: 6.8 }, /* a transition count on an edge: .wg-m-count-label, 9.5px */
    backlog: { pad: 8, advance: 6.5 }, /* a +N chip on a slot grid or an edge: .wg-backlog-label, 9.5px semibold */
  };
  const pillWidth = (label, kind) => kind.pad + String(label).length * kind.advance;

  /* Monochrome icons drawn in currentColor. */
  const ICON = {
    chevrons: (n) => `<svg viewBox="0 0 24 24" aria-hidden="true">${Array.from({ length: n }, (_, k) => `<path d="M${7 + k * 5 - (n - 1) * 2.5} 6l6 6-6 6"/>`).join('')}</svg>`,
    pause: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg>',
    reset: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19.5 12a7.5 7.5 0 1 1-2.2-5.3"/><path d="M19.5 4v4.5H15"/></svg>',
    sliders: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h10M18 7h2M4 17h4M12 17h8"/><circle cx="16" cy="7" r="2.2"/><circle cx="10" cy="17" r="2.2"/></svg>',
    monkey: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="5" cy="11" r="2.6"/><circle cx="19" cy="11" r="2.6"/><circle cx="12" cy="12.5" r="6.5"/><circle cx="9.7" cy="11.2" r="0.9" class="fill"/><circle cx="14.3" cy="11.2" r="0.9" class="fill"/><path d="M9.5 15.3q2.5 1.8 5 0"/></svg>',
    play: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5l10 7-10 7z" class="fill"/></svg>',
    step: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 5l10 7-10 7z" class="fill"/><rect x="17" y="5" width="3" height="14" rx="1" class="fill"/></svg>',
    expand: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 4H4v5M20 9V4h-5M15 20h5v-5M4 15v5h5"/></svg>',
    contract: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9h5V4M20 9h-5V4M15 20v-5h5M9 20v-5H4"/></svg>',
  };

  /* ------------------------------------------------------------------ */
  /* Disclosures: what each region of the picture means                  */
  /* ------------------------------------------------------------------ */

  /* Everything about an encoding, a layout or an interaction rots the moment the drawing changes, so it is written beside the
     drawing and never anywhere else. Each region of the picture registers its disclosure next to the function that draws it,
     with `disclose`, and the help mode collects them: a change to a region and the change to its explanation land in one diff.
     A disclosure explains its own region and nothing else, in a sentence or three, with a legend where the region carries a
     visual encoding, and ends on a link to the state machine it belongs to and to the ADR that decides it.

     `body(ctx)` is the prose, `legend(ctx)` the swatches, `tab` the machine the link opens; `ctx` is `{ sim, node }`, the
     transformation the mark was placed on where the region is a card's. `anchor` finds the element the mark sits over and
     `corner` says which of its corners, `tl` where the top-right is occupied. */
  const DISCLOSURES = {};
  const disclose = (key, def) => (DISCLOSURES[key] = Object.assign({ key }, def));

  /* The ADRs, resolved against this script rather than the page, so the links hold under any deployment path. */
  const ADR = {
    '002': ['DX-ADR-002_overview', 'DX-ADR-002 · the Transformation System'],
    '004': ['DX-ADR-004_schema', 'DX-ADR-004 · the schema'],
    '005': ['DX-ADR-005_state_machines', 'DX-ADR-005 · the state machines'],
    '006': ['DX-ADR-006_extensions', 'DX-ADR-006 · the extension points'],
  };

  /* ------------------------------------------------------------------ */
  /* Legends                                                             */
  /* ------------------------------------------------------------------ */

  /* A legend row's swatch is the real element. Every one of them is drawn by the same function that draws the thing in the
     picture, over the same state→style map, so a swatch cannot come to disagree with what it describes and a state added to
     an enum appears in its legend by itself. The checks assert both halves of that: every state has a style and a label, and
     every style belongs to a live state. Nothing here restates a class name or a colour. */
  function swatch(body, w, h, scale) {
    const k = scale || 1;
    return `<svg class="wgsim-swatch" viewBox="0 0 ${w} ${h}" width="${(w * k).toFixed(1)}" height="${(h * k).toFixed(1)}" aria-hidden="true">${body}</svg>`;
  }

  /* A legend: one row per entry, its swatch drawn and its label beside it. */
  function legendHtml(rows) {
    return `<ul class="wgsim-legend">${rows.map((r) => `<li${r.tip ? ` title="${esc(r.tip)}"` : ''}>${r.swatch}<span>${esc(r.label)}</span></li>`).join('')}</ul>`;
  }

  /* The automation rail on a card's right edge, and the workgraph's two pills on the header: one row per sweep in taxonomy
     order, its mode on the right, a run button in its place while the row is by hand and has work. The left cell names the
     sweep: its glyph while the row is automatic, its name once the reader has taken it by hand, so the rows under the
     reader's control name themselves and the rest stay quiet. Rails exist only in expert mode; their width is fixed, so
     switching a row never lays the graph out again, and leaving expert mode is what does. */
  const RAIL = { w: 112, pad: 11, btn: 22 };
  /* A rail row stands for the engine rows it covers, which is one apiece but for the hooks: HandleFailedInput and the status
     hooks are set apart in the matrix and drawn as one row here, because five rows would cost the card the height the
     compaction reclaimed. A row whose parts disagree reads `mixed`. */
  const RAIL_ROWS = {
    feeder: { glyph: '▷▷', name: 'feeder', label: 'the feeder', parts: ['feeder'] },
    packer: { glyph: '⬡', name: 'packer', label: 'the packer', parts: ['packer'] },
    hooks: { glyph: '⚡︎', name: 'hooks', label: 'the hooks', parts: ['failedInput', 'hooks'] },
    actions: { glyph: '☑︎', name: 'actions', label: 'the action lists', parts: ['actions'] },
  };
  const RAIL_ORDER = { node: ['feeder', 'packer', 'hooks', 'actions'], workgraph: ['hooks', 'actions'] };
  /* Which rows an entity has at all: the workgraph has no feeder, no packer and no failed inputs. */
  const hasRow = (id, row) => (id === 'workgraph' ? MODE_ROWS.workgraph : MODE_ROWS.node).includes(row);
  /* The cells one rail row governs on one entity, which is what a click on it sets and what its state is a tally of. */
  const railCells = (id, railRow) => RAIL_ROWS[railRow].parts.filter((row) => hasRow(id, row)).map((row) => ({ id, row }));

  /* ------------------------------------------------------------------ */
  /* Layout                                                              */
  /* ------------------------------------------------------------------ */

  /* A card's geometry: a title row, the pool beside the slot grid, and the bar along the bottom edge. The slot grid, the
     tallest thing in the card, sets the height, and the pool is sized to the grid rather than to what it holds, so the cards
     of a layer are all the same height and the layers stay easy to scan. */
  const G = {
    pad: { t: 8, x: 9, b: 9 },
    title: 17, /* the baseline the title and the status share */
    head: { status: 92, ch: 6.2 }, /* the room kept for the longest status; ch is the advance per character of the 11px title */
    body: 25, /* the top of the pool and of the slot grid */
    gap: 8, /* the gutter between the pool and the grid */
    grid: { side: 58, gap: 6 }, /* the square the grid always fills, whatever the capacity: 26px slots two by two */
    bar: 3, /* the bottom edge: the card's border and its consumption bar are one thing */
    radius: 4, /* the card's corner and the boxes inside it; small, so the bar's first and last segments lose almost nothing to the rounding */
  };

  /* A card's size never changes while the model runs: every disclosure is local, so nothing lays the graph out again. The
     body is the card without its rail; in expert mode a card is the body plus the rail, and the graph is laid out again
     for the width the rails take. */
  const BODY = { w: 300, h: G.body + G.grid.side + G.pad.b + G.bar };
  const cardW = (rail) => BODY.w + (rail ? RAIL.w : 0);
  const bodyW = (it) => (it.rail ? it.w - RAIL.w : it.w);
  /* The boxes beside the cards. An input query and an output box are the same thing seen from either end, a line with a count
     that holds the files behind a click, so they are the same shape; a histogram keeps the height it needs to bin them. */
  const OUT = { w: 170, h: 44, hist: 112 };
  const outputH = (o) => (o.show === 'histogram' ? OUT.hist : OUT.h);
  const PAD = 28;

  /* The action list a transformation's state belongs to, if any; Completed still shows the finalizing results. */
  function listFor(status) {
    if (status === 'Completed') return 'finalize';
    for (const [key, meta] of Object.entries(LISTS)) if (status === meta.state || status === meta.blocked || status === meta.done) return key;
    return null;
  }

  /* ------------------------------------------------------------------ */
  /* Layout: elkjs places the nodes and routes the edges orthogonally.   */
  /* ------------------------------------------------------------------ */

  /* Boxy on purpose: a right angle is unambiguous where a curve is not, and an edge that never
     passes beneath a node is an edge that never disappears. */
  const ELK_OPTIONS = {
    'elk.algorithm': 'layered',
    'elk.direction': 'DOWN',
    'elk.edgeRouting': 'ORTHOGONAL',
    /* a fan-out leaves its port as one trunk and splits at a junction */
    'elk.layered.mergeEdges': 'true',
    /* no edge may pass beneath a node: this is the clearance that guarantees it */
    'elk.spacing.edgeNode': '28',
    'elk.layered.spacing.edgeNodeBetweenLayers': '28',
    /* parallel channels for skip edges, so two never share one */
    'elk.spacing.edgeEdge': '16',
    'elk.layered.spacing.edgeEdgeBetweenLayers': '16',
    'elk.spacing.nodeNode': '44',
    'elk.layered.spacing.nodeNodeBetweenLayers': '70',
    'elk.layered.nodePlacement.strategy': 'BRANDES_KOEPF',
    'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
    'elk.layered.cycleBreaking.strategy': 'GREEDY',
    'elk.portConstraints': 'FIXED_SIDE',
    'elk.padding': `[top=${PAD},left=${PAD},bottom=${PAD},right=${PAD}]`,
  };

  const CORNER = 10;

  /* The three situations the picture says it is stopped for, and what each says under the word. `armed` is not among them:
     nothing has run yet, and the transport glows for that. */
  const HOLD_WORDS = [
    ['paused', 'press space to resume'],
    ['dialog', 'while this dialog is open'],
    ['help', 'while help is on'],
  ];

  /* Where this script was loaded from, captured now because document.currentScript is null once we
     are asynchronous. The layout engine sits beside it, so it is found under any deployment path. */
  const SELF_SRC = typeof document !== 'undefined' && document.currentScript ? document.currentScript.src : '';
  const ELK_URL = SELF_SRC ? new URL('vendor/elk.bundled.js', SELF_SRC).href : 'vendor/elk.bundled.js';
  /* The mark, for the lockup the maximised band carries. Beside the script like the layout engine, so it is found under
     any deployment path. It stays off the canvas: its two hues mean nothing on a surface where green, red, amber, blue
     and the accent each mean something, and help mode teaches exactly those. */
  const LOGO_URL = SELF_SRC ? new URL('../images/logo.svg', SELF_SRC).href : '../images/logo.svg';

  /* elkjs is 1.6 MB, which has no business on the critical path of the documentation pages that
     carry no model at all. It is fetched once per page, the first time a model comes into view,
     and what progress there is goes to every widget waiting on it. */
  let elkLoad = null;
  const elkWatchers = new Set();

  function watchElk(fn) {
    elkWatchers.add(fn);
    return () => elkWatchers.delete(fn);
  }

  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const el = document.createElement('script');
      el.src = src;
      el.onload = () => resolve();
      el.onerror = () => reject(new Error(`could not load ${src}`));
      document.head.appendChild(el);
    });
  }

  /* Read the body as it arrives so the bar can be a real one; a response without a length or a
     readable stream still resolves, it just cannot say how far along it is. */
  async function fetchProgressively(url) {
    const res = await fetch(url, { credentials: 'same-origin' });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    if (!res.body || typeof res.body.getReader !== 'function') return res.blob();
    const total = Number(res.headers.get('content-length')) || 0;
    const reader = res.body.getReader();
    const chunks = [];
    let loaded = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      loaded += value.length;
      for (const fn of elkWatchers) fn(loaded, total);
    }
    return new Blob(chunks, { type: 'text/javascript' });
  }

  function ensureElk() {
    if (root.ELK) return Promise.resolve(root.ELK);
    if (elkLoad) return elkLoad;
    if (typeof document === 'undefined') return Promise.reject(new Error('elkjs is not loaded: see tests/workgraph-sim/elk-shim.js'));
    elkLoad = (async () => {
      try {
        const blob = await fetchProgressively(ELK_URL);
        const url = URL.createObjectURL(blob);
        try {
          await loadScript(url);
        } finally {
          URL.revokeObjectURL(url);
        }
      } catch (e) {
        /* no fetch, no streams, or a policy that forbids blob: — a plain script tag always works,
           it just cannot report progress */
        for (const fn of elkWatchers) fn(0, 0);
        await loadScript(ELK_URL);
      }
      if (!root.ELK) throw new Error('the layout engine loaded but did not register');
      return root.ELK;
    })();
    /* a failure is not cached: another model on the page may try again */
    elkLoad.catch(() => {
      elkLoad = null;
    });
    return elkLoad;
  }

  async function elkEngine() {
    const E = await ensureElk();
    if (!elkEngine.instance) elkEngine.instance = new E();
    return elkEngine.instance;
  }

  const outPortId = (owner, port) => `${owner}#out:${port}`;
  const inPortId = (owner, from) => `${owner}#in:${from}`;

  /* The ELK graph for a spec. Ports carry the meaning: edges may only merge into one trunk when
     they leave the same port, so a transformation's main and artifact outputs never share one,
     and a fan-in cannot merge at all because every upstream lands on its own input port. */
  function elkGraph(spec, rail) {
    const children = [];
    const edges = [];
    const byId = {};
    const add = (c) => {
      byId[c.id] = c;
      children.push(c);
      return c;
    };
    const southPort = (owner, port) => {
      const id = outPortId(owner, port);
      const node = byId[owner];
      if (node && !node.ports.some((q) => q.id === id)) node.ports.push({ id, layoutOptions: { 'elk.port.side': 'SOUTH' } });
      return id;
    };

    for (const s of Object.values(spec.sources)) add({ id: s.id, kind: 'source', width: OUT.w, height: OUT.h, ports: [] });
    for (const n of Object.values(spec.transformations)) add({ id: n.id, kind: 'node', width: cardW(rail), height: BODY.h, ports: [] });
    for (const o of Object.values(spec.outputs)) add({ id: o.id, kind: 'output', width: OUT.w, height: outputH(o), ports: [] });

    const northPort = (owner, from) => {
      const id = inPortId(owner, from);
      byId[owner].ports.push({ id, layoutOptions: { 'elk.port.side': 'NORTH' } });
      return id;
    };

    for (const n of Object.values(spec.transformations)) {
      for (const f of n.feeder.from) {
        if (!byId[f]) continue;
        const producerPort = spec.transformations[f] ? n.feeder.port : 'main';
        edges.push({ id: `edge:${f}>${n.id}`, from: f, to: n.id, kind: 'edge', port: producerPort, sources: [southPort(f, producerPort)], targets: [northPort(n.id, f)] });
      }
      /* the wait and join dependencies are what push a consumer down a layer, so ELK must see them
         too; each leaves its own port, since a wait carries no files and must not share the trunk */
      for (const a of n.feeder.after) {
        if (!byId[a]) continue;
        edges.push({ id: `wait:${a}>${n.id}`, from: a, to: n.id, kind: 'wait', port: 'wait', sources: [southPort(a, 'wait')], targets: [northPort(n.id, `wait:${a}`)] });
      }
      for (const j of [n.packer.join, n.packer.lookup]) {
        if (!j || !byId[j]) continue;
        edges.push({ id: `join:${j}>${n.id}`, from: j, to: n.id, kind: 'join', port: 'join', sources: [southPort(j, 'join')], targets: [northPort(n.id, `join:${j}`)] });
      }
    }
    for (const o of Object.values(spec.outputs)) {
      if (!byId[o.from]) continue;
      edges.push({ id: `edge:${o.from}>${o.id}`, from: o.from, to: o.id, kind: 'edge', port: o.port, sources: [southPort(o.from, o.port)], targets: [northPort(o.id, o.from)] });
    }
    return { id: 'root', layoutOptions: ELK_OPTIONS, children, edges };
  }

  /* An orthogonal polyline as a path with rounded corners. */
  function roundedPath(pts, r) {
    const p = pts.filter((q, i) => i === 0 || Math.abs(q.x - pts[i - 1].x) > 0.01 || Math.abs(q.y - pts[i - 1].y) > 0.01);
    if (p.length < 2) return '';
    const f = (v) => (Math.round(v * 10) / 10).toFixed(1);
    let d = `M${f(p[0].x)} ${f(p[0].y)}`;
    for (let i = 1; i < p.length - 1; i++) {
      const a = p[i - 1];
      const c = p[i];
      const b = p[i + 1];
      const inLen = Math.hypot(c.x - a.x, c.y - a.y);
      const outLen = Math.hypot(b.x - c.x, b.y - c.y);
      const rr = Math.min(r, inLen / 2, outLen / 2);
      d += ` L${f(c.x + ((a.x - c.x) / inLen) * rr)} ${f(c.y + ((a.y - c.y) / inLen) * rr)}`;
      d += ` Q${f(c.x)} ${f(c.y)} ${f(c.x + ((b.x - c.x) / outLen) * rr)} ${f(c.y + ((b.y - c.y) / outLen) * rr)}`;
    }
    const last = p[p.length - 1];
    return `${d} L${f(last.x)} ${f(last.y)}`;
  }

  function sectionPoints(edge) {
    const out = [];
    for (const s of edge.sections || []) out.push(s.startPoint, ...(s.bendPoints || []), s.endPoint);
    return out;
  }

  /* Every point where a fan-out splits. Walks the branches together along their geometry rather
     than by index, so a branch running straight through a point where another turns still counts
     as sharing the trunk, and recurses so a trunk that splits twice gets a dot at each split. */
  function junctionsOf(edges) {
    const byPort = new Map();
    for (const e of edges) {
      if (!byPort.has(e.sourcePort)) byPort.set(e.sourcePort, []);
      byPort.get(e.sourcePort).push(e.points);
    }
    const dots = [];
    const walk = (cursors, cur) => {
      const live = cursors.filter((c) => c.i < c.pts.length);
      if (live.length < 2) return;
      const groups = new Map();
      for (const c of live) {
        const n = c.pts[c.i];
        const key = `${Math.sign(n.x - cur.x)},${Math.sign(n.y - cur.y)}`;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(c);
      }
      if (groups.size > 1) {
        dots.push(cur);
        for (const gp of groups.values()) walk(gp.map((c) => ({ pts: c.pts, i: c.i })), cur);
        return;
      }
      const dist = live.map((c) => Math.hypot(c.pts[c.i].x - cur.x, c.pts[c.i].y - cur.y));
      const step = Math.min(...dist);
      if (step < 0.01) return;
      const [dx, dy] = [...groups.keys()][0].split(',').map(Number);
      walk(live.map((c, k) => ({ pts: c.pts, i: dist[k] <= step + 0.01 ? c.i + 1 : c.i })), { x: cur.x + dx * step, y: cur.y + dy * step });
    };
    for (const [, paths] of byPort) {
      if (paths.length < 2) continue;
      walk(paths.map((q) => ({ pts: q, i: 1 })), { x: paths[0][0].x, y: paths[0][0].y });
    }
    return dots;
  }

  /* A point a given distance back from the end of a path, with the unit normal there: where a
     backlog chip goes, offset to the side so a file travelling the same path cannot sit under it. */
  function beforeEnd(pts, back) {
    let left = back;
    for (let i = pts.length - 1; i > 0; i--) {
      const b = pts[i];
      const a = pts[i - 1];
      const len = Math.hypot(b.x - a.x, b.y - a.y);
      if (len >= left || i === 1) {
        const t = len ? Math.min(1, left / len) : 0;
        return { x: b.x + (a.x - b.x) * t, y: b.y + (a.y - b.y) * t, nx: len ? (b.y - a.y) / len : 1, ny: len ? -(b.x - a.x) / len : 0 };
      }
      left -= len;
    }
    return { x: pts[0].x, y: pts[0].y, nx: 1, ny: 0 };
  }

  /* Lay the model out. Asynchronous because elkjs is; the result depends on the spec and on whether the cards carry
     their rails, so a model is laid out once per mode, the first time that mode is shown, and never again. */
  async function layout(sim, opts) {
    const rail = !!(opts && opts.rail);
    const spec = sim.spec;
    const graph = elkGraph(spec, rail);
    const laid = await (await elkEngine()).layout(graph);

    const items = {};
    for (const c of laid.children) {
      const ports = {};
      for (const q of c.ports || []) ports[q.id] = { x: c.x + q.x + (q.width || 0) / 2, y: c.y + q.y + (q.height || 0) / 2 };
      const kind = graph.children.find((g) => g.id === c.id).kind;
      items[c.id] = { kind, id: c.id, w: c.width, h: c.height, x: c.x, y: c.y, ports, rail: kind === 'node' && rail };
    }
    /* the card's own anchor, used by the pictures drawn inside it */
    for (const it of Object.values(items)) it.ax = it.w / 2;

    const edges = [];
    for (const e of laid.edges) {
      const spec_ = graph.edges.find((q) => q.id === e.id);
      const points = sectionPoints(e);
      if (points.length < 2) continue;
      edges.push({ id: e.id, from: spec_.from, to: spec_.to, kind: spec_.kind, port: spec_.port, sourcePort: spec_.sources[0], points, d: roundedPath(points, CORNER) });
    }
    return { items, edges, junctions: junctionsOf(edges), width: Math.ceil(laid.width), height: Math.ceil(laid.height), rail };
  }

  /* ------------------------------------------------------------------ */
  /* SVG helpers                                                         */
  /* ------------------------------------------------------------------ */

  function shapeSvg(shape, cx, cy, r, fill, extra) {
    extra = extra || '';
    const p = (v) => Math.round(v * 10) / 10;
    switch (shape) {
      case 'triangle':
        return `<path d="M${p(cx)} ${p(cy - r)} L${p(cx + r * 0.95)} ${p(cy + r * 0.72)} L${p(cx - r * 0.95)} ${p(cy + r * 0.72)} Z" fill="${fill}" ${extra}/>`;
      case 'diamond':
        return `<path d="M${p(cx)} ${p(cy - r)} L${p(cx + r)} ${p(cy)} L${p(cx)} ${p(cy + r)} L${p(cx - r)} ${p(cy)} Z" fill="${fill}" ${extra}/>`;
      case 'square':
        return `<rect x="${p(cx - r * 0.82)}" y="${p(cy - r * 0.82)}" width="${p(r * 1.64)}" height="${p(r * 1.64)}" rx="1.5" fill="${fill}" ${extra}/>`;
      default:
        return `<circle cx="${p(cx)}" cy="${p(cy)}" r="${p(r)}" fill="${fill}" ${extra}/>`;
    }
  }

  function retryPath(it) {
    const x = it.x + 18;
    const yb = it.y + it.h - G.bar; /* above the bar: the arc is about one input, and the edge is about all of them */
    const yt = it.y + 8;
    return `M${x} ${yb} C${x - 46} ${yb}, ${x - 46} ${yt}, ${x} ${yt}`;
  }

  /* The grid a capacity is drawn on: two columns up to four slots, three beyond, always inside the same square, so a card's size never depends on its slots. */
  function gridOf(capacity) {
    const n = Math.max(1, capacity || 1);
    const cols = n <= 4 ? 2 : 3;
    const side = (G.grid.side - (cols - 1) * G.grid.gap) / cols;
    return { cols, rows: Math.ceil(n / cols), side };
  }

  /* The square the grid fills, at the right end of the card's body. */
  const gridOrigin = (it) => ({ x: it.x + bodyW(it) - G.pad.x - G.grid.side, y: it.y + G.body });

  function slotBoxes(it, capacity) {
    const g = gridOf(capacity);
    const o = gridOrigin(it);
    const out = [];
    for (let r = 0; r < g.rows; r++) for (let c = 0; c < g.cols; c++) out.push({ x: o.x + c * (g.side + G.grid.gap), y: o.y + r * (g.side + G.grid.gap), w: g.side, h: g.side });
    return out.slice(0, Math.max(1, capacity || 1));
  }

  function gridBox(it, capacity) {
    const g = gridOf(capacity);
    const o = gridOrigin(it);
    return { x: o.x, y: o.y, w: g.cols * g.side + (g.cols - 1) * G.grid.gap, h: g.rows * g.side + (g.rows - 1) * G.grid.gap };
  }

  /* The pool, left of the slot grid: what waits before the slots, in cells. Problematic inputs first, since they wait for a
     person; then the parcels the packer has made that wait for a slot, each a small box in its state's border holding its
     inputs' shapes; then the loose Unassigned inputs. What the cells cannot hold is counted in the corner, and the +N chip
     on the grid counts the queued parcels exactly. Static, in the help dialog, nothing in it is clickable. */
  const POOL = { box: 16, gap: 5 };
  /* One cell is a parcel's box with air around it, and the "+N" in the corner is measured from the number it will draw: both
     are what they hold rather than a count fixed here, so the pool of a narrower card shows fewer shapes instead of a cap
     that lies about what it holds. */
  const POOL_CELL = POOL.box + POOL.gap;
  const MORE = { ch: 6, pad: 3 }; /* the advance per character of the 10px "+N", and the air kept around it */

  /* The pool's box: the card's body less its padding, the slot grid and the gutter between them. As tall as the grid,
     whatever it holds. */
  function poolBox(it) {
    return { x: it.x + G.pad.x, y: it.y + G.body, w: bodyW(it) - G.pad.x * 2 - G.grid.side - G.gap, h: G.grid.side };
  }

  /* The cells the pool's own width and height hold, as many as fit in each direction, spread over the box. */
  function poolGrid(it) {
    const b = poolBox(it);
    const cols = Math.max(1, Math.floor(b.w / POOL_CELL));
    const rows = Math.max(1, Math.floor(b.h / POOL_CELL));
    return { box: b, cols, rows, cw: b.w / cols, ch: b.h / rows };
  }

  function poolCells(it) {
    const g = poolGrid(it);
    const out = [];
    for (let r = 0; r < g.rows; r++) for (let c = 0; c < g.cols; c++) out.push({ cx: g.box.x + (c + 0.5) * g.cw, cy: g.box.y + (r + 0.5) * g.ch });
    return out;
  }

  /* The cells of the last row the count takes, measured from the text it will draw: the corner is reserved rather than
     shared, so a "+N" can never come to sit over a shape. */
  const moreRoom = (text, cw) => Math.ceil((text.length * MORE.ch + MORE.pad * 2) / cw);

  /* Where the count sits: the pool's bottom-right corner, the cells behind it left empty. */
  const moreAt = (it) => {
    const b = poolBox(it);
    return { x: b.x + b.w - MORE.pad, y: b.y + b.h - MORE.pad };
  };

  /* What a loose shape in the pool says about its input, one class per condition, in the order the pool stacks them: a
     quarantine first, since it waits for a person, then the packed parcels, then the loose inputs. The renderer picks its
     class from this map and the legend draws from it, so neither can carry a treatment the other does not. */
  const POOL_CLS = { problematic: 'wg-problematic', delayed: 'wg-delayed', child: 'wg-child', unassigned: '' };

  /* What waits before the slots, in the order the pool stacks it. */
  function poolEntries(sim, node) {
    const out = [];
    for (const i of node.inputs.values()) if (i.status === 'Problematic') out.push({ kind: 'input', input: i, cls: POOL_CLS.problematic });
    for (const p of node.parcels) if (p.status === 'Unassigned') out.push({ kind: 'parcel', parcel: p });
    for (const i of node.inputs.values()) if (i.status === 'Unassigned') out.push({ kind: 'input', input: i, cls: sim.delayed(node, i) ? POOL_CLS.delayed : i.mask ? POOL_CLS.child : POOL_CLS.unassigned });
    return out;
  }

  /* One cell of the pool. The legend calls this too, on a sample entry, which is what keeps the two the same drawing. */
  function poolEntrySvg(e, cx, cy, nodeId) {
    if (e.kind === 'parcel') {
      const p = e.parcel;
      const s = POOL.box;
      const shown = p.inputs.slice(0, 3);
      const gap = 5;
      const x0 = cx - ((shown.length - 1) * gap) / 2;
      const n = p.inputs.length;
      return `<g class="wg-pool-parcel"${nodeId != null ? ` data-act="open-parcels" data-node="${esc(nodeId)}"` : ''}><title>parcel ${esc(p.tag)} · ${p.status}: packed, waiting for a slot · ${n} input${n === 1 ? '' : 's'}${nodeId != null ? ' · click for the parcel counts' : ''}</title>${slotBoxSvg(cx - s / 2, cy - s / 2, s, s, 3, p.status, 0)}${shown.map((i, j) => shapeSvg(i.file.shape, x0 + j * gap, cy, 2.2, PALETTE[i.file.colour])).join('')}</g>`;
    }
    const r = e.input.mask ? 4.6 : 5.6;
    const extra = e.cls ? `class="${e.cls}"` : '';
    return nodeId != null ? fileShape(e.input.file, cx, cy, r, extra) : shapeSvg(e.input.file.shape, cx, cy, r, PALETTE[e.input.file.colour], extra);
  }

  /* As many of what waits as the cells hold, and the rest counted in the corner. The count's own room comes off the cells,
     which changes how many are hidden, which changes the count: it settles in a pass or two and is left alone once it has.
     With nothing hidden nothing is counted, and the corner goes back to the shapes. */
  function poolSvg(it, entries, nodeId) {
    const g = poolGrid(it);
    const cells = poolCells(it);
    let room = cells.length;
    let hidden = entries.length - room;
    for (let i = 0; hidden > 0 && i < 3; i++) {
      const next = Math.max(0, cells.length - moreRoom(`+${hidden}`, g.cw));
      if (next === room) break;
      room = next;
      hidden = entries.length - room;
    }
    const out = entries.slice(0, room).map((e, k) => poolEntrySvg(e, cells[k].cx, cells[k].cy, nodeId));
    return { svg: out.join(''), more: hidden > 0 ? `+${hidden}` : '' };
  }

  /* While a list of actions is eligible the pool shows the running action in its place. A list becomes eligible only once the
     member has drained (DX-ADR-005), so nothing is waiting there to be covered up. */
  function poolActionSvg(it, action) {
    const b = poolBox(it);
    return `<text x="${b.x + 5}" y="${(b.y + b.h / 2 + 4).toFixed(1)}" class="wg-list-item wg-pool-action ${action.cls}" clip-path="url(#wg-pool-${esc(it.id)})"><title>${esc(action.tip)}</title>${action.glyph} ${esc(action.text)}</text>`;
  }

  /* The pool's legend: one sample per treatment, drawn by `poolEntrySvg`, so a shape here is the shape there. */
  const POOL_SAMPLES = {
    unassigned: { kind: 'input', input: { file: { shape: 'circle', colour: 0 } } },
    delayed: { kind: 'input', input: { file: { shape: 'diamond', colour: 2 } } },
    child: { kind: 'input', input: { file: { shape: 'circle', colour: 3 }, mask: '0-4' } },
    problematic: { kind: 'input', input: { file: { shape: 'triangle', colour: 5 } } },
  };
  const POOL_LABELS = {
    unassigned: 'an Unassigned input: colour by source file, shape by type',
    delayed: 'delayed: a condition has not held yet',
    child: 'smaller: the unfinished part of a file a split cut down',
    problematic: 'Problematic: set aside for a person',
  };

  function poolLegendHtml() {
    const rows = Object.keys(POOL_CLS).map((key) => ({
      label: POOL_LABELS[key],
      swatch: swatch(poolEntrySvg(Object.assign({ cls: POOL_CLS[key] }, POOL_SAMPLES[key]), 9, 9, null), 18, 18),
    }));
    rows.push({ label: 'a parcel the packer has made, waiting for a slot', swatch: swatch(poolEntrySvg({ kind: 'parcel', parcel: { tag: 'p1', status: 'Unassigned', inputs: [{ file: { shape: 'circle', colour: 0 } }, { file: { shape: 'triangle', colour: 1 } }] } }, 11, 11, null), 22, 22) });
    return legendHtml(rows);
  }

  disclose('pool', {
    title: 'the inputs and parcels waiting',
    corner: 'tl',
    tab: 'inputs',
    adr: '004',
    body: () => 'Everything that waits before the slots, one cell each. The <b>Problematic</b> inputs come first, waiting for an operator; then the parcels the packer has made, waiting for a slot; then the loose <b>Unassigned</b> inputs. As many as the region holds are drawn, and the rest are counted in its corner. While an action list runs, the running action takes the region instead. The member has drained by then, so nothing is waiting here.',
    legend: poolLegendHtml,
  });

  function fileTitle(f, sim) {
    const name = (id) => (sim && sim.nodes[id] ? sim.nodes[id].spec.label : id);
    const where = f.seed != null ? `seed ${f.seed} of ${name(f.producer)}` : f.producer ? `produced by ${name(f.producer)}` : 'from the input query';
    return `${f.port === 'artifact' ? 'artifact' : 'file'} ${f.tag} · ${where} · click for its lineage`;
  }

  /* A clickable file shape with its provenance as a tooltip. */
  function fileShape(f, cx, cy, r, extra) {
    return `<g data-act="lineage" data-file="${f.id}" class="wg-file"><title>${esc(fileTitle(f))}</title>${shapeSvg(f.shape, cx, cy, r, PALETTE[f.colour], extra)}</g>`;
  }

  /* One token's own element, drawn at the origin so that a frame moves it with a transform rather than drawing it again. The
     shape and the title are the picture's, built by the same functions that draw a file anywhere else. */
  function tokenSvg(tk) {
    const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    g.setAttribute('class', 'wg-file');
    g.setAttribute('data-act', 'lineage');
    g.setAttribute('data-file', tk.file.id);
    const shape =
      tk.kind === 'join'
        ? shapeSvg(tk.file.shape, 0, 0, 5, 'var(--wg-card)', `stroke="${PALETTE[tk.file.colour]}" stroke-width="1.6" class="wg-token"`)
        : shapeSvg(tk.file.shape, 0, 0, tk.file.port === 'artifact' ? 4 : 5.5, PALETTE[tk.file.colour], 'class="wg-token"');
    g.innerHTML = `<title>${esc(fileTitle(tk.file))}</title>${shape}`;
    return g;
  }

  /* An icon drawn inside the picture at a given box. */
  function iconAt(icon, x, y, size) {
    return icon.replace('<svg ', `<svg x="${x}" y="${y}" width="${size}" height="${size}" `);
  }

  /* A title that never reaches the buttons: cut with an ellipsis, the full name in the tooltip. */
  function fitTitle(label, max, ch) {
    const chars = Math.floor(max / (ch || G.head.ch));
    if (label.length <= chars) return { text: label, cut: false };
    return { text: label.slice(0, Math.max(1, chars - 1)) + '…', cut: true };
  }

  /* One row of a rail, its state baked in where given and set each frame otherwise: `auto`, `manual`, `mixed` where the two
     hooks it covers disagree, or `run` while any cell of it is by hand with work, when the mode word gives way to the run
     button. The left cell holds both the sweep's glyph and its name, and the state picks which shows: the glyph while
     automatic, the name once the reader has taken any of it by hand. Static, in the help dialog, a row is inert. */
  function railRowSvg(x, y, w, h, row, id, st, opts) {
    opts = opts || {};
    const meta = RAIL_ROWS[row];
    const state = st ? st.state : 'auto';
    const mode = st ? st.mode : state;
    const live = !opts.static;
    const act = live ? `data-act="toggle-mode" data-node="${id}" data-row="${row}"` : '';
    const b = RAIL.btn;
    const bx = x + w - RAIL.pad - b;
    const by = y + (h - b) / 2;
    const ty = y + h / 2 + 3.5;
    return `<g class="wg-rail-row" data-dyn="rail-${row}" data-id="${id}" data-state="${state}" ${act}>
        <title data-dyn="railtip-${row}" data-id="${id}">${esc(st ? st.tip : `${meta.label}: automatic`)}</title>
        <rect x="${x}" y="${y}" width="${w}" height="${h}" class="wg-rail-hit"/>
        <text x="${x + RAIL.pad}" y="${ty}" class="wg-rail-glyph">${meta.glyph}</text>
        <text x="${x + RAIL.pad}" y="${ty}" class="wg-rail-name">${meta.name}</text>
        <text x="${x + w - RAIL.pad}" y="${ty}" text-anchor="end" class="wg-rail-mode" data-dyn="railmode-${row}" data-id="${id}">${mode}</text>
        <g class="wg-rail-run" ${live ? `data-act="run-row" data-node="${id}" data-row="${row}"` : ''}><rect x="${bx}" y="${by}" width="${b}" height="${b}" rx="5" class="wg-rail-run-bg"/>${iconAt(ICON.play, bx + 4, by + 4, b - 8)}</g>
      </g>`;
  }

  /* The rail: a tinted column with a hairline on its left and hairlines between its rows, the right corners rounded to the
     card's. Every row is drawn automatic, and the frame sets each one's state from `railStates`. */
  function railSvg(x, y, w, h, r, rows, id) {
    const rowH = h / rows.length;
    const out = [`<g class="wg-rail" data-rail="${id}"><path d="M${x} ${y} H${x + w - r} Q${x + w} ${y} ${x + w} ${y + r} V${y + h - r} Q${x + w} ${y + h} ${x + w - r} ${y + h} H${x} Z" class="wg-rail-bg"/>`];
    rows.forEach((row, k) => {
      const ry = y + k * rowH;
      if (k) out.push(`<line x1="${x}" y1="${ry}" x2="${x + w}" y2="${ry}" class="wg-rail-line"/>`);
      out.push(railRowSvg(x, ry, w, rowH, row, id, null));
    });
    out.push(`<line x1="${x}" y1="${y}" x2="${x}" y2="${y + h}" class="wg-rail-line"/></g>`);
    return out.join('');
  }

  /* The rail's legend: a real row per sweep, drawn inert by `railRowSvg`, plus the three a row that is not plainly automatic
     becomes. */
  function railLegendHtml() {
    const row = (name, state) => swatch(railRowSvg(0, 0, RAIL.w, 26, name, 'legend', { state, mode: state === 'run' ? 'manual' : state, tip: '' }, { static: true }), RAIL.w, 26, 0.82);
    const rows = RAIL_ORDER.node.map((name) => ({ label: RAIL_ROWS[name].label, swatch: row(name, 'auto') }));
    rows.push({ label: 'by hand: it sweeps only when you run it', swatch: row('feeder', 'manual') });
    rows.push({ label: 'mixed: HandleFailedInput and the Active hook disagree', swatch: row('hooks', 'mixed') });
    rows.push({ label: 'by hand with work: ▶ runs one sweep', swatch: row('feeder', 'run') });
    return legendHtml(rows);
  }

  disclose('rail', {
    title: 'the sweep controls',
    corner: 'tl',
    tab: 'transformation',
    adr: '006',
    body: () => 'One row per sweep this transformation runs: the feeder, the packer, the hooks, the action lists. A row is <b>auto</b>, sweeping on its own period, or <b>manual</b>, sweeping only when you run it. A click on the row switches it. The hooks row covers two, HandleFailedInput and the Active hook, and reads <b>mixed</b> where they differ; <b>expert</b> opens the matrix that sets them apart. Leaving expert mode sets every row back to auto.',
    legend: railLegendHtml,
  });

  /* What one sweep of one engine row would do now, or why there is nothing for it to do: the sentence the rail's tooltip and
     the matrix's cell titles both say. */
  function rowWork(sim, id, row) {
    const node = id === 'workgraph' ? null : sim.nodes[id];
    const pending = sim.rowPending(id, row);
    if (row === 'feeder') return pending ? 'something to feed' : node.feederEnabled ? 'nothing to feed' : 'disabled';
    if (row === 'packer') return pending ? 'a ready input to pack' : 'nothing to pack';
    if (row === 'failedInput') {
      const failed = [...node.inputs.values()].filter((i) => i.status === 'Failed').length;
      return failed ? `${failed} failed input${failed === 1 ? '' : 's'} for HandleFailedInput` : 'nothing for HandleFailedInput';
    }
    if (row === 'hooks') {
      if (node) return pending ? 'the Active hook would pause it' : 'nothing for the Active hook';
      const plan = sim.wg.status === 'Scouting' ? sim.scoutPlan() : null;
      const scout = plan ? (plan.raise != null ? 'ScoutingToApproving would raise the scout to the next stage' : plan.accept === 'failing' ? 'ScoutingToApproving would accept early: too much of the sample failed' : 'ScoutingToApproving would accept the sample') : 'the Active hook would change a feeder';
      return pending ? scout : 'nothing for the hooks to do';
    }
    const next = sim.nextAction(node ? id : undefined);
    return pending ? `${next.action.name} is next` : next && next.why === 'waiting' ? `waiting for ${next.on.map((d) => d.spec.label).join(', ')}` : next && next.why === 'blocked' ? `${next.action.name} failed: force it or reset it` : 'no action to run';
  }

  /* Each rail row's state and tooltip: the tally of the cells it covers, and `run` while any cell of it is by hand with work,
     since a sweep that would do nothing has no button. A row whose cells disagree is `mixed`, which is reported and never set:
     a click on it makes the whole row uniform. */
  function railStates(sim, id) {
    const out = {};
    for (const row of RAIL_ORDER[id === 'workgraph' ? 'workgraph' : 'node']) {
      const cells = railCells(id, row);
      const meta = RAIL_ROWS[row];
      const mode = sim.modeTally(cells).mode;
      /* only a cell that is by hand can have work the reader must run, so a pending row is one the ▶ belongs on */
      const pending = cells.some((c) => sim.modeOf(c.id, c.row) === 'manual' && sim.rowPending(c.id, c.row));
      const work = cells.map((c) => rowWork(sim, c.id, c.row)).join(' · ');
      const state = pending ? 'run' : mode;
      const says = mode === 'auto' ? 'sweeps by itself' : mode === 'manual' ? 'by hand' : 'HandleFailedInput and the Active hook disagree';
      const to = `${cells.length > 1 ? 'all ' : ''}${mode === 'auto' ? 'by hand' : 'automatic'}`;
      const tip = `${meta.label}: ${says} · ${work} · click the row to set it ${to}${state === 'run' ? ', ▶ to run one sweep' : ''}`;
      out[row] = { state, mode, tip };
    }
    return out;
  }

  /* ------------------------------------------------------------------ */
  /* What runs by hand: the matrix behind the expert chip                 */
  /* ------------------------------------------------------------------ */

  /* Toggling one rail row at a time stops scaling the moment a workgraph has more than one member, so expert mode opens a
     matrix: a column per sweep, a row per entity, one cell each. The sweeps are the five the engine sets and there are never
     more, so they are the axis that cannot grow; the members are the axis that can, and a workgraph of a dozen of them adds
     rows the dialog scrolls rather than columns it cannot show. `HandleFailedInput` comes before the status hooks
     deliberately — both are hooks, so their order between themselves is free, and this gathers the cells the workgraph does
     not have into one block instead of interleaving blanks with content. */
  const MATRIX_SWEEPS = [
    { row: 'feeder', label: 'feeders', none: 'the workgraph has no feeder of its own' },
    { row: 'packer', label: 'packers', none: 'the workgraph has no packer of its own' },
    { row: 'failedInput', label: 'HandleFailedInput', none: 'the workgraph holds no inputs, so none of them fails in it' },
    { row: 'hooks', label: 'status hooks' },
    { row: 'actions', label: 'actions' },
  ];

  /* The rows, the workgraph first and then the members in graph order. The workgraph is a row of the matrix and not a strip
     beside it: as a row the column logic reaches it with no case of its own, so "status hooks" clicked includes it and
     "feeders" clicked correctly does not. A gutter separates it from the members rather than a heavier rule, since it owns
     them rather than standing beside them, and the gutter says so without competing with the hairlines. */
  function matrixEntities(sim) {
    return [{ id: 'workgraph', label: 'workgraph' }].concat(sim.topoOrder().map((n) => ({ id: n.id, label: n.spec.label })));
  }

  /* The mark every aggregate carries, one hue and the fill as the scale: hollow where none of the scope is by hand, the left
     half filled where some of it is, solid where all of it is. No third colour, and `mixed` never needs a word. */
  const MODE_DOTS = [
    ['auto', 'hollow: none of it runs by hand'],
    ['mixed', 'half filled: some of it runs by hand'],
    ['manual', 'solid: all of it runs by hand'],
  ];
  function modeDot(mode) {
    const fill = mode === 'manual'
      ? '<circle cx="6" cy="6" r="4.1" class="wgsim-mx-fill"/>'
      : mode === 'mixed'
        ? '<path d="M6 1.9 A4.1 4.1 0 0 0 6 10.1 Z" class="wgsim-mx-fill"/>'
        : '';
    return `<svg class="wgsim-mx-dot" viewBox="0 0 12 12" width="12" height="12" aria-hidden="true" data-mode="${mode}"><circle cx="6" cy="6" r="4.1" class="wgsim-mx-ring"/>${fill}</svg>`;
  }

  /* A sweep's column header, an entity's row label, or the corner, each of which is its own aggregate. The click makes the
     scope uniform: all automatic goes by hand, and anything else — all by hand, or mixed — goes automatic. One predictable
     click, never a cycle through three states, where a reader has to click once to find out where they are.

     A bare label does not look clickable, so the hover does both jobs at once: the scope tints, and the label says what the
     click will do. Both forms sit in one grid cell, so the wider of them reserves the width and nothing reflows on the swap;
     the dot goes with the name it belongs to. Focus applies the same swap, which is what covers the keyboard. */
  function modeAggregate(scope, label, tally, what) {
    const attrs = `data-scope="${scope.kind}"${scope.sweep ? ` data-row="${scope.sweep}"` : ''}${scope.entity ? ` data-node="${esc(scope.entity)}"` : ''}`;
    const to = tally.mode === 'auto' ? 'all manual' : 'all auto';
    const says = tally.mode === 'auto' ? 'every one automatic' : tally.mode === 'manual' ? 'every one by hand' : `${tally.manual} of ${tally.total} by hand`;
    return `<button type="button" class="wgsim-mx-label" data-act="mode-scope" ${attrs} title="${esc(`${what}: ${says} · click to set ${to}`)}"><span class="wgsim-mx-shown">${modeDot(tally.mode)}<span class="wgsim-mx-name">${esc(label)}</span></span><span class="wgsim-mx-swap" aria-hidden="true">→ ${to}</span></button>`;
  }

  /* One cell: auto, manual, or the em dash of a sweep the entity does not have. Never a ternary — the aggregates on the
     headers are where mixed is reported. A dash rather than a blank, since a blank reads as "not loaded" where the dash
     states that the workgraph has no feeder, which is worth knowing. */
  function modeCell(sim, sweep, id, entity) {
    if (!hasRow(id, sweep.row)) return `<td class="wgsim-mx-cell wgsim-mx-dash" data-sweep="${sweep.row}" title="${esc(sweep.none)}">—</td>`;
    const mode = sim.modeOf(id, sweep.row);
    const tip = `${sweep.label} of ${entity}: ${mode === 'auto' ? 'sweeps by itself' : 'by hand'} · ${rowWork(sim, id, sweep.row)} · click to set it ${mode === 'auto' ? 'by hand' : 'automatic'}`;
    return `<td class="wgsim-mx-cell" data-sweep="${sweep.row}"><button type="button" class="wgsim-mx-mode" data-mode="${mode}" data-act="mode-cell" data-node="${esc(id)}" data-row="${sweep.row}" title="${esc(tip)}">${mode}</button></td>`;
  }

  /* The dialog expert mode opens. The count in the header band is the one place the reader sees at a glance that something
     was left on, which is what matters when a node is not progressing; it is suppressed while nothing is by hand. The footer
     reads the dot and offers nothing that sets a mode: `everything` in the corner is where the whole matrix is taken at once,
     and a second control for the same move is the duplication this surface keeps accumulating. */
  function modesHtml(sim) {
    const rows = matrixEntities(sim);
    const all = sim.modeTally(sim.modeCells());
    const head = panelHead({
      title: 'what runs by hand',
      right: all.manual ? `${all.manual} of ${all.total} manual` : '',
      rightCls: 'wgsim-modes-count',
      control: '',
    });
    const corner = `<th class="wgsim-mx-corner" scope="col">${modeAggregate({ kind: 'all' }, 'everything', all, 'every sweep of every entity')}</th>`;
    const heads = MATRIX_SWEEPS.map((sweep) => {
      const tally = sim.modeTally(sim.modeCells().filter((c) => c.row === sweep.row));
      return `<th class="wgsim-mx-head" scope="col" data-sweep="${sweep.row}">${modeAggregate({ kind: 'sweep', sweep: sweep.row }, sweep.label, tally, `${sweep.label}, everywhere`)}</th>`;
    }).join('');
    /* the gutter is a row of its own: the hairline above it is the workgraph's own, and the space below it is what says the
       workgraph owns the members rather than standing beside them */
    const gutter = `<tr class="wgsim-mx-gutter" aria-hidden="true"><td colspan="${MATRIX_SWEEPS.length + 1}"></td></tr>`;
    const body = rows.map((e, k) => {
      const tally = sim.modeTally(sim.modeCells().filter((c) => c.id === e.id));
      const cells = MATRIX_SWEEPS.map((sweep) => modeCell(sim, sweep, e.id, e.id === 'workgraph' ? 'the workgraph' : e.label)).join('');
      const label = `<th class="wgsim-mx-rowhead" scope="row">${modeAggregate({ kind: 'entity', entity: e.id }, e.label, tally, `every sweep of ${e.id === 'workgraph' ? 'the workgraph' : e.label}`)}</th>`;
      return `${k === 1 ? gutter : ''}<tr data-entity="${esc(e.id)}">${label}${cells}</tr>`;
    }).join('');
    const legend = MODE_DOTS.map(([mode, what]) => `<span title="${esc(what)}">${modeDot(mode)}${mode}</span>`).join('');
    const foot = `<div class="wgsim-modes-foot"><div class="wgsim-modes-legend">${legend}</div><button type="button" class="wgsim-btn wgsim-btn-accent" data-act="close-modes" autofocus>done</button></div>`;
    return `${head}<div class="wgsim-modes-body"><table class="wgsim-mx"><thead><tr>${corner}${heads}</tr></thead><tbody>${body}</tbody></table></div>${foot}`;
  }

  /* The cells one aggregate covers, read from the engine's own list so that a scope can hold nothing the matrix does not draw. */
  function scopeCells(sim, scope) {
    const all = sim.modeCells();
    if (scope.kind === 'sweep') return all.filter((c) => c.row === scope.sweep);
    if (scope.kind === 'entity') return all.filter((c) => c.id === scope.entity);
    return all;
  }

  /* The card's static frame: the pool region opens the card's details, the backlog chip over the last slot the parcel counts,
     the bottom edge carries the consumption bar, and in expert mode the rail on the right edge holds each sweep's mode and its
     run button; outside it no rail is drawn at all.

     The bar is the bottom edge rather than a band of its own: the whole edge is clipped to the card's box, so the remainder
     segment in the border's own colour is the border, and the colours are what work has made of it. The other three edges stay
     the plain hairline the card has always had, which is what makes the bottom one read as a measure. */
  function cardSvg(it, label, kind, doc) {
    const x = it.x;
    const y = it.y;
    const id = esc(it.id);
    const bw = bodyW(it);
    const act = (a) => `data-act="${a}" data-node="${id}"`;
    const titleMax = bw - G.pad.x * 2 - G.head.status - 8;
    const title = fitTitle(label, titleMax);
    const pb = poolBox(it);
    const more = moreAt(it);
    return `<g class="wg-card wg-kind-${kind}" data-card="${id}" ${act('open-details')}>
        <title>${doc ? esc(doc) + ' · ' : ''}click for the feeder, the packer, the inputs and every action list</title>
        <rect x="${x}" y="${y}" width="${it.w}" height="${it.h}" rx="${G.radius}" class="wg-card-bg"/>
        <text x="${x + G.pad.x}" y="${y + G.title}" class="wg-card-title">${title.cut ? `<title>${esc(label)}</title>` : ''}${esc(title.text)}</text>
        <g class="wg-status">
          <title data-dyn="statustitle" data-id="${id}">the transformation's status</title>
          <text x="${x + bw - G.pad.x}" y="${y + G.title}" text-anchor="end" class="wg-status-label" data-dyn="status" data-id="${id}"></text>
        </g>
        ${it.rail ? railSvg(x + bw, y, RAIL.w, it.h, G.radius, RAIL_ORDER.node, id) : ''}
        <clipPath id="wg-pool-${id}"><rect x="${pb.x}" y="${pb.y}" width="${pb.w}" height="${pb.h}"/></clipPath>
        <g>
          <title data-dyn="pooltitle" data-id="${id}">the transformation's pool</title>
          <rect x="${pb.x}" y="${pb.y}" width="${pb.w}" height="${pb.h}" rx="${G.radius}" class="wg-pool"/>
          <g data-dyn="pool" data-id="${id}"></g>
          <text x="${more.x.toFixed(1)}" y="${more.y.toFixed(1)}" text-anchor="end" class="wg-more" data-dyn="more" data-id="${id}"></text>
        </g>
        <g data-dyn="slots" data-id="${id}"></g>
        <clipPath id="wg-edge-${id}"><rect x="${x}" y="${y}" width="${it.w}" height="${it.h}" rx="${G.radius}"/></clipPath>
        <g clip-path="url(#wg-edge-${id})">
          <title data-dyn="bartip" data-id="${id}"></title>
          <g data-dyn="bar" data-id="${id}"></g>
        </g>
      </g>`;
  }

  /* The one vocabulary of action results (DX-ADR-004): `dot` in a card's pool, `tick` in the lists and the state line,
     `word` beside a row, `title` in a sentence, `cls` for the colour. A sign-off nobody has given is recorded as Failed, so
     that forcing it passed is the sign-off, but it is a wait rather than a problem and is drawn as one: `Unsigned` is how
     the renderer shows that result on a manual action. */
  const RESULTS = {
    null: { dot: '○', tick: '○', word: 'not run', title: 'not run yet', cls: 'todo' },
    Running: { dot: '◔', tick: '◷', word: 'running', title: 'running', cls: 'pending' },
    Pending: { dot: '◑', tick: '◷', word: 'pending', title: 'pending: the answer is not knowable yet', cls: 'pending' },
    Passed: { dot: '●', tick: '✓', word: 'passed', title: 'passed', cls: 'passed' },
    Done: { dot: '●', tick: '✓', word: 'done', title: 'done', cls: 'passed' },
    Failed: { dot: '✕', tick: '✕', word: 'failed', title: 'failed', cls: 'failed' },
    Unsigned: { dot: '◑', tick: '◷', word: 'not signed off', title: 'waiting for a sign-off', cls: 'pending' },
  };
  /* The vocabulary in the order a result is reached, `null` first: what has not run, what is under way, what has landed. */
  const RESULT_ORDER = ACTION_RESULTS.concat(['Unsigned']);
  const res = (r) => RESULTS[r] || RESULTS.null;
  /* An action's result as it is drawn: a failed sign-off is a wait. */
  const resultOf = (a) => (a.result === 'Failed' && a.manual ? 'Unsigned' : a.result);
  /* Why an entity is blocked, if it is: a sign-off nobody has given is a `wait`, any other failed action a `problem`. The two
     share the Blocked state of DX-ADR-005 and nothing else: a wait is amber wherever the state shows, a problem red. */
  function blockedKind(sim, node) {
    const a = node ? node.run.blocked : sim.wg.run.blocked;
    return a ? (a.manual ? 'wait' : 'problem') : null;
  }
  /* Why a member's pool is pulsing, and which tab of its dialog answers it. Three situations share the pulse — a
     quarantine, an action that failed or waiting on a sign-off, and a hold of the member's own — and the pulse said only
     that one of them held, never which. That is the reader's first question, and the one that wants no clicking: the
     sentence goes on the pool itself, where it is read before anything is opened.

     `held` is the frame's own set, since `heldMembers()` walks every member and every input of each, and a per-member call
     from a per-member loop is that walk squared. `counts` likewise: the loop has it already.

     The pulse reads this and nothing else, so the light and the sentence are one condition and cannot come apart. Ordered
     by which answer the reader would take first, and `waitsOnOperator` already implies a quarantine, so the hold that
     reaches the third line is the paused one. */
  function poolWants(sim, node, counts, held) {
    if (node.status === 'Paused' && held.has(node.id)) return { why: 'paused with work left: resume it on the transformation machine', tab: 'transformation' };
    if (counts.Pb) return { why: `${counts.Pb} Problematic input${counts.Pb === 1 ? '' : 's'} wait for you: reset or write off on the inputs machine`, tab: 'inputs' };
    const a = node.run.blocked;
    if (a) return { why: `${a.name} ${blockedKind(sim, node) === 'wait' ? 'waits for a sign-off' : 'failed'}: the actions panel above the picture carries both ways past it`, tab: null };
    /* a member held by neither of those has a quarantine by definition, so nothing reaches here; kept as the honest end of
       an enumeration the checks hold against the engine's own three conditions */
    return held.has(node.id) ? { why: 'held open for you', tab: null } : null;
  }

  /* An action that has an outcome, as opposed to one still running or still waiting to be knowable. */
  const settled = (r) => !!r && r !== 'Failed' && r !== 'Running' && r !== 'Pending';

  /* A log line ends on the state it reached, in that state's colour, so the outcome reads before the sentence does. The engine
     records which state that is, so the word is looked up rather than found: nothing here reads a sentence the model wrote. */
  const LOG_TONE = { Passed: 'ok', Done: 'ok', Pending: 'wait', Failed: 'bad', Problematic: 'bad', NotProcessed: 'off' };
  /* A transition's state, drawn as the card's badge is: the state's own colour, out of the one tone map the stylesheet
     keeps, so the badge and the line cannot come to say different things about the same state. Not the strip's pill —
     that geometry is a control in a row of eight, and a line of a log is a record rather than a control, so the chip takes
     the row's own rhythm and the row's tint goes on saying "transition" by itself. The two situations a blocked state
     covers are told apart here as they are on the card, from what the engine recorded at the transition.
     `LOG_TONE`, below, is the other question — not where the run got to but how a piece of work ended. */
  const logState = (e) => `<span class="wgsim-log-to" data-status="${esc(e.to)}"${e.manual ? ' data-blocked="wait"' : ''}>${esc(e.to)}</span>`;
  function logText(e) {
    if (e.kind === 'state') return `→ ${logState(e)}${e.why ? ` <span class="wgsim-log-why">${esc(e.why)}</span>` : ''}`;
    const text = esc(e.text);
    const tone = e.to && LOG_TONE[e.to];
    if (!tone) return text;
    return text.replace(`→ ${e.to}`, `→ <span class="wgsim-log-${tone}">${e.to}</span>`);
  }

  /* One line of the log: its number, its subject and its message. The legend draws its rows with this, so it cannot show a
     line the log will not. */
  /* A line is clickable, and what it does is show its subject alone: the card is the dialog's now, and the log is where
     the picture is narrowed to one member. */
  function logRowHtml(e) {
    return `<li class="wgsim-log-${e.kind}" data-subject="${esc(e.subject)}" data-act="filter-log" data-node="${esc(e.subject)}" title="show only ${esc(e.subject)} in the log"><span class="wgsim-log-n">${e.seq}</span><span class="wgsim-log-s">${esc(e.subject)}</span><span class="wgsim-log-m">${logText(e)}</span></li>`;
  }

  /* The time, on a row of its own above the lines that happened at it: everything under a divider is at that time, which is
     what the blanked column used to say and says without a column to blank. It is not a line of the log — no subject, no
     click, nothing to filter to — so it is built here rather than by `logRowHtml`, and the log never offers it as one. */
  const logTimeHtml = (time) => `<li class="wgsim-log-time">${time}</li>`;

  /* The three levels of emphasis a line can carry, loudest first. A run reads from the transitions alone, which is what the
     levels buy; each legend row is a real line, drawn by `logRowHtml`. */
  const LOG_KINDS = [
    { kind: 'state', label: 'a state was reached', text: '→ Active', to: 'Active' },
    { kind: 'note', label: 'something happened', text: 'the feeder fed 12 seeds' },
    { kind: 'running', label: 'a sweep that changed nothing', text: 'the packer made no parcel' },
  ];

  const logKindRow = (k, i) => ({ seq: i + 1, subject: 'workgraph', text: k.text, kind: k.kind, to: k.to || null });

  function logLegendHtml() {
    return legendHtml(LOG_KINDS.map((k, i) => ({
      label: k.label,
      swatch: `<ul class="wgsim-log wgsim-legend-log">${logRowHtml(logKindRow(k, i))}</ul>`,
    })));
  }

  disclose('log', {
    title: 'the event log',
    corner: 'tl',
    tab: 'workgraph',
    adr: '005',
    body: () => 'Every event of the run, oldest first. Hovering a line lights the card it is about. A click on a card shows only that card’s lines. The band under it, while there is one, is not the run’s: it says what the last press of step moved, and goes when the model runs again.',
    legend: logLegendHtml,
  });

  /* The workgraph's state line, one per state: what it is doing or waiting on inside the state. It carries no button: what
     an operator can do lives in the actions dialog. It says nothing while a list of actions runs, since the dialog shows
     that, and nothing in New, so the header is then one row tall. */
  function stateCard(sim) {
    const w = sim.wg.status;
    const own = sim.wgHooks();
    const row = (glyph, cls, text, tip) => ({ glyph, cls, text, tip: tip || text });
    const wait = res('Pending').tick;
    if (w === 'New') return row(res(null).tick, 'todo', '');
    if (w === 'Scouting') {
      const a = own.find((x) => x.name === 'ScoutingToApproving');
      return row(wait, 'pending', a ? a.text : 'the sample runs', a && a.full);
    }
    if (w === 'Approving' || w === 'ApprovingBlocked') {
      /* blocked on a failed check is a problem; blocked on a sign-off nobody has given is a wait */
      const problem = w === 'ApprovingBlocked' && blockedKind(sim, null) === 'problem';
      return row(problem ? res('Failed').tick : wait, problem ? 'failed' : 'pending', '');
    }
    if (w === 'Active') {
      /* draining, with every external feeder off, is a condition inside Active rather than a state: the line says which */
      const a = own.find((x) => x.name === 'Active');
      const cancelling = Object.values(sim.nodes).reduce((n, m) => n + m.parcels.filter((p) => p.cancelling && !PARCEL_TERMINAL.has(p.status)).length, 0);
      /* A member only a person can release outranks every phase word: a workgraph whose quarantine nobody has decided, or whose
         paused member still has work, closes for nobody however done its feeders are, and `draining` would read as a workgraph
         that is finishing. A halt keeps its own line, having written the quarantine off and cancelled what was in flight, so
         the hold it leaves behind clears without an operator; a drain stops the outermost feeders and leaves both holds where
         they were, so the hold is still what the workgraph waits on (DX-ADR-005). */
      const held = sim.wg.ending === 'halt' ? [] : sim.heldMembers();
      if (held.length) {
        const pb = (n) => sim.counts(n).Pb;
        const why = (n) => (n.status === 'Paused' ? 'paused' : `${pb(n)} quarantined`);
        const full = (n) => (n.status === 'Paused' ? `${n.spec.label} is paused with work left, which holds the workgraph until an operator resumes it` : `${n.spec.label} has ${pb(n)} quarantined input${pb(n) === 1 ? '' : 's'}, which hold${pb(n) === 1 ? 's' : ''} the drain until an operator resets or writes them off`);
        const names = held.map((n) => n.spec.label).join(', ');
        return row(wait, 'pending', held.length === 1 ? `held by ${names} · ${why(held[0])}` : `held by ${names}`, `${held.map(full).join('; ')} (DX-ADR-005)`);
      }
      const phase = sim.wg.ending === 'halt' ? `halting${cancelling ? ` · ${cancelling} parcel${cancelling === 1 ? '' : 's'} cancelling` : ''}` : sim.feedersActive() ? 'requesting' : sim.wg.ending === 'drain' ? 'draining' : 'feeders done, draining';
      return row(wait, 'pending', a && !sim.wg.ending ? `${phase} · ${a.text}` : phase, a && !sim.wg.ending ? `${phase} · ${a.full}` : `${phase}: the members run`);
    }
    if (w === 'Completed') return row(res('Passed').tick, 'passed', `archiving in ${Math.max(0, Math.ceil(sim.spec.workgraph.archiveAfter - (sim.t - sim.wg.since)))} s`);
    if (w === 'Finalizing' || w === 'Archiving' || w === 'Cancelling') return row(wait, 'pending', '');
    return row(res('Passed').tick, 'passed', '');
  }

  /* The two backward edges of DX-ADR-005 an operator takes on the workgraph, offered in the actions panel's footer only
     once something is blocked, since an operator acts on a blocked state and nothing else: scout further from
     ApprovingBlocked, back to Active while a member is FinalizingBlocked. While the lists run by themselves the dialog
     offers nothing, so it never reads as a prompt. */
  function wgEdges(sim) {
    const w = sim.wg.status;
    if (w === 'ApprovingBlocked' && sim.spec.workgraph.scouting) return [{ act: 'extend-scout', label: 'scout further', text: 'back to Scouting with a larger sample, keeping everything the scout produced' }];
    if (w === 'Finalizing' && Object.values(sim.nodes).some((n) => n.status === 'FinalizingBlocked')) return [{ act: 'resume-active', label: 'back to Active', text: 'reopen the workgraph after a finalisation you cannot recover: every member returns to Active' }];
    return [];
  }

  /* The strip of workgraph states, the lit one now and the passed ones marked. It carries no control: what an operator can do
     to the workgraph lives on its state machine, in the dialog the band around the strip opens. */
  function statesHtml(sim) {
    const w = sim.wg.status;
    const blockedWg = w === 'ApprovingBlocked';
    const kind = blockedWg ? blockedKind(sim, null) || 'problem' : null;
    const visited = sim.wg.visited;
    /* The strip is the path this run can have taken, and grey on it means not yet: a workgraph with no scout never reaches
       these two, so drawing them promises a phase that is not coming. Read from the run's own scout, so the strip follows
       whatever decides that a workgraph scouts. The machine keeps every state, being DX-ADR-005's rather than this run's. */
    let states = sim.wg.scout ? WG_STATES : WG_STATES.filter((s) => s !== 'Scouting' && s !== 'Approving');
    if (w === 'Cancelling' || w === 'Cleaned') {
      /* the happy path up to where it was left, then the cancelling tail */
      const last = states.filter((s) => visited.includes(s) || (s === 'Approving' && visited.includes('ApprovingBlocked'))).pop();
      states = states.slice(0, states.indexOf(last) + 1).concat(['Cancelling', 'Cleaned']);
    }
    return states
      .map((s) => {
        const on = w === s || (blockedWg && s === 'Approving');
        const seen = !on && (visited.includes(s) || (s === 'Approving' && visited.includes('ApprovingBlocked')));
        const blocked = !on ? null : s === 'Cancelling' || s === 'Cleaned' || (blockedWg && kind === 'problem') ? 'problem' : blockedWg ? 'wait' : null;
        return statePill(on && blockedWg ? 'ApprovingBlocked' : s, { on, seen, blocked });
      })
      .join('<span class="wgsim-arrow">→</span>');
  }

  disclose('states', {
    title: 'the workgraph’s states',
    tab: 'workgraph',
    adr: '005',
    body: () => `The workgraph's own states, in the order a run passes through them, leaving out any this run cannot reach: a workgraph with no scout shows no <b>Scouting</b> and no <b>Approving</b>, since grey here means not yet. The strip carries no control. Every edge an operator can take, ${Object.values(WG_OPS).map((o) => `<b>${esc(o.label)}</b>`).join(', ')} and the two backward ones, is a button on that edge in the state machine, shown only while the edge is legal.`,
    legend: () => legendHtml(PILL_TONES.map((t) => ({ label: t.label, swatch: statePill('Active', t.tone) }))),
  });

  /* One pill of the state strip, in its tone. The legend draws its swatches with this same call, so a tone cannot be named
     in the legend and drawn differently on the strip. */
  function statePill(label, tone) {
    const cls = `wgsim-pill${tone.on ? ' on' : tone.seen ? ' seen' : ''}${tone.blocked === 'problem' ? ' blocked' : tone.blocked === 'wait' ? ' waiting' : ''}`;
    return `<span class="${cls}" title="${tone.on ? 'now' : tone.seen ? 'passed' : 'not reached'}">${esc(label)}</span>`;
  }

  /* Every tone a state can wear, wherever it is drawn: where it is now, where it has been, where it has not been reached, and
     the two situations a blocked state covers, which one colour each tells apart. A machine's boxes and the strip's pills are
     the same vocabulary in two shapes, so the list is one and each drawing maps it. */
  const MACHINE_TONES = [
    { label: 'now', lit: 'on' },
    { label: 'passed', lit: 'seen' },
    { label: 'not reached', lit: '' },
    { label: 'blocked: an action failed', lit: 'on', blocked: 'problem' },
    { label: 'blocked: a sign-off waits', lit: 'on', blocked: 'wait' },
  ];
  const PILL_TONES = MACHINE_TONES.map((t) => ({ label: t.label, tone: { on: t.lit === 'on', seen: t.lit === 'seen', blocked: t.blocked } }));


  /* What an operator can do to the workgraph from its current state, as DX-ADR-005 allows, drawn as buttons on the edges of
     its state machine: drain and halt close an Active workgraph through Finalizing, cancel from New and every running state.
     Each tooltip says what becomes of the work in flight, since the destination alone does not tell drain and halt apart. */
  const WG_OPS = {
    drain: { act: 'drain', label: 'drain', tip: 'drain: stops the outermost feeders and lets everything already in flight finish' },
    halt: { act: 'halt', label: 'halt', tip: 'halt: stops every feeder, packer and hook, cancels the parcels in flight, and closes each transformation as its slots empty' },
    cancel: { act: 'cancel', label: 'cancel', tip: 'cancel: the members run their cleaning lists instead of finishing; the parcels in flight are cancelled and the outputs removed', danger: true },
  };
  function wgOps(sim) {
    const w = sim.wg.status;
    const out = [];
    if (w === 'Active' && !sim.wg.ending) out.push(WG_OPS.drain, WG_OPS.halt);
    if (CANCELLABLE.has(w)) out.push(WG_OPS.cancel);
    return out;
  }

  /* The members held back for an operator (DX-ADR-005), a start button each in the toolbar beside the playback, offered only
     while the workgraph is `Active`. Each one glows like the playback does: help mode is off by default, and nothing else on
     screen says the run has stopped for a person. */
  function controlsHtml(sim) {
    const w = sim.wg.status;
    const parts = [];
    for (const node of Object.values(sim.nodes)) {
      if (node.status === 'Paused' && node.spec.hold === 'operator' && w === 'Active') parts.push(`<button type="button" class="wgsim-btn wgsim-btn-accent wgsim-glow" data-act="start-node" data-node="${esc(node.id)}">start ${esc(node.spec.label)}</button>`);
    }
    return parts.join('');
  }

  /* The workgraph's two pills, its hooks and its approving list, with the semantics of a rail row: the glyph while
     automatic and the name once by hand, the mode word on the right giving way to a run button while by hand with work. */
  function wgPillsHtml(states) {
    return `<span class="wgsim-wgrail">${RAIL_ORDER.workgraph
      .map((row) => {
        const meta = RAIL_ROWS[row];
        const st = states[row];
        const run = st.state === 'run' ? `<button type="button" class="wgsim-wgpill-run" data-act="run-row" data-node="workgraph" data-row="${row}" aria-label="run ${meta.name} once">${ICON.play}</button>` : '';
        return `<span class="wgsim-wgpill" role="button" tabindex="0" data-state="${st.state}" data-act="toggle-mode" data-node="workgraph" data-row="${row}" title="${esc(st.tip)}"><span class="wgsim-wgpill-glyph" aria-hidden="true">${meta.glyph}</span><span class="wgsim-wgpill-name">${meta.name}</span>${run || `<span class="wgsim-wgpill-mode">${st.mode}</span>`}</span>`;
      })
      .join('')}</span>`;
  }

  /* The four results the line's glyph can be showing, in the order it reaches them. `res` gives each its mark and its colour,
     from the one vocabulary of DX-ADR-004; the checks assert that `stateCard` returns no tone outside this list. */
  const LINE_RESULTS = [null, 'Pending', 'Passed', 'Failed'];

  disclose('stateline', {
    title: 'the workgraph’s current activity',
    tab: 'workgraph',
    adr: '005',
    body: () => 'What the workgraph is doing, or waiting on, inside its current state: the scout’s stage, an approving check, whether an active workgraph is requesting or draining. These are conditions within a state, not states of their own, so they are here and not on the strip. The line is empty while there is nothing to say.',
    legend: () => legendHtml(LINE_RESULTS.map((r) => ({ label: res(r).title, swatch: `<span class="wgsim-state-glyph wgsim-state-${res(r).cls}">${res(r).tick}</span>` }))),
  });

  /* The workgraph's header, two rows of bare content under the toolbar: the state strip, and the line saying what the
     workgraph is doing or waiting on, with the buttons for what an operator can do in the state. The line is suppressed
     while there is nothing to say. In expert mode the workgraph's pills sit at the right end of the line, or of the strip
     while the line is suppressed. */
  function headHtml(sim, expert) {
    const card = stateCard(sim);
    const pills = expert ? wgPillsHtml(railStates(sim, 'workgraph')) : '';
    /* the band is the one way into the workgraph's dialog; the hint shows on hover, at the right end of its last row */
    /* the hint takes its width whether or not it shows, so the line truncates before it rather than running under it */
    const hint = '<span class="wgsim-head-hint" aria-hidden="true">members, hooks and checks ›</span>';
    const line = card.text ? `<div class="wgsim-state-line"><span class="wgsim-state-glyph wgsim-state-${card.cls}">${card.glyph}</span><span class="wgsim-state-line-text" title="${esc(card.tip)}">${esc(card.text)}</span>${hint}${pills}</div>` : '';
    return `<div class="wgsim-states">${statesHtml(sim)}${line ? '' : hint + pills}</div>${line}`;
  }

  /* Every counts table in the dialogs is the same shape: the occupied states, non-terminal above a hairline and terminal below,
     each row a count, a name and what the state means. `key` indexes the counts, `name` is what the reader sees. */
  const R = (key, name, cls, what, terminal) => ({ key, name, cls, what, terminal: !!terminal });

  const MEMBER_ROWS = [
    R('New', 'New', 'u', 'not started'),
    R('Active', 'Active', 'p', 'its feeder, packer and parcels run'),
    R('Paused', 'Paused', 'u', 'held by an operator, by its own Active hook, or by the workgraph'),
    R('Finalizing', 'Finalizing', 'a', 'its finalizing list runs'),
    R('FinalizingBlocked', 'FinalizingBlocked', 'f', 'a finalizing action failed'),
    R('Archiving', 'Archiving', 'a', 'its archiving list runs'),
    R('ArchivingBlocked', 'ArchivingBlocked', 'f', 'an archiving action failed'),
    R('Cancelling', 'Cancelling', 'a', 'its cleaning list runs'),
    R('CancellingBlocked', 'CancellingBlocked', 'f', 'a cleaning action failed'),
    R('Finalized', 'Finalized', 'p', 'its finalizing list passed', true),
    R('Completed', 'Completed', 'p', 'the workgraph completed', true),
    R('Archived', 'Archived', 'np', 'its archiving list done', true),
    R('Cleaned', 'Cleaned', 'np', 'its cleaning list done', true),
  ];

  function countsTable(rows, counts, note, lead) {
    const cell = (r) => `<tr><td class="wg-mono wgsim-num wg-n-${r.cls}">${counts[r.key]}</td><td class="wg-mono" title="${esc(r.what)}">${lead ? lead(r) : ''}${esc(r.name)}${note ? note(r) : ''}</td></tr>`;
    const live = rows.filter((r) => !r.terminal && counts[r.key]).map(cell);
    const done = rows.filter((r) => r.terminal && counts[r.key]).map(cell);
    if (!live.length && !done.length) return '';
    return `<table class="wgsim-table wgsim-counts">${live.length ? `<tbody>${live.join('')}</tbody>` : ''}${done.length ? `<tbody class="wgsim-terminal">${done.join('')}</tbody>` : ''}</table>`;
  }

  /* The workgraph's dialog: its members by state on the left, its hooks and its approving list on the right, in the card dialog's form. */
  function membersHtml(sim) {
    const counts = {};
    for (const n of Object.values(sim.nodes)) counts[n.status] = (counts[n.status] || 0) + 1;
    const total = Object.values(sim.nodes).length;
    const table = countsTable(MEMBER_ROWS, counts);
    return `<section data-region="members">${heading('members', `${total} total`)}${table}</section>`;
  }

  disclose('members', {
    title: 'the members',
    tab: 'workgraph',
    adr: '005',
    body: () => 'Every transformation of this workgraph, counted by state. The ones still working are above the hairline; the ones that have finished are below. A click on a member’s card opens its own dialog, with its feeder, packer, hooks and action lists.',
  });

  /* A hooks section: what each hook last decided, not that it exists. */
  function hooksSection(rows, empty) {
    if (!rows.length) return `<section data-region="hooks">${heading('hooks')}<p class="wgsim-dim">${esc(empty)}</p></section>`;
    return `<section data-region="hooks">${heading('hooks')}<ul class="wgsim-plain wgsim-hooks">${rows
      .map((r) => `<li class="${r.fired ? 'fired' : 'idle'}"><span class="wgsim-dot${r.fired ? ' on' : ''}"></span><span class="wgsim-hook-name" title="${esc(r.tip)}">${esc(r.name)}</span><span class="wgsim-hook-value${r.fired ? ' wgsim-live' : ''}" title="${esc(r.text.replace(/<\/div><div>/g, ' · ').replace(/<[^>]+>/g, ''))}">${r.text}</span></li>`)
      .join('')}</ul></section>`;
  }

  disclose('hooks', {
    title: 'the hooks',
    tab: 'transformation',
    adr: '006',
    body: () => 'What each hook last decided, and how long ago. A hook runs on a sweep of its own while the entity is in a state and returns one operation for it, or none. It is not an action: it belongs to no list, runs in no fixed order, and records no result. The dot is lit where this hook has returned something. A hook that returns decisions, the fate of failed inputs, has no row until it has decided one.',
  });

  /* When a hook last swept, and what it changed. */
  function hookSweep(sim, last) {
    return last ? esc(`${fmt(sim.t - last.t)}s ago · ${last.text}`) : 'not run yet';
  }

  function wgHooksHtml(sim) {
    const rows = sim.wgHooks().map((a) => ({ fired: !!sim.wg.hooks[a.name], name: a.name, text: hookSweep(sim, sim.wg.hooks[a.name]), tip: a.full || a.text }));
    return hooksSection(rows, 'none: no scouting and no target');
  }

  /* The action lists as tabs, one per list with its count: the list the state runs shows its progress and is selected by default,
     and the body keeps one height so switching never resizes the dialog. Results per DX-ADR-004; a blocked action carries its buttons. */
  function actionListsHtml(sim, opts) {
    const key = opts.selected !== undefined ? opts.selected : opts.relevant;
    const tabs = opts.lists.map(([k, label, items]) => {
      const done = items.filter((a) => settled(a.result)).length;
      const text = k === opts.relevant && items.length ? `${label} · ${done}/${items.length}` : items.length ? `${label} · ${items.length}` : label;
      return `<button type="button" class="wgsim-atab${k === key ? ' on' : ''}${k === opts.relevant ? ' relevant' : ''}${items.length ? '' : ' empty'}" data-act="${opts.tabAct}"${opts.node ? ` data-node="${esc(opts.node)}"` : ''} data-list="${k}">${text}</button>`;
    });
    const chosen = opts.lists.find(([k]) => k === key);
    const items = chosen ? chosen[2] : null;
    let body;
    if (!items) body = `<p class="wgsim-dim">none applicable while ${esc(opts.status)}</p>`;
    else if (!items.length) body = `<p class="wgsim-dim">none declared</p>`;
    else {
      const live = key === opts.relevant;
      body = `<ul class="wgsim-plain wgsim-alist-items">${items
        .map((a) => {
          const r = live ? resultOf(a) : null;
          const btn = live && a.result === 'Failed' ? panelButton('force', opts.node || '', 'force passed', `force ${a.name} to pass`, true) + panelButton('rerun', opts.node || '', 'reset', `reset ${a.name} and run it again`) : '';
          return `<li class="wgsim-state-${res(r).cls}"><span class="wgsim-state-glyph">${res(r).tick}</span><span class="wgsim-state-item">${esc(a.name)}</span>${live ? `<span class="wgsim-atab-status wgsim-state-${res(r).cls}">${res(r).word}${btn}</span>` : ''}</li>`;
        })
        .join('')}</ul>`;
    }
    return `<section data-region="actions">${heading('actions')}<div class="wgsim-atabs">${tabs.join('')}</div><div class="wgsim-atab-body">${body}</div></section>`;
  }

  disclose('actions', {
    title: 'the action lists',
    tab: 'transformation',
    adr: '004',
    body: () => 'An ordered list of checks a state runs before the entity can leave it. Four states have one: the workgraph’s Approving, and a transformation’s Finalizing, Archiving and Cancelling. One tab per list. The selected tab is the list the current state is running, and counts how far through it is; the others can be read at any time. A failed action carries the two ways past it: force it passed, or reset it to run again.',
  });

  function wgActionsHtml(sim, selected) {
    const w = sim.wg.status;
    return actionListsHtml(sim, {
      lists: [['approving', 'approving', sim.wgList]],
      relevant: w === 'Approving' || w === 'ApprovingBlocked' ? 'approving' : null,
      selected,
      tabAct: 'wg-tab',
      status: w,
    });
  }

  function workgraphHtml(sim, tab) {
    return `<div class="wgsim-machine-region">${wgMachineHtml(sim)}</div><div class="wgsim-panel-cols wgsim-cols-wg"><div>${membersHtml(sim)}</div><div>${wgHooksHtml(sim)}${wgActionsHtml(sim, tab)}</div></div>`;
  }

  /* Every machine is drawn on one canvas at one scale: the same box and the same type size on the workgraph's machine, a
     member's, its inputs' and its parcels', so switching tabs never resizes the dialog and never changes the scale. The
     canvas is as wide as the workgraph's spine, eight full names in a row, and every other machine is centred in it. */
  const MV = { w: 1060, h: 336 };
  const MB = { w: 118, h: 42 };

  /* One box of a machine on the spine, in its tone. Both the drawing and the legend call this. */
  function machineBoxSvg(name, x, y, w, h, lit, blocked, title) {
    const tone = blocked === 'wait' ? ' wg-m-waiting' : blocked ? ' wg-m-blocked' : '';
    return `<g class="wg-m-box${lit === 'on' ? ' wg-m-on' : lit === 'seen' ? ' wg-m-seen' : ' zero'}${tone}">${title ? `<title>${esc(title)}</title>` : ''}<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${Math.min(8, h / 2)}" class="wg-m-box-bg"/>${name ? `<text x="${x + w / 2}" y="${y + h / 2 + 4}" text-anchor="middle" class="wg-m-name">${esc(name)}</text>` : ''}</g>`;
  }

  /* The legend under a machine: the tones where the machine lights its states, the occupancy where its boxes count, and the
     operator's edge where it has one. Every swatch is drawn by the function that draws the real thing. */
  function machineLegend(opts) {
    const rows = [];
    const K = 0.62; /* the legend is a caption in 8.5px type: the swatches are the real drawing at the size that line holds */
    if (opts.tones) for (const t of MACHINE_TONES) rows.push({ label: t.label, swatch: swatch(machineBoxSvg('', 1, 1, 34, 14, t.lit, t.blocked, ''), 36, 16, K) });
    if (opts.occupancy) rows.push({ label: 'boxes hold the occupancy \u00b7 a click lists what is in one', swatch: swatch(machineBoxSvg('', 1, 1, 34, 14, 'seen', null, ''), 36, 16, K) });
    rows.push({ label: 'edges count the transitions', swatch: swatch(`<g class="wg-m-edge"><path d="M2 8 L34 8"/></g>`, 36, 16, K) });
    if (opts.op) rows.push({ label: 'an edge an operator takes', swatch: swatch(`<g class="wg-m-edge wg-m-op"><path d="M2 8 L34 8"/></g>`, 36, 16, K) });
    return `<div class="wgsim-machine-legend">${legendHtml(rows)}</div>`;
  }

  disclose('machine', {
    title: 'the state machine',
    tab: null,
    adr: '005',
    body: () => 'The machine of whatever the dialog is about, from DX-ADR-005, with this run drawn on it. On the input and parcel machines a box counts what is in that state and a box with something in it shows it — a click puts its inputs or its parcels under the picture, and the same box again shows them all; on the transformation’s and the workgraph’s, the states reached so far are lit. An edge counts the transitions taken along it. A dashed edge is one an operator takes, and its button names the work it would move. Every machine is drawn at the same scale.',
  });

  /* An operator's button on an edge: one or more segments, each a verb, with a tooltip saying what becomes of the work in
     flight. Rendered only while the edge is legal, never disabled. `leader` is the line back to the edge, for a button the
     edge is too short to hold: it is the button's and not the edge's, since an edge an operator can still take is often one
     nothing has taken yet, and drawn in that edge's group it would fade away exactly when the button is there to be tied to. */
  const OPB = 22; /* the pill's height, in the canvas's units: what .wgsim-wgpill wears, so the machines' controls are the app's */

  function opButton(cx, cy, segs, cls, leader) {
    const widths = segs.map((sg) => pillWidth(sg.label, PILL.op));
    const total = widths.reduce((a, b) => a + b, 0);
    let x = cx - total / 2;
    const out = [`<g class="wg-m-btn${cls ? ' ' + cls : ''}">${leader ? `<path d="${leader}" class="wg-m-leader"/>` : ''}<rect x="${x.toFixed(1)}" y="${cy - OPB / 2}" width="${total.toFixed(1)}" height="${OPB}" rx="${OPB / 2}" class="wg-m-btn-bg"/>`];
    segs.forEach((sg, k) => {
      const w = widths[k];
      if (k) out.push(`<line x1="${x.toFixed(1)}" y1="${cy - OPB / 2 + 4}" x2="${x.toFixed(1)}" y2="${cy + OPB / 2 - 4}" class="wg-m-btn-div"/>`);
      out.push(`<g class="wg-m-seg" data-act="${sg.act}"${sg.node ? ` data-node="${esc(sg.node)}"` : ''}><title>${esc(sg.tip)}</title><rect x="${x.toFixed(1)}" y="${cy - OPB / 2}" width="${w.toFixed(1)}" height="${OPB}" rx="${OPB / 2}" class="wg-m-seg-hit"/><text x="${(x + w / 2).toFixed(1)}" y="${cy + 4}" text-anchor="middle" class="wg-m-btn-label">${esc(sg.label)}</text></g>`);
      x += w;
    });
    out.push('</g>');
    return out.join('');
  }

  /* Both machine drawings share their arrowheads and their transition pills. */
  function machineDefs(prefix) {
    const head = (id, cls) => `<marker id="${id}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L9 5 L1 9" fill="none" class="${cls}"/></marker>`;
    return `<defs>${head(`${prefix}-arrow`, 'wg-m-head')}${head(`${prefix}-arrow-op`, 'wg-m-head wg-m-head-op')}</defs>`;
  }

  function countPill(at, n, cls) {
    if (!n) return '';
    const w = pillWidth(n, PILL.count);
    return `<g class="wg-m-count${cls ? ' ' + cls : ''}"><rect x="${at[0] - w / 2}" y="${at[1] - 9}" width="${w}" height="13" rx="6.5" class="wg-m-count-bg"/><text x="${at[0]}" y="${at[1] + 1}" text-anchor="middle" class="wg-m-count-label">${n}</text></g>`;
  }

  /* One edge of a machine: dashed when an operator takes it, faded until it has been taken. An edge marked `runs` carries
     a button to run the component that takes it rather than being the operator's own, so it keeps that component's stroke
     and its own note, and the button's tip is where the operator's verb goes. A stub into a shared trunk has no head of
     its own. */
  function machineEdge(e, n, prefix, cls) {
    const op = !!e.by && !e.runs;
    const why = e.runs ? e.note : e.by || e.note;
    return `<g class="wg-m-edge${op ? ' wg-m-op' : ''}${n ? '' : ' zero'}"><title>${e.from} → ${e.to}${why ? `: ${why}` : ''}${n ? ` · ${n} so far` : ''}</title><path d="${e.d}"${e.noHead ? '' : ` marker-end="url(#${prefix}-arrow${op ? '-op' : ''})"`}/>${countPill(e.at, n, cls)}</g>`;
  }

  /* Which off-path states are a blocked counterpart of the state they sit beside, and which stand on their own. A blocked
     counterpart has no edges but the pair it shares with its partner, so a transition out of it to anywhere else is drawn on
     its partner's edge; `Paused` is off the path too but has its own edges, and must never be folded. Both machines read this,
     so the next counterpart added is counted and toned without touching either caller. */
  const blockedStates = (spec) => new Map([...(spec.held || []), ...(spec.below || [])].filter((h) => h.blocked).map((h) => [h.state, h.partner]));

  /* Which edge of a machine shows a given transition: its own where the machine draws one, and otherwise its partner's where it
     leaves a blocked counterpart, since such a state has no edge of its own to anywhere but its partner. Forcing a blocked
     finalizing action and reopening the workgraph is a transition out of FinalizingBlocked, and the edge the operator took is
     Finalizing's; without this it lands nowhere. Asking for the edge rather than testing the destination's name is what keeps
     a counterpart that does grow an edge of its own from being counted twice, on its edge and on its partner's. */
  function edgeCarrying(spec, from, to) {
    const own = spec.edges.find((e) => e.from === from && e.to === to);
    if (own) return own;
    const partner = blockedStates(spec).get(from);
    return (partner && spec.edges.find((e) => e.from === partner && e.to === to)) || null;
  }

  /* The count an edge carries: every transition this edge is the one that shows. */
  const foldedCount = (spec, transitions) => (from, to) => {
    let n = transitions[`${from}>${to}`] || 0;
    for (const [state, partner] of blockedStates(spec)) {
      if (partner !== from) continue;
      const carrier = edgeCarrying(spec, state, to);
      if (carrier && carrier.from === from && carrier.to === to) n += transitions[`${state}>${to}`] || 0;
    }
    return n;
  };

  /* The tone a lit box wears: a blocked counterpart is red for a failed action and amber for a sign-off that waits, and no
     other state is ever blocked. Read from the spec, so the two machines cannot come to disagree about which states those are. */
  const blockedTone = (spec, sim, node) => (name) => (blockedStates(spec).has(name) ? blockedKind(sim, node) || 'problem' : null);

  /* A machine laid out on a spine, the states a successful run passes through, left to right on one row. Above the spine
     the off-path band: the states that wait on a person or on an action, Paused and the blocked counterparts, each directly
     above the state it is a variant of, with a short pair of arrows. Below the spine the cancelled band: Cancelling and
     Cleaned under the last two spine states, so both terminal states land at the right edge, and a blocked counterpart under
     Cancelling, on the far side from the spine. The states that can cancel drop a stub each into one shared trunk, a thin
     faint dashed line on the cancelled band's own row that enters Cancelling from the left, with a junction dot where each
     joins; each stub carries its own count, so which state a cancellation came from stays readable, and the trunk carries
     none. Each band's caption sits just above the band's leftmost box, so it stays with the band whatever the spine's length.
     A skip forward runs over the top of the picture as a right-angled detour, clear of the band. */
  const SPINE = { gap: 10, held: 40, spine: 126, cancelled: 222, below: 290, over: 10 };

  function spineMachine(spec, ctx) {
    const S = SPINE;
    const bw = MB.w;
    const bh = MB.h;
    const n = spec.spine.length;
    const x0 = Math.round((MV.w - (n * bw + (n - 1) * S.gap)) / 2);
    const pos = {};
    const kind = {};
    spec.spine.forEach((name, i) => {
      pos[name] = { x: x0 + i * (bw + S.gap), y: S.spine };
      kind[name] = 'spine';
    });
    for (const h of spec.held) {
      pos[h.state] = { x: pos[h.partner].x, y: S.held };
      kind[h.state] = 'held';
    }
    pos.Cancelling = { x: pos[spec.spine[n - 2]].x, y: S.cancelled };
    pos.Cleaned = { x: pos[spec.spine[n - 1]].x, y: S.cancelled };
    kind.Cancelling = kind.Cleaned = 'cancelled';
    for (const b of spec.below) {
      pos[b.state] = { x: pos[b.partner].x, y: S.below };
      kind[b.state] = 'below';
    }
    const idx = (name) => spec.spine.indexOf(name);
    const partnerOf = (a, b) => spec.held.some((h) => (h.state === a && h.partner === b) || (h.state === b && h.partner === a)) || spec.below.some((h) => (h.state === a && h.partner === b) || (h.state === b && h.partner === a));
    const cx = (name) => pos[name].x + bw / 2;
    const leftX = x0 - 16; /* the channel left of the spine, where a held state's stub comes down */
    const trunkY = S.cancelled + bh / 2; /* the trunk runs on the cancelled band's row and enters Cancelling from the left */

    /* each edge gets its geometry from what it joins: the spine, the held band, the cancelled band, the trunk */
    const out = [`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${MV.w} ${MV.h}" class="wg-machine">${machineDefs(spec.prefix)}`];
    /* the captions follow their bands: each over the leftmost box of its band */
    const heldLeft = Math.min(...spec.held.map((h) => pos[h.state].x));
    out.push(`<text x="${heldLeft}" y="${S.held - 8}" class="wg-m-band">${esc(spec.captions.held)}</text>`);
    out.push(`<text x="${pos.Cancelling.x}" y="${S.cancelled - 8}" class="wg-m-band">${esc(spec.captions.cancelled)}</text>`);
    const junctions = [];
    let cancelled = 0;
    const buttons = [];
    for (const e of spec.edges) {
      const a = pos[e.from];
      const b = pos[e.to];
      const count = ctx.count(e.from, e.to);
      let d;
      let at;
      let btn = null;
      let leader = null;
      let noHead = false;
      const mid = bh / 2;
      if (e.to === 'Cancelling' && kind[e.from] === 'spine') {
        /* a stub into the trunk, its count on the stub, a dot where it joins */
        d = `M${cx(e.from)} ${S.spine + bh} L${cx(e.from)} ${trunkY}`;
        at = [cx(e.from) + 14, trunkY - 26];
        noHead = true;
        junctions.push(cx(e.from));
        cancelled += count;
      } else if (e.to === 'Cancelling' && kind[e.from] === 'held') {
        d = `M${a.x} ${a.y + mid} L${leftX} ${a.y + mid} L${leftX} ${trunkY}`;
        at = [leftX, S.spine - 20];
        noHead = true;
        junctions.push(leftX);
        cancelled += count;
      } else if (partnerOf(e.from, e.to)) {
        /* the short pair between a state and its variant: up on the left, down on the right. Both the count and the button
           stand on the stub they belong to, the count by the head and the button at the tail, so neither covers the other
           and neither drifts into the pair's other stub. */
        const top = Math.min(a.y, b.y) + bh;
        const bottom = Math.max(a.y, b.y);
        const goesUp = b.y < a.y;
        const x = goesUp ? a.x + bw * 0.3 : a.x + bw * 0.7;
        d = goesUp ? `M${x} ${bottom} L${x} ${top}` : `M${x} ${top} L${x} ${bottom}`;
        at = [x, goesUp ? top + 12 : bottom - 7];
        btn = [x, goesUp ? bottom - 14 : top + 14];
      } else if (kind[e.from] === 'spine' && kind[e.to] === 'spine') {
        const i = idx(e.from);
        const j = idx(e.to);
        if (j === i + 1) {
          /* the count sits just above the gap, clear of both boxes; a button above that, clear of the count. Neither fits on
             an edge ten units long, so a leader runs down the middle of the gap from the button, down to the line where
             nothing is in the way and down to the count where something is: a count already sits on that edge and says
             which it is, so the leader has only to reach it. */
          d = `M${a.x + bw} ${a.y + mid} L${b.x} ${b.y + mid}`;
          const gx = (a.x + bw + b.x) / 2;
          at = [gx, a.y - 5];
          btn = [gx, a.y - 27];
          leader = `M${gx} ${a.y - 27 + OPB / 2} L${gx} ${count ? a.y - 15 : a.y + mid}`;
        } else if (j > i) {
          /* a skip forward detours over the top of the picture, clear of the band */
          d = roundedPath([{ x: cx(e.from), y: a.y }, { x: cx(e.from), y: S.over }, { x: cx(e.to), y: S.over }, { x: cx(e.to), y: b.y }], 8);
          at = [(cx(e.from) + cx(e.to)) / 2, S.over];
        } else {
          /* a step back loops under the spine, above the trunk */
          d = `M${a.x + 14} ${a.y + bh} C${a.x + 14} ${a.y + bh + 28}, ${b.x + bw - 14} ${b.y + bh + 28}, ${b.x + bw - 14} ${b.y + bh}`;
          at = [(a.x + 14 + b.x + bw - 14) / 2, a.y + bh + 10];
          btn = [(a.x + 14 + b.x + bw - 14) / 2, a.y + bh + 30];
        }
      } else if (kind[e.from] === 'spine' && kind[e.to] === 'held') {
        /* from a spine state's top to the left end of a held state that is not its own */
        d = `M${a.x + bw * 0.73} ${a.y} C${a.x + bw * 0.73} ${a.y - 30}, ${b.x + 24} ${b.y + bh + 20}, ${b.x + 24} ${b.y + bh}`;
        at = [(a.x + bw * 0.73 + b.x + 24) / 2, a.y - 34];
      } else if (kind[e.from] === 'held' && kind[e.to] === 'spine') {
        /* from a held state's right side down to the top of a later spine state; the count in the gap under the band */
        d = `M${a.x + bw} ${a.y + 12} C${b.x - 8} ${a.y + 12}, ${b.x + 16} ${b.y - 30}, ${b.x + 16} ${b.y}`;
        at = [b.x - 14, a.y + bh + 22];
      } else if (kind[e.from] === 'cancelled' && kind[e.to] === 'cancelled') {
        d = `M${a.x + bw} ${a.y + mid} L${b.x} ${b.y + mid}`;
        at = [(a.x + bw + b.x) / 2, a.y + 7];
      } else continue;
      const op = btn && ctx.button(e.from, e.to);
      out.push(machineEdge({ from: e.from, to: e.to, by: e.by, note: e.note, d, at, noHead }, count, spec.prefix));
      if (op) buttons.push({ at: btn, op, leader });
    }
    /* the trunk: from the leftmost junction along the cancelled band's row into Cancelling's left side; faint until a cancellation has taken it */
    if (junctions.length) {
      const xc = pos.Cancelling.x;
      const x1 = Math.min(...junctions);
      out.push(`<g class="wg-m-trunk${cancelled ? '' : ' zero'}"><title>into Cancelling: ${cancelled} so far, over every branch</title><path d="M${x1} ${trunkY} L${xc} ${trunkY}" marker-end="url(#${spec.prefix}-arrow)"/>${junctions.map((x) => `<circle cx="${x}" cy="${trunkY}" r="2.4" class="wg-m-junction"/>`).join('')}</g>`);
      const op = ctx.button('*', 'Cancelling');
      if (op) buttons.push({ at: [xc - 44, trunkY], op });
    }
    for (const b of buttons) out.push(opButton(b.at[0], b.at[1], b.op.segs, b.op.cls, b.leader));
    for (const [name, q] of Object.entries(pos)) {
      const lit = ctx.lit(name);
      /* a blocked state lit is red for a failed action and amber for a sign-off that waits: the same state, two situations */
      const blocked = lit === 'on' && ctx.blocked ? ctx.blocked(name) : null;
      const title = `${name}${lit === 'on' ? `: now${blocked === 'wait' ? ', waiting for a sign-off' : blocked ? ', an action failed' : ''}` : lit === 'seen' ? ': passed' : ': not reached'}`;
      out.push(machineBoxSvg(name, q.x, q.y, bw, bh, lit, blocked, title));
    }
    out.push('</svg>');
    return `<div class="wgsim-machine">${out.join('')}${machineLegend({ tones: true, op: true })}</div>`;
  }

  /* The workgraph's machine (DX-ADR-005) on the spine. Every edge; `by` names an operator's edge, drawn dashed. */
  const WG_MACHINE = {
    prefix: 'wg-w',
    spine: WG_STATES, /* the happy path is the engine's, and the strip draws the same list */
    held: [{ state: 'ApprovingBlocked', partner: 'Approving', blocked: true }],
    below: [],
    captions: { held: 'blocked — a check failed, or a sign-off waits', cancelled: 'cancelled — the workgraph ended this early' },
    edges: [
      { from: 'New', to: 'Scouting' },
      { from: 'New', to: 'Active', note: 'no scouting configured' },
      { from: 'Scouting', to: 'Approving', note: 'the ScoutingToApproving hook accepted' },
      { from: 'Approving', to: 'Scouting', by: 'the operator scouts further' },
      { from: 'Approving', to: 'ApprovingBlocked', note: 'an approving action failed' },
      { from: 'ApprovingBlocked', to: 'Approving', by: 'the operator forces or resets the action' },
      { from: 'Approving', to: 'Active', note: 'every approving action passed' },
      { from: 'Active', to: 'Finalizing', note: 'every member drained' },
      { from: 'Finalizing', to: 'Active', by: 'the operator reopens the workgraph' },
      { from: 'Finalizing', to: 'Completed', note: 'the last member Finalized' },
      { from: 'Completed', to: 'Archiving', note: 'after the archiving delay' },
      { from: 'Archiving', to: 'Archived', note: 'every member Archived' },
      { from: 'New', to: 'Cancelling', by: 'the operator cancels' },
      { from: 'Scouting', to: 'Cancelling', by: 'the operator cancels' },
      { from: 'Approving', to: 'Cancelling', by: 'the operator cancels' },
      { from: 'Active', to: 'Cancelling', by: 'the operator cancels' },
      { from: 'Finalizing', to: 'Cancelling', by: 'the operator cancels' },
      { from: 'Cancelling', to: 'Cleaned', note: 'every member Cleaned' },
    ],
  };

  function wgMachineHtml(sim) {
    const w = sim.wg.status;
    const tr = sim.wg.transitions;
    const ops = wgOps(sim);
    const edges = wgEdges(sim);
    return spineMachine(WG_MACHINE, {
      count: foldedCount(WG_MACHINE, tr),
      lit: (name) => (w === name ? 'on' : sim.wg.visited.includes(name) ? 'seen' : ''),
      blocked: blockedTone(WG_MACHINE, sim, null),
      /* the operator's edges carry their buttons while legal: drain and halt both end in Completed, so the button is the only
         place the difference lives; cancel is danger-styled on the trunk; scout further and back to Active once something is blocked */
      button: (from, to) => {
        if (from === 'Active' && to === 'Finalizing' && ops.some((o) => o.act === 'drain')) return { segs: [WG_OPS.drain, WG_OPS.halt] };
        if (from === '*' && to === 'Cancelling' && ops.some((o) => o.act === 'cancel')) return { segs: [WG_OPS.cancel], cls: 'danger' };
        if (from === 'Approving' && to === 'Scouting' && edges.some((x) => x.act === 'extend-scout')) return { segs: [{ act: 'extend-scout', label: 'scout further', tip: 'scout further: back to Scouting with a larger sample; everything the scout produced is kept and nothing in flight is stopped' }] };
        if (from === 'Finalizing' && to === 'Active' && edges.some((x) => x.act === 'resume-active')) return { segs: [{ act: 'resume-active', label: 'back to Active', tip: 'back to Active: every member returns to Active and its finalizing results are reset; nothing in flight is stopped' }] };
        return null;
      },
    });
  }

  /* A member's machine (DX-ADR-005) on the spine: Paused and the blocked counterparts held above their states, the cancelling
     branch below. Termination is the workgraph's: a member passes through Cancelling when the workgraph cascades down, and
     cannot be sent there on its own, so those edges are not the operator's. */
  const NODE_MACHINE = {
    prefix: 'wg-n',
    spine: ['New', 'Active', 'Finalizing', 'Finalized', 'Completed', 'Archiving', 'Archived'],
    held: [{ state: 'Paused', partner: 'Active' }, { state: 'FinalizingBlocked', partner: 'Finalizing', blocked: true }, { state: 'ArchivingBlocked', partner: 'Archiving', blocked: true }],
    below: [{ state: 'CancellingBlocked', partner: 'Cancelling', blocked: true }],
    captions: { held: 'paused, or blocked on an action', cancelled: 'cancelled — the workgraph ended this early' },
    edges: [
      { from: 'New', to: 'Active', note: 'the workgraph started' },
      { from: 'New', to: 'Paused', note: 'the workgraph started, this member held back' },
      { from: 'Active', to: 'Paused', by: 'the operator, or the Active hook' },
      { from: 'Paused', to: 'Active', by: 'the operator, or the workgraph on approval' },
      { from: 'Active', to: 'Finalizing', note: 'the workgraph to Finalizing' },
      { from: 'Finalizing', to: 'Active', by: 'the operator reopens the workgraph' },
      { from: 'Paused', to: 'Finalizing', note: 'the workgraph to Finalizing' },
      { from: 'Finalizing', to: 'FinalizingBlocked', note: 'a finalizing action failed' },
      { from: 'FinalizingBlocked', to: 'Finalizing', by: 'the operator forces or resets the action' },
      { from: 'Finalizing', to: 'Finalized', note: 'every finalizing action passed' },
      { from: 'Finalized', to: 'Completed', note: 'the workgraph to Completed' },
      { from: 'Completed', to: 'Archiving', note: 'the workgraph to Archiving' },
      { from: 'Archiving', to: 'ArchivingBlocked', note: 'an archiving action failed' },
      { from: 'ArchivingBlocked', to: 'Archiving', by: 'the operator forces or resets the action' },
      { from: 'Archiving', to: 'Archived', note: 'every archiving action done, the rows cleaned, the outputs kept' },
      { from: 'New', to: 'Cancelling', note: 'the workgraph to Cancelling' },
      { from: 'Active', to: 'Cancelling', note: 'the workgraph to Cancelling' },
      { from: 'Paused', to: 'Cancelling', note: 'the workgraph to Cancelling' },
      { from: 'Finalizing', to: 'Cancelling', note: 'the workgraph to Cancelling' },
      /* a member with no finalizing actions is Finalized at once, so it is where a cancel from Finalizing usually finds it */
      { from: 'Finalized', to: 'Cancelling', note: 'the workgraph to Cancelling' },
      { from: 'Cancelling', to: 'CancellingBlocked', note: 'a cleaning action failed' },
      { from: 'CancellingBlocked', to: 'Cancelling', by: 'the operator forces or resets the action' },
      { from: 'Cancelling', to: 'Cleaned', note: 'every cleaning action done, the rows cleaned, the outputs removed' },
    ],
  };

  function nodeMachineHtml(sim, node) {
    const w = node.status;
    const tr = node.statusTransitions;
    const running = RUNNING.has(sim.wg.status);
    return spineMachine(NODE_MACHINE, {
      count: foldedCount(NODE_MACHINE, tr),
      lit: (name) => (w === name ? 'on' : node.visited.includes(name) ? 'seen' : ''),
      blocked: blockedTone(NODE_MACHINE, sim, node),
      /* pause and resume are the member's own operator edges, legal while the workgraph runs */
      button: (from, to) => {
        if (running && w === 'Active' && from === 'Active' && to === 'Paused') return { segs: [{ act: 'toggle-node', node: node.id, label: 'pause', tip: 'pause: stops feeding and packing for this transformation; parcels already running finish' }] };
        /* and resume pulses while this member is what holds the workgraph open. The machine is a spine of a dozen boxes
           with two buttons on it, and a reader who followed the pool's pulse and then the tab's came for one control. The
           stroke pulse and not `wgsim-glow`: that is a box-shadow keyframe, right for the tab, which is an HTML button,
           and nothing at all on an SVG group — `wg-glow-stroke` is the same family's SVG member and is what the pool
           itself pulses with, so the two ends of the chain are one thing. */
        if (running && w === 'Paused' && from === 'Paused' && to === 'Active') return { segs: [{ act: 'toggle-node', node: node.id, label: 'resume', tip: 'resume: feeding and packing carry on where they stopped; nothing in flight is touched' }], cls: sim.heldMembers().some((n) => n.id === node.id) ? 'wg-m-wants' : '' };
        return null;
      },
    });
  }

  /* A birth edge into a machine: an origin dot, an arrow, and the count, which a zero suppresses like every other zero. */
  function birthSvg(b, n, prefix) {
    const o = b.d.match(/M(\d+) (\d+)/);
    return `<g class="wg-m-edge wg-m-birth${n ? '' : ' zero'}"><title>${n} born ${b.to}: ${esc(b.by)}</title><circle cx="${o[1]}" cy="${o[2]}" r="3" class="wg-m-origin"/><path d="${b.d}" marker-end="url(#${prefix}-arrow)"/>${n ? `<text x="${b.at[0]}" y="${b.at[1] - 4}" text-anchor="middle" class="wg-m-born">born ${n}</text>` : ''}</g>`;
  }

  /* A box of a hand-placed machine, its name at the left and its occupancy at the right. A box with something in it is also
     the selector for the list beneath the machine, and `sel` says where that state stands in it; an empty box selects
     nothing, being already drawn `zero` and having nothing to list. */
  function occupancyBox(name, pos, n, row, sel) {
    const pick = n && sel ? ` data-act="machine-list" data-node="${esc(sel.node)}" data-state="${esc(name)}" role="button" tabindex="0"` : '';
    const on = !!pick && sel.on;
    const tip = `${name}: ${row.what}${pick ? (on ? ' \u00b7 click again to show them all' : ' \u00b7 click to show these alone') : ''}`;
    return `<g class="wg-m-box wg-m-${row.cls}${n ? '' : ' zero'}${pick ? ' wg-m-pick' : ''}${on ? ' wg-m-picked' : ''}"${pick}><title>${esc(tip)}</title><rect x="${pos.x}" y="${pos.y}" width="${MB.w}" height="${MB.h}" rx="8" class="wg-m-box-bg"/><text x="${pos.x + 10}" y="${pos.y + MB.h / 2 + 4}" class="wg-m-name">${name}</text>${n ? `<text x="${pos.x + MB.w - 10}" y="${pos.y + MB.h / 2 + 5}" text-anchor="end" class="wg-m-num">${n}</text>` : ''}</g>`;
  }

  /* The parcel machine (DX-ADR-005), hand-placed on the shared canvas at its scale: the dispatcher's states on one row,
     Unassigned to Done, and the other three terminal states above and below Done in the right-hand column. Boxes hold the
     occupancy, edges the cumulative transitions; no operator edge. The paths are in the boxes' coordinates, so the picture
     is 850 units wide and is centred on the canvas. */
  const PARCEL_MACHINE = {
    states: {
      Unassigned: { x: 20, y: 96 },
      Reserved: { x: 180, y: 96 },
      Assigned: { x: 340, y: 96 },
      Completing: { x: 500, y: 96 },
      Failed: { x: 660, y: 16 },
      Done: { x: 660, y: 96 },
      PartiallyDone: { x: 660, y: 176 },
      Cancelled: { x: 660, y: 256 },
    },
    edges: [
      { from: 'Unassigned', to: 'Reserved', d: 'M138 117 L 180 117', at: [159, 108], note: 'the dispatcher claims it' },
      { from: 'Reserved', to: 'Assigned', d: 'M298 117 L 340 117', at: [319, 108], note: 'the backend accepts it' },
      /* not accepted: a loop under the row, back into the pool */
      { from: 'Reserved', to: 'Unassigned', d: 'M223 138 C 223 172, 105 172, 105 138', at: [164, 172], note: 'not accepted: back to the dispatcher' },
      { from: 'Assigned', to: 'Completing', d: 'M458 117 L 500 117', at: [479, 108], note: 'the job finished: outputs are being registered' },
      /* asked to stop before any backend accepted it: still a Completing step, since the claim has to be given back. It runs
         in the clear band under the row rather than over the top, where it would cross the failure edge; the picture crosses
         nothing, and the channel below is empty between the not-accepted loop and the cancellation that leaves Completing. */
      { from: 'Reserved', to: 'Completing', d: 'M262 138 L 262 163 L 520 163 L 520 138', at: [391, 180], note: 'asked to stop before a backend accepted it' },
      { from: 'Assigned', to: 'Failed', d: 'M423 96 C 423 37, 469 37, 660 37', at: [541, 28], note: 'the backend reported a failure' },
      { from: 'Completing', to: 'Done', d: 'M618 117 L 660 117', at: [639, 108], note: 'outputs registered, inputs Processed' },
      { from: 'Completing', to: 'PartiallyDone', d: 'M579 138 C 579 197, 629 197, 660 197', at: [620, 188], note: 'part of each input finished; the rest went back as smaller inputs' },
      { from: 'Completing', to: 'Cancelled', d: 'M539 138 C 539 277, 599 277, 660 277', at: [519, 222], note: 'asked to stop, and the backend confirmed' },
      /* never submitted: round the bottom, so it crosses nothing on its way to the terminal column */
      { from: 'Unassigned', to: 'Cancelled', d: 'M59 138 L 59 318 L 719 318 L 719 298', at: [389, 310], note: 'never submitted: stopped where it was' },
    ],
    births: [
      { to: 'Unassigned', d: 'M79 70 L 79 96', at: [79, 64], by: 'made by the packer' },
      { to: 'Done', d: 'M828 117 L 778 117', at: [803, 110], by: 'a recovery parcel, born Done from a status report' },
    ],
    width: 850,
    prefix: 'wg-p',
    legend: { occupancy: true },
  };

  function parcelMachineHtml(sim, node, picked) {
    const c = sim.parcelCounts(node);
    const tr = node.parcelTransitions;
    return handPlacedMachine(PARCEL_MACHINE, {
      rows: PARCEL_ROWS,
      born: (to) => tr[`born>${to}`] || 0,
      count: (from, to) => tr[`${from}>${to}`] || 0,
      occupancy: (name) => c[name] || 0,
      select: (name) => ({ node: node.id, on: name === picked }),
    });
  }

  /* What a card's pool says while the state's list of actions runs, or has run: the failed action, the running one with its place in the list, or the list's outcome. Null while no list is eligible, so the pool shows what waits. */
  function eligibleAction(lists, status) {
    const key = listFor(status);
    const items = key ? lists[key] || [] : [];
    if (!items.length) return null;
    const meta = LISTS[key];
    const failed = items.find((a) => a.result === 'Failed');
    if (failed && failed.manual) return { glyph: res('Unsigned').dot, cls: 'wg-res-pending', text: `${failed.name}: not signed off`, tip: `${failed.name} waits for a sign-off: open the card to give it` };
    if (failed) return { glyph: res('Failed').dot, cls: 'wg-res-failed', text: `${failed.name} failed`, tip: `${failed.name} failed: open the card to force it or run it again` };
    const k = items.findIndex((a) => a.result === 'Running' || a.result === 'Pending');
    if (k >= 0) return { glyph: res(items[k].result).dot, cls: `wg-res-${items[k].result.toLowerCase()}`, text: `${items[k].name} · ${k + 1}/${items.length}`, tip: `${meta.label}: ${items[k].name} is ${res(items[k].result).title}, action ${k + 1} of ${items.length}` };
    const done = items.filter((a) => settled(a.result)).length;
    if (done === items.length) return { glyph: res('Passed').dot, cls: `wg-res-${meta.result.toLowerCase()}`, text: `${items.length} ${meta.label} action${items.length === 1 ? '' : 's'} ${res(meta.result).title}`, tip: `${meta.label}: every action ${res(meta.result).title}` };
    return { glyph: res(null).dot, cls: 'wg-res-none', text: `${meta.label} · ${done}/${items.length}`, tip: `${meta.label}: ${done} of ${items.length} actions done` };
  }

  /* A "+N" backlog chip centred on a point: the same shape on the slot grid and on an edge, so the two backlogs read as one kind of quantity. */
  function backlogChip(n, cx, cy, tip, extra) {
    const label = `+${n}`;
    const w = pillWidth(label, PILL.backlog);
    const h = 14;
    return `<g class="wg-backlog" ${extra || ''}><title>${esc(tip)}</title><rect x="${(cx - w / 2).toFixed(1)}" y="${cy - h / 2}" width="${w}" height="${h}" rx="7" class="wg-backlog-bg"/><text x="${cx.toFixed(1)}" y="${cy + 3.5}" text-anchor="middle" class="wg-backlog-label">${label}</text></g>`;
  }

  /* The parcel backlog, straddling the bottom-right corner of the last slot: the card's padding is all the room there is under
     the grid, and the bar has the edge itself, so the chip sits over the corner rather than below it. */
  function chipSvg(it, queued, capacity, nodeId) {
    if (!queued) return '';
    const gb = gridBox(it, capacity);
    return backlogChip(queued, gb.x + gb.w - pillWidth(`+${queued}`, PILL.backlog) / 2, gb.y + gb.h + 1, `${queued} parcels packed and waiting for a slot · click for the parcel counts`, nodeId != null ? `data-act="open-parcels" data-node="${esc(nodeId)}"` : '');
  }

  /* The input backlogs on the edges: files made upstream that the consumer's feeder has not swept yet, just above the consumer's input port. */
  /* The backlog a consumer's feeder has not swept, on its own branch rather than on the shared trunk:
     what is pending is per-consumer. Offset to the side of the path so a file travelling the same
     branch can never sit under the chip. Zero is not drawn. */
  function edgeChipsSvg(sim, L) {
    const out = [];
    for (const e of L.edges) {
      if (e.kind !== 'edge') continue;
      const node = sim.nodes[e.to];
      const producer = sim.nodes[e.from];
      if (!node || !producer) continue;
      const n = edgeBacklog(sim, node, e.from);
      if (!n) continue;
      const q = beforeEnd(e.points, 40);
      out.push(backlogChip(n, q.x + q.nx * 14, q.y + q.ny * 14, `${n} files produced by ${producer.spec.label}, not yet fed to ${node.spec.label}: they wait for its feeder's sweep`, `data-owner="${esc(node.id)}"`));
    }
    return out.join('');
  }

  disclose('edge', {
    title: 'the files waiting on an edge',
    tab: 'inputs',
    adr: '002',
    body: () => 'Files the producer has already made that this consumer’s feeder has not swept yet. The count belongs to one consumer, so where an output fans out each branch carries its own.',
    legend: () => legendHtml([{ label: 'files made, not yet fed', swatch: swatch(backlogChip(4, 18, 9, ''), 36, 18) }]),
  });

  /* A small button in a dialog or a panel: its act, its label, and whatever attributes the caller needs on it. */
  function btn(act, label, attrs, accent) {
    return `<button type="button" class="wgsim-btn small${accent ? ' wgsim-btn-accent' : ''}" data-act="${act}"${attrs || ''}>${esc(label)}</button>`;
  }

  /* The same, for the one caller that always names a transformation and always carries a tooltip. */
  /* The header band every dialog and docked panel wears: what it is about, what kind of thing that is, one thing on the right
     — a state, where a file came from — and the control at the end. The files dialog keeps its own: its count belongs in the
     title line rather than beside it, which is a different shape and not this one. */
  function panelHead(p) {
    const right = p.right ? `<span class="wgsim-panel-state${p.rightCls ? ' ' + p.rightCls : ''}">${esc(p.right)}</span>` : p.rightSlot || '';
    return `<div class="wgsim-panel-head"><b class="wgsim-panel-title">${p.title ? esc(p.title) : ''}</b><span class="wgsim-panel-sub">${p.sub ? '· ' + esc(p.sub) : ''}</span>${right}${p.control}</div>`;
  }

  /* The round control at a band's right end: closes what it belongs to, or whatever else the panel needs there. */
  /* One element from its own markup, so a surface the widget keeps across frames is still written as the markup beside it. */
  function element(html) {
    const t = document.createElement('template');
    t.innerHTML = html;
    return t.content.firstElementChild;
  }

  const roundBtn = (act, glyph, label, tip, cls, extra) =>
    `<button type="button" class="wgsim-tb wgsim-round ${cls}" data-act="${act}" aria-label="${esc(label)}" title="${esc(tip)}"${extra || ''}>${glyph}</button>`;
  const closeBtn = (act) => roundBtn(act, '×', 'close', 'close', 'wgsim-panel-close');

  /* The ? that runs help mode. There is one on every surface it can mark, since a dialog covers the toolbar and the reader
     cannot reach back through it: the toolbar's marks the picture, the dialog's marks the dialog. Both are the same control
     and the same act, so the one in front of the reader is the one that works. */
  const helpChip = (what) =>
    `<button type="button" role="switch" aria-checked="false" class="wgsim-tb wgsim-round wgsim-chip-help" data-act="chip" data-chip="help" aria-label="help" title="help (?): mark every part of ${what}; click a mark and it explains itself">?</button>`;

  function panelButton(act, nodeId, label, tip, accent) {
    return btn(act, label, ` data-node="${esc(nodeId)}" title="${esc(tip)}"`, accent);
  }

  /* The inputs section of the card's dialog: a stacked bar, the drain guard, and the counts by state, non-terminal above a hairline and terminal below, with the operator's three edges as buttons on the rows they act on. */
  const INPUT_ROWS = [
    R('U', 'Unassigned', 'u', 'in the pool, waiting for the packer'),
    R('A', 'Assigned', 'a', 'claimed by a parcel that runs or waits to run'),
    R('F', 'Failed', 'f', 'its parcel failed; HandleFailedInput decides retry, split or Problematic'),
    R('P', 'Processed', 'p', 'processed by exactly one parcel', true),
    R('S', 'Split', 's', 'replaced by smaller inputs that carry on in its place', true),
    R('Pb', 'Problematic', 'f', 'set aside for an operator, who resets it or writes it off; it holds the drain until then'),
    R('NP', 'NotProcessed', 'np', 'written off by an operator or a hook', true),
  ];
  const inputRow = (key) => INPUT_ROWS.find((r) => r.key === key);

  function heading(text, aside) {
    return `<h4><span>${text}</span>${aside ? `<span class="wgsim-aside">${aside}</span>` : ''}</h4>`;
  }

  /* A right-column section: a header row with the name, a status and the section's button, then a label/value grid. */
  function block(name, status, buttons, rows) {
    const right = (status || '') + (buttons || []).join('');
    return `<section data-region="${name}"><h4><span>${name}</span>${right ? `<span class="wgsim-h4-right">${right}</span>` : ''}</h4><dl class="wgsim-kv2">${rows.filter(Boolean).map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join('')}</dl></section>`;
  }

  function inputsHtml(sim, node) {
    const c = sim.counts(node);
    const p = feederProgress(sim, node);
    const filled = INPUT_ROWS.reduce((n, r) => n + (c[r.key] || 0), 0);
    const denom = Math.max(p.edge ? 0 : p.total || 0, filled);
    const bar = denom ? `<div class="wgsim-hbar">${INPUT_ROWS.filter((r) => c[r.key]).map((r) => `<span class="wg-bar-${r.cls}" style="flex-basis:${((100 * c[r.key]) / denom).toFixed(2)}%" title="${c[r.key]} ${r.name}"></span>`).join('')}</div>` : '';
    const note = (r) => (r.key === 'U' && c.delayed ? ` <span class="wgsim-dim">(${c.delayed} delayed)</span>` : '');
    const table = countsTable(INPUT_ROWS, c, note) || '<p class="wgsim-dim">none yet</p>';
    return `<section data-region="inputs">${heading('inputs', filled ? `${filled} total` : '')}${bar}${table}</section>`;
  }

  disclose('inputs', {
    title: 'the inputs',
    tab: 'inputs',
    adr: '004',
    body: () => 'Every input this transformation has, counted by state. The bar is the same stack the card’s bottom edge draws. States still to settle come first and the settled ones after a space. What has not settled is what keeps this transformation open, and the feeder’s first row counts it, beside the feeder’s own state: a member is drained when the feeder is off and nothing is non-terminal.',
  });

  const PARCEL_ROWS = [
    R('Unassigned', 'Unassigned', 'u', 'made by the packer, not yet claimed by the dispatcher'),
    R('Reserved', 'Reserved', 'u', 'claimed by the dispatcher; the backend has not acknowledged it yet'),
    R('Assigned', 'Assigned', 'a', 'running on the backend'),
    R('Completing', 'Completing', 'a', 'finished, or asked to stop; its outputs are being registered and its outcome worked out'),
    R('Done', 'Done', 'p', 'outputs registered, inputs Processed', true),
    R('PartiallyDone', 'PartiallyDone', 's', 'part of each input finished; the rest went back as smaller inputs', true),
    R('Failed', 'Failed', 'f', 'the backend reported a failure; its inputs went to HandleFailedInput', true),
    R('Cancelled', 'Cancelled', 'np', 'stopped by the cleaning list', true),
  ];
  /* every parcel state has a row, which the checks assert both ways, so there is nothing to fall back to */
  const parcelRow = (name) => PARCEL_ROWS.find((r) => r.name === name);

  /* What a parcel's box says, one dimension per channel: the border's colour is the parcel's state, in the counts' colours; a
     dashed border is a parcel waiting on the backend's answer, Reserved before it is accepted and Completing before its outcome
     is known; the fill is the job's progress, full at Completing; a halo behind the box is a parcel that ended a moment ago,
     fading; the shapes inside are its inputs, and an outlined shape a partner the packer added. A swatch of each state's box
     leads its row in the parcels table, so the table is the legend for whatever the card shows. */
  const SLOT_CLS = { Unassigned: 'unassigned', Reserved: 'reserved', Assigned: 'assigned', Completing: 'completing', Done: 'done', PartiallyDone: 'partiallydone', Failed: 'failed', Cancelled: 'cancelled' };
  /* The halo a parcel that has just ended wears, one per outcome, fading over FADE.glow. */
  const SLOT_GLOW = { Done: ['wg-glow-done', 0.5], Failed: ['wg-glow-failed', 0.55], PartiallyDone: ['wg-glow-partial', 0.55] };
  const SLOT_CHANNELS = 'dashed = waiting on the backend · fill = how far the work has got · halo = ended a moment ago · shapes = its inputs, outlined = a partner';

  /* One parcel's box, wherever it is drawn: the border in its state's style and the fill to the level the job has reached. The
     slot grid, the pool and every legend swatch call this, which is what stops a swatch describing a box it does not draw. */
  function slotBoxSvg(x, y, w, h, r, status, level, clip) {
    const box = `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}" class="wg-slot wg-slot-${SLOT_CLS[status] || String(status).toLowerCase()}"/>`;
    if (!level) return box;
    const fh = h * level;
    return `${box}<clipPath id="${clip}"><rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}"/></clipPath><rect x="${x}" y="${(y + h - fh).toFixed(1)}" width="${w}" height="${fh.toFixed(1)}" clip-path="url(#${clip})" class="wg-slot-fill${status === 'Completing' ? ' wg-slot-fill-completing' : ''}"/>`;
  }

  /* How far a parcel's box has filled: it rises with the job and stands full from Completing on. */
  const slotLevel = (p, t) => (p.status === 'Assigned' ? Math.min(1, (t - p.since) / p.dur) : p.status === 'Completing' ? 1 : 0);

  /* The halo behind a box that ended a moment ago, at the radius the grid draws it. */
  function slotGlowSvg(status, cx, cy, r, fade) {
    const g = SLOT_GLOW[status];
    return g ? `<circle cx="${cx}" cy="${cy}" r="${r.toFixed(1)}" class="${g[0]}" opacity="${(g[1] * fade).toFixed(2)}"/>` : '';
  }

  /* One state's box, as the grid draws it: the border, the fill it would carry in that state, and the halo where the state
     ends a parcel. Nothing about the swatch is written out; it is the same three calls the grid makes. */
  function slotSwatch(status) {
    const level = slotLevel({ status, since: 0, dur: 2 }, 1);
    return swatch(`${slotGlowSvg(status, 8, 8, 7, 1)}${slotBoxSvg(2.5, 2.5, 11, 11, 3, status, level, `wg-sw-${status}`)}`, 16, 16);
  }

  /* The slot grid's legend: every parcel state, in the order the dispatcher takes them, and the channels a box carries besides
     its state. Driven by the engine's enum, so a state added there shows up here or fails the checks. */
  function slotLegendHtml() {
    return legendHtml(PARCEL_STATES.map((st) => ({ label: st, tip: parcelRow(st).what, swatch: slotSwatch(st) })));
  }

  disclose('slots', {
    title: 'the parcels on the backend',
    tab: 'parcels',
    adr: '005',
    body: () => `One box per parcel the backend runs at once, so the grid reads as running against free. A compute parcel is submitted as a job, and a pilot picks it up and runs it on a worker node. A data parcel is a request in the RMS, a copy or a delete. The border colour is the parcel’s state. The other channels: ${SLOT_CHANNELS}.`,
    legend: slotLegendHtml,
  });

  function parcelsHtml(pc) {
    const table = countsTable(PARCEL_ROWS, pc, null, (r) => slotSwatch(r.name));
    return table ? `${table}<p class="wgsim-dim wgsim-slot-channels">${SLOT_CHANNELS}</p>` : '<p class="wgsim-dim">none yet</p>';
  }

  disclose('parcels', {
    title: 'the parcels',
    tab: 'parcels',
    adr: '004',
    body: () => 'Every parcel this transformation has made, counted by state. Each row starts with the box the card’s grid draws for that state, so the table is also that grid’s legend. The line under it names the other channels a box carries.',
  });

  disclose('datasets', {
    title: 'an output',
    tab: 'parcels',
    adr: '004',
    body: () => 'A sink: every file a transformation has registered on that port. It exists before it holds anything, so it shows a zero. A click lists the files, newest first. A click on a file opens its lineage: what it came from and what it became.',
  });

  /* How long ago, in model time, in the coarsest unit that says it. */
  function ago(seconds) {
    if (seconds < 1) return 'just now';
    if (seconds < 60) return `${Math.floor(seconds)}s ago`;
    return `${Math.floor(seconds / 60)}m ago`;
  }

  /* An output box's files, newest at the top: a header band with the count and the order, then a row per file, its shape in
     its colour, its id, the transformation that produced it where the rows do not all share one, and its age; the two oldest
     rows shown are dimmed so recency reads without a control. The list is capped, with the rest behind a real expander. A
     click on a row opens the file's lineage. */
  const FILES = { cap: 10, dim: 2 };

  function filesHtml(sim, out, ui) {
    ui = Object.assign({ more: false }, ui || {});
    /* a query returns its files in one moment and in an order of its own, so they are listed in it and carry no age: every
       row would say the same thing, which is how long the model has been running */
    const query = !!(out.id != null && sim.sources && sim.sources[out.id]);
    const files = query ? out.files.slice() : out.files.slice().reverse();
    const n = files.length;
    const shown = ui.more ? files : files.slice(0, FILES.cap);
    const older = n - shown.length;
    const name = (id) => (sim.nodes[id] ? sim.nodes[id].spec.label : id);
    const producers = new Set(files.map((f) => f.producer));
    const cols = producers.size > 1;
    const rows = shown.map((f, k) => {
      const dim = !query && shown.length > FILES.dim && k >= shown.length - FILES.dim;
      const what = f.merged ? `merged from ${f.merged}` : name(f.producer);
      return `<li class="wgsim-file${dim ? ' old' : ''}" data-act="lineage" data-file="${f.id}" title="${esc(fileTitle(f, sim))}"><svg viewBox="0 0 12 12" aria-hidden="true">${shapeSvg(f.shape, 6, 6, 4.5, PALETTE[f.colour])}</svg><span class="wgsim-file-tag">${f.tag}</span>${cols || f.merged ? `<span class="wgsim-file-from">${esc(what)}</span>` : ''}<span class="wgsim-file-age">${query ? '' : ago(sim.t - f.t)}</span><span class="wgsim-file-more" aria-hidden="true">›</span></li>`;
    });
    const head = `<div class="wgsim-files-head"><span><b>${esc(out.spec.label)}</b> · <b class="wgsim-files-n">${n}</b> file${n === 1 ? '' : 's'}</span><span class="wgsim-files-order">${query ? 'as the query returns them' : 'newest first'}</span>${closeBtn('close-files')}</div>`;
    const list = n ? `<ul class="wgsim-plain wgsim-files-list${ui.more ? ' all' : ''}">${rows.join('')}</ul>` : '<p class="wgsim-dim wgsim-files-empty">none yet</p>';
    const more = older ? `<button type="button" class="wgsim-files-older" data-act="files-more" title="show every file">+${older} ${query ? 'more' : 'older'}</button>` : '';
    return `${head}${list}${more}`;
  }

  /* The right column: the four extension points in the order DX-ADR-006 lists them. */
  const FEEDER_STATE = { running: 'on', 'waiting on the hook': 'wait', disabled: 'off', off: 'off', 'not running': 'off' };

  /* The drain guard: what has not settled, and so keeps the member open. A member is drained when its feeder is disabled
     and none of its inputs or parcels is non-terminal (DX-ADR-005), and the feeder block's heading already carries the
     first half at its right end — so the count as that block's first row puts both halves in one place. Amber, because
     amber is a wait everywhere else in the widget and that is what this is. */
  function drainGuard(sim, node) {
    const c = sim.counts(node);
    const open = c.U + c.A + c.F + c.Pb;
    if (!open) return null;
    /* a Paused member with a pool still holds the workgraph open, and the operator's way out is on the state machine;
       so does a quarantine, which only an operator can move (DX-ADR-005) */
    const held = node.status === 'Paused' ? ' · this member is Paused, so nothing will move them' : c.Pb ? ` · ${c.Pb} Problematic wait for an operator` : '';
    /* outside the inputs section the unit is no longer implied, so the row says it */
    return ['open', `<span class="wgsim-guard">${open} input${open === 1 ? '' : 's'} · keeping this open${held}</span>`];
  }

  function feederHtml(sim, node) {
    const f = node.spec.feeder;
    const info = sim.feederInfo(node);
    const id = node.id;
    const p = feederProgress(sim, node);
    const kind = node.spec.kind === 'compute' ? '' : `${node.spec.kind} · `;
    const rows = [drainGuard(sim, node), ['kind', esc(kind + info.kind)]];
    if (f.seeds) rows.push(['limit', `${node.seedRequested} of at most ${f.seeds}${f.batch ? `, ${f.batch} at a time` : ''}`], ['seeds', `<span class="wgsim-live">${p.done} of ${p.total}</span>`]);
    else if (p.edge) rows.push(['source', esc(f.from.join(', '))], ['fed', `<span class="wgsim-live">${p.done}${node.unfed.length ? ` · ${node.unfed.length} waiting` : ''}</span>`]);
    else rows.push(['source', esc(f.from.join(', '))], ['fed', `<span class="wgsim-live">${p.done} of ${p.total}</span>`]);
    if (f.port !== 'main') rows.push(['port', esc(f.port)]);
    if (f.type) rows.push(['type', `${esc(f.type)} files only`]);
    if (f.after.length) rows.push(['after', `${esc(f.after.join(', '))} has processed the file`]);
    /* no disable here: setting the feeder's rail row by hand is the one way to stop it, and drain and halt are the workgraph's */
    const btns = [];
    if (info.extendable) btns.push(panelButton('extend', id, `raise by ${info.step}`, 'as the Active hook would'));
    return block('feeder', `<span class="wgsim-st-${FEEDER_STATE[info.state] || 'off'}">${esc(info.state)}</span>`, btns, rows);
  }

  disclose('feeder', {
    title: 'the feeder',
    tab: 'transformation',
    adr: '006',
    body: () => 'What makes this transformation’s inputs, and the first of the four extension points of DX-ADR-006. A seed feeder counts what it has made against its limit. A feeder fed by another transformation names its source and counts what it has swept from it. The word at the head is what the feeder is doing now. The workgraph’s Active hook can change that, and where the hook could raise the limit, the same raise is offered as a button.',
  });

  /* Where the packer stands: what the pool holds, what of it a run could take now, and what it is holding back. A ready
     input is one nothing delays; the packer takes full groups only while the feeder is active, by identity where it keeps
     one together, so what is left over is what only a flush would move. The machine's button, the packer block and the
     dialog all read this, so the count on the button is the count the dialog explains. */
  function packerStand(sim, node) {
    const pk = node.spec.packer;
    const pool = [...node.inputs.values()].filter((i) => i.status === 'Unassigned');
    const ready = pool.filter((i) => sim.ready(node, i));
    const groups = {};
    for (const i of ready) {
      const key = pk.by === 'colour' ? String(i.file.colour) : '';
      groups[key] = (groups[key] || 0) + 1;
    }
    const full = Object.values(groups).reduce((n, g) => n + Math.floor(g / pk.size) * pk.size, 0);
    const held = ready.length - full;
    return { pool: pool.length, ready: ready.length, delayed: pool.length - ready.length, held, size: pk.size, by: pk.by, waiting: held > 0 && node.status === 'Active' && sim.feederActive(node) };
  }

  /* Why a pool is standing still and what a flush would do about it. Two entrances reach it — a manual packer sweep that
     made nothing, and the flush on the input machine's edge — so the text is here rather than in either of them, and only
     the opening sentence knows which one the reader came through. */
  function flushAskHtml(sim, node, swept) {
    const st = packerStand(sim, node);
    const pk = node.spec.packer;
    const by = st.by ? ` grouped by ${st.by}` : '';
    const parts = [
      `<p>The packer of <b>${esc(node.spec.label)}</b> makes parcels of ${st.size} input${st.size === 1 ? '' : 's'}${by}. The pool holds ${st.pool} Unassigned input${st.pool === 1 ? '' : 's'}, ${st.ready} ready and ${st.delayed} delayed${swept ? ', and the sweep made no parcel' : ''}.</p>`,
    ];
    if (st.delayed) {
      const why = [];
      if (pk.join || pk.lookup) why.push(`its partner from <b>${esc(pk.join || pk.lookup)}</b> does not exist yet`);
      if (node.spec.feeder.after.length) why.push(`<b>${node.spec.feeder.after.map(esc).join('</b>, <b>')}</b> has not processed the file yet`);
      parts.push(`<p>A delayed input stays Unassigned while ${why.join(' and ') || 'its condition does not hold'} (DelayedUntil, DX-ADR-004). No packer run can take it before then.</p>`);
    }
    if (st.held) {
      parts.push(`<p>While the feeder is active the packer waits for full groups${by ? ' within each group' : ''}, so parcels stay the size the transformation asked for. Once the feeder is disabled or exhausted it packs whatever is left. A <b>flush</b> is an on-demand packer run with <code>flush=True</code> (DX-ADR-005): it packs the ${st.held} input${st.held === 1 ? '' : 's'} it is holding short of a group now, into smaller parcels.</p>`);
    } else if (st.ready) {
      parts.push(`<p>A full group is ready, so the packer takes it on its own next sweep and a flush would make the same parcels.</p>`);
    } else {
      parts.push(`<p>Nothing is ready, so a flush would make no parcel either.</p>`);
    }
    return { html: parts.join(''), flushable: st.ready > 0 };
  }

  function packerHtml(sim, node) {
    const pk = node.spec.packer;
    const rows = [['group size', `${pk.size} input${pk.size === 1 ? '' : 's'} per parcel`]];
    if (pk.by) rows.push(['grouped by', esc(pk.by)]);
    if (pk.join) rows.push(['join', `the partner ${esc(pk.join)} made from the same file`]);
    if (pk.lookup) rows.push(['lookup', `each input's ancestor in ${esc(pk.lookup)}`]);
    /* Where it stands, which is the half of a stalled pool the rules do not answer. The flush that moves the held inputs
       is on the input machine's Unassigned → Assigned edge, where the operator's other edges are; the rail's ▶ stays the
       way to run a sweep by hand, in expert mode. */
    const st = packerStand(sim, node);
    if (st.pool) rows.push(['now', st.held ? `${st.ready} ready, ${st.held} short of a group` : `${st.ready} ready, ${st.delayed} delayed`]);
    return block('packer', '', [], rows);
  }

  disclose('packer', {
    title: 'the packer',
    tab: 'parcels',
    adr: '006',
    body: () => 'What groups ready inputs into parcels, and the rules it groups by: how many inputs go into one parcel, what they must share to travel together, and, where the workgraph joins two outputs, the partner or ancestor it looks up for each input.',
  });

  /* What the hooks decided, not that they exist. DX-ADR-006 gives a transformation an Active hook of its own, alongside the
     workgraph's, so the two are listed apart: this one can pause its transformation, the workgraph's changes its feeder. */
  function hooksHtml(sim, node) {
    const d = node.decisions;
    const limit = node.spec.pauseAbove;
    const failedInput = [d.retried ? `<div>${d.retried} → Unassigned</div>` : '', d.subdivided ? `<div>${d.subdivided} → Split</div>` : '', d.quarantined ? `<div><span class="wg-n-f">${d.quarantined}</span> → Problematic</div>` : ''].join('');
    const rows = [
      {
        fired: !!node.lastActive,
        name: 'Active',
        text: hookSweep(sim, node.lastActive),
        tip: `the transformation's own Active hook: runs each sweep while it is Active and returns one operation for it, or none${limit != null ? `. Here it pauses the transformation once more than ${Math.round(limit * 100)}% of its inputs have failed` : ''}`,
      },
      { fired: !!node.lastWgActive, name: 'Active (workgraph)', text: hookSweep(sim, node.lastWgActive), tip: "the workgraph's Active hook, as it applies to this transformation: it changes feeder arguments, disables the feeder, or pauses the member" },
      { fired: !!failedInput, name: 'HandleFailedInput', text: failedInput, tip: `decides each failed input: back to the pool up to ${node.spec.retries} times${node.spec.subdivide ? ', replaced by smaller inputs' : ''}, or Problematic` },
      { fired: d.split > 0, name: 'status report', text: d.split ? `${d.split} → Split` : '', tip: 'a job that finished part of an input reports which portions; those are recorded as Processed and the rest go back to the pool as smaller inputs' },
    ];
    /* a hook that only ever decides, and has decided nothing, has no row: an empty outcome is not information */
    return hooksSection(rows.filter((r) => r.text), 'none');
  }

  function actionsHtml(sim, node, selected) {
    return actionListsHtml(sim, {
      lists: Object.keys(LISTS).map((k) => [k, LISTS[k].label, node.lists[k] || []]),
      relevant: listFor(node.status),
      selected,
      tabAct: 'actions-tab',
      node: node.id,
      status: node.status,
    });
  }

  /* The card's dialog: a tab strip over four panes, and one of them has the body to itself. `status` is the live state on the
     left and the extension points on the right, the two columns; the other three are the machines, the transformation's, its
     inputs' and its parcels', each on the one canvas at the one scale, so switching between them changes nothing but which
     machine is drawn. A pane with the body to itself is what lets a machine be drawn at a size worth reading and the columns
     be set at the size of the rest of the dialog, which a machine and the columns sharing the height could not both be.
     `status` opens first, and its two counts are the way in to the machines that count the same things: a click on either
     section is that machine's tab. `ui` holds which pane is showing and the actions tab. */
  const MACHINE_TABS = ['status', 'transformation', 'inputs', 'parcels'];
  /* Which box a reader picked, filed per member and per machine: `Failed` is a state of both machines and `Unassigned` of
     both, so one key for the pair would carry a choice across the tab strip into a list about something else. */
  const listKey = (id, tab) => `${id}:${tab || 'status'}`;

  function detailsHtml(sim, node, ui) {
    ui = Object.assign({ machine: 'status', actions: undefined, list: undefined }, ui || {});
    const pc = sim.parcelCounts(node);
    const parcels = Object.values(pc).reduce((a, b) => a + b, 0);
    const c = sim.counts(node);
    const counts = { status: 0, transformation: 0, inputs: node.inputs.size, parcels };
    /* A tab glows for what its own machine answers, and for nothing else: the inputs tab for a quarantine, whose reset and
       write-off are edges of the input machine, and the transformation tab for a member paused with work left, whose
       resume is an edge of that one. A member waiting on an operator over its quarantine is held too, and its tab is the
       inputs tab — the transformation machine has no control to offer it, and a glow that marks a step the reader cannot
       take is worse than none. A blocked action glows no tab at all: the actions panel docks itself over the picture with
       force passed and reset on the action that failed, which is louder than a glow and is already where that work lives. */
    const paused = node.status === 'Paused' && sim.heldMembers().some((n) => n.id === node.id);
    const mark = { inputs: c.Pb ? `${c.Pb} Problematic input${c.Pb === 1 ? '' : 's'} wait for you` : '', transformation: paused ? 'this transformation is paused with work left: resume it on this machine' : '' };
    const tabs = MACHINE_TABS.map((k) => `<button type="button" class="wgsim-mtab${k === ui.machine ? ' on' : ''}${mark[k] ? ' wgsim-glow' : ''}" data-act="machine-tab" data-node="${esc(node.id)}" data-tab="${k}"${mark[k] ? ` title="${esc(mark[k])}"` : ''}>${k}${counts[k] ? ` <span class="wgsim-mtab-n">· ${counts[k]}</span>` : ''}</button>`).join('');
    const picked = ui.list && ui.list.state;
    const machine = ui.machine === 'inputs' ? machineHtml(sim, node, picked) : ui.machine === 'parcels' ? parcelMachineHtml(sim, node, picked) : nodeMachineHtml(sim, node);
    /* the list belongs to the machine that selects it, so it is inside the region the machine's disclosure marks, and the
       region's own hairline stays under both */
    const list = ui.machine === 'inputs' || ui.machine === 'parcels' ? machineListHtml(sim, node, ui.machine, ui.list) : '';
    /* A section that counts what a machine holds is the way to that machine, since the count is the question and the
       machine is the answer: the whole section is the control, there being nothing inside it to press instead. The
       attributes go on the `<section>` rather than on a div wrapped round it, so that the panel's one rule policy —
       `section + section` draws a line between sections and nothing else does — reaches this column too. Wrapped, the
       sections were never adjacent siblings and the rule never matched here, which is how a 0.5px line came to mean one
       thing down the right column and another down the left. */
    const jump = (tab, html) => html.replace('<section ', `<section class="wgsim-jump" data-act="machine-tab" data-node="${esc(node.id)}" data-tab="${tab}" role="button" tabindex="0" title="the ${tab} machine" `);
    const body = ui.machine === 'status'
      ? `<div class="wgsim-panel-cols wgsim-cols-node"><div>${jump('inputs', inputsHtml(sim, node))}${jump('parcels', `<section data-region="parcels">${heading('parcels', parcels ? `${parcels} total` : '')}${parcelsHtml(pc)}</section>`)}</div><div>${feederHtml(sim, node)}${packerHtml(sim, node)}${hooksHtml(sim, node)}${actionsHtml(sim, node, ui.actions)}</div></div>`
      : `<div class="wgsim-machine-region">${machine}${list}</div>`;
    return `<div class="wgsim-mtabs">${tabs}</div>${body}`;
  }

  disclose('tabs', {
    title: 'a transformation’s three machines',
    tab: null,
    adr: '005',
    body: () => 'A transformation has three machines: its own, its inputs’ and its parcels’. The number on a tab is what that machine holds now, the inputs it has or the parcels it has made. The inputs tab glows while a Problematic input waits for an operator.',
  });

  /* The input state machine (DX-ADR-005), hand-placed on the shared canvas at its scale: the pool on the left, the working
     states in the middle column and the terminal states in the right one; boxes hold the occupancy, edges the cumulative
     transitions, and the operator's three edges are dashed in the accent with a button carrying the count they would move.
     The picture is 720 units wide and is centred on the canvas. */
  const MACHINE = {
    width: 720,
    prefix: 'wg-m',
    legend: { occupancy: true, op: true },
    states: {
      Unassigned: { x: 60, y: 129 },
      Assigned: { x: 300, y: 30 },
      Processed: { x: 542, y: 30 },
      Failed: { x: 300, y: 129 },
      Split: { x: 542, y: 129 },
      Problematic: { x: 300, y: 228 },
      NotProcessed: { x: 542, y: 228 },
    },
    /* from, to, path (in box-edge coordinates), label point, who takes it; op edges carry the operator's act */
    edges: [
      /* The packer's own edge, and the one operator's edge on it: a flush. Its leader runs out to the left of the curve,
         since the label already sits inside it. The verb is `flush` rather than `run packer`: an automatic packer sweeps
         twice a model second, so running one is nothing the model was not about to do anyway, where a flush packs the
         groups the packer is holding short and no sweep ever will. `runs` and not an operator's edge: the operator does
         not put an input in Assigned, the packer does, and they ask it to run now — so the edge keeps the packer's own
         solid stroke, which is who takes it on every sweep, and only the button is the operator's. */
      { from: 'Unassigned', to: 'Assigned', d: 'M178 140 C 240 140, 240 51, 300 51', at: [236, 88], note: 'the packer', runs: true, btn: [150, 62], leader: 'M150 74 L 150 96 L 216 96', by: 'the operator flushes', verb: 'flush', act: 'ask-flush' },
      { from: 'Assigned', to: 'Processed', d: 'M418 51 L 542 51', at: [480, 42], note: 'the job finished' },
      { from: 'Assigned', to: 'Failed', d: 'M359 72 L 359 129', at: [372, 104], note: 'the job failed' },
      { from: 'Assigned', to: 'Split', d: 'M418 66 C 480 66, 500 129, 542 140', at: [498, 96], note: 'the status report: part of the input finished' },
      { from: 'Failed', to: 'Unassigned', d: 'M300 150 L 178 150', at: [239, 141], note: 'HandleFailedInput retries' },
      { from: 'Failed', to: 'Split', d: 'M418 150 L 542 150', at: [480, 141], note: 'HandleFailedInput subdivides' },
      { from: 'Failed', to: 'Problematic', d: 'M359 171 L 359 228', at: [372, 212], note: 'HandleFailedInput gives up' },
      /* the quarantine and the pool are a pair in their own channel under Unassigned, each a right angle in its own lane:
         one leaves that bottom edge going down and the other arrives at it pointing up, which is the idiom the pair on
         Unassigned's right edge already uses. Two curves through the one corridor is what this replaced, and they lay along
         each other closely enough to read as a single line. */
      { from: 'Problematic', to: 'Unassigned', d: 'M300 256 L 120 256 L 120 171', at: [134, 205], btn: [215, 256], by: 'the operator resets', verb: 'reset', act: 'reset-problematic' },
      { from: 'Problematic', to: 'NotProcessed', d: 'M418 258 L 542 258', at: [460, 252], btn: [480, 276], by: 'the operator writes off', verb: 'write off', act: 'writeoff-problematic' },
      /* the write-off from the pool goes round the bottom, so it crosses nothing on its way to the terminal column; its
         button stands over that run rather than under it, where the canvas ends */
      { from: 'Unassigned', to: 'NotProcessed', d: 'M100 171 L 100 300 L 601 300 L 601 270', at: [400, 294], btn: [280, 300], by: 'the operator writes off', verb: 'write off', act: 'writeoff-unassigned' },
      { from: 'Unassigned', to: 'Problematic', d: 'M160 171 L 160 242 L 300 242', at: [174, 200], note: 'the packer rejects it: the partner will never come' },
      /* a release drops through the gap between the columns; it crosses the split, which is the one crossing the picture has */
      { from: 'Assigned', to: 'NotProcessed', d: 'M400 72 C 400 130, 486 130, 486 200 C 486 240, 490 240, 542 240', at: [486, 192], note: "the cleaning list's cancellation released it" },
    ],
    births: [
      { to: 'Unassigned', d: 'M14 140 L 60 140', at: [34, 136], by: 'fed, or born of a split' },
      { to: 'Processed', d: 'M600 6 L 600 30', at: [634, 16], by: 'born Processed of a split, in a recovery parcel' },
    ],
  };

  /* A machine placed by hand rather than on a spine: the input machine and the parcel machine. Both are the same picture in
     different coordinates — the births, then each edge with the transitions taken along it, then a box per state holding its
     occupancy — centred on the shared canvas at the shared scale. The shape is here and each spec brings its own geometry,
     the rows that name and colour its states, and whichever operator edges it has. */
  function handPlacedMachine(spec, ctx) {
    const out = [`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${MV.w} ${MV.h}" class="wg-machine">${machineDefs(spec.prefix)}<g transform="translate(${((MV.w - spec.width) / 2).toFixed(1)}, 0)">`];
    for (const b of spec.births) out.push(birthSvg(b, ctx.born(b.to), spec.prefix));
    for (const e of spec.edges) {
      /* dashed and in the accent means an operator's edge, which is who takes it; an edge that merely carries a button to
         run the component that takes it keeps that component's own stroke */
      const op = !!e.act && !e.runs;
      out.push(machineEdge(e, ctx.count(e.from, e.to), spec.prefix, op ? 'op' : ''));
      if (e.act && ctx.button) {
        const button = ctx.button(e);
        if (button) out.push(button);
      }
    }
    for (const [name, pos] of Object.entries(spec.states)) out.push(occupancyBox(name, pos, ctx.occupancy(name), ctx.rows.find((r) => r.name === name), ctx.select && ctx.select(name)));
    if (ctx.extra) out.push(ctx.extra());
    out.push('</g></svg>');
    return `<div class="wgsim-machine">${out.join('')}${machineLegend(spec.legend)}</div>`;
  }

  function machineHtml(sim, node, picked) {
    const c = sim.counts(node);
    const tr = node.transitions;
    return handPlacedMachine(MACHINE, {
      rows: INPUT_ROWS,
      born: (to) => tr[`born>${to}`] || 0,
      count: (from, to) => tr[`${from}>${to}`] || 0,
      occupancy: (name) => c[COUNT_KEY[name]],
      select: (name) => ({ node: node.id, on: name === picked }),
      /* An operator's edge names its verb and the count it would move, and is offered only while there is something to
         move. The flush is the exception on both halves: what it would move is what the packer is holding short of a
         group rather than everything the pool holds, and it is worth offering only while a sweep would not do the same
         thing a moment later — a full group is the packer's own business. */
      button: (e) => {
        if (e.act === 'ask-flush') {
          const st = packerStand(sim, node);
          return st.waiting ? opButton(e.btn[0], e.btn[1], [{ act: e.act, node: node.id, label: `${e.verb} · ${st.held}`, tip: `${e.by} ${st.held} ready input${st.held === 1 ? '' : 's'} the packer is holding short of a group of ${st.size}` }], '', e.leader) : '';
        }
        const src = c[COUNT_KEY[e.from]];
        return src ? opButton(e.btn[0], e.btn[1], [{ act: e.act, node: node.id, label: `${e.verb} · ${src}`, tip: `${e.by} ${src} ${e.from} → ${e.to}` }]) : '';
      },
      /* and while the hooks are by hand, the Failed box carries the run of HandleFailedInput over what waits */
      extra: () => {
        if (!c.F || sim.modeOf(node.id, 'failedInput') !== 'manual') return '';
        const label = `run hook · ${c.F}`;
        const pos = MACHINE.states.Failed;
        const w = pillWidth(label, PILL.op);
        return opButton(pos.x + MB.w - w / 2, pos.y + MB.h + 14, [{ act: 'run-failed', node: node.id, label, tip: `run HandleFailedInput over the ${c.F} input${c.F === 1 ? '' : 's'} in Failed` }]);
      },
    });
  }

  /* What a machine's box opens: the members of one state, listed under the picture. A member's dialog said how many inputs
     and parcels were in each state and never which, and the identities are everywhere else in the widget — the pool draws
     the waiting inputs as shapes, an output box lists its files, the failed-input step shows one input's whole record — so
     the one surface a reader goes to when a member is stuck was the one with no list at all. The machine is the selector
     because it already holds the vocabulary and the occupancy: the states, the counts and the choice are then one thing,
     and a reader who followed the inputs tab's glow arrives at the nine files rather than at a nine in a box.

     No operator's button on a row. The input machine carries reset and write-off on its edges, acting over a whole state,
     and the lineage dialog is the one place they act on a single input; a row with the same verbs would be a third place
     the same edges appear, at a third scale. The list is for seeing, the machine for acting over a state, the lineage
     dialog for acting on one file — which is the widget's rule, that an operator acts at the scale the surface is about. */

  /* The order the list walks its states in: the quarantine first, then the failures, then the machine's own order. That is
     what a reader is looking for — the inputs tab glows for the Problematic, and `pending()` walks the failed inputs for
     the same reason — and within a state the model's own order, which is the order they were made. With the whole set
     shown, this ordering is what the filtering used to do: what needs a reader is in the first line and the count at the
     end swallows the settled tail, so nothing is held back by a rule the reader cannot see. */
  const listRank = (rows, name) => (name === 'Problematic' ? 0 : name === 'Failed' ? 1 : 2 + rows.findIndex((r) => r.name === name));

  /* How far a parcel's job has got, in words, from the same level the box's fill is drawn at: the level while it runs, what
     it is doing while the backend's answer is still coming, and how long ago it ended once it has. */
  function parcelStand(sim, p) {
    if (p.status === 'Assigned') return `${Math.round(100 * slotLevel(p, sim.t))}%`;
    if (p.status === 'Completing') return 'registering its outputs';
    if (PARCEL_TERMINAL.has(p.status)) return p.ended != null ? ago(sim.t - p.ended) : '';
    return 'not running yet';
  }

  /* One input's chip: its file's shape, which is the pool's own encoding, and its tag, tinted with the state's own colour
     out of the map the counts table reads. Everything else the reader might want of it — what the state means, the sections
     it covers where it is the child of a split, its failures against the retries the hook allows — is in the title and in
     the lineage the click already opens, which is where all of it already lived. */
  function mlistInput(sim, node, i) {
    const r = inputRow(COUNT_KEY[i.status]);
    const s = node.spec;
    const tried = i.errors ? ` · ${Math.min(i.errors, s.retries)} of ${s.retries} retr${s.retries === 1 ? 'y' : 'ies'}` : '';
    const title = `${i.file.tag} · ${i.status}: ${r.what}${i.mask ? ` · sections ${i.mask}` : ''}${tried} · click for its lineage`;
    return `<li class="wgsim-mchip wg-n-${r.cls}" data-act="lineage" data-file="${i.file.id}" title="${esc(title)}"><svg class="wgsim-mlist-shape" viewBox="0 0 12 12" width="12" height="12" aria-hidden="true">${shapeSvg(i.file.shape, 6, 6, 4.5, PALETTE[i.file.colour])}</svg><span class="wgsim-file-tag">${i.file.tag}</span></li>`;
  }

  /* One parcel's chip: the swatch of its state's box, which is what the parcels table already leads each row with and which
     carries the state itself, then its tag. No click: a parcel is a surface nowhere in the widget, and the shapes that were
     the way to one of its files do not survive being packed this small — the title names them, and a file is opened from the
     picture or from the inputs tab, where a file is what the reader is pointing at. */
  function mlistParcel(sim, p) {
    const r = parcelRow(p.status);
    const tags = p.inputs.map((i) => i.file.tag).join(', ');
    const title = `parcel ${p.tag} · ${p.status}: ${r.what} · ${parcelStand(sim, p)}${tags ? ` · from ${tags}` : ''}`;
    return `<li class="wgsim-mchip wg-n-${r.cls}" title="${esc(title)}"><span class="wgsim-mlist-swatch">${slotSwatch(p.status)}</span><span class="wgsim-file-tag">${esc(p.tag)}</span></li>`;
  }

  /* The strip under the machine, on the inputs and the parcels tab. Packed rather than stacked, which is the pool's own
     rule — as many as the region holds are drawn and the rest are counted at the end — so it adds no vocabulary, help mode
     having already explained it there. A stacked row is the full width of the pane and the pane has room for about one of
     them once the machine above has taken its share, so a member with forty-three inputs read as a heading, a sliver of a
     row and a count. A wrapped strip has a height its line count predicts, and it settles into the slack the machine
     leaves rather than scrolling inside it.

     That is what makes the whole set affordable, so the strip opens on all of them. There is no set to name and none to
     explain, the boxes above are still the only filter, and `listRank` puts what needs a reader in the first line. `ui` is
     the box the reader picked and whether they have opened the count at the end. */
  /* Three lines of chips, and what a line holds at the width the dialog is drawn at: a chip is a shape, a four-character
     tag and its padding, all of which the stylesheet declares, so how many fit is arithmetic rather than a guess and
     `invariants.test.js` does that arithmetic against the stylesheet's own numbers. Wider than that — maximised — a line
     holds more and the strip is shorter than its three; narrower, it is longer and gives way by scrolling. */
  const CHIPS = { lines: 3, perLine: 14 };

  function machineListHtml(sim, node, tab, ui) {
    ui = Object.assign({ state: null, more: false }, ui || {});
    const isParcels = tab === 'parcels';
    const rows = isParcels ? PARCEL_ROWS : INPUT_ROWS;
    const picked = rows.some((r) => r.name === ui.state) ? ui.state : null;
    /* what the model still holds: a run past four hundred parcels drops the oldest terminal ones, so the heading counts the
       chips the strip has rather than the box's number, and never names one it has not got */
    const held = isParcels ? node.parcels.slice() : [...node.inputs.values()];
    const items = held.filter((x) => { const r = rows.find((y) => y.name === x.status); return !!r && (!picked || r.name === picked); });
    items.sort((a, b) => listRank(rows, a.status) - listRank(rows, b.status) || a.id - b.id);
    /* the count at the end is a chip like the others, so it takes one of the places rather than spilling on to a fourth line */
    const cap = CHIPS.lines * CHIPS.perLine;
    const shown = ui.more || items.length <= cap ? items : items.slice(0, cap - 1);
    const rest = items.length - shown.length;
    const noun = isParcels ? 'parcel' : 'input';
    /* the count is the last chip rather than a line of its own: the strip is one shape, and a row under it would be the
       stacked list again in miniature */
    const more = rest > 0 ? `<li class="wgsim-mchip wgsim-mchip-more" data-act="machine-list-more" data-node="${esc(node.id)}" title="${esc(`show the other ${rest} ${noun}${rest === 1 ? '' : 's'}`)}">+${rest}</li>` : '';
    const body = shown.length
      ? `<ul class="wgsim-plain wgsim-mlist-chips">${shown.map((x) => (isParcels ? mlistParcel(sim, x) : mlistInput(sim, node, x))).join('')}${more}</ul>`
      : `<p class="wgsim-dim wgsim-mlist-empty">${picked ? `nothing is in ${esc(picked)} now` : `no ${noun}s yet`}</p>`;
    return `<div class="wgsim-machine-list">${heading(picked || 'all', items.length ? `${items.length} ${noun}${items.length === 1 ? '' : 's'}` : '')}${body}</div>`;
  }

  /* The walk through what waits while the hooks are by hand: one failed input at a time, the first of the engine's pending
     list the reader has not put off. It shows the fact of the failure, the input's record of attempts, and the three states
     the hook can send it to, the hook's own preselected. An action to run is not a step here: the lists dialog carries it.
     `ui` is what the reader has done in the dialog: the step shown, the state picked, the attempts and the explanation unfolded. */
  function stepKey(st) {
    return `i:${st.node.id}:${st.input.id}`;
  }

  /* The three states HandleFailedInput can send an input to (DX-ADR-006), each with what it means. */
  const DECISION_ROWS = [
    { to: 'Unassigned', what: 'retry it: back to the pool' },
    { to: 'Split', what: 'replace it with smaller inputs' },
    { to: 'Problematic', what: 'set it aside for an operator' },
  ];

  function stepsHtml(sim, steps, ui) {
    ui = Object.assign({ shown: 0, to: null, what: false, more: false }, ui || {});
    const st = steps[ui.shown];
    if (!st) return '';
    const count = steps.length > 1 ? `<span class="wgsim-steps-count">${ui.shown + 1} of ${steps.length} waiting</span>` : '';
    const link = (act, label, attrs) => `<button type="button" class="wgsim-link" data-act="${act}"${attrs || ''}>${label}</button>`;
    const later = `${link('step-later', 'decide later')}${link('step-later-pause', 'decide later &amp; pause')}`;
    const head = (title) => `<div class="wgsim-steps-head"><span>${title}</span>${count}</div>`;
    const foot = (right) => `<div class="wgsim-steps-foot">${later}<span class="wgsim-grow"></span>${right}</div>`;
    if (st.kind === 'input') {
      const node = st.node;
      const i = st.input;
      const s = node.spec;
      const parcel = sim.parcelIndex.get(i.parcel);
      const mask = sim.sections(i) < s.sections ? ` ${i.lo}-${i.hi}` : '';
      const hook = i.decision || 'Unassigned';
      const splittable = sim.sections(i) >= 2;
      const to = ui.to && !(ui.to === 'Split' && !splittable) ? ui.to : hook;
      const at = ` data-node="${esc(node.id)}" data-input="${i.id}"`;
      const rows = DECISION_ROWS.map((r) => {
        const off = r.to === 'Split' && !splittable;
        const on = r.to === to && !off;
        const isHook = r.to === hook;
        return `<button type="button" role="radio" aria-checked="${on}"${off ? ' aria-disabled="true"' : ''} class="wgsim-steps-choice${on ? ' on' : ''}" data-act="step-pick" data-to="${r.to}"${isHook ? ` title="the hook's own decision: ${esc(sim.decisionWhy(node, i, hook))}"` : ''}><span class="wgsim-radio" aria-hidden="true"></span><span class="wgsim-steps-dest">→ ${r.to}</span><span class="wgsim-steps-what">${off ? 'already a single section, so it cannot be split' : r.what}</span>${isHook ? '<span class="wgsim-steps-default">default</span>' : ''}</button>`;
      }).join('');
      const strip = lineageStripSvg(sim, { file: i.file, input: i, node, ancestorDepth: 1, showAttempts: true, interactive: false, expanded: ui.more, width: LIN.fill });
      const prose = ui.what
        ? `<div class="wgsim-steps-prose"><p>HandleFailedInput runs for each input in <b>Failed</b> and returns its fate: back to the pool, replaced by smaller inputs, or set aside for an operator (DX-ADR-006). Here it retries ${s.retries} time${s.retries === 1 ? '' : 's'}${s.subdivide ? ` and subdivides with probability ${s.subdivide}` : ''}. The decision is drawn when the parcel fails and applied when the hook runs, so the outcome is the same whether the hook sweeps by itself or is run by hand. <b>apply</b> on the hook's own row is that run. <b>apply</b> on any other row is the operator deciding in its place, and the log says so.</p><p>The strip is the input's record. On the left is what produced its file. On the right is every parcel the input has been in, its parent's before its own where it is the child of a split (ParentInputID, DX-ADR-004). The failure count in the fact line is taken from that record. <b>decide later</b> leaves the input in Failed and moves on; the chip beside <b>expert</b> keeps counting it.</p></div>`
        : '';
      return `${head(`input <code>${i.file.tag}${mask}</code> failed`)}
      <div class="wgsim-steps-body">
        <div class="wgsim-steps-fact"><b>${esc(s.label)}</b> · parcel <code>${parcel ? parcel.tag : '?'}</code> failed · ${Math.min(i.errors, s.retries)} of ${s.retries} retr${s.retries === 1 ? 'y' : 'ies'} used${i.errors > s.retries ? ', none left' : ''}</div>
        <div class="wgsim-steps-strip">${strip}${link('lineage', 'full lineage ›', ` data-file="${i.file.id}"`)}</div>
        <div class="wgsim-steps-choices" role="radiogroup" aria-label="where the input goes">${rows}</div>
        <div class="wgsim-steps-what-row">${link('step-what', `what is this? ${ui.what ? '⌄' : '›'}`, ` aria-expanded="${ui.what}"`)}</div>
        ${prose}
      </div>
      ${foot(btn('step-apply', 'apply', `${at} data-to="${to}" autofocus`, true))}`;
    }
    return '';
  }

  /* Of the lists the panel is showing, the ones it goes on showing: every list running now, and every list whose actions
     all landed, which stays in view, dimmed, for a beat after the run so the results can be read. A list the run abandoned is
     neither, and goes at once. Everywhere but one transition that distinction is invisible, since a change of phase leaves an
     idle gap in which the panel empties: a cancel out of Finalizing has none, every member going straight from its finalizing
     list to its cleaning one, and the abandoned entry would sit beside the list that replaced it, pairing the member's new
     state with the old list and showing an action as running that nothing is running. A blocked list is live — `runningList`
     maps a blocked state to its list's own key — so it is never dropped for the action that blocked it. A finished list is
     kept for `FADE.linger` from the moment it finished, its own beat rather than the panel's: the row is a queue of the work
     left, and a list that waited for the last of its neighbours to end would be saying it still had something to do. */
  function keptLists(shown, running, t) {
    const live = new Set(running.map((l) => `${l.id}:${l.key}`));
    return shown.filter((e) => {
      if (live.has(`${e.id}:${e.key}`)) {
        e.done = null; /* a member sent back to Active runs its list again, and the beat starts over when it next finishes */
        return true;
      }
      if (!e.items.every((a) => settled(a.result))) return false;
      if (e.done == null) e.done = t;
      return t - e.done < FADE.linger;
    });
  }

  /* The order the run takes the lists in, which is the order the row holds them: the workgraph's list first, then the members
     upstream first, and a member's own lists in the order it runs them. That is `runningLists()`'s order, rebuilt here from
     the same two sources because a list kept past its run has dropped out of `runningLists()` and insertion order would leave
     it wherever it happened to start — and a row whose order is not the run's says nothing by draining to the left. */
  const LIST_ORDER = ['approving'].concat(Object.keys(LISTS));
  function orderLists(sim, entries) {
    const at = new Map(sim.topoOrder().map((n, i) => [n.id, i + 1]));
    const rank = (e) => (e.id === 'workgraph' ? 0 : at.has(e.id) ? at.get(e.id) : at.size + 1) * LIST_ORDER.length + Math.max(0, LIST_ORDER.indexOf(e.key));
    return entries.slice().sort((a, b) => rank(a) - rank(b));
  }

  /* One entity's column of the row: every action with its result as it lands, the running one marked, and beneath it only
     what the ticks do not say — which action is running or next, what it waits for, what failed. A finished list says nothing
     more, since a column of ticks already says it. The buttons are the ones the state allows: force passed and reset on a
     failed action, the run and the sign-off while the list is by hand. The section carries its own key, which is what lets
     the widget keep the column across frames rather than redrawing it, and so what lets the row slide as it drains. */
  function listSection(sim, e, kind) {
    const wg = e.id === 'workgraph';
    const node = wg ? null : sim.nodes[e.id];
    if (!wg && !node) return '';
    const now = wg ? sim.wg.status : node.status;
    const live = wg ? now === 'Approving' || now === 'ApprovingBlocked' : runningList(now) === e.key;
    /* A row kept past its run says where its own list got to, not where the entity has gone since: the member that finished
       finalizing reads `finalizing · Finalized` while it archives, rather than `finalizing · Archiving`, and carries no
       blocked mark, which belongs to whatever list the entity is running now. The approving list ends in no state of its
       own, so the workgraph's row says what the approval left it in. */
    const status = live || !LISTS[e.key] ? now : LISTS[e.key].done;
    const next = live ? sim.nextAction(wg ? undefined : e.id) : null;
    const byHand = sim.modeOf(e.id, 'actions') === 'manual';
    const at = wg ? '' : ` data-node="${esc(e.id)}"`;
    /* what the column is about, which for the workgraph's own list is the workgraph: `sim.spec.name` is the document's title,
       a line of prose, where a member's label is a name. The log's subject column and the mode matrix both call this entity
       `workgraph`, and the panel was the one surface calling it something else. */
    const who = wg ? 'workgraph' : node.spec.label;
    const blocked = live ? blockedKind(sim, node) : null;
    const items = e.items
      .map((a) => {
        const r = res(resultOf(a));
        return `<li class="wgsim-state-${r.cls}${next && a === next.action ? ' wgsim-steps-current' : ''}"><span class="wgsim-state-glyph">${r.tick}</span><span class="wgsim-state-item">${esc(a.name)}</span><span class="wgsim-atab-status wgsim-state-${r.cls}">${r.word}</span></li>`;
      })
      .join('');
    let why = '';
    let buttons = '';
    if (!live || !next) {
      /* the ticks say it */
    } else if (next.why === 'blocked') {
      why = next.action.manual ? `${next.action.name} waits for a sign-off: forcing it passed is the sign-off` : `${next.action.name} failed: force it passed, or reset it to run again`;
      buttons = btn('force', next.action.manual ? 'force passed (sign off)' : 'force passed', at, true) + btn('rerun', 'reset', at);
    } else if (next.why === 'waiting') why = `waiting for ${next.on.map((d) => d.spec.label).join(', ')}`;
    else if (byHand) {
      const run = ` data-node="${wg ? 'workgraph' : esc(e.id)}" data-row="actions"`;
      if (next.action.manual) {
        why = `${next.action.name} is a sign-off: nobody has given it, so running it fails; forcing it passed is the sign-off`;
        buttons = btn('run-row', `run ${next.action.name}`, run) + btn('force', 'force passed (sign off)', at, true);
      } else {
        why = `${next.action.name} is ${next.why === 'running' ? 'running: a sweep would settle it' : 'next'}, by hand`;
        buttons = btn('run-row', `run ${next.action.name}`, run, true);
      }
    } else why = next.action.manual && next.why === 'running' ? `${next.action.name}: waiting for the sign-off` : `${next.action.name} is ${next.why === 'running' ? 'running' : 'next'}`;
    const foot = why || buttons ? `<div class="wgsim-lists-why"><span>${esc(why)}</span>${buttons ? `<span class="wgsim-lists-btns">${buttons}</span>` : ''}</div>` : '';
    const label = kind ? `<span class="wgsim-dim">${e.label}</span>` : ''; /* the panel's header says it where every list is of one kind */
    return `<section class="wgsim-lists-entity${live ? ' live' : ' done'}" data-list="${esc(e.id)}:${esc(e.key)}"><h4><span>${esc(who)}</span><span class="wgsim-h4-right">${label}<span class="wgsim-lists-status" data-status="${esc(status)}"${blocked ? ` data-blocked="${blocked}"` : ''}>${esc(status)}</span></span></h4><ul class="wgsim-plain wgsim-alist-items">${items}</ul>${foot}</section>`;
  }

  /* Whether a column has to say what kind of list it holds: only where the panel holds more than one kind. With one, the
     header above the row says it once for all of them, and a column repeating it is a word taken from the entity's name. */
  const listKinds = (entries) => new Set(entries.map((e) => e.key)).size;

  /* The header has the dialogs' grammar: the name, the kind, and the control at the right end, which collapses the panel in
     place rather than closing it. The count is every list the panel holds, those past the right end of the row included, and
     it names the kind while they are all of one, which is where the columns do not. */
  function listsHead(entries, collapsed) {
    const one = listKinds(entries) === 1 ? entries[0].label : '';
    return panelHead({
      title: 'actions',
      sub: entries.length === 1 ? `${entries[0].label} list` : `${entries.length}${one ? ' ' + one : ''} lists`,
      control: roundBtn('lists-toggle', collapsed ? '+' : '–', collapsed ? 'show the lists' : 'collapse the lists', collapsed ? 'show the lists' : 'collapse: the lists run on, and the header stays', 'wgsim-lists-toggle', ` aria-expanded="${!collapsed}"`),
    });
  }

  /* Under the row, the two backward edges of DX-ADR-005, which are the workgraph's: repeated here because the blocked list is
     where the reader is looking. Nothing while nothing is blocked, so the panel never reads as a prompt. */
  function listsFootInner(sim) {
    const edges = wgEdges(sim);
    return edges.length ? `<span class="wgsim-dim">${esc(edges.map((e) => e.text).join(' · '))}</span><span class="wgsim-lists-btns">${edges.map((e) => btn(e.act, e.label)).join('')}</span>` : '';
  }

  /* `+N` at the right end: the lists the row has no room for. Nothing is lost behind the count — a member's own dialog holds
     its lists with their results, and its card says how its list ended — which is what lets the row stay one row. */
  const listsMore = (n) => `<div class="wgsim-lists-more" data-list="+" title="${n} more list${n === 1 ? '' : 's'} than the row has room for · a transformation's own dialog holds its lists">+${n}</div>`;

  /* The row: the lists in the order the run takes them, as many as there is room for, and the count of the rest. */
  function listsRow(sim, entries, fit) {
    const room = fit == null ? entries.length : Math.max(1, fit);
    const more = entries.length - room;
    /* over every list the panel holds, not only the drawn ones: the header names the kind on the same count */
    const kind = listKinds(entries) > 1;
    return entries.slice(0, room).map((e) => listSection(sim, e, kind)).join('') + (more > 0 ? listsMore(more) : '');
  }

  /* The action lists as they run, in a panel docked under the control band, above the workgraph's header, that appears by
     itself when a list starts and goes when the last one has been read: the workgraph's approving list first, then each
     member's list upstream first, left to right in one row that drains as the work is done, so what is left on the row is
     what is left to do. The panel holds nothing, so a watched list runs on, no faster than one action per `T.watch`. An entry
     outlives its own run by a beat, so a result can be read once the list is done. Collapsed, the header row stays where it
     is. The widget patches the row column by column out of these same pieces, since a column that is redrawn cannot slide;
     this composes them, which is how the checks read the panel and what keeps the two from drifting apart. */
  function listsHtml(sim, entries, opts) {
    opts = opts || {};
    const head = listsHead(entries, !!opts.collapsed);
    if (opts.collapsed) return head;
    const foot = listsFootInner(sim);
    return `${head}<div class="wgsim-lists-body">${listsRow(sim, entries, opts.fit)}</div>${foot ? `<div class="wgsim-lists-foot">${foot}</div>` : ''}`;
  }

  const PANEL = {
    details: { title: 'transformation', html: (sim, node, w) => detailsHtml(sim, node, w ? { machine: w.machineTab[node.id], actions: w.actionsTab[node.id], list: w.machineList[listKey(node.id, w.machineTab[node.id])] } : undefined) },
    /* no title: the sub is read only for a panel about a transformation, and the workgraph's is named where it is used */
    workgraph: { html: (sim, node, w) => workgraphHtml(sim, w ? w.wgTab : undefined) },
  };

  /* Where this file is an input, and what an operator may do to it there. A file becomes one row of a transformation's
     input table per input made from it (DX-ADR-004), so a file split into smaller inputs shows each of them, named by the
     sections it covers. The state is the input machine's, named and coloured from the rows the counts and that machine
     read, and the member's name opens that machine, since the picture above says what became of the file and only the
     machine says where it stands. The operator's edges are offered on this one input rather than on every input in its
     state, and only while the edge is legal, which is the rule every machine keeps. */
  function lineageInputsHtml(sim, f) {
    const rows = (f.consumedBy || [])
      .map((c) => ({ node: sim.nodes[c.node], input: sim.nodes[c.node] && sim.nodes[c.node].inputs.get(c.input) }))
      .filter((r) => r.node && r.input); /* a list with an effect of clean empties the rows, and then there is nothing to show */
    if (!rows.length) return '';
    const items = rows.map(({ node, input }) => {
      const row = INPUT_ROWS.find((r) => r.name === input.status);
      const ops = (OPERATOR_EDGES[input.status] || []).map((to) => {
        /* the verb is the one that edge wears on the machine, read from its spec so the two cannot come to name it differently;
           what becomes of the input is this button's own to say, since the machine's tooltip counts a population and this one does not */
        const edge = MACHINE.edges.find((e) => e.act && e.from === input.status && e.to === to);
        const tip = to === 'Unassigned' ? `${edge.verb} this input to Unassigned: back into ${node.spec.label}'s pool, for the packer to take again` : `${edge.verb} this input as NotProcessed: ${node.spec.label} gives up on it and stops waiting for it`;
        return `<button type="button" class="wgsim-btn small" data-act="input-decide" data-node="${esc(node.id)}" data-input="${input.id}" data-to="${esc(to)}" title="${esc(tip)}">${esc(edge.verb)}</button>`;
      }).join('');
      return `<li><button type="button" class="wgsim-link" data-act="open-machine" data-node="${esc(node.id)}" data-tab="inputs" title="${esc(`the input machine of ${node.spec.label}, where this state and its edges are`)}">${esc(node.spec.label)} ›</button><span class="wg-mono wg-n-${row.cls}" title="${esc(row.what)}">${esc(input.status)}</span><span class="wgsim-dim wg-mono">${input.mask ? `sections ${esc(input.mask)}` : ''}</span><span class="wgsim-lineage-ops">${ops}</span></li>`;
    }).join('');
    return `<div class="wgsim-lineage-inputs">${heading('as an input')}<ul class="wgsim-plain wgsim-lineage-rows">${items}</ul></div>`;
  }

  /* The lineage dialog, in the shape every entity's dialog has: a header band naming the file, its kind and where it came
     from, the picture, where the file stands as an input, and the explanation folded behind "what is this?" as the
     failed-input step folds its own. */
  function lineageHtml(sim, f, ui) {
    ui = Object.assign({ what: false }, ui || {});
    const origin = f.seed != null ? `seed ${f.seed} of ${sim.nodes[f.producer] ? sim.nodes[f.producer].spec.label : f.producer}` : f.producer ? `produced by ${sim.nodes[f.producer] ? sim.nodes[f.producer].spec.label : f.producer}` : 'from the input query';
    const inputs = lineageInputsHtml(sim, f);
    /* what the thing is, and not what it is made of: a file some transformation holds as an input is an input, and one no
       transformation has taken is a file, which is what a dataset nothing consumes stays */
    const kind = f.port === 'artifact' ? 'artifact' : inputs ? 'input' : 'file';
    const head = panelHead({ title: f.tag, sub: `${kind} lineage`, right: origin, rightCls: 'wgsim-panel-origin', control: closeBtn('close-lineage') });
    const prose = ui.what
      ? '<div class="wgsim-steps-prose wgsim-lineage-prose"><p>What the file came from on the left, the file in the ring, what it became on the right. Shapes are files, boxes are parcels with their status; a stadium is a parcel of a transformation that moves data rather than computing it, a greyed box is an attempt that failed, a dashed link a partner the packer looked up. Every edge is a row of ParcelInputs or ParcelOutputs (DX-ADR-004). Click a file to move the focus; the model holds still while a dialog is open.</p></div>'
      : '';
    return `${head}<div class="wgsim-lineage-body">${lineageStripSvg(sim, { file: f, ancestorDepth: 3, showAttempts: true, interactive: true })}</div>${inputs}<div class="wgsim-steps-what-row wgsim-lineage-what"><button type="button" class="wgsim-link" data-act="lineage-what" aria-expanded="${ui.what}">what is this? ${ui.what ? '⌄' : '›'}</button></div>${prose}`;
  }

  /* ------------------------------------------------------------------ */
  /* Renderer                                                            */
  /* ------------------------------------------------------------------ */

  function staticSvg(sim, L) {
    const spec = sim.spec;
    const out = [];
    out.push(`<defs>
      <pattern id="wg-grid" width="28" height="28" patternUnits="userSpaceOnUse"><circle cx="1.5" cy="1.5" r="1.2" class="wg-griddot"/></pattern>
      <marker id="wg-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L8 5 L1 9" fill="none" class="wg-arrowhead"/></marker>
    </defs>`);
    out.push(`<rect x="0" y="0" width="${L.width}" height="${L.height}" fill="url(#wg-grid)" class="wg-bg"/>`);
    /* Every edge is drawn twice, in sequence: a casing in the background colour, then the stroke on
       top. That is what stops an edge disappearing where another crosses it, and it only works if
       the two are adjacent, so a later edge cases over an earlier one. */
    for (const e of L.edges) {
      const cls = e.kind === 'wait' ? 'wg-edge wg-edge-wait' : e.kind === 'join' ? 'wg-edge wg-edge-join' : e.port === 'artifact' ? 'wg-edge wg-edge-artifact' : 'wg-edge';
      const marker = e.kind === 'wait' ? '' : ' marker-end="url(#wg-arrow)"';
      out.push(`<path d="${e.d}" class="wg-edge-case"/>`);
      out.push(`<path d="${e.d}" class="${cls}" data-edge="${esc(e.from)}>${esc(e.to)}"${marker}/>`);
    }
    /* a fan-out splits at a junction, marked so the split reads as deliberate and not as a crossing */
    for (const j of L.junctions) out.push(`<circle cx="${j.x.toFixed(1)}" cy="${j.y.toFixed(1)}" r="3.5" class="wg-junction"/>`);
    for (const it of Object.values(L.items)) {
      if (it.kind !== 'node') continue;
      out.push(`<path d="${retryPath(it)}" class="wg-retry" data-retry="${esc(it.id)}"/>`);
      out.push(`<text x="${it.x - 17}" y="${it.y + it.h / 2 + 4}" text-anchor="middle" class="wg-retry-label" data-retrylabel="${esc(it.id)}">retry</text>`);
    }
    /* An input query in the shape of an output box: the count on one line and the files themselves behind a click, since a
       list of the first three of two hundred says less than the number does. Its label sits above the box, where the edges
       leaving the bottom cannot run through it. */
    for (const it of Object.values(L.items)) {
      if (it.kind !== 'source') continue;
      const s = spec.sources[it.id];
      const n = s.files;
      out.push(`<g class="wg-source" data-act="open-files" data-node="${esc(it.id)}">
        <title>the files ${esc(s.label)} returns: click to list them</title>
        <text x="${it.x + it.w / 2}" y="${it.y - 9}" text-anchor="middle" class="wg-box-label">${esc(s.label)}</text>
        <rect x="${it.x}" y="${it.y}" width="${it.w}" height="${it.h}" rx="10" class="wg-box"/>
        <text x="${it.x + it.w / 2}" y="${it.y + it.h / 2 + 4.5}" text-anchor="middle" class="wg-out-count${n ? '' : ' wg-out-empty'}"><tspan class="wg-out-n">${n}</tspan> file${n === 1 ? '' : 's'} <tspan class="wg-out-more">›</tspan></text>
      </g>`);
    }
    for (const it of Object.values(L.items)) {
      if (it.kind !== 'node') continue;
      const n = spec.transformations[it.id];
      out.push(cardSvg(it, n.label, n.kind, n.doc));
    }
    for (const it of Object.values(L.items)) {
      if (it.kind !== 'output') continue;
      const o = spec.outputs[it.id];
      const files = o.show !== 'histogram';
      out.push(`<g class="wg-output${files ? ' wg-output-files' : ''}" data-owner="${esc(o.from)}"${files ? ` data-act="open-files" data-node="${esc(it.id)}"` : ''}>
        ${files ? `<title>the files ${esc(o.label)} holds: click to list them, newest first</title>` : ''}
        <rect x="${it.x}" y="${it.y}" width="${it.w}" height="${it.h}" rx="10" class="wg-box"/>
        <g data-dyn="output" data-id="${esc(it.id)}"></g>
        <text x="${it.x + it.w / 2}" y="${it.y + it.h + 20}" text-anchor="middle" class="wg-box-label">${esc(o.label)}</text>
      </g>`);
    }
    out.push(`<g data-dyn="tokens"></g>`, `<g data-dyn="edgechips"></g>`);
    /* Why the model is not advancing, over the picture and in the viewBox's own units, so it keeps its size against the
       graph in a column-width block and in a maximised window alike, with no second set of rules. The size is a fraction
       of the layout rather than a step of the type scale for that reason. Over rather than under: these graphs run
       top-down, so the centre of one is a card, and a word drawn behind comes out cut in half. A watermark instead —
       faint, on a canvas that has lost its colour anyway, so nothing under it stops being readable. One word for all
       three causes, since the reader is looking for the same thing each time; only the caption changes, and the
       stylesheet picks it from the cause on the root. Hidden from a screen reader: the transport's pause button already
       carries the state, and nobody should be told "paused" on top of the dialog they have just opened. */
    {
      const cx = (L.width / 2).toFixed(1);
      const word = Math.max(30, Math.min(72, L.width * 0.08));
      const top = L.height / 2;
      out.push('<g class="wg-hold" aria-hidden="true">');
      out.push(`<text x="${cx}" y="${top.toFixed(1)}" text-anchor="middle" class="wg-hold-word" font-size="${word.toFixed(1)}">paused</text>`);
      for (const [cause, why] of HOLD_WORDS)
        out.push(`<text x="${cx}" y="${(top + word * 0.74).toFixed(1)}" text-anchor="middle" class="wg-hold-why wg-hold-${cause}" font-size="${(word * 0.3).toFixed(1)}">${why}</text>`);
      out.push('</g>');
    }
    return out.join('\n');
  }

  /* The slot grid: one box per slot the backend runs at once, so it always reads as running against free. A running parcel's box fills like a container as its job progresses; the backlog chip sits over the last box's corner. */
  function slotsSvg(it, parcels, t, capacity, queued, nodeId) {
    const boxes = slotBoxes(it, capacity);
    const slots = [];
    if (capacity) slots.push(`<title>the backend runs ${capacity} parcels of this transformation at once</title>`);
    boxes.forEach((b, si) => {
      const p = parcels[si];
      const cx = b.x + b.w / 2;
      const cy = b.y + b.h / 2;
      if (!p) {
        slots.push(`<rect x="${b.x}" y="${b.y}" width="${b.w}" height="${b.h}" rx="${G.radius}" class="wg-slot wg-slot-empty"/>`);
        return;
      }
      const k0 = b.w / 40; /* a denser grid draws the same slot, scaled */
      const age = p.ended == null ? 0 : t - p.ended;
      const fade = Math.max(0, 1 - age / FADE.glow);
      slots.push(slotGlowSvg(p.status, cx, cy, (24 + 5 * (1 - fade)) * k0, fade));
      /* the border says the state and nothing else: a recovery parcel is Done, and the lineage says where it was born */
      slots.push(slotBoxSvg(b.x, b.y, b.w, b.h, G.radius, p.status, slotLevel(p, t), `wg-slot-${esc(it.id)}-${si}`));
      const n = p.inputs.length;
      const r = 3.6 * k0;
      if (n <= 3) {
        const gap = 9.5 * k0;
        const x0 = cx - ((n - 1) * gap) / 2;
        p.inputs.forEach((i, k) => slots.push(fileShape(i.file, x0 + k * gap, cy, r)));
      } else if (n === 4) {
        p.inputs.forEach((i, k) => slots.push(fileShape(i.file, cx - 5 * k0 + (k % 2) * 10 * k0, cy - 5 * k0 + Math.floor(k / 2) * 10 * k0, r)));
      } else {
        p.inputs.slice(0, 3).forEach((i, k) => slots.push(fileShape(i.file, cx - 9.5 * k0 + k * 9.5 * k0, cy - 4 * k0, r)));
        slots.push(`<text x="${cx}" y="${cy + 12 * k0}" text-anchor="middle" class="wg-tiny">+${n - 3}</text>`);
      }
      (p.extras || []).slice(0, 2).forEach((e, k) => slots.push(`<g data-act="lineage" data-file="${e.id}" class="wg-file"><title>${esc(fileTitle(e))}</title>${shapeSvg(e.shape, b.x + b.w - 5 * k0 - k * 8 * k0, b.y + b.h - 5 * k0, 3.4 * k0, 'var(--wg-card)', `stroke="${PALETTE[e.colour]}" stroke-width="1.5"`)}</g>`));
    });
    slots.push(chipSvg(it, queued, capacity, nodeId));
    return slots.join('');
  }

  /* How far the feeder has got: seeds or files against the limit. An edge feeder has no total, its producer is still producing, so only what it has fed. */
  function feederProgress(sim, node) {
    const f = node.spec.feeder;
    if (f.seeds) return { done: node.seedNext, total: sim.seedTarget(node) };
    const srcs = f.from.filter((x) => sim.sources[x]);
    if (srcs.length) {
      let done = 0;
      let total = 0;
      for (const s of srcs) {
        done += node.cursor[s] || 0;
        total += sim.scoutLimit(sim.sources[s]);
      }
      return { done, total };
    }
    let fed = 0;
    for (const i of node.inputs.values()) if (!i.mask) fed++;
    return { done: fed, edge: true };
  }

  /* Files a producer has made for a consumer that the consumer's feeder has not swept yet: waiting in its queue or still on the edge. */
  function edgeBacklog(sim, node, from) {
    let n = 0;
    for (const f of node.unfed) if (f.producer === from) n++;
    for (const tk of sim.tokens) if (tk.to === node.id && tk.from === from && tk.kind === 'edge') n++;
    return n;
  }

  /* The order the bar stacks the input states in, left to right: what is finished, what is in a parcel, what went wrong, what
     waits. The colour is not repeated here — it is INPUT_ROWS', the one the counts, the tables and the machine all use. */
  const BAR_ORDER = ['P', 'S', 'A', 'F', 'Pb', 'NP', 'U'];

  /* One segment of the bar. The edge stacks these and the legend draws one of each, so a colour cannot mean two things.
     `flush` is a segment of the card's edge, which is a continuous run and carries no corner of its own; a legend's swatch
     is a thing on its own and keeps its rounding. */
  function barSegmentSvg(x, y, w, h, row, n, flush) {
    return `<rect x="${x.toFixed(1)}" y="${y}" width="${w.toFixed(1)}" height="${h}" rx="${flush ? 0 : 2}" class="wg-bar-${row.cls}">${n == null ? '' : `<title>${n} ${row.name}</title>`}</rect>`;
  }

  disclose('bar', {
    title: 'the transformation’s progress',
    tab: 'inputs',
    adr: '005',
    body: () => 'The transformation’s inputs by state, stacked along the card’s bottom edge. The full width is what the feeder was asked for, so the edge fills in as work happens and the unfilled part stays the border’s own colour. An edge feeder has no total while its producer is still producing, so its inputs fill the whole edge.',
    legend: () => legendHtml(BAR_ORDER.map((k) => ({ label: inputRow(k).name, tip: inputRow(k).what, swatch: swatch(barSegmentSvg(0, 3, 22, 5, inputRow(k), null, null), 22, 11) }))),
  });

  /* The card's bottom edge: the inputs by state, stacked left to right across the whole of it, against the limit for a seed or
     query feeder and filling the edge for an edge feeder whose producer is still producing. The track under the segments is
     the border's own colour, so a card that has consumed nothing shows a plain edge and a zero needs no suppressing. */
  function barSvg(it, node, sim) {
    const p = feederProgress(sim, node);
    const c = sim.counts(node);
    const filled = BAR_ORDER.reduce((n, k) => n + (c[k] || 0), 0);
    const denom = Math.max(p.edge ? 0 : p.total || 0, filled);
    const x = it.x;
    const w = it.w;
    const y = it.y + it.h - G.bar;
    const out = [`<rect x="${x}" y="${y}" width="${w}" height="${G.bar}" class="wg-bar-track"/>`];
    let sx = x;
    if (denom) {
      for (const k of BAR_ORDER) {
        if (!c[k]) continue;
        const sw = (w * c[k]) / denom;
        out.push(barSegmentSvg(sx, y, sw, G.bar, inputRow(k), c[k], true));
        sx += sw;
      }
    }
    return out.join('');
  }

  /* What the edge says of itself on hover: what the feeder is, where it has got to, and what the packer will do with it. */
  function barTip(node, info, sim) {
    const p = feederProgress(sim, node);
    const kind = node.spec.kind === 'compute' ? '' : `${node.spec.kind} · `;
    const f = node.spec.feeder;
    const tip = [`${kind}${info.kind} ${info.text}`, info.state, p.edge ? `${p.done} fed so far` : `${p.done} of ${p.total} fed`];
    if (f.seeds) tip.push(`limit ${node.seedRequested} of at most ${f.seeds}${f.batch ? `, raised ${f.batch} at a time by the workgraph's Active hook` : ''}`);
    if (f.after.length) tip.push(`the packer waits for ${f.after.join(', ')}`);
    if (node.spec.packer.join) tip.push(`the packer joins the output of ${node.spec.packer.join}`);
    if (node.spec.packer.lookup) tip.push(`the packer looks up ancestors in ${node.spec.packer.lookup}`);
    tip.push(`packer: ${node.spec.packer.size} per parcel${node.spec.packer.by ? ` by ${node.spec.packer.by}` : ''}`);
    return tip.join(' · ');
  }

  /* One lineage picture for both dialogs: provenance on the left, the focus in the centre with a ring, what it became on the
     right, files as shapes and parcels as boxes with failed attempts greyed. `ancestorDepth` is the generations of provenance
     drawn, a parcel and its files each; `showAttempts` whether the parcels the focus was in are drawn; `interactive` whether a
     click on a file moves the focus. With an input as the focus the right side is that input's own record: every parcel it
     has been in, its parent's before its own, stacked oldest at the top and numbered, the newest at full contrast and the
     earlier ones dimmed, capped at four with the rest folded into a row that unfolds in place. */
  /* `fill` is the width the failed-input step's strip spreads to, capped so an edge never stretches into a long flat line.
     `px` and `ch` are the label's face and the advance per character to fit it by: 9.5px IBM Plex Sans regular measures
     4.29 to 5.06 per character over the member names the documentation ships, widest at `MCSimulationRemoval`, so 5.2 is
     a little above the widest as `G.head.ch`'s 6.2 is a little above the 6.06 of the 11px semibold it was fitted to. `px`
     is here so the checks can hold it against the size the stylesheet gives that class: the advance is right for one size
     and one weight, and a token changed in the stylesheet would leave it quietly measuring the wrong face. */
  const LIN = { genW: 100, rowH: 34, cap: 4, box: 56, fill: 400, px: 9.5, ch: 5.2 };

  function lineageStripSvg(sim, props) {
    const p = Object.assign({ ancestorDepth: 3, showAttempts: true, interactive: true, expanded: false, width: null }, props);
    const f0 = p.file;
    const lin = sim.lineageAround(f0.id, { ancestorDepth: p.ancestorDepth, descendantDepth: p.input || !p.showAttempts ? 0 : Infinity });
    const items = new Map();
    const edges = [];
    for (const [id, f] of lin.files) items.set(`f:${id}`, { kind: 'file', file: f, gen: lin.gen.get(`f:${id}`) });
    for (const [id, pc] of lin.parcels) items.set(`p:${id}`, { kind: 'parcel', parcel: pc, gen: lin.gen.get(`p:${id}`) });
    for (const e of lin.edges) if (items.has(e.from) && items.has(e.to)) edges.push(e);
    let folded = 0;
    let total = 0;
    if (p.input && p.showAttempts) {
      const all = sim.inputAttempts(p.node, p.input);
      total = all.length;
      const shown = p.expanded || all.length <= LIN.cap ? all : all.slice(all.length - LIN.cap);
      folded = all.length - shown.length;
      shown.forEach((a, k) => {
        const n = folded + k + 1;
        const key = `p:${a.parcel.id}`;
        items.set(key, { kind: 'parcel', parcel: a.parcel, gen: 1, n, latest: n === all.length, via: a.input });
        edges.push({ from: `f:${f0.id}`, to: key, kind: 'in', dim: n !== all.length });
      });
    }
    let minG = 0;
    let maxG = 0;
    const columns = new Map();
    for (const [key, it] of items) {
      minG = Math.min(minG, it.gen);
      maxG = Math.max(maxG, it.gen);
      if (!columns.has(it.gen)) columns.set(it.gen, []);
      columns.get(it.gen).push(key);
    }
    const extraOf = (g) => (g === 1 && folded ? 1 : 0);
    let rows = 1;
    for (const [g, col] of columns) rows = Math.max(rows, col.length + extraOf(g));
    /* columns a generation apart, spread across a given width when there is one, so a focus without provenance is not left with an empty side */
    const right = total ? 44 : 0; /* room for the label beside the attempts */
    const span = maxG - minG;
    const step = p.width && span ? Math.max(LIN.genW, (p.width - 140 - right) / span) : LIN.genW;
    const colX = (g) => 70 + (g - minG) * step;
    const pos = new Map();
    let fold = null;
    for (const [g, col] of columns) {
      const extra = extraOf(g);
      const offset = ((rows - col.length - extra) * LIN.rowH) / 2;
      col.forEach((key, k) => pos.set(key, { x: colX(g), y: 30 + offset + (k + extra) * LIN.rowH }));
      if (extra) fold = { x: colX(g), y: 30 + offset };
    }
    const width = Math.max(colX(maxG) + 70 + right, p.width || 0);
    const height = Math.max(rows * LIN.rowH + 20, 66); /* as tall as its rows, and never shorter than a labelled focus */
    const out = [`<rect x="0" y="0" width="${width}" height="${height}" class="wg-bg"/>`];
    for (const e of edges) {
      const a = pos.get(e.from);
      const b = pos.get(e.to);
      const x1 = a.x + (items.get(e.from).kind === 'parcel' ? LIN.box : 8);
      const x2 = b.x - (items.get(e.to).kind === 'parcel' ? LIN.box : 8);
      const table = e.kind === 'out' ? 'ParcelOutputs: the parcel produced the file' : e.kind === 'partner' ? 'a partner the packer added by lookup' : 'ParcelInputs: the file was an input of the parcel';
      out.push(`<path d="M${x1} ${a.y} C${(x1 + x2) / 2} ${a.y}, ${(x1 + x2) / 2} ${b.y}, ${x2} ${b.y}" class="wg-lin-edge wg-lin-edge-${e.kind}${e.dim ? ' wg-lin-dim' : ''}"><title>${table}</title></path>`);
    }
    if (fold) out.push(`<g class="wg-lin-fold" data-act="step-more"><title>show every attempt</title><rect x="${fold.x - LIN.box}" y="${fold.y - 12}" width="${LIN.box * 2}" height="24" rx="6" class="wg-lin-fold-bg"/><text x="${fold.x}" y="${fold.y + 3.5}" text-anchor="middle" class="wg-lin-fold-label">+${folded} earlier ›</text></g>`);
    for (const [key, it] of items) {
      const q = pos.get(key);
      if (it.kind === 'parcel') {
        const pc = it.parcel;
        const node = sim.nodes[pc.node];
        const label = node ? node.spec.label : pc.node;
        /* a masked input covers part of its file, and ParentInputID (DX-ADR-004) says which input it was cut from */
        const masks = pc.inputs
          .filter((i) => i.mask)
          .map((i) => {
            const parent = node && i.parent != null && node.inputs.get(i.parent);
            return `sections ${i.mask}${parent ? ` of input ${parent.tag}` : ''}`;
          });
        const age = it.n ? (it.latest ? ' wg-lin-latest' : ' wg-lin-earlier') : '';
        const when = it.n ? `<text x="${q.x + LIN.box + 8}" y="${q.y + 3.5}" class="wg-lin-sub wg-lin-when">${it.latest ? 'just now' : 'earlier'}</text>` : '';
        const num = it.n && total > 1 ? `<text x="${q.x - LIN.box - 6}" y="${q.y + 3.5}" text-anchor="end" class="wg-lin-sub wg-lin-n">${it.n}</text>` : '';
        const at = it.n ? ` · at ${fmt(pc.ended != null ? pc.ended : pc.since)}s` : '';
        const via = it.via && it.via !== p.input ? ` · as its parent input ${it.via.tag}${it.via.mask ? ` ${it.via.mask}` : ''}` : '';
        /* the member's kind, in the class the card already wears, so the two surfaces share one vocabulary rather than
           growing a second; what the stylesheet does with it is draw the shape, which is the one channel on this box that
           is not already spoken for */
        /* the name is fitted to the box rather than the box to the name: the strip lays its generations out on a fixed
           `genW` pitch, so a box grown to its content would push its generation into the next one. A cut name needs no
           title of its own — the group already carries one naming the parcel, its member, its status and its inputs. */
        out.push(`<g class="wg-lin-parcel wg-kind-${esc(node ? node.spec.kind : 'compute')} wg-lin-${pc.status.toLowerCase()}${pc.recovery ? ' wg-lin-recovery' : ''}${age}"><title>parcel ${pc.tag} of ${esc(label)}: ${pc.status}${pc.recovery ? ' (recovery parcel, born Done)' : ''} · ${pc.inputs.length} input(s), ${pc.outputs.length} output(s)${masks.length ? ` · ${esc(masks.join(', '))}` : ''}${it.n ? ` · attempt ${it.n} of ${total}${esc(via)}` : ''}${at}</title>${num}<rect x="${q.x - LIN.box}" y="${q.y - 12}" width="${LIN.box * 2}" height="24" rx="6" class="wg-lin-box"/><text x="${q.x}" y="${q.y - 1}" text-anchor="middle" class="wg-lin-label">${esc(fitTitle(label, LIN.box * 2 - 8, LIN.ch).text)}</text><text x="${q.x}" y="${q.y + 9}" text-anchor="middle" class="wg-lin-sub">${pc.tag} · ${pc.status}</text>${when}</g>`);
      } else {
        const f = it.file;
        const focus = f.id === f0.id;
        if (focus) out.push(`<circle cx="${q.x}" cy="${q.y}" r="11" class="wg-lin-focus"/>`);
        const shape = p.interactive ? fileShape(f, q.x, q.y, 6) : `<g class="wg-file-still"><title>${esc(fileTitle(f, sim).replace(' · click for its lineage', ''))}</title>${shapeSvg(f.shape, q.x, q.y, 6, PALETTE[f.colour])}</g>`;
        out.push(shape);
        const named = focus && p.input;
        const label = named ? `${f.tag}${p.input.mask ? ` ${p.input.mask}` : ''}` : f.seed != null ? `seed ${f.seed}` : f.tag;
        out.push(`<text x="${q.x}" y="${q.y + 19}" text-anchor="middle" class="wg-lin-sub${named ? ' wg-lin-name' : ''}">${esc(label)}</text>`);
        if (named && f.seed != null) out.push(`<text x="${q.x}" y="${q.y + 29}" text-anchor="middle" class="wg-lin-sub">seed ${f.seed}</text>`);
      }
    }
    if (lin.truncated) out.push(`<text x="${width - 10}" y="${height - 8}" text-anchor="end" class="wg-lin-sub">lineage truncated</text>`);
    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" style="max-width:${width}px">${out.join('')}</svg>`;
  }

  /* ------------------------------------------------------------------ */
  /* Help mode                                                           */
  /* ------------------------------------------------------------------ */

  /* Which element each disclosure explains and which of its corners the mark sits over: `tl` where the top-right is taken by
     something the mark would cover. A region that is not on screen — an edge with no backlog, the state line with nothing to
     say, a rail outside expert mode — gets no mark at all, which is the honest answer and needs no special case.

     A mark is drawn on a layer of its own, absolutely positioned over the picture and reserving nothing, so turning help on
     and off moves no pixel of the widget. */
  const ANCHORS = {
    states: '.wgsim-states .wgsim-pill:last-of-type',
    stateline: '.wgsim-state-line-text',
    bar: '[data-dyn="bar"]',
    pool: 'rect.wg-pool',
    slots: '[data-dyn="slots"]',
    rail: '.wg-rail',
    edge: '[data-dyn="edgechips"] .wg-backlog',
    datasets: '.wg-output',
    log: '.wgsim-log-wrap',
    /* and the dialog's, which name themselves rather than being found by the shape of their markup */
    tabs: '.wgsim-mtabs',
    machine: '.wgsim-machine-region',
    inputs: '[data-region="inputs"]',
    parcels: '[data-region="parcels"]',
    members: '[data-region="members"]',
    feeder: '[data-region="feeder"]',
    packer: '[data-region="packer"]',
    hooks: '[data-region="hooks"]',
    actions: '[data-region="actions"]',
  };
  /* The order the marks are laid out in, which is the order the surface reads: for the picture, the header, then a card, its
     body first and then its rail and the edge along its foot, then what the cards feed, then the log; for the dialog, the tabs
     and the machine over it, then the columns under it, left to right. A dialog shows one shape or the other, a transformation's or the workgraph's, and a region the
     shape in front of the reader does not draw takes no mark, the same as a region of the picture that is not on screen. */
  const DISCLOSURE_ORDER = ['states', 'stateline', 'pool', 'slots', 'rail', 'bar', 'edge', 'datasets', 'log'];
  const PANEL_ORDER = ['tabs', 'machine', 'inputs', 'parcels', 'members', 'feeder', 'packer', 'hooks', 'actions'];
  const MARK = 17; /* the mark's box: it straddles its region's corner, so it covers as little of the region as it can */

  /* The transformation a region belongs to, where it belongs to one: the card it is drawn in, or the owner an element that
     sits outside a card names. */
  function ownerOf(el) {
    if (el.dataset && el.dataset.owner) return el.dataset.owner;
    const card = el.closest ? el.closest('[data-card]') : null;
    return card ? card.dataset.card : null;
  }

  /* The links a disclosure ends on: the machine that decides its region, and the ADR that decides the machine. */
  function discLinks(d, nodeId) {
    const [slug, label] = ADR[d.adr];
    const href = SELF_SRC ? new URL(`../../adr/${slug}/`, SELF_SRC).href : `../../adr/${slug}/`;
    /* the machine region is the machine: it is already what the link would open, so it carries only its ADR */
    const machine = d.tab === null
      ? ''
      : d.tab === 'workgraph' || !nodeId
        ? `<button type="button" class="wgsim-btn small" data-act="open-workgraph">the workgraph's machine</button>`
        : `<button type="button" class="wgsim-btn small" data-act="open-machine" data-node="${esc(nodeId)}" data-tab="${d.tab}">the ${esc(d.tab)} machine</button>`;
    return `<div class="wgsim-disc-links">${machine}<a class="wgsim-btn small" href="${esc(href)}">${esc(label)}</a></div>`;
  }

  /* One disclosure, opened over the region it explains. */
  function disclosureHtml(d, ctx) {
    const legend = d.legend ? d.legend(ctx) : '';
    return `<div class="wgsim-disc-head"><b>${esc(d.title)}</b>${roundBtn('close-disc', '×', 'close', 'close', 'wgsim-disc-close')}</div><div class="wgsim-disc-body"><p>${d.body(ctx)}</p>${legend}</div>${discLinks(d, ctx.node)}`;
  }

  /* ------------------------------------------------------------------ */
  /* The ? panel: only what a change to the picture cannot invalidate    */
  /* ------------------------------------------------------------------ */

  /* Nothing here may say where something sits, what colour it is or what shape. The moment a sentence would need editing
     because a node moved, it belongs in that node's disclosure instead, beside the code that moved it. */
  const CONTROLS = [
    ['space', 'pause, and resume'],
    ['>', 'on to the next thing that happens, and hold it there'],
    ['1 2 3', 'normal, faster, fastest'],
    ['R', 'run it again from the start, with a fresh seed'],
    ['E', 'expert: the rails, and the matrix of what runs by hand'],
    ['C', 'chaos: every parcel fails half the time'],
    ['M', 'maximise: the model fills the window, and Esc puts it back'],
  ];
  const NOUNS = [
    ['workgraph', 'a graph of transformations that together produce what a user asked for'],
    ['transformation', 'one step of the workgraph, with a pool of inputs of its own'],
    ['input', 'one unit of work to be done: a file, part of one, or a seed'],
    ['parcel', 'a group of inputs handed to a backend as one job or request'],
    ['feeder', 'the sweep that brings new inputs into a transformation'],
    ['packer', 'the sweep that groups inputs into parcels'],
    ['hook', 'an extension that runs on a sweep of its own and returns what the core should change'],
    ['action', 'one check in the ordered list a state runs before it can end; each records a result'],
  ];

  function guideHtml() {
    const rows = (pairs, cls) => `<dl class="${cls}">${pairs.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('')}</dl>`;
    const adr = ['005', '004'].map((k) => {
      const [slug, label] = ADR[k];
      const href = SELF_SRC ? new URL(`../../adr/${slug}/`, SELF_SRC).href : `../../adr/${slug}/`;
      return `<a class="wgsim-btn small" href="${esc(href)}">${esc(label)}</a>`;
    }).join('');
    return `<div class="wgsim-disc-head"><b>what this is</b>${roundBtn('close-guide', '×', 'close', 'close', 'wgsim-disc-close')}</div>
      <div class="wgsim-disc-body">
        <p>A model of the DiracX Transformation System, run on a made-up workgraph so that the state machines of the ADRs can be watched running. It talks to nothing: no DIRAC installation is involved and no request leaves the page. Nothing done here has any effect anywhere else.</p>
        <p class="wgsim-disc-note">While help is on, every region of the picture carries a mark. Click one and that region explains itself, in place.</p>
        <h5>controls</h5>${rows(CONTROLS, 'wgsim-guide-keys')}
        <h5>the words</h5>${rows(NOUNS, 'wgsim-guide-nouns')}
      </div>
      <div class="wgsim-disc-links">${adr}</div>`;
  }

  class Widget {
    constructor(host, spec) {
      this.spec = spec;
      this.host = host;
      this.paused = false;
      this.speed = SPEEDS[0].mult;
      this.visible = true;
      this.last = null;
      this.pinned = true; /* the log follows its newest line until the reader scrolls up */
      this.logFilter = null;
      this.pathCache = {};
      this.tokenEls = new Map(); /* one element per file in flight, kept across frames and moved */
      this.userSettings = {};
      this.chaos = false;
      this.maxOn = false; /* filling the window over the page, rather than the browser's own fullscreen */
      this.expert = false; /* the rails and the workgraph's pills exist only while this is on; off, every row is auto */
      this.layouts = {}; /* one layout per mode, with and without the rails, each computed once */
      this.actionsTab = {};
      this.machineTab = {}; /* the machine tab shown in each card's dialog */
      this.machineList = {}; /* the box picked under each machine, and whether its fold is open, by `listKey` */
      this.reducedMotion = typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
      this.seen = new Promise((resolve) => (this.markSeen = resolve));
      this.buildDom();
      this.observe();
      /* nothing is downloaded or laid out until the model is actually on screen */
      this.ready = this.seen.then(() => this.load());
      requestAnimationFrame((ts) => this.frame(ts));
    }

    buildDom() {
      const root = document.createElement('div');
      root.className = 'wgsim';
      root.tabIndex = 0;
      const speedButtons = SPEEDS.map((s) => `<button type="button" role="radio" aria-checked="false" class="wgsim-tb" data-act="speed" data-speed="${s.mult}" aria-label="${s.name}" title="${s.name} (${s.key}) · ${s.mult}× model time">${ICON.chevrons(s.chevrons)}</button>`).join('');
      const chip = (key, icon, label, on, title) => `<button type="button" role="switch" aria-checked="${on}" class="wgsim-chip wgsim-chip-${key}" data-act="chip" data-chip="${key}" title="${title}" aria-label="${label}">${icon}<span class="label">${label}</span></button>`;
      root.innerHTML = `
        <div class="wgsim-bar">
          <div class="wgsim-left">
            <div class="wg-lockup wgsim-lockup"><img class="wg-logo" src="${LOGO_URL}" alt="" width="20" height="20"><span class="wg-lockup-name">Workgraph simulator</span><span class="wgsim-lockup-model"></span></div>
            <span class="wgsim-divider wgsim-lockup-rule" aria-hidden="true"></span>
            <div class="wgsim-playback">
              <button type="button" class="wgsim-tb" data-act="reset" aria-label="reset" title="reset: run the model again from New (R)">${ICON.reset}</button>
              <button type="button" class="wgsim-tb" data-act="step" aria-label="step" title="step (>): on to the next thing that happens, and hold it there">${ICON.step}</button>
              <div class="wgsim-speed" role="radiogroup" aria-label="playback">
                <button type="button" role="radio" aria-checked="false" class="wgsim-tb" data-act="pause" aria-label="pause" title="pause (space)">${ICON.pause}</button>
                ${speedButtons}
              </div>
            </div>
            <div class="wgsim-actions"></div>
          </div>
          <div class="wgsim-ctl">
            ${chip('expert', ICON.sliders, 'expert', false, 'expert (E): show the rails, where each sweep can be taken by hand and run one at a time, and open the matrix that sets them all at once. Leaving expert mode sets every row back to auto')}
            <button type="button" class="wgsim-chip wgsim-chip-steps" data-act="open-steps" hidden title="what waits for you: click to walk through it"></button>
            ${chip('chaos', ICON.monkey, 'chaos', false, 'chaos (C): every parcel fails half the time')}
            <span class="wgsim-divider" aria-hidden="true"></span>
            ${helpChip('the picture')}
            <button type="button" class="wgsim-tb wgsim-themebtn" data-act="theme" hidden aria-label="switch the colour scheme" title="switch the colour scheme"></button>
            <button type="button" class="wgsim-tb wgsim-maxbtn" data-act="maximise" aria-pressed="false" aria-label="maximise" title="maximise (M): the model fills the window, over the page">${ICON.expand}</button>
          </div>
        </div>
        <div class="wgsim-lists" role="region" aria-label="the action lists running" hidden></div>
        <div class="wgsim-head" data-act="open-workgraph" role="button" tabindex="0" title="the workgraph: click for its state machine, members, hooks and checks"></div>
        <div class="wgsim-canvas"><div class="wgsim-loading" hidden role="status"><span class="wgsim-loading-label">preparing the layout</span><span class="wgsim-loading-track"><span class="wgsim-loading-bar"></span></span></div><svg xmlns="http://www.w3.org/2000/svg"></svg></div>
        <div class="wgsim-log-wrap"><ul class="wgsim-log"></ul><div class="wgsim-log-said" role="status"></div><div class="wgsim-log-now"></div><button type="button" class="wgsim-log-chip wgsim-log-only" data-act="filter-log" title="show every subject again" hidden></button><button type="button" class="wgsim-log-chip wgsim-log-pill" data-act="log-latest" title="follow the newest line again" hidden>↓ latest</button></div>
        <div class="wgsim-marks wgsim-picture-marks"></div>
        <dialog class="wgsim-help wgsim-ask"><div class="wgsim-help-body"><div class="wgsim-help-head"><b>The packer made nothing</b><button type="button" class="wgsim-btn" data-act="close-ask">close</button></div><div class="wgsim-ask-body"></div><div class="wgsim-ask-actions"><button type="button" class="wgsim-btn wgsim-btn-accent" data-act="flush-confirm">pack what is ready</button><button type="button" class="wgsim-btn" data-act="close-ask">leave it</button></div></div></dialog>
        <dialog class="wgsim-help wgsim-panel">${panelHead({ rightSlot: '<span class="wgsim-panel-state"></span>', control: helpChip('this dialog') + closeBtn('close-panel') })}<div class="wgsim-panel-body"></div><div class="wgsim-marks wgsim-panel-marks"></div></dialog>
        <dialog class="wgsim-help wgsim-modes" aria-label="what runs by hand"></dialog>
        <dialog class="wgsim-help wgsim-steps" aria-label="what waits for you"></dialog>
        <dialog class="wgsim-help wgsim-files" aria-label="the files of an output"></dialog>
        <dialog class="wgsim-help wgsim-lineage" aria-label="the lineage of a file"></dialog>`;
      this.host.replaceWith(root);
      this.root = root;
      this.svg = root.querySelector('.wgsim-canvas svg');
      this.loading = root.querySelector('.wgsim-loading');
      this.loadingLabel = root.querySelector('.wgsim-loading-label');
      this.loadingBar = root.querySelector('.wgsim-loading-bar');
      this.head = root.querySelector('.wgsim-head');
      this.actions = root.querySelector('.wgsim-actions');
      this.logEl = root.querySelector('.wgsim-log');
      this.logNow = root.querySelector('.wgsim-log-now');
      this.logWrap = root.querySelector('.wgsim-log-wrap');
      this.logSaid = root.querySelector('.wgsim-log-said');
      this.said = null; /* what the last press moved, which the log's foot carries until the model runs again */
      this.logPill = root.querySelector('.wgsim-log-pill');
      this.logOnly = root.querySelector('.wgsim-log-only');
      /* by its own class: every dialog shares .wgsim-help for its look */
      this.marks = root.querySelector('.wgsim-picture-marks');
      this.panelMarks = root.querySelector('.wgsim-panel-marks');
      this.lineage = root.querySelector('.wgsim-lineage');
      this.ask = root.querySelector('.wgsim-ask');
      this.panel = root.querySelector('.wgsim-panel');
      this.steps = root.querySelector('.wgsim-steps');
      this.modes = root.querySelector('.wgsim-modes');
      /* the aggregate the pointer or the focus is over, which tints its scope and swaps its label; not in the html, so a
         redraw after a click cannot make it flicker under the cursor */
      this.modesUi = { hover: null, focus: null };
      this.files = root.querySelector('.wgsim-files');
      this.filesUi = { output: null, more: false };
      this.lists = root.querySelector('.wgsim-lists');
      /* the lists in view, by entity and list, each kept a beat past its own run; collapsed, the header stays */
      this.listsUi = { shown: new Map(), collapsed: false, runs: null };
      this.lineageUi = { file: null, what: false };
      this.stepsChip = root.querySelector('.wgsim-chip-steps');
      this.stepsKeys = new Set();
      this.deferred = new Set(); /* the steps the reader put off: skipped until the chip opens the dialog again */
      this.stepsUi = { key: null, to: null, what: false, more: false };
      this.dialogs = [this.ask, this.lineage, this.panel, this.steps, this.files, this.modes];
      /* Help mode: whether the marks are showing, which disclosure is open, and whether the ? panel is. None of it is a
         dialog, so a disclosure never has to be dismissed before the next is opened, and nothing the model does clears the
         mode; the model itself holds while the mode is on, as it does for a dialog. */
      this.helpOn = false;
      this.disc = null;
      this.guideOn = false;
      this.marksHtml = null; /* the layer is redrawn only when it would change, so a mark never flickers under the frame loop */
      for (const d of this.dialogs) {
        d.addEventListener('close', () => {
          this.renderBar();
          /* whatever was open belonged to the dialog: the marks go back to the picture with the reader */
          this.disc = null;
          this.renderMarks();
        });
      }
      root.addEventListener('click', (ev) => {
        const el = ev.target.closest('[data-act]');
        if (!el || !root.contains(el)) return;
        this.act(el.dataset.act, el);
        this.renderFrame();
      });
      root.addEventListener('keydown', (ev) => {
        const tag = ev.target.tagName;
        if (tag === 'INPUT' || tag === 'SELECT' || tag === 'TEXTAREA') return;
        /* Every key the widget binds is a bare one, so a combination is the browser's and is left alone, unread and
           unprevented. A reader opens a card by clicking it, which puts the focus in here for the rest of the page's life,
           and the widget was answering `Cmd+C` with chaos and cancelling the copy, `Cmd+R` with a reset instead of a reload,
           `Cmd+3` with the fastest speed. Shift is not a modifier for this purpose: `R`, `C`, `E` and `M` are bound in upper
           case deliberately, and a shifted key is still a bare key. */
        if (ev.metaKey || ev.ctrlKey || ev.altKey) return;
        const key = ev.key;
        /* a region that stands in for a button acts on Enter and Space, wherever it is, a dialog's included */
        const region = (key === 'Enter' || key === ' ') && ev.target.closest && ev.target.closest('[data-act][role="button"]');
        if (region) {
          ev.preventDefault();
          this.act(region.dataset.act, region);
          this.renderFrame();
          return;
        }
        if (this.dialogOpen()) return;
        if (key === ' ') this.act('pause');
        /* `>` and not `.`: the documentation theme these models are drawn in binds `.` to its own next page, on a stream
           of its own over the window, so a reader stepping the model would have been carried off the page. Shift is not a
           modifier for this purpose — `?` is bound the same way — and the theme's switch matches `.` rather than the key
           the shift produces, so `>` collides with nothing and still reads as one step forward. */
        else if (key === '>') this.act('step');
        else if (key === '1' || key === '2' || key === '3') this.setSpeed(SPEEDS[Number(key) - 1].mult);
        else if (key === 'r' || key === 'R') this.act('reset');
        else if (key === 'e' || key === 'E') this.toggleChip('expert');
        else if (key === 'c' || key === 'C') this.toggleChip('chaos');
        else if (key === '?') this.toggleChip('help');
        else if (key === 'm' || key === 'M') this.setMax(!this.maxOn);
        else if (key === 'Escape' && this.helpOn) this.setHelp(false);
        else if (key === 'Escape' && this.maxOn) this.setMax(false);
        else return;
        ev.preventDefault();
        this.renderFrame();
      });
      /* An aggregate's scope shows before the click: the row or column tints and the rest of the matrix fades. The label's own
         swap is the stylesheet's, on :hover and :focus-visible, so only the label under the pointer says what it would do
         while the whole scope it covers lights up. */
      const scopeOf = (ev, keyboard) => {
        const el = ev.target.closest && ev.target.closest('[data-act="mode-scope"]');
        if (!el) return null;
        /* the tint follows :focus-visible, which is what the label's own swap follows, so a label the reader clicked with
           the pointer does not keep its scope lit once the pointer has left it */
        if (keyboard && el.matches && !el.matches(':focus-visible')) return null;
        return { kind: el.dataset.scope, sweep: el.dataset.row, entity: el.dataset.node };
      };
      this.modes.addEventListener('mouseover', (ev) => { this.modesUi.hover = scopeOf(ev); this.applyModeScope(); });
      this.modes.addEventListener('mouseleave', () => { this.modesUi.hover = null; this.applyModeScope(); });
      this.modes.addEventListener('focusin', (ev) => { this.modesUi.focus = scopeOf(ev, true); this.applyModeScope(); });
      this.modes.addEventListener('focusout', () => { this.modesUi.focus = null; this.applyModeScope(); });

      /* The log follows its newest line until the reader scrolls up; the pill in the corner pins it again. */
      this.logEl.addEventListener('scroll', () => {
        const atEnd = this.logEl.scrollHeight - this.logEl.scrollTop - this.logEl.clientHeight < 8;
        if (atEnd === this.pinned) return;
        this.pinned = atEnd;
        this.logPill.hidden = atEnd;
      });
      /* Hovering a line lights the card it is about, so a subject in the log and a card in the picture are one thing. */
      this.logEl.addEventListener('mouseover', (ev) => {
        const line = ev.target.closest('li');
        this.highlight(line ? line.dataset.subject : null);
      });
      this.logEl.addEventListener('mouseleave', () => this.highlight(null));
      if (typeof ResizeObserver !== 'undefined') {
        /* a documentation column is narrow by this measure, so `narrow` only shapes what must give; `tight` is a phone */
        new ResizeObserver((entries) => {
          const width = entries[entries.length - 1].contentRect.width;
          const narrow = width < NARROW;
          const tight = width < TIGHT;
          if (root.classList.contains('narrow') !== narrow || root.classList.contains('tight') !== tight) requestAnimationFrame(() => {
            root.classList.toggle('narrow', narrow);
            root.classList.toggle('tight', tight);
          });
        }).observe(root);
      }
    }

    act(act, el) {
      /* the widget's own, so both work while the layout is still coming */
      if (act === 'maximise') return this.setMax(!this.maxOn);
      if (act === 'theme') return this.switchTheme();
      if (!this.sim) return; /* the model is created when it is first scrolled to */
      const node = el && el.dataset.node;
      if (act === 'pause') this.playPause();
      else if (act === 'step') this.stepOnce();
      else if (act === 'speed') this.setSpeed(Number(el.dataset.speed));
      else if (act === 'reset') { this.setHelp(false); this.said = null; this.sim.reset(); }
      else if (act === 'chip') this.toggleChip(el.dataset.chip);
      else if (act === 'force') this.sim.forceAction(node || 'workgraph');
      else if (act === 'rerun') this.sim.rerunAction(node || 'workgraph');
      else if (act === 'cancel') this.sim.cancel();
      else if (act === 'extend-scout') this.sim.extendScout();
      else if (act === 'resume-active') this.sim.resumeFromFinalizing();
      else if (act === 'start-node') this.sim.startNode(node);
      else if (act === 'toggle-node') this.sim.toggleNode(node);
      else if (act === 'drain') this.sim.drain();
      else if (act === 'halt') this.sim.halt();
      else if (act === 'extend') this.sim.extendFeeder(node, 'operator');
      else if (act === 'disclose') this.openDisclosure(el.dataset.disc);
      else if (act === 'close-disc') this.closeDisclosure();
      else if (act === 'close-guide') this.closeGuide();
      /* a jump to where the state lives, not a dialog on top of a dialog: the lineage dialog gives way to the machine */
      else if (act === 'open-machine') { this.lineage.close(); this.openPanel('details', node, el.dataset.tab); }
      /* a rail row stands for the cells it covers, so a click makes those uniform and the ▶ sweeps whichever of them is by
         hand with work; the matrix sets one cell at a time, which is the only way to reach HandleFailedInput on its own */
      else if (act === 'toggle-mode') this.sim.setCellsUniform(railCells(node, el.dataset.row));
      /* the matrix's own clicks say nothing in the log: they are the reader arranging the controls, not the model acting */
      else if (act === 'mode-cell') this.sim.toggleMode(node, el.dataset.row, true);
      else if (act === 'mode-scope') this.sim.setCellsUniform(scopeCells(this.sim, { kind: el.dataset.scope, sweep: el.dataset.row, entity: node }), true);
      else if (act === 'close-modes') this.modes.close();
      else if (act === 'ask-flush') this.askFlush(node);
      else if (act === 'run-row') {
        for (const c of railCells(node, el.dataset.row)) {
          if (this.sim.modeOf(c.id, c.row) !== 'manual' || !this.sim.rowPending(c.id, c.row)) continue;
          if (c.row === 'packer') this.runPackerByHand(c.id);
          else this.sim.runRow(c.id, c.row);
        }
      }
      else if (act === 'flush-confirm') {
        this.sim.flush(this.askNode);
        this.ask.close();
      } else if (act === 'close-ask') this.ask.close();
      else if (act === 'open-workgraph') this.openPanel('workgraph');
      else if (act === 'run-failed') this.sim.runFailedInputs(node, 'hand');
      else if (act === 'open-steps') {
        this.deferred.clear(); /* asked for, so what was put off is shown again */
        this.openSteps();
      }
      else if (act === 'step-pick') {
        if (el.getAttribute('aria-disabled') !== 'true') this.stepsUi.to = el.dataset.to;
      }
      else if (act === 'step-what') this.stepsUi.what = !this.stepsUi.what;
      else if (act === 'step-more') this.stepsUi.more = true;
      else if (act === 'step-apply') {
        const i = this.sim.nodes[node] && this.sim.nodes[node].inputs.get(Number(el.dataset.input));
        /* the hook's own row is the hook's run; any other is the operator deciding in its place */
        this.sim.decideFailedInput(node, Number(el.dataset.input), i && el.dataset.to !== (i.decision || 'Unassigned') ? el.dataset.to : undefined);
      }
      else if (act === 'step-later' || act === 'step-later-pause') {
        if (this.stepsUi.key) this.deferred.add(this.stepsUi.key);
        if (act === 'step-later-pause') {
          this.paused = true;
          this.steps.close();
        }
      }
      else if (act === 'open-parcels') this.openPanel('details', node, 'parcels');
      else if (act === 'open-details') this.openPanel('details', node);
      else if (act === 'machine-tab') this.machineTab[node] = el.dataset.tab;
      /* a box shows what is in it, and the same box again shows them all; either way the count at the end closes,
         since it was opened over a list that is no longer the one on screen */
      else if (act === 'machine-list') {
        const key = listKey(node, this.machineTab[node]);
        const cur = this.machineList[key];
        this.machineList[key] = { state: cur && cur.state === el.dataset.state ? null : el.dataset.state, more: false };
      }
      else if (act === 'machine-list-more') {
        const key = listKey(node, this.machineTab[node]);
        this.machineList[key] = Object.assign({ state: null }, this.machineList[key], { more: true });
      }
      else if (act === 'open-files') this.openFiles(node);
      else if (act === 'files-more') this.filesUi.more = true;
      else if (act === 'close-files') this.files.close();
      else if (act === 'lists-toggle') this.listsUi.collapsed = !this.listsUi.collapsed;
      else if (act === 'lineage-what') this.lineageUi.what = !this.lineageUi.what;
      /* the operator's edge of the input machine, taken on the one input the lineage dialog is about */
      else if (act === 'input-decide') this.sim.decideInput(node, Number(el.dataset.input), el.dataset.to);
      else if (act === 'close-panel') this.panel.close();
      else if (act === 'reset-problematic') this.sim.resetProblematic(node);
      else if (act === 'writeoff-unassigned') this.sim.writeOffUnassigned(node);
      else if (act === 'wg-tab') this.wgTab = this.wgTab === el.dataset.list ? null : el.dataset.list;
      else if (act === 'actions-tab') this.actionsTab[node] = el.dataset.list;
      else if (act === 'writeoff-problematic') this.sim.writeOffProblematic(node);
      else if (act === 'filter-log') this.setLogFilter(node);
      else if (act === 'log-latest') this.pinLog();
      else if (act === 'lineage') this.openLineage(Number(el.dataset.file));
      else if (act === 'close-lineage') this.lineage.close();
      this.renderBar();
    }

    /* Choosing a speed also starts a model still in New and resumes a paused one. */
    setSpeed(mult) {
      this.setHelp(false);
      this.said = null;
      this.speed = mult;
      this.paused = false;
      if (this.sim.wg.status === 'New') this.sim.start();
    }

    /* One press of step: the model stops where it is and goes on to the next thing that happens. The same act whether it
       was running or already paused, which is what lets a reader take a falling-over member a beat at a time. Help mode is
       a hold of the reader's own and the press is them asking the model to move, so it ends as it does on a speed; a
       dialog is not, and `stepModel` returns nothing while one is open. */
    stepOnce() {
      if (!this.sim || !this.L) return;
      this.setHelp(false);
      const before = stepSnap(this.sim);
      const took = stepModel(this.sim, this.dialogOpen());
      if (!took) return;
      this.paused = true;
      this.said = stepSaid(this.sim, before, stepSnap(this.sim), took);
      this.renderFrame();
    }

    /* Pause on a model in New starts it paused, so the reader can step it by hand; otherwise it toggles the pause. */
    playPause() {
      this.setHelp(false);
      this.said = null;
      if (this.sim.wg.status === 'New') {
        this.paused = true;
        this.sim.start();
      } else this.paused = !this.paused;
    }

    /* Expert mode shows and hides the rails and opens the matrix of what runs by hand, which is the one way into it: the chip
       stays a switch, so turning it off closes the dialog again. Leaving expert mode resets every row to auto, deliberately
       and without asking, whether it was set from a rail or from the matrix: a hidden manual row would stall the model with
       no visible cause, since the ▶ that would explain it is hidden too. Expert off is a guarantee that everything runs, not
       a view filter. */
    toggleChip(key) {
      if (key === 'help') this.setHelp(!this.helpOn);
      else if (key === 'expert') {
        this.expert = !this.expert;
        if (this.expert) this.openModes();
        else {
          this.sim.setAllModes('auto');
          this.modes.close();
        }
        this.relayout();
      } else if (key === 'chaos') {
        this.chaos = !this.chaos;
        this.setSetting('failOverride', this.chaos ? CHAOS_RATE : null);
      }
    }

    /* Maximised: the widget fills the window over the page, rather than taking the screen, so the browser's own chrome stays
       and Esc brings the page back. Fixed is the window only while no ancestor is a containing block, and a transform, a
       filter or a `contain` anywhere above silently boxes the widget into that ancestor instead — the playground dims a stale
       panel with a filter, and any page may do the like. So the widget moves to the body for as long as it is maximised,
       where nothing stands above it but the document, and a spacer of its height holds its place in the flow, so the page
       behind neither reflows nor loses where the reader was. The document's scrolling is held meanwhile: a scrollbar is drawn
       over a fixed layer however deep it sits, and scrolling under the window would put the reader somewhere else. The
       picture is fitted to what the band and the log leave it and nothing is laid out again: the layout depends on the spec
       alone, so only the scale changes. */
    setMax(on) {
      if (this.maxOn === on) return;
      this.maxOn = on;
      const html = document.documentElement;
      if (on) {
        this.spacer = document.createElement('div');
        this.spacer.style.height = `${Math.round(this.root.getBoundingClientRect().height)}px`;
        this.root.after(this.spacer);
        document.body.appendChild(this.root);
        this.scrollLock = html.style.overflow;
        html.style.overflow = 'hidden';
      } else if (this.spacer) {
        this.spacer.replaceWith(this.root); /* back into the place the spacer kept, exactly */
        this.spacer = null;
        html.style.overflow = this.scrollLock || '';
      }
      this.root.classList.toggle('wgsim-max', on);
      const btn = this.root.querySelector('.wgsim-maxbtn');
      btn.setAttribute('aria-pressed', String(on));
      btn.setAttribute('aria-label', on ? 'restore' : 'maximise');
      btn.title = on ? 'restore (M, or Esc): put the model back in the page' : 'maximise (M): the model fills the window, over the page';
      btn.innerHTML = on ? ICON.contract : ICON.expand;
      this.fitCanvas();
      this.measureRow(); /* the row is as wide as the widget, which has just changed */
      this.syncTheme(); /* the page's switch is behind the widget now, so the band carries one while it is */
      this.renderMarks(); /* a mark is placed over the region it explains, and every region has just moved */
      /* the keys are the widget's while it holds the window, and leaving it puts the reader back where it sat */
      this.root.focus({ preventScroll: true });
      if (!on) this.root.scrollIntoView({ block: 'nearest' });
    }

    /* The colour scheme is the page's, and maximised the widget covers the switch that sets it: the band carries one for as
       long as it does. It is the page's own switch that it presses, found wherever the page keeps it — the documentation's
       header, or the playground's toolbar, which is the same markup moved — rather than a second scheme of its own, which
       would be a second thing to be wrong. A page with no switch shows no button. Each option's label follows its input and
       points at the next, so the label after the checked one is the switch as the reader sees it. */
    paletteLabel() {
      const form = document.querySelector('[data-md-component="palette"]');
      if (!form) return null;
      /* The theme leaves one label of the set showing, the one that says where the reader is and takes them on; that is the
         switch as they see it. Where nothing has marked one yet, the checked option names it, and failing that the first. */
      const checked = form.querySelector('input[name="__palette"]:checked');
      const next = checked && checked.nextElementSibling;
      return form.querySelector('label:not([hidden])') || (next && next.tagName === 'LABEL' ? next : null) || form.querySelector('label');
    }

    /* The button wears the switch's own icon and title, so it says what the page's switch says and cannot drift from it. */
    syncTheme() {
      const btn = this.root.querySelector('.wgsim-themebtn');
      const label = this.maxOn ? this.paletteLabel() : null;
      btn.hidden = !label;
      if (!label) return;
      btn.innerHTML = label.innerHTML;
      btn.title = label.title || 'switch the colour scheme';
      btn.setAttribute('aria-label', btn.title);
    }

    switchTheme() {
      const label = this.paletteLabel();
      if (!label) return;
      label.click();
      setTimeout(() => this.syncTheme(), 0); /* the theme swaps which label is shown, and the button follows it */
    }

    /* How large the picture is drawn. In the page it is held a little under the layout's own width, so a model is never
       stretched past the size it was laid out at; maximised it fits the box that is left, by width or by height, whichever
       runs out first. The scaling is the viewBox's own, so every label and every card grows with the drawing. */
    fitCanvas() {
      const s = this.svg.style;
      if (this.maxOn || !this.L) {
        s.maxWidth = 'none';
        s.aspectRatio = 'auto';
      } else {
        s.maxWidth = `${Math.round(this.L.width * 0.9)}px`;
        s.aspectRatio = `${this.L.width} / ${this.L.height}`;
      }
    }

    /* Help mode. The chip turns it on: every region that is on screen takes a mark, and the ? panel opens beside them with the
       part of the explanation a change to the picture cannot invalidate. It ends on ?, on Esc, or on a click that is neither a
       mark nor inside an open disclosure — not on opening one, and not because the model moved on underneath. */
    setHelp(on) {
      if (this.helpOn === on) return;
      this.helpOn = on;
      this.disc = null;
      /* the ? panel introduces the widget, which is the picture's surface: in a dialog the ? marks and says nothing else */
      this.guideOn = on && !this.panel.open;
      this.renderMarks();
      this.renderBar();
    }

    /* A mark opens its region's disclosure in place. The marks stay, so the next can be opened without pressing ? again.
       One panel is open at a time: the ? panel introduces the marks and a mark's own disclosure takes its place, since the
       two are anchored independently — one under the chip, one over its region — and a widget the width of a documentation
       column has no room to show both without one covering the other. */
    openDisclosure(key) {
      if (!DISCLOSURES[key]) return;
      this.disc = this.disc === key ? null : key;
      if (this.disc) this.guideOn = false;
      this.renderMarks();
    }

    closeDisclosure() {
      this.disc = null;
      this.renderMarks();
    }

    closeGuide() {
      this.guideOn = false;
      this.renderMarks();
    }

    /* A panel sits under what it belongs to, pushed back inside the widget where the corner it wants would take it outside. */
    panelStyle(left, top, width, base) {
      const x = Math.max(8, Math.min(left, Math.max(8, base.width - width - 8)));
      return `left:${x.toFixed(1)}px;top:${Math.max(8, top).toFixed(1)}px;width:${width}px`;
    }

    /* The surface help marks: the dialog while it is open, since it covers the picture and has the reader with it, and the
       picture otherwise. Each carries its own ?, its own layer and its own regions. A dialog that discloses nothing of its
       own covers the picture all the same, and marks behind it would be neither visible nor reachable, so it has no surface
       and nothing is drawn until it goes. In the dialog every section's top right carries a status or a count, so its marks
       straddle the left corner instead, in the column's gutter. */
    surface() {
      if (this.panel.open) return { host: this.panel, layer: this.panelMarks, order: PANEL_ORDER, node: this.panelNode, corner: 'tl', guide: false };
      if (this.dialogOpen()) return null;
      return { host: this.root, layer: this.marks, order: DISCLOSURE_ORDER, node: null, corner: 'tr', guide: true };
    }

    /* The marks layer: one mark per region of the surface that is drawn, each absolutely over its region's corner, and the
       open disclosure or the ? panel beside it. Nothing in the layer is in the flow and the layer is always in the DOM, so
       turning help on and off moves no pixel of anything. A region that is not drawn — an edge with no backlog, a rail
       outside expert mode, a feeder in the workgraph's shape of the dialog — takes no mark, and needs no case of its own. */
    renderMarks() {
      const s = this.surface();
      /* the layer the marks were last drawn on is emptied before they move to another, or they would be left on both */
      if (this.marksLayer && this.marksLayer !== (s && s.layer)) {
        this.marksLayer.innerHTML = '';
        this.marksHtml = null;
      }
      this.marksLayer = s && s.layer;
      if (!s) return;
      if (!this.helpOn) {
        if (this.marksHtml !== '') {
          this.marksHtml = '';
          s.layer.innerHTML = '';
        }
        return;
      }
      const base = s.host.getBoundingClientRect();
      if (!base.width) return;
      /* a surface that scrolls carries its layer with it, so a mark is placed from the content's origin rather than the box's */
      const sx = s.host.scrollLeft || 0;
      const sy = s.host.scrollTop || 0;
      const at = (el) => {
        const r = el.getBoundingClientRect();
        return { left: r.left - base.left + sx, top: r.top - base.top + sy, right: r.right - base.left + sx, bottom: r.bottom - base.top + sy, w: r.width, h: r.height };
      };
      const parts = [];
      let open = null;
      for (const key of s.order) {
        const el = s.host.querySelector(ANCHORS[key]);
        if (!el) continue;
        const box = at(el);
        if (!box.w && !box.h) continue;
        const d = DISCLOSURES[key];
        const x = ((d.corner || s.corner) === 'tl' ? box.left : box.right) - MARK / 2;
        parts.push(`<button type="button" class="wgsim-mark${this.disc === key ? ' on' : ''}" data-act="disclose" data-disc="${key}" style="left:${x.toFixed(1)}px;top:${(box.top - MARK / 2).toFixed(1)}px" aria-label="explain ${esc(d.title)}" title="${esc(d.title)}">?</button>`);
        if (this.disc === key) open = { box, el };
      }
      if (open) {
        const d = DISCLOSURES[this.disc];
        const w = Math.min(340, base.width - 16);
        parts.push(`<div class="wgsim-disc" style="${this.panelStyle(open.box.left, open.box.bottom + 6, w, base)}">${disclosureHtml(d, { sim: this.sim, node: s.node != null ? s.node : ownerOf(open.el) })}</div>`);
      }
      if (this.guideOn && s.guide) {
        const chip = s.host.querySelector('[data-chip="help"]');
        const box = chip ? at(chip) : { left: base.width, bottom: 8 };
        const w = Math.min(400, base.width - 16);
        parts.push(`<div class="wgsim-disc wgsim-guide" style="${this.panelStyle(box.left - w + MARK, box.bottom + 6, w, base)}">${guideHtml()}</div>`);
      }
      const html = parts.join('');
      if (this.marksHtml === html) return;
      this.marksHtml = html;
      s.layer.innerHTML = html;
      /* a panel whose region is near the foot would hang below the surface, which clips it: pull it back up to fit */
      for (const el of s.layer.querySelectorAll('.wgsim-disc')) {
        const r = el.getBoundingClientRect();
        const over = r.bottom - base.bottom + 8;
        if (over > 0) el.style.top = `${Math.max(8, r.top - base.top + sy - over).toFixed(1)}px`;
      }
    }

    /* Laying a model out is asynchronous, so the canvas carries its loading state until elkjs
       answers. The result depends on the spec alone, so this runs once and never again. */
    async load() {
      this.sim = new Sim(this.spec);
      Object.assign(this.sim.settings, this.userSettings);
      /* a spec that starts rows by hand, with a period of 0, needs the rails in view to run them: it starts in expert mode */
      if (this.sim.modeSummary() !== 'auto') this.expert = true;
      const stop = this.beginLoading();
      let L;
      try {
        L = await this.layoutFor(this.expert);
      } catch (e) {
        stop();
        this.failLoading(e);
        return;
      }
      stop();
      this.L = L;
      this.rebuildScene();
      this.hideLoading();
    }

    /* The layout for a mode, computed the first time that mode is shown and kept: the cards' width is the only thing that
       differs between the two, so each is laid out once. */
    layoutFor(rail) {
      const key = rail ? 'rail' : 'plain';
      if (!this.layouts[key]) {
        this.layouts[key] = layout(this.sim, { rail }).catch((e) => {
          delete this.layouts[key];
          throw e;
        });
      }
      return this.layouts[key];
    }

    /* Entering or leaving expert mode changes the cards' width, so the graph is laid out again for it; a toggle that
       happens while a layout is still in flight is settled by the last one. */
    async relayout() {
      if (!this.sim) return;
      const seq = (this.layoutSeq = (this.layoutSeq || 0) + 1);
      const want = this.expert;
      let L;
      try {
        L = await this.layoutFor(want);
      } catch (e) {
        this.failLoading(e);
        return;
      }
      if (seq !== this.layoutSeq || !this.L) return;
      this.L = L;
      this.rebuildScene();
    }

    /* The wait, shown honestly: a real bar while the layout engine downloads, and nothing at all for
       a layout that resolves quickly enough that an indicator would only flicker. */
    beginLoading() {
      const show = () => {
        this.root.classList.add('loading');
        this.loading.hidden = false;
      };
      let timer = null;
      if (root.ELK) timer = setTimeout(show, 150);
      else {
        show();
        this.loading.classList.add('indeterminate');
        this.loadingLabel.textContent = 'loading the layout engine';
      }
      const off = watchElk((loaded, total) => {
        if (!total) return;
        this.loading.classList.remove('indeterminate');
        this.loadingBar.style.width = `${Math.round((100 * loaded) / total)}%`;
        this.loadingLabel.textContent = `loading the layout engine · ${Math.round(loaded / 1024)} of ${Math.round(total / 1024)} kB`;
      });
      return () => {
        clearTimeout(timer);
        off();
      };
    }

    hideLoading() {
      this.root.classList.remove('loading');
      this.loading.hidden = true;
      this.loading.classList.remove('indeterminate');
    }

    /* A model that cannot be laid out says so where the picture would have been, rather than
       leaving an empty frame that looks like it is still working. */
    failLoading(err) {
      this.root.classList.add('loading');
      this.loading.hidden = false;
      this.loading.classList.remove('indeterminate');
      this.loadingBar.style.width = '0%';
      this.loadingLabel.textContent = `the picture needs the layout engine, and it could not be loaded. ${(err && err.message) || err}`;
    }

    /* Draw the static picture for the layout already computed. */
    rebuildScene() {
      this.svg.setAttribute('viewBox', `0 0 ${this.L.width} ${this.L.height}`);
      this.fitCanvas();
      this.svg.innerHTML = staticSvg(this.sim, this.L);
      /* each edge's path and its length: the length is settled the moment the layout is, and asking a path for it once per
         token per frame was half of all the geometry a frame did — nine hundred calls on the larger models */
      this.pathCache = {};
      this.svg.querySelectorAll('path[data-edge]').forEach((p) => (this.pathCache[p.dataset.edge] = { el: p, len: p.getTotalLength() }));
      this.tokenEls = new Map(); /* the layer's elements go with the scene they were drawn into */
      this.dyn = {};
      this.dynHtml = {};
      this.svg.querySelectorAll('[data-dyn]').forEach((el) => {
        const key = el.dataset.dyn + (el.dataset.id ? ':' + el.dataset.id : '');
        this.dyn[key] = el;
      });
      /* the card frame, the retry arc and its label are static nodes the frame only toggles classes on */
      this.cardEls = {};
      for (const id of Object.keys(this.sim.nodes)) {
        this.cardEls[id] = {
          card: this.svg.querySelector(`[data-card="${id}"]`),
          retry: this.svg.querySelector(`[data-retry="${id}"]`),
          retryLabel: this.svg.querySelector(`[data-retrylabel="${id}"]`),
        };
      }
      this.logKey = null;
      this.logDrawn = null;
      this.logView = undefined;
      this.renderBar();
      this.renderFrame();
    }

    /* Replace a dynamic group's content only when it changed, so that an animation inside it keeps running. */
    setDyn(key, html) {
      if (this.dynHtml[key] === html) return;
      this.dynHtml[key] = html;
      this.dyn[key].innerHTML = html;
    }

    /* A manual packer sweep that makes nothing while the pool holds inputs deserves an explanation and the offer of a flush. */
    runPackerByHand(id) {
      const sim = this.sim;
      const node = sim.nodes[id];
      if (!node) return;
      const before = node.parcels.length;
      sim.runPacker(id);
      if (node.parcels.length !== before) return;
      if (![...node.inputs.values()].some((i) => i.status === 'Unassigned') || node.status !== 'Active') return;
      this.askFlush(id, true);
    }

    /* The same explanation from the input machine's edge, where a reader who never opened expert mode can reach it. A
       click there opens this rather than flushing: `write off · 34` says exactly what it will do, where a flush makes some
       number of undersized parcels the label cannot predict, so the one confirm is worth keeping. */
    askFlush(id, swept) {
      const node = this.sim.nodes[id];
      if (!node) return;
      const { html, flushable } = flushAskHtml(this.sim, node, !!swept);
      this.askNode = id;
      this.ask.querySelector('.wgsim-ask-body').innerHTML = html;
      this.ask.querySelector('[data-act="flush-confirm"]').hidden = !flushable;
      this.ask.showModal();
    }

    /* A box opens the list of its files, an output's newest first and a query's in the order it returns them; a click on one
       opens its lineage over it. The two boxes are the same component, so they open the same list. */
    openFiles(id) {
      if (!this.fileBox(id)) return;
      this.filesUi = { output: id, more: false };
      this.filesShown = null;
      this.renderFiles();
      if (!this.files.open) this.files.showModal();
    }

    /* The box a list of files belongs to, whichever end of the graph it sits at. */
    fileBox(id) {
      return (id != null && (this.sim.outputs[id] || this.sim.sources[id])) || null;
    }

    renderFiles() {
      const out = this.fileBox(this.filesUi.output);
      if (!out) return;
      const html = filesHtml(this.sim, out, this.filesUi);
      if (this.filesShown === html) return;
      this.filesShown = html;
      this.files.innerHTML = html;
    }

    /* A card opens its dialog on `status`, its backlog chip on the parcels' machine, and a lineage on the machine its button
       names; the header band opens the workgraph's. The panel follows the model while it is open. */
    openPanel(kind, id, machine) {
      const node = id != null ? this.sim.nodes[id] : null;
      if (id != null && !node) return;
      this.panel.dataset.kind = kind;
      this.panelNode = node ? id : null;
      if (node) {
        delete this.actionsTab[id]; /* the tab follows the state again each time the dialog opens */
        this.machineTab[id] = machine || 'status';
        /* and so does the strip under a machine: a dialog opens on everything, not on the box the last visit left */
        for (const t of MACHINE_TABS) delete this.machineList[listKey(id, t)];
      }
      this.wgTab = undefined;
      this.panel.querySelector('.wgsim-panel-title').textContent = node ? node.spec.label : this.sim.spec.name;
      this.panel.querySelector('.wgsim-panel-sub').textContent = `· ${node ? PANEL[kind].title : 'workgraph'}`;
      this.panel.querySelector('.wgsim-panel-body').innerHTML = PANEL[kind].html(this.sim, node, this);
      this.panelState();
      this.panel.showModal();
      /* the mode follows the reader in: the marks move to the dialog, and the ? panel, which is the picture's, gives way */
      this.disc = null;
      this.guideOn = false;
      this.renderMarks();
    }

    /* The dialog's header names the state of what it is about, once, live. */
    panelState() {
      const el = this.panel.querySelector('.wgsim-panel-state');
      const node = this.panelNode != null ? this.sim.nodes[this.panelNode] : null;
      const status = node ? node.status : this.sim.wg.status;
      if (el.textContent !== status) el.textContent = status;
      el.dataset.status = status;
      const kind = blockedKind(this.sim, node);
      if (kind) el.dataset.blocked = kind;
      else if (el.dataset.blocked) delete el.dataset.blocked;
    }

    /* A dialog has the screen: the model's keys belong to it rather than to the widget underneath. */
    dialogOpen() {
      return this.dialogs.some((d) => d.open);
    }

    /* The model holds still while a dialog is open, and while help mode is on: a mark sits over the region it explains, and a
       region that is moving is one the mark is no longer over. What the reader paused stays paused when the mode ends, since
       nothing here touches the pause itself. */
    held() {
      return this.dialogOpen() || this.helpOn;
    }

    /* Show where a file came from and what it became, three generations of provenance deep, with a click on a file moving the
       focus. The explanation stays folded from one file to the next once opened. */
    openLineage(fileId) {
      const f = this.sim.files.get(fileId);
      if (!f) return;
      this.lineageUi.file = fileId;
      this.renderLineage();
      if (!this.lineage.open) this.lineage.showModal();
    }

    renderLineage() {
      const f = this.lineageUi.file != null && this.sim.files.get(this.lineageUi.file);
      if (!f) return;
      const html = lineageHtml(this.sim, f, this.lineageUi);
      if (this.lineageShown === html) return;
      this.lineageShown = html;
      this.lineage.innerHTML = html;
    }

    setSetting(key, value) {
      this.sim.settings[key] = value;
      this.userSettings[key] = value;
    }

    /* The failed inputs waiting for the reader: the walkthrough's steps. An action to run is the lists dialog's. */
    inputSteps() {
      return this.sim.pending().filter((s) => s.kind === 'input');
    }

    /* The action lists as they run: the panel under the control band appears by itself when a list starts and holds one row of
       columns, in the order the run takes them, which drains to the left as the work is done — a list leaves a beat after it
       finishes, or at once if the run abandoned it, and the ones behind it slide into its place, so what is left on the row is
       what is left to do and the space at the right end is how much has been done. The panel goes when the row is empty, and
       the action sweep is slowed while a list is watched so the results can be read as they land. Collapsed, its header stays
       in place and the lists run at their own pace; a list of a new kind, the next phase of the run, opens it again. */
    renderLists() {
      const sim = this.sim;
      const ui = this.listsUi;
      if (ui.runs !== sim.runs) {
        ui.runs = sim.runs;
        ui.shown.clear();
        ui.collapsed = false;
      }
      const running = sim.runningLists();
      for (const l of running) {
        const k = `${l.id}:${l.key}`;
        if (ui.shown.has(k)) continue;
        const newKind = ![...ui.shown.values()].some((e) => e.key === l.key);
        ui.shown.set(k, { id: l.id, key: l.key, label: l.label, items: l.items, done: null });
        if (newKind) ui.collapsed = false;
      }
      const kept = keptLists([...ui.shown.values()], running, sim.t);
      if (kept.length < ui.shown.size) ui.shown = new Map(kept.map((e) => [`${e.id}:${e.key}`, e]));
      if (!ui.shown.size) {
        ui.collapsed = false;
        this.clearLists();
      } else this.paintLists(orderLists(sim, kept), ui.collapsed);
      sim.watchActions = ui.shown.size > 0 && !ui.collapsed && running.length > 0;
    }

    /* The row is patched rather than redrawn: each column is kept across frames under its own key and only what changed is
       touched, since a column replaced every frame — which is how every other surface here is drawn — could never be seen to
       move. What moved is then let go from where it was, so a list leaving pulls the row left instead of teleporting it; under
       `prefers-reduced-motion` the columns simply land. */
    paintLists(entries, collapsed) {
      const sim = this.sim;
      const head = listsHead(entries, collapsed);
      if (!this.listsRow) {
        this.lists.innerHTML = `${head}<div class="wgsim-lists-body"></div><div class="wgsim-lists-foot"></div>`;
        this.listsHeadHtml = head;
        this.listsRow = this.lists.querySelector('.wgsim-lists-body');
        this.listsFoot = this.lists.querySelector('.wgsim-lists-foot');
        this.listsCols = new Map();
        this.listsOrder = '';
        /* The row's width is watched rather than read while the frame draws: `renderFrame` writes the tokens, the pools and
           the bars and then asked the row how wide it was, which is a write-read-write and forces a layout flush on every
           frame the panel is up. The observer answers both of the reasons the count needs the width — the row's own content
           and the widget's, maximised or in the page — and the first reading is taken here, before any frame has drawn. */
        if (typeof ResizeObserver !== 'undefined') new ResizeObserver((rs) => (this.listsWidth = rs[rs.length - 1].contentRect.width)).observe(this.listsRow);
      } else if (this.listsHeadHtml !== head) {
        this.listsHeadHtml = head;
        this.lists.firstElementChild.outerHTML = head;
      }
      const foot = collapsed ? '' : listsFootInner(sim);
      if (this.listsFootHtml !== foot) {
        this.listsFootHtml = foot;
        this.listsFoot.innerHTML = foot;
      }
      this.listsFoot.hidden = !foot;
      this.listsRow.hidden = collapsed;
      if (this.lists.hidden) this.lists.hidden = false;
      /* the one reading taken in a frame, on the first frame the panel is up, since a row inside a hidden panel has no width
         and the observer has not run yet; after that the observer answers and nothing here reads the layout again */
      if (!this.listsWidth) this.measureRow();
      if (!collapsed) {
        const keys = entries.map((e) => `${e.id}:${e.key}`);
        /* what the row holds decides whether a column says its kind, so it is read off every list the panel holds rather than
           the ones that fit — the header names the kind on the same count */
        this.patchRow(sim, entries.slice(0, this.listsFit(keys)), entries.length, listKinds(entries) > 1);
      }
    }

    /* How many columns the row has room for: the widths it drew last time, laid end to end against the width it has now, and
       the floor for a column it has not drawn yet — one frame's grace, since a column is measured as soon as it is in. A
       column never grows into the space a list leaves, so a width measured once stands until that list's own content changes.
       The row is measured every frame rather than watched, because it changes with the widget's width as well as its own:
       maximised, it holds more. `+N` stands in a column's place, so it takes room of its own when there is a rest. */
    listsFit(keys) {
      const w = this.listsWidth;
      if (!(w > 0)) return keys.length; /* the panel has not been laid out yet: draw them all, and the next frame counts */
      const width = (k) => {
        const col = this.listsCols.get(k);
        return col && col.w ? col.w : LIST_ROW.col;
      };
      const room = (spare) => {
        let used = 0;
        let n = 0;
        for (const k of keys) {
          const next = used + (n ? LIST_ROW.gap : 0) + width(k);
          if (n && next > spare) break;
          used = next;
          n += 1;
        }
        return Math.max(1, n);
      };
      return room(w) >= keys.length ? keys.length : room(w - LIST_ROW.more - LIST_ROW.gap);
    }

    patchRow(sim, row, total, kind) {
      /* the row it wants, in order and each with its markup: the lists it has room for, and the count of the rest as a column
         of its own, so that the count is kept, moved and dropped by everything below exactly as a list is */
      const cols = [];
      for (const e of row) {
        const html = listSection(sim, e, kind);
        if (html) cols.push([`${e.id}:${e.key}`, html]); /* nothing to draw for an entity the model no longer holds */
      }
      if (total > cols.length) cols.push(['+', listsMore(total - cols.length)]);
      const order = cols.map(([k]) => k);
      const was = new Map();
      /* what moved is worked out from where the columns stood, and only on the frames the row's order changes: every other
         frame it is the same columns in the same places, and measuring them would cost a layout to learn nothing */
      if (this.listsOrder !== order.join(' ') && !this.reducedMotion) for (const el of this.listsRow.children) was.set(el.dataset.list, el.getBoundingClientRect().left);
      const measure = [];
      for (const [k, html] of cols) {
        const col = this.listsCols.get(k);
        if (!col) {
          this.listsCols.set(k, { el: element(html), html, w: 0 });
          measure.push(k);
        } else if (col.html !== html) {
          measure.push(k);
          /* a column is written as markup like everything else here, and its parts are copied onto the column that stands
             already, so the two forms of the panel cannot come to draw a list differently */
          const fresh = element(html);
          col.el.className = fresh.className;
          col.el.innerHTML = fresh.innerHTML;
          if (fresh.title) col.el.title = fresh.title;
          col.html = html;
        }
      }
      let at = this.listsRow.firstElementChild;
      for (const k of order) {
        const el = this.listsCols.get(k).el;
        if (el === at) at = at.nextElementSibling;
        else this.listsRow.insertBefore(el, at);
      }
      /* whatever the row still holds past the columns it wants is a list that has gone */
      while (at) {
        const next = at.nextElementSibling;
        this.listsCols.delete(at.dataset.list);
        at.remove();
        at = next;
      }
      /* what a column came out as, kept for the count above: only the ones drawn for the first time or redrawn are measured,
         since a column nothing changed cannot have changed width */
      for (const k of measure) {
        const col = this.listsCols.get(k);
        if (col && col.el.parentNode) col.w = Math.ceil(col.el.getBoundingClientRect().width);
      }
      for (const el of was.size ? this.listsRow.children : []) {
        const from = was.get(el.dataset.list);
        if (from == null) continue;
        const dx = from - el.getBoundingClientRect().left;
        if (!dx) continue;
        el.style.transition = 'none';
        el.style.transform = `translateX(${dx}px)`;
        void el.offsetWidth; /* the flush is what makes the line below a change to transition rather than a no-op */
        el.style.transition = '';
        el.style.transform = '';
      }
      this.listsOrder = order.join(' ');
    }

    /* The one read of the row's width, taken where a layout is going to happen anyway: when the panel is first built, and
       when the widget takes the window, which changes the row's width before the observer has had a frame to say so. */
    measureRow() {
      if (this.listsRow) this.listsWidth = this.listsRow.clientWidth - 2 * LIST_ROW.pad;
    }

    clearLists() {
      if (this.listsRow) {
        this.listsRow.textContent = '';
        this.listsCols.clear();
        this.listsOrder = '';
      }
      if (!this.lists.hidden) this.lists.hidden = true;
    }

    /* The matrix of what runs by hand, which the expert chip is the one way into. It is drawn from the model each time, so the
       count in its header, the dots on its aggregates and its cells cannot come to disagree with what the sim holds. */
    openModes() {
      if (!this.sim) return; /* the model is created when it is first scrolled to; the rails wait for it too */
      this.modesUi = { hover: null, focus: null };
      this.modesShown = null;
      this.fillModes();
      if (!this.modes.open) this.modes.showModal();
    }

    fillModes() {
      const html = modesHtml(this.sim);
      if (this.modesShown !== html) {
        this.modesShown = html;
        this.modes.innerHTML = html;
      }
      this.applyModeScope();
    }

    /* The scope under the pointer or the focus, marked on the cells it covers so the stylesheet can tint them and fade the
       rest. It is applied after every redraw rather than rendered into the html, so a click that changes the matrix does not
       drop the highlight the reader is still hovering. */
    applyModeScope() {
      const table = this.modes.querySelector('.wgsim-mx');
      if (!table) return;
      const sc = this.modesUi.hover || this.modesUi.focus;
      table.querySelectorAll('.wgsim-mx-in').forEach((el) => el.classList.remove('wgsim-mx-in'));
      if (!sc || !sc.kind) return table.removeAttribute('data-scope');
      table.setAttribute('data-scope', sc.kind);
      const sel = sc.kind === 'all' ? 'thead th, tr[data-entity] > *' : sc.kind === 'entity' ? `tr[data-entity="${sc.entity}"] > *` : `[data-sweep="${sc.sweep}"]`;
      table.querySelectorAll(sel).forEach((el) => el.classList.add('wgsim-mx-in'));
    }

    /* The walk through what waits: it opens by itself whenever something new waits and no other dialog is open, shows
       the first step the reader has not put off, closes by itself once nothing is left, and while it is closed the
       chip beside expert counts what waits. */
    openSteps() {
      const steps = this.inputSteps();
      const shown = steps.findIndex((s) => !this.deferred.has(stepKey(s)));
      if (shown < 0) return;
      this.stepsShown = null;
      this.fillSteps(steps, shown);
      if (!this.steps.open) this.steps.showModal();
    }

    /* The dialog keeps what the reader has done in it, the row picked and what they unfolded, until the step changes. */
    fillSteps(steps, shown) {
      const key = stepKey(steps[shown]);
      if (this.stepsUi.key !== key) this.stepsUi = { key, to: null, what: false, more: false };
      const html = stepsHtml(this.sim, steps, Object.assign({ shown }, this.stepsUi));
      if (this.stepsShown === html) return;
      this.stepsShown = html;
      this.steps.innerHTML = html;
    }

    renderSteps() {
      const steps = this.inputSteps();
      const keys = steps.map(stepKey);
      for (const k of this.deferred) if (!keys.includes(k)) this.deferred.delete(k);
      if (this.steps.open) {
        const shown = steps.findIndex((s) => !this.deferred.has(stepKey(s)));
        if (shown < 0) this.steps.close();
        else this.fillSteps(steps, shown);
      } else if (keys.some((k) => !this.stepsKeys.has(k)) && !this.held()) this.openSteps();
      this.stepsKeys = new Set(keys);
      const show = steps.length > 0 && !this.steps.open;
      if (this.stepsChip.hidden === show) this.stepsChip.hidden = !show;
      if (!show) return;
      const label = `${steps.length} waiting`;
      if (this.stepsChip.textContent !== label) this.stepsChip.textContent = label;
      const tip = `${steps.length} failed input${steps.length === 1 ? '' : 's'} waiting for HandleFailedInput by hand · click to walk through them`;
      if (this.stepsChip.title !== tip) this.stepsChip.title = tip;
    }

    /* Reflect the model's state in the toolbar. */
    renderBar() {
      if (!this.sim) return;
      const root = this.root;
      const armed = this.sim.wg.status === 'New';
      /* Why the model is not advancing, rather than that it is not: one class folded four situations into one and the
         stylesheet read none of them. `armed` is nothing having run yet, where the transport glows and the picture says
         nothing; the other three each get a word over the canvas. A reader who paused and then opened a dialog is still
         paused, since closing the dialog will not resume, so the pause outranks what covers it. */
      const hold = armed ? 'armed' : this.paused ? 'paused' : this.dialogOpen() ? 'dialog' : this.helpOn ? 'help' : '';
      if (hold) root.dataset.hold = hold;
      else if (root.dataset.hold) delete root.dataset.hold;
      const pause = root.querySelector('[data-act="pause"]');
      pause.setAttribute('aria-checked', String(this.paused && !armed));
      pause.title = armed ? 'start the model paused (space); a speed starts it running' : this.paused ? 'resume (space)' : 'pause (space)';
      root.querySelectorAll('[data-act="speed"]').forEach((b) => b.setAttribute('aria-checked', String(!armed && !this.paused && Number(b.dataset.speed) === this.speed)));
      const model = root.querySelector('.wgsim-lockup-model');
      const named = this.sim.spec.name ? ` · ${this.sim.spec.name}` : '';
      if (model.textContent !== named) model.textContent = named;
      const chips = {
        expert: String(this.expert),
        chaos: String(this.chaos),
        help: String(this.helpOn),
      };
      for (const [key, on] of Object.entries(chips)) root.querySelectorAll(`[data-chip="${key}"]`).forEach((el) => el.setAttribute('aria-checked', on));
    }

    /* A model the page has replaced: the frame loop ends by itself once the root has left the document, so disposing is
       taking the root out and letting go of the observer. The playground rebuilds on demand and would otherwise leak an
       observer and a model per build. */
    dispose() {
      if (this.io) this.io.disconnect();
      for (const d of this.dialogs) if (d.open) d.close();
      this.setMax(false); /* the body, the scroll and the place it held in the flow all go back first */
      if (this.root && this.root.parentNode) this.root.parentNode.removeChild(this.root);
      const k = widgets.indexOf(this);
      if (k >= 0) widgets.splice(k, 1);
    }

    observe() {
      if (typeof IntersectionObserver === 'undefined') {
        this.markSeen();
        return;
      }
      const io = new IntersectionObserver((entries) => {
        for (const e of entries) {
          this.visible = e.isIntersecting;
          if (e.isIntersecting) this.markSeen();
        }
      });
      this.io = io;
      io.observe(this.root);
    }

    frame(ts) {
      /* the widget has left the page — instant navigation replaced it — so the loop ends with it */
      if (!document.body.contains(this.root)) return;
      if (this.last == null) this.last = ts;
      const dt = Math.min(0.1, (ts - this.last) / 1000);
      this.last = ts;
      /* Nothing advances until the model has been laid out — which does not happen until it is
         scrolled to — but the loop keeps its place throughout. */
      if (this.L && !this.paused && !this.held() && this.visible) {
        const w = this.sim.wg.status;
        if (w !== 'New' && w !== 'Archived' && w !== 'Cleaned') this.advance(dt * this.speed);
      }
      requestAnimationFrame((t) => this.frame(t));
    }

    advance(seconds) {
      let rem = seconds;
      while (rem > 0) {
        const h = Math.min(0.05, rem);
        this.sim.step(h);
        rem -= h;
      }
      this.renderFrame();
    }

    tokenPoint(tk) {
      const p = this.pathCache[`${tk.from}>${tk.to}`];
      if (!p) return null;
      const u = Math.min(1, Math.max(0, (this.sim.t - tk.t0) / (tk.t1 - tk.t0)));
      const e = u < 0.5 ? 2 * u * u : -1 + (4 - 2 * u) * u;
      return p.el.getPointAtLength(p.len * e);
    }

    renderFrame() {
      if (!this.L) return;
      const sim = this.sim;
      const L = this.L;
      const spec = sim.spec;
      const t = sim.t;
      /* the header is drawn once per change, as one piece, so the strip is never in the DOM twice */
      const head = headHtml(sim, this.expert);
      if (this.headShown !== head) {
        this.headShown = head;
        this.head.innerHTML = head;
        this.head.classList.toggle('expert', this.expert);
      }
      if (this.files.open) this.renderFiles();
      if (this.modes.open) this.fillModes();
      if (this.lineage.open) this.renderLineage();
      if (this.panel.open && this.panel.dataset.kind === 'workgraph') {
        const body = this.panel.querySelector('.wgsim-panel-body');
        const html = PANEL.workgraph.html(sim, null, this);
        if (body.innerHTML !== html) body.innerHTML = html;
        this.panelState();
      }
      /* once per frame, not once per card: the members the state line names as holding the workgraph open for a person */
      const held = new Set(sim.heldMembers().map((n) => n.id));
      for (const node of Object.values(sim.nodes)) {
        const it = L.items[node.id];
        const c = sim.counts(node);
        /* the pool, or the action the member is running in its place: a list is eligible only once the member has drained,
           so the one thing the pool could be holding then is what that list is doing */
        const action = eligibleAction(node.lists, node.status);
        const pool = poolSvg(it, action ? [] : poolEntries(sim, node), node.id);
        this.setDyn('pool:' + node.id, action ? poolActionSvg(it, action) : pool.svg);
        const more = this.dyn['more:' + node.id];
        if (more.textContent !== pool.more) more.textContent = pool.more;
        const live = node.parcels.filter((p) => p.status === 'Reserved' || p.status === 'Assigned' || p.status === 'Completing').sort((a, b) => a.since - b.since);
        const recent = node.parcels.filter((p) => PARCEL_TERMINAL.has(p.status) && t - p.ended < FADE.glow).sort((a, b) => b.ended - a.ended);
        const queued = node.parcels.filter((p) => p.status === 'Unassigned').length;
        this.dyn['slots:' + node.id].innerHTML = slotsSvg(it, live.concat(recent).slice(0, node.spec.slots), t, node.spec.slots, queued, node.id);
        const info = sim.feederInfo(node);
        this.setDyn('bar:' + node.id, barSvg(it, node, sim));
        const bartip = barTip(node, info, sim);
        if (this.dyn['bartip:' + node.id].textContent !== bartip) this.dyn['bartip:' + node.id].textContent = bartip;
        this.dyn['status:' + node.id].textContent = node.status;
        this.dyn['statustitle:' + node.id].textContent = `the transformation is ${node.status}`;
        if (this.panel.open && this.panelNode === node.id) {
          const body = this.panel.querySelector('.wgsim-panel-body');
          const html = PANEL[this.panel.dataset.kind].html(sim, node, this);
          if (body.innerHTML !== html) body.innerHTML = html;
          this.panelState();
        }
        const els = this.cardEls[node.id];
        if (els.card) {
          els.card.dataset.status = node.status;
          const kind = blockedKind(sim, node);
          if (kind) els.card.dataset.blocked = kind;
          else if (els.card.dataset.blocked) delete els.card.dataset.blocked;
          els.card.classList.toggle('wg-picked', this.logFilter === node.id);
        }
        if (L.rail) {
          const states = railStates(sim, node.id);
          for (const row of RAIL_ORDER.node) {
            const el = this.dyn[`rail-${row}:` + node.id];
            const st = states[row];
            if (el.dataset.state !== st.state) el.dataset.state = st.state;
            const mode = this.dyn[`railmode-${row}:` + node.id];
            if (mode.textContent !== st.mode) mode.textContent = st.mode;
            const tip = this.dyn[`railtip-${row}:` + node.id];
            if (tip.textContent !== st.tip) tip.textContent = st.tip;
          }
        }
        /* the pool waits for a person over a quarantine, a failed action, and a hold of its own that keeps the workgraph
           open: each of the three is something the region itself is showing, and the pool says which rather than leaving
           the reader to find out by opening the dialog */
        const wants = poolWants(sim, node, c, held);
        this.dyn['pool:' + node.id].parentNode.classList.toggle('wg-glow', !!wants);
        const poolTip = wants ? wants.why : 'the inputs waiting for the packer';
        if (this.dyn['pooltitle:' + node.id].textContent !== poolTip) this.dyn['pooltitle:' + node.id].textContent = poolTip;
        const retrying = t - node.lastRetryAt < FADE.retry;
        if (els.retry) els.retry.classList.toggle('on', retrying);
        if (els.retryLabel) els.retryLabel.classList.toggle('on', retrying);
      }
      for (const out of Object.values(sim.outputs)) {
        const it = L.items[out.id];
        const el = this.dyn['output:' + out.id];
        if (!el) continue;
        const n = out.files.length;
        const parts = [];
        if (out.spec.show === 'histogram') {
          parts.push(`<text x="${it.x + 14}" y="${it.y + 24}" class="wg-box-title"><tspan class="wg-strong">${n}</tspan> entries</text>`);
          const bins = new Array(8).fill(0);
          const colourCount = {};
          for (const f of out.files) {
            bins[Math.min(7, Math.floor((f.size / 5) * 8))]++;
            colourCount[f.colour] = (colourCount[f.colour] || 0) + 1;
          }
          const top = Object.keys(colourCount).sort((a, b) => colourCount[b] - colourCount[a])[0];
          const fill = top == null ? '#999' : PALETTE[top];
          const max = Math.max(1, ...bins);
          bins.forEach((b, k) => {
            const h = (b / max) * 56;
            parts.push(`<rect x="${it.x + 16 + k * 18}" y="${it.y + 98 - h}" width="13" height="${h}" rx="2" fill="${fill}"/>`);
          });
        } else {
          /* one line, the count: the files themselves are behind a click, newest first */
          const merged = n === 1 && out.files[0].merged;
          /* the sink exists before it holds anything, so a zero is drawn, muted */
          parts.push(`<text x="${it.x + it.w / 2}" y="${it.y + it.h / 2 + 4.5}" text-anchor="middle" class="wg-out-count${n ? '' : ' wg-out-empty'}"><tspan class="wg-out-n">${n}</tspan> ${merged ? `merged from ${merged}` : `file${n === 1 ? '' : 's'}`} <tspan class="wg-out-more">›</tspan></text>`);
        }
        this.setDyn('output:' + out.id, parts.join(''));
      }
      /* The layer holds one element per token and the frame moves it, rather than writing the whole layer as a string: four
         hundred files are in flight at once on the larger models, and rebuilding that was ninety thousand characters reparsed
         a frame for what is a translate on each of them. The token object is the key, so a file bound for several consumers,
         which travels each branch as a token of its own, keeps a shape of its own on each; where two of them meet at a
         junction they are two identical opaque shapes at one point, which is what one of them looked like. */
      const live = new Set();
      if (!this.reducedMotion) {
        for (const tk of sim.tokens) {
          const pt = this.tokenPoint(tk);
          if (!pt) continue;
          live.add(tk);
          let el = this.tokenEls.get(tk);
          if (!el) {
            el = tokenSvg(tk);
            this.tokenEls.set(tk, el);
            this.dyn.tokens.appendChild(el);
          }
          el.setAttribute('transform', `translate(${pt.x.toFixed(1)} ${pt.y.toFixed(1)})`);
        }
      }
      for (const [tk, el] of this.tokenEls) {
        if (live.has(tk)) continue;
        el.remove();
        this.tokenEls.delete(tk);
      }
      this.setDyn('edgechips', edgeChipsSvg(sim, L));
      this.renderControls();
      this.renderLists();
      this.renderSteps();
      this.renderLog();
      this.renderLogNow();
      this.renderSaid();
      if (this.helpOn) this.renderMarks();
    }

    /* What the last press moved, on a foot of the log's own. The log is the model's record and this is the widget's, so it
       is not written into `sim.log`: two readers on one seed would otherwise hold different logs of it, and a run stepped
       and a run played would not be the same run, which is the thing the slices were chosen to keep true. It sits outside
       the list for the reason the clock does — it never touches the append path, the follow-the-newest test or the
       `\u2193 latest` pill — and it is a live region, a press being the one control here that announces nothing by moving.
       The band gives back the line and a half it costs the moment the model runs again. */
    renderSaid() {
      const said = this.paused ? this.said : null;
      if (this.saidShown === (said ? said.text : null)) return;
      this.saidShown = said ? said.text : null;
      this.logWrap.classList.toggle('wgsim-said', !!said);
      if (!said) { this.logSaid.textContent = ''; return; }
      this.logSaid.innerHTML = `<span class="wgsim-log-said-t">+${fmt(said.took)}s</span><span class="wgsim-log-said-m">${esc(said.text)}</span>`;
      this.logSaid.title = said.title;
    }

    /* Pinned to the foot of the log: the run's clock and the phase it is in, so that the timeline above it is anchored to
       something. The elapsed time is nowhere else in the widget, and the phase, restated at the far end of a column that
       runs the whole height of the window while the strip sits at the top of the other one, anchors rather than repeats.
       The pill is `statePill`, so it is the strip's pill by construction. It lives outside the list, so it never touches
       the append path, the follow-the-newest test or the ↓ latest pill; the stylesheet shows it only in that column, since
       in an 11em band a footer would eat one and a half of nine visible lines. */
    renderLogNow() {
      const sim = this.sim;
      const html = `<span class="wgsim-log-now-t">${fmt(sim.t)}s</span>${statePill(sim.wg.status, { on: true })}`;
      if (this.logNowHtml === html) return;
      this.logNowHtml = html;
      this.logNow.innerHTML = html;
    }

    /* The event log under the picture: oldest at the top, newest at the bottom. The time heads the lines that
       happened at it, on a divider of its own, so everything of one instant reads as one group, and the three
       levels of emphasis let the transitions carry a run on their own. A sparse stretch gives a divider per
       line; knowing a bucket's size before drawing it would mean holding the bucket back a frame, which is
       append-only given up for very little, and the divider carries the time either way.

       The lines that are not drawn yet are appended rather than the log being drawn again, so an event
       costs one row however long the run is, and an unpinned reader keeps their place without being held
       there. Only a new filter, or a model run again, starts the log over. */
    renderLog() {
      const filter = this.logFilter;
      const log = this.sim.log;
      const refilter = this.logView !== filter;
      if (this.logKey === this.sim.logSeq && !refilter) return;
      this.logKey = this.sim.logSeq;
      this.logView = filter;
      this.logOnly.hidden = !filter;
      if (filter) this.logOnly.textContent = `${filter} ✕`;
      const restart = refilter || this.logDrawn == null || this.sim.logSeq < this.logDrawn;
      if (restart) {
        this.logDrawn = 0;
        this.logStamp = '';
        this.logEl.textContent = '';
      }
      const rows = [];
      /* the log holds the whole run, so the entry at index n is the (n + 1)th event */
      for (let k = this.logDrawn; k < log.length; k++) {
        const e = log[k];
        if (filter && e.subject !== filter) continue;
        const time = `${fmt(e.t)}s`;
        if (time !== this.logStamp) rows.push(logTimeHtml(time));
        this.logStamp = time;
        rows.push(logRowHtml(e));
      }
      this.logDrawn = log.length;
      if (rows.length) {
        const empty = this.logEl.firstElementChild;
        if (empty && empty.classList.contains('wgsim-log-empty')) empty.remove();
        this.logEl.insertAdjacentHTML('beforeend', rows.join(''));
      } else if (restart && !this.logEl.firstElementChild) {
        this.logEl.innerHTML = `<li class="wgsim-log-empty">${filter ? `nothing from ${esc(filter)} yet` : 'no events yet'}</li>`;
      }
      if (this.pinned) this.logEl.scrollTop = this.logEl.scrollHeight;
    }

    /* Clicking a card shows only its lines, clicking it again shows them all; either way the log re-pins. */
    setLogFilter(id) {
      const next = id && id !== this.logFilter ? id : null;
      if (next === this.logFilter) return;
      this.logFilter = next;
      this.pinned = true;
      this.logPill.hidden = true;
      this.renderLog();
    }

    pinLog() {
      this.pinned = true;
      this.logPill.hidden = true;
      this.logEl.scrollTop = this.logEl.scrollHeight;
    }

    /* The card a log line is about, lit while the pointer rests on the line. */
    highlight(id) {
      if (!this.cardEls || this.hovered === id) return;
      this.hovered = id;
      for (const key of Object.keys(this.cardEls)) {
        const els = this.cardEls[key];
        if (els.card) els.card.classList.toggle('wg-hi', key === id);
      }
    }

    renderControls() {
      const html = controlsHtml(this.sim);
      if (this.actions.innerHTML !== html) this.actions.innerHTML = html;
    }
  }

  /* ------------------------------------------------------------------ */
  /* Mounting                                                            */
  /* ------------------------------------------------------------------ */

  /* One block, one model: a block holding a list of them used to draw tabs, which asked the reader
     to find the differences between two pictures they could not see at once. Alternatives belong in
     the prose around the block, each with its own model.

     A block that pins no seed is given a random one here, so each load of the page runs the workgraph
     differently: an input that fails three times and lands in Problematic, or a quarantine that holds a
     member's drain and keeps the workgraph Active, happen on some loads and not others. The engine keeps
     its own default of 1, which is what the checks build on, and `reset` reseeds from `spec.seed` plus the
     run count, so this only moves where that sequence starts. */
  function parseSpec(text) {
    const value = new Function('return (' + text + ')')();
    if (Array.isArray(value)) throw new Error('a workgraph block holds one model, not a list; give each model its own block');
    /* The sign-off is the one thing in a model that is the reader's to give, so a block cannot sign itself off: `approval` is the
       checks' switch alone, and a block that carries it says so rather than running a model nobody approved. */
    if (value && value.workgraph && value.workgraph.approval) throw new Error('approval is not a block key: a sign-off waits for the reader, and only the checks pass it, on the specs they build');
    if (value && value.seed == null) value.seed = 1 + Math.floor(Math.random() * 1000000);
    return value;
  }

  function mount(el) {
    let spec;
    try {
      spec = parseSpec(el.textContent);
    } catch (e) {
      el.textContent = 'workgraph spec error: ' + e.message;
      return null;
    }
    return mountSpec(el, spec);
  }

  const widgets = [];

  /* A model from a spec rather than from a block of text: what the playground mounts, since its spec is compiled from a
     CWL document and never written as markup. `mount` is this plus the parse, and both keep the widget in `widgets`. */
  function mountSpec(el, spec) {
    let widget;
    try {
      widget = new Widget(el, spec);
    } catch (e) {
      el.textContent = 'workgraph error: ' + e.message;
      return null;
    }
    widgets.push(widget);
    return widget;
  }

  function mountAll() {
    /* A maximised widget sits in the body, so instant navigation replaces the page under it and leaves it floating over the
       next one with the document still held. Its spacer marks where it belongs: the spacer gone from the document is the
       page gone, and the widget goes with it. */
    for (const w of widgets.slice()) if (w.maxOn && w.spacer && !document.body.contains(w.spacer)) w.dispose();
    document.querySelectorAll('.workgraph').forEach((el) => {
      if (el.dataset.wgMounted) return;
      el.dataset.wgMounted = '1';
      mount(el);
    });
  }

  const api = { Sim, layout, parseSpec, mount, mountSpec, mountAll, widgets, WG_MACHINE, NODE_MACHINE, MACHINE, PARCEL_MACHINE, blockedStates, edgeCarrying, foldedCount, eligibleAction, railStates, railCells, rowWork, modesHtml, modeDot, scopeCells, MATRIX_SWEEPS, MODE_DOTS, RAIL_ORDER, cardSvg, stepsHtml, listsHtml, listSection, listsRow, keptLists, orderLists, LIST_ROW, lineageHtml, lineageInputsHtml, stepKey, fitTitle, LIN, edgeBacklog, logText, logRowHtml, logKindRow, HOLD_WORDS, stateCard, wgEdges, wgOps, controlsHtml, headHtml, statesHtml, statePill, wgPillsHtml, filesHtml, workgraphHtml, wgMachineHtml, nodeMachineHtml, parcelMachineHtml, lineageStripSvg, detailsHtml, inputsHtml, feederHtml, hooksHtml, actionsHtml, parcelsHtml, machineHtml, poolSvg, poolCells, poolEntries, poolEntrySvg, slotLegendHtml, slotSwatch, slotBoxSvg, barSegmentSvg, machineLegend, machineBoxSvg, poolWants, stepModel, stepMark, stepSnap, stepSaid, STEP, SAID, machineListHtml, listRank, CHIPS, parcelStand, guideHtml, disclosureHtml, blockedKind, DISCLOSURES, DISCLOSURE_ORDER, PANEL_ORDER, ANCHORS, SLOT_CLS, SLOT_GLOW, POOL_CLS, INPUT_ROWS, PARCEL_ROWS, MEMBER_ROWS, RESULTS, RESULT_ORDER, LINE_RESULTS, MACHINE_TONES, PILL_TONES, LOG_KINDS, LOG_TONE, BAR_ORDER, CONTROLS, NOUNS, BODY, RAIL, OUT, MV, MB, PALETTE, T, FADE, WG_STATES, IDENTITIES };
  root.WorkgraphSim = api;
  if (typeof document !== 'undefined') {
    if (root.document$ && typeof root.document$.subscribe === 'function') root.document$.subscribe(() => mountAll());
    else if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mountAll);
    else mountAll();
  }
})(typeof window !== 'undefined' ? window : globalThis);
