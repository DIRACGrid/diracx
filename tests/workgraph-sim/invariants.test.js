// The invariants that keep a drawing honest, and the hygiene that keeps the source honest.
//
// Two kinds of check live here. The first reconciles the model against the pictures that claim to show it: a machine draws
// every transition the engine can record, and its boxes and edges agree about how many things are where. A count the engine
// keeps and no edge draws is worse than a missing feature — the picture is confidently wrong, and nothing says so. The second
// is hygiene the eye cannot do: a selector matching nothing, a class nobody styles, a constant that has to agree with one in
// the other file.
(function (g) {
  g.WG_TEST.suite('invariants', async () => {
  const { check, until, readText, writeOffQuarantine, HANG } = g.WG_TEST;
  const E = g.WorkgraphSimEngine;
  const W = g.WorkgraphSim;
  const { Sim, PARCEL_STATES, PARCEL_TERMINAL, INPUT_STATES, NODE_STATES, WG_ALL_STATES } = E;

  const src = readText('docs/assets/js/workgraph-sim.js');
  const engineSrc = readText('docs/assets/js/workgraph-sim-engine.js');
  const css = readText('docs/assets/css/workgraph-sim.css');
  const pgCss = readText('docs/assets/css/playground.css');
  const pgSrc = readText('docs/assets/js/playground.js');

  /* Drive every operator action from every state it is legal in, and collect what the engine recorded. Kept small and quick:
     the point is to reach each edge once, not to run a workgraph. */
  function sweep() {
    const node = new Map();
    const wg = new Map();
    const parcel = new Map();
    const sims = [];
    const take = (sim, why) => {
      sims.push(sim);
      for (const n of Object.values(sim.nodes)) {
        for (const k of Object.keys(n.statusTransitions)) if (!node.has(k)) node.set(k, why);
        for (const k of Object.keys(n.parcelTransitions)) if (!parcel.has(k)) parcel.set(k, why);
      }
      for (const k of Object.keys(sim.wg.transitions)) if (!wg.has(k)) wg.set(k, why);
    };
    /* a Problematic input holds the drain until an operator moves it (DX-ADR-005), so the sweep writes each quarantine off
       as soon as it is all that holds a member — otherwise the workgraph never leaves Active and the later states are never
       reached at all */
    const run = (sim, pred, limit) => {
      for (let i = 0; i < (limit || 30000) && !pred(sim); i++) {
        sim.step(0.05);
        writeOffQuarantine(sim);
      }
      return pred(sim);
    };
    const base = (extra) =>
      Object.assign(
        {
          name: 'sweep',
          transformations: {
            a: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0.3, partial: 0.3, submitFail: 0.2 },
            b: { feeder: { from: 'a' }, packer: { size: 1 }, run: [0.3, 0.6], hold: 'approval' },
          },
          outputs: { out: { from: 'b' } },
        },
        extra
      );
    const blocking = (kind, extra) => {
      const raw = base(extra);
      for (const n of Object.values(raw.transformations)) n[kind] = [{ name: kind, fail: true }];
      return raw;
    };

    /* the operator cancels from each state DX-ADR-005 allows, so every stub into the trunk is taken */
    for (const at of ['New', 'Scouting', 'Approving', 'ApprovingBlocked', 'Active', 'Finalizing']) {
      const raw = at === 'New' ? base({}) : base({ workgraph: { scouting: { count: 4 }, approving: [{ name: 'chk', fail: at === 'ApprovingBlocked' }], approval: 'auto' } });
      const sim = new Sim(raw);
      if (at !== 'New') sim.start();
      if (!run(sim, (s) => s.wg.status === at)) continue;
      sim.cancel();
      run(sim, (s) => s.wg.status === 'Cleaned');
      take(sim, `cancel at ${at}`);
    }
    /* every list blocked and then forced or reset, so each blocked counterpart is entered and left */
    for (const kind of ['finalize', 'archive']) {
      const sim = new Sim(blocking(kind, { workgraph: { approval: 'auto' } }));
      sim.start();
      run(sim, (s) => Object.values(s.nodes).some((n) => n.status.endsWith('Blocked')));
      for (let i = 0; i < 600; i++) {
        if (sim.blocked) sim.forceAction();
        sim.step(0.05);
      }
      take(sim, `blocked ${kind}, forced`);
    }
    {
      const sim = new Sim(blocking('clean', { workgraph: { approval: 'auto' } }));
      sim.start();
      run(sim, (s) => s.wg.status === 'Active');
      sim.cancel();
      for (let i = 0; i < 600; i++) {
        if (sim.blocked) sim.forceAction();
        sim.step(0.05);
      }
      take(sim, 'cancel with a blocked cleaning list');
    }
    /* cancelled while a member is blocked on its finalizing list, which is a transition out of a blocked counterpart to
       somewhere that is not its partner — the one shape the fold exists for, and the one a sweep is most likely to miss */
    {
      const sim = new Sim(blocking('finalize', { workgraph: { approval: 'auto' } }));
      sim.start();
      run(sim, (s) => Object.values(s.nodes).some((n) => n.status === 'FinalizingBlocked'));
      sim.cancel();
      run(sim, (s) => s.wg.status === 'Cleaned');
      take(sim, 'cancel with a member blocked on its finalizing list');
    }
    /* the two backward edges, drain, halt, and a member paused and resumed */
    {
      const sim = new Sim(blocking('finalize', { workgraph: { approval: 'auto' } }));
      sim.start();
      run(sim, (s) => Object.values(s.nodes).some((n) => n.status === 'FinalizingBlocked'));
      sim.resumeFromFinalizing();
      run(sim, (s) => s.wg.status === 'Archived');
      take(sim, 'back to Active out of FinalizingBlocked');
    }
    for (const [what, act] of [['drain', (s) => s.drain()], ['halt', (s) => s.halt()], ['pause and resume', (s) => { s.toggleNode('a'); s.step(0.05); s.toggleNode('a'); }]]) {
      const sim = new Sim(base({ workgraph: { scouting: { count: 4 }, approval: 'auto' } }));
      sim.start();
      run(sim, (s) => s.wg.status === 'Active');
      act(sim);
      run(sim, (s) => s.wg.status === 'Archived');
      take(sim, what);
    }
    {
      const sim = new Sim(base({ workgraph: { scouting: { count: 4 }, approving: [{ name: 'chk', check: 'success rate' }], approval: 'auto' }, transformations: Object.assign(base({}).transformations, { a: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0.9, retries: 0 } }) }));
      sim.start();
      if (run(sim, (s) => s.wg.status === 'ApprovingBlocked')) {
        sim.extendScout();
        run(sim, (s) => s.wg.status === 'Archived');
      }
      take(sim, 'scout further out of ApprovingBlocked');
    }
    return { node, wg, parcel, sims };
  }

  const taken = sweep();
  const sweepSims = taken.sims;

  // ---- every transition the engine records is drawn by the machine that claims to show it ----
  //
  // A machine's boxes are its states and its edges are its transitions, so a `from>to` pair the engine counts and the machine
  // has no edge for is a count that lands nowhere: the operator takes an edge and the picture still says it was never taken.
  // A blocked counterpart has no edges of its own beyond the pair it shares with its partner, so its outbound transitions are
  // drawn on the partner's edge; `blockedStates` is where that relation is declared, and both machines read it.
  {
    const drawnEdges = (spec) => new Set(spec.edges.map((e) => `${e.from}>${e.to}`));
    /* `edgeCarrying` is the drawing's own answer to "which edge shows this transition", so the check cannot drift from the
       rule it checks: restating the rule here would just be the same bug written twice. */
    const audit = (label, spec, recorded) => {
      const missing = [];
      for (const [key, why] of recorded) {
        const [from, to] = key.split('>');
        if (from === 'born' || to === 'removed') continue; /* births have their own edges, removals are drawn nowhere by design */
        if (!W.edgeCarrying(spec, from, to)) missing.push(`${key} (${why})`);
      }
      check(!missing.length, `the ${label} draws every transition the engine records: missing ${missing.join('; ')}`);
      return drawnEdges(spec);
    };
    check(taken.node.size > 12 && taken.wg.size > 12, `the sweep reached the machines: ${taken.node.size} member and ${taken.wg.size} workgraph transitions`);
    const nodeDrawn = audit('member machine', W.NODE_MACHINE, taken.node);
    const wgDrawn = audit('workgraph machine', W.WG_MACHINE, taken.wg);
    audit('parcel machine', W.PARCEL_MACHINE, taken.parcel);

    /* and no machine draws an edge between states its enum does not have, which is how a typo in a spec hides */
    const endpoints = (drawn) => [...new Set([...drawn].flatMap((k) => k.split('>')))];
    for (const [label, drawn, states] of [['member machine', nodeDrawn, NODE_STATES], ['workgraph machine', wgDrawn, WG_ALL_STATES], ['parcel machine', drawnEdges(W.PARCEL_MACHINE), PARCEL_STATES], ['input machine', drawnEdges(W.MACHINE), INPUT_STATES]]) {
      const stray = endpoints(drawn).filter((s) => !states.includes(s));
      check(!stray.length, `every edge of the ${label} joins two live states: stray ${stray}`);
    }
    /* the same for the boxes each machine places */
    for (const [label, names, states] of [['member machine', [...W.NODE_MACHINE.spine, ...W.NODE_MACHINE.held.map((h) => h.state), ...W.NODE_MACHINE.below.map((h) => h.state), 'Cancelling', 'Cleaned'], NODE_STATES], ['workgraph machine', [...W.WG_MACHINE.spine, ...W.WG_MACHINE.held.map((h) => h.state), 'Cancelling', 'Cleaned'], WG_ALL_STATES], ['parcel machine', Object.keys(W.PARCEL_MACHINE.states), PARCEL_STATES], ['input machine', Object.keys(W.MACHINE.states), INPUT_STATES]]) {
      const stray = names.filter((s) => !states.includes(s));
      check(!stray.length, `every box of the ${label} is a live state: stray ${stray}`);
    }

    /* Landing somewhere is not enough — it has to land exactly once. Summed per destination state, what the machine's edges
       carry has to equal what the engine recorded arriving there: a transition dropped makes the picture understate, and one
       folded onto a partner's edge while keeping its own makes it overstate, and both look perfectly reasonable on screen. */
    const reconcileEdges = (label, spec, recorded, transitions) => {
      const arriving = new Map();
      for (const key of recorded.keys()) {
        const [from, to] = key.split('>');
        if (from === 'born' || to === 'removed') continue;
        arriving.set(to, (arriving.get(to) || 0) + (transitions[key] || 0));
      }
      const carried = new Map();
      const count = W.foldedCount(spec, transitions);
      for (const e of spec.edges) carried.set(e.to, (carried.get(e.to) || 0) + count(e.from, e.to));
      const bad = [];
      for (const to of arriving.keys()) if ((carried.get(to) || 0) !== arriving.get(to)) bad.push(`${to}: ${arriving.get(to)} arrived but the edges carry ${carried.get(to) || 0}`);
      check(!bad.length, `the ${label} shows each transition exactly once: ${bad.join('; ')}`);
    };
    for (const sim of sweepSims) {
      for (const n of Object.values(sim.nodes)) {
        reconcileEdges('member machine', W.NODE_MACHINE, taken.node, n.statusTransitions);
        reconcileEdges('parcel machine', W.PARCEL_MACHINE, taken.parcel, n.parcelTransitions);
      }
      reconcileEdges('workgraph machine', W.WG_MACHINE, taken.wg, sim.wg.transitions);
    }

    /* a counterpart is folded onto a partner that exists, and never onto a state that has edges of its own */
    for (const [label, spec] of [['member machine', W.NODE_MACHINE], ['workgraph machine', W.WG_MACHINE]]) {
      for (const [state, partner] of W.blockedStates(spec)) {
        check(spec.spine.includes(partner) || partner === 'Cancelling', `${label}: ${state} folds onto ${partner}, which is on the spine`);
      }
      const off = [...spec.held, ...spec.below].filter((h) => !h.blocked).map((h) => h.state);
      for (const state of off) {
        const own = spec.edges.filter((e) => e.from === state && e.to !== state);
        check(own.length > 0, `${label}: ${state} is not folded, so it carries its own edges`);
      }
    }
  }

  // ---- a machine's boxes and its edges agree: inbound minus outbound is the occupancy ----
  //
  // The engine checks this after every step for inputs (`settings.verify`). The parcel machine makes the same claim — boxes
  // hold the occupancy, edges the cumulative transitions — so it earns the same check, births and removals included.
  {
    const net = (transitions, state) => {
      let n = 0;
      for (const [key, count] of Object.entries(transitions)) {
        const [from, to] = key.split('>');
        if (to === state) n += count;
        if (from === state) n -= count;
      }
      return n;
    };
    const reconcile = (sim, why) => {
      const bad = [];
      for (const node of Object.values(sim.nodes)) {
        const c = sim.parcelCounts(node);
        for (const st of PARCEL_STATES) {
          const want = c[st] || 0;
          const got = net(node.parcelTransitions, st);
          if (want !== got) bad.push(`${node.id}.${st}: ${want} there but the transitions net ${got}`);
        }
      }
      check(!bad.length, `parcel transitions reconcile with the occupancy ${why}: ${bad.join('; ')}`);
    };
    /* The default is the harness's hang ceiling. What the checks below assert is termination, so a budget fitted to what
       these runs take today would fail on data that shifted while the model was fine. */
    /* the operator writes off each quarantine as the sweep above does: a Problematic input holds the drain (DX-ADR-005),
       so without it these models never leave Active and the check turns on whether the run happened to quarantine anything */
    const run = (sim, pred, limit) => {
      for (let i = 0; i < (limit || HANG) && !pred(sim); i++) {
        sim.step(0.05);
        writeOffQuarantine(sim);
      }
      return pred(sim);
    };
    const raw = { name: 'reconcile', workgraph: { approval: 'auto' }, transformations: { a: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0.3, partial: 0.3, submitFail: 0.2 } }, outputs: { out: { from: 'a' } } };

    const mid = new Sim(raw);
    mid.start();
    run(mid, (s) => s.nodes.a.parcels.some((p) => p.status === 'Assigned'));
    reconcile(mid, 'in flight');

    /* to Archived, whose archiving list carries the DX-ADR-006 example's `clean`: the rows go and the history stays */
    /* An action with `effect: 'clean'` empties the rows, and records each removal as an outbound transition, so the check
       holds over one: the boxes go to nothing and the edges keep the history. The default archiving list carries the
       DX-ADR-006 example's `clean`, so every run that reaches Archived passes through this. */
    const done = new Sim(raw);
    done.start();
    run(done, (s) => s.wg.status === 'Archived');
    check(done.wg.status === 'Archived', 'the reconciling model reaches Archived, whose archiving list cleans the rows');
    reconcile(done, 'once the archiving list has cleaned the rows');

    const cancelled = new Sim(raw);
    cancelled.start();
    run(cancelled, (s) => s.wg.status === 'Active');
    for (let i = 0; i < 60; i++) cancelled.step(0.05);
    cancelled.cancel();
    run(cancelled, (s) => s.wg.status === 'Cleaned');
    check(cancelled.wg.status === 'Cleaned', 'and the cancelled model reaches Cleaned, whose cleaning list cleans the rows');
    reconcile(cancelled, 'once the cleaning list has run');

    /* the pruning of a long parcel list keeps the terminal counters, which is what makes the check hold over a long run */
    const long = new Sim({ name: 'pruned', workgraph: { approval: 'auto' }, transformations: { a: { feeder: { seeds: 500 }, packer: { size: 1 }, run: [0.05, 0.1] } } });
    long.start();
    run(long, (s) => s.nodes.a.parcels.length > 400, 40000);
    check(long.nodes.a.parcels.length > 400, 'the long model filled its parcel list');
    for (let i = 0; i < 200; i++) long.step(0.05);
    reconcile(long, 'after the parcel list was pruned');
  }

  /* the `d` of a machine edge, flattened to segments: M, L and C, all absolute, which is all these pictures use. Read by the
     two checks below, which ask different questions of the same geometry. */
  const flatten = (d) => {
    const pts = [];
    let cur = null;
    for (const cmd of d.match(/[MLC][^MLC]*/g) || []) {
      const n = (cmd.slice(1).match(/-?[\d.]+/g) || []).map(Number);
      if (cmd[0] === 'M') pts.push((cur = { x: n[0], y: n[1] }));
      else if (cmd[0] === 'L') for (let i = 0; i + 1 < n.length; i += 2) pts.push((cur = { x: n[i], y: n[i + 1] }));
      else {
        const [x1, y1, x2, y2, x3, y3] = n;
        const p0 = cur;
        for (let k = 1; k <= 16; k++) {
          const u = k / 16;
          const v = 1 - u;
          pts.push({ x: v * v * v * p0.x + 3 * v * v * u * x1 + 3 * v * u * u * x2 + u * u * u * x3, y: v * v * v * p0.y + 3 * v * v * u * y1 + 3 * v * u * u * y2 + u * u * u * y3 });
        }
        cur = { x: x3, y: y3 };
      }
    }
    return pts.slice(1).map((q, i) => [pts[i], q]);
  };

  // ---- a hand-placed machine crosses only what it says it crosses ----
  //
  // Where two edges cross, a reader cannot tell which way either goes, so these pictures are placed by hand to avoid it. That
  // is a property of the path strings and nothing checks it as they are edited, which is how a new edge routed over the top of
  // the picture looks fine in the diff and lands on the failure edge. The input machine keeps one crossing on purpose, where a
  // release drops through the gap between its columns; the parcel machine keeps none.
  {
    /* a proper crossing; two edges that meet at a box share an endpoint and are not one */
    const crosses = (a, b) => {
      const side = (p, q, r) => (q.x - p.x) * (r.y - p.y) - (q.y - p.y) * (r.x - p.x);
      for (const p of a) for (const q of b) if (Math.abs(p.x - q.x) < 2 && Math.abs(p.y - q.y) < 2) return false;
      const [d1, d2, d3, d4] = [side(b[0], b[1], a[0]), side(b[0], b[1], a[1]), side(a[0], a[1], b[0]), side(a[0], a[1], b[1])];
      return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0));
    };
    const crossingsOf = (spec) => {
      const edges = spec.edges.map((e) => ({ id: `${e.from}>${e.to}`, segs: flatten(e.d) }));
      const found = new Set();
      for (let i = 0; i < edges.length; i++) {
        for (let j = i + 1; j < edges.length; j++) {
          for (const a of edges[i].segs) for (const b of edges[j].segs) if (crosses(a, b)) found.add(`${edges[i].id} x ${edges[j].id}`);
        }
      }
      return [...found];
    };
    const parcels = crossingsOf(W.PARCEL_MACHINE);
    check(!parcels.length, `the parcel machine crosses nothing: ${parcels.join(', ')}`);
    const inputs = crossingsOf(W.MACHINE);
    check(inputs.length === 1 && inputs[0] === 'Failed>Split x Assigned>NotProcessed', `the input machine keeps its one crossing and no other: ${inputs.join(', ')}`);
  }

  // ---- nor runs alongside another close enough to read as one line ----
  //
  // Two edges that cross are caught above; two that lie along each other are not, and are worse, since neither is visible at
  // all. It is also what the crossing check cannot see: its own tolerance for edges meeting at a box reads a pair running a
  // unit apart as one edge and passes them. Measured away from the boxes, where every edge legitimately converges.
  {
    const SEP = 6; /* how far apart two edges must run to be two edges */
    const HALO = 20; /* and how far from a box before that is asked, since edges fan in and out at one */
    const boxes = (spec) => Object.values(spec.states).map((q) => ({ x: q.x, y: q.y, w: W.MB.w, h: W.MB.h }));
    const nearBox = (p, bs) => bs.some((b) => Math.max(b.x - p.x, 0, p.x - (b.x + b.w)) <= HALO && Math.max(b.y - p.y, 0, p.y - (b.y + b.h)) <= HALO);
    /* where two of these point sets first come within SEP of each other, and null where they never do */
    const meeting = (a, b) => {
      for (const p of a) for (const q of b) if (Math.hypot(p.x - q.x, p.y - q.y) < SEP) return p;
      return null;
    };
    const alongsideIn = (spec) => {
      const bs = boxes(spec);
      /* the sampled points of each edge, those in the open between the boxes */
      const edges = spec.edges.map((e) => ({ id: `${e.from}>${e.to}`, pts: flatten(e.d).flatMap(([p, q], i) => (i ? [q] : [p, q])).filter((q) => !nearBox(q, bs)) }));
      const found = [];
      for (let i = 0; i < edges.length; i++) {
        for (let j = i + 1; j < edges.length; j++) {
          const at = meeting(edges[i].pts, edges[j].pts);
          if (at) found.push(`${edges[i].id} alongside ${edges[j].id} at ${at.x.toFixed(0)},${at.y.toFixed(0)}`);
        }
      }
      return found;
    };
    for (const [name, spec] of [['input', W.MACHINE], ['parcel', W.PARCEL_MACHINE]]) {
      const along = alongsideIn(spec);
      check(!along.length, `no two edges of the ${name} machine run within ${SEP} units of each other in the open: ${along.join(', ')}`);
    }
  }

  // ---- and everything a machine draws is on the canvas ----
  //
  // A machine's pills are placed relative to the line they belong to, so one added at the edge of the picture lands past the
  // viewBox and is clipped away: the button is there, takes the click, and shows a sliver. The canvas has no overflow to
  // reveal it and nothing in the source says how close to the edge is too close, so it is measured off the markup.
  {
    /* a member fed by another, failing almost everything and retrying nothing: the run where the quarantine, the pool and
       Failed all hold something at once, so every button any of these machines offers is on the canvas to be measured */
    const sim = new Sim({ transformations: {
      simulation: { feeder: { seeds: 200 }, packer: { size: 1 }, run: [1, 2], fail: 0 },
      reco: { feeder: { from: 'simulation' }, packer: { size: 1 }, fail: 0.9, retries: 0 },
    } });
    sim.start();
    const node = sim.nodes.reco;
    const held = () => { const c = sim.counts(node); return c.Pb && c.U && c.F; };
    /* the hook gives up at once, so the quarantine fills first; then the packer by hand keeps everything its edge feeder
       brings after that in the pool, and the hooks by hand leave the next failure in Failed */
    for (let i = 0; i < 400 && !sim.counts(node).Pb; i++) sim.step(0.5);
    sim.setMode(node.id, 'packer', 'manual');
    sim.setMode(node.id, 'failedInput', 'manual');
    for (let i = 0; i < 400 && !held(); i++) sim.step(0.5);
    check(held(), `the run holds a quarantine, a pool and a failure at once: ${JSON.stringify(sim.counts(node))}`);
    const num = (tag, attr) => Number((new RegExp(`\\b${attr}="([-\\d.]+)"`).exec(tag) || [])[1]);
    const drawings = [
      ['the workgraph', W.wgMachineHtml(sim)],
      ['a transformation', W.nodeMachineHtml(sim, node)],
      ['the inputs', W.machineHtml(sim, node)],
      ['the parcels', W.parcelMachineHtml(sim, node)],
    ];
    for (const [name, html] of drawings) {
      const svg = html.split('</svg>')[0];
      /* a hand-placed machine is centred by one translate around the whole picture, and the defs above it hold no shape */
      const shift = /<g transform="translate\(([-\d.]+),/.exec(svg);
      const dx = shift ? Number(shift[1]) : 0;
      const out = [];
      /* the boxes, the pills and the labels: what is placed relative to something else, and so what drifts off the edge.
         A label is measured at its anchor, since its own extent is the font's and not the renderer's to know. */
      for (const tag of svg.match(/<(?:rect|circle|text)\b[^>]*>/g) || []) {
        const round = tag.startsWith('<circle');
        const r = round ? num(tag, 'r') : 0;
        const x0 = (round ? num(tag, 'cx') - r : num(tag, 'x')) + dx;
        const y0 = round ? num(tag, 'cy') - r : num(tag, 'y');
        const x1 = x0 + (round ? 2 * r : num(tag, 'width') || 0);
        const y1 = y0 + (round ? 2 * r : num(tag, 'height') || 0);
        if (Number.isNaN(x0) || Number.isNaN(y0)) continue;
        if (x0 < 0 || y0 < 0 || x1 > W.MV.w || y1 > W.MV.h) out.push(`${tag.slice(1, 5)} at ${x0.toFixed(0)},${y0.toFixed(0)}-${x1.toFixed(0)},${y1.toFixed(0)}`);
      }
      check(!out.length, `${name} machine draws nothing past its ${W.MV.w}x${W.MV.h} canvas: ${out.join('; ')}`);
    }
    /* which is only worth measuring while the buttons that are placed last are drawn */
    const inputs = drawings.find(([n]) => n === 'the inputs')[1];
    const acts = ['reset-problematic', 'writeoff-problematic', 'writeoff-unassigned', 'run-failed'].filter((a) => !inputs.includes(`data-act="${a}"`));
    check(!acts.length, `and it was measured with every operator button drawn: missing ${acts.join(', ')}`);
  }

  // ---- the input machine draws the operator's edges the engine permits, and no others ----
  //
  // Three things offer an operator an edge out of an input's state: the machine's own dashed edges, the actions over a whole
  // state, and the rows of the lineage dialog. The engine's OPERATOR_EDGES is what the last two read, so a machine drawing a
  // fourth dashed edge would offer a move the engine refuses, and a fourth permitted edge no machine drew would be a move
  // nothing on the picture says is possible. Neither can be seen by looking at either file alone.
  {
    /* An edge marked `runs` is not one of them: the operator does not move the input, they ask the component that moves it
       to run now, so it keeps that component's own stroke and offers a button all the same. */
    const drawn = W.MACHINE.edges.filter((e) => e.act && !e.runs).map((e) => `${e.from}>${e.to}`).sort();
    const allowed = Object.entries(E.OPERATOR_EDGES).flatMap(([from, tos]) => tos.map((to) => `${from}>${to}`)).sort();
    check(drawn.join(' ') === allowed.join(' '), `the machine's dashed edges are the operator's: drawn ${drawn.join(', ')} against permitted ${allowed.join(', ')}`);
    /* and each says the verb its button wears, who takes it, and where that button stands, since none of the three is derivable */
    const thin = W.MACHINE.edges.filter((e) => e.act && !(e.verb && e.by && e.btn)).map((e) => `${e.from}>${e.to}`);
    check(!thin.length, `and each names its verb, its actor and where its button stands: ${thin.join(', ')}`);
    /* the one edge that runs a component rather than moving an input keeps that component's stroke and its own note */
    const runs = W.MACHINE.edges.filter((e) => e.runs);
    check(runs.length === 1 && runs[0].from === 'Unassigned' && runs[0].to === 'Assigned' && runs[0].note, `one edge runs the component that takes it: ${runs.map((e) => `${e.from}>${e.to}`).join(', ')}`);
  }

  // ---- no selector matches nothing, and no class is emitted for nobody ----
  //
  // Both directions rot in silence. A rule for an element that went reads as intent and is dead weight; a class the renderer
  // emits that neither the stylesheet nor the widget reads is a hook to a feature that went. Neither shows up in a diff, and
  // neither can be seen on screen, which is how a stylesheet comes to describe a picture that no longer exists.
  {
    /* every class the renderer writes out literally, and the stems it builds the rest from — `wg-slot-` plus a parcel state,
       `wgsim-state-` plus a tone — which the stylesheet is entitled to name in full */
    const emitted = new Set();
    for (const m of src.matchAll(/class="([^"${]*)"/g)) for (const c of m[1].split(/\s+/)) if (/^(wg|wgsim)-/.test(c)) emitted.add(c);
    const stems = [...new Set(src.match(/(?:wg|wgsim)-[a-z-]*(?=\$\{)/g) || [])];

    const selectors = [...new Set((css.match(/\.(?:wg|wgsim)-[a-zA-Z0-9-]+/g) || []).map((c) => c.slice(1)))];
    const dead = selectors.filter((c) => !emitted.has(c) && !stems.some((stem) => c.startsWith(stem)) && !new RegExp(`['"\`\\s]${c}\\b`).test(src));
    check(!dead.length, `every selector in the stylesheet matches something the renderer draws: dead ${dead.join(', ')}`);

    /* a class may be read by the stylesheet or held as a handle for the widget; anything else is emitted for nobody */
    const unread = [...emitted].filter((c) => !css.includes(`.${c}`) && !new RegExp(`querySelector\\w*\\('[^']*\\.${c}\\b`).test(src)).sort();
    check(!unread.length, `every class the renderer emits is read by the stylesheet or the widget: unread ${unread.join(', ')}`);

    /* and the structural ones, where the selector names an element or an attribute rather than a class */
    check(!/\.wgsim-table th/.test(css) || /<th/.test(src), 'a rule for a table header means a table that has one');
    check(!/\[disabled\]/.test(css) || /\sdisabled[=">]/.test(src), 'a rule for a disabled control means a control that is disabled');

    /* the rail's three parts are always drawn and shown by state alone, which is a contract with the stylesheet */
    for (const part of ['wg-rail-name', 'wg-rail-glyph', 'wg-rail-mode', 'wg-rail-run']) {
      check(new RegExp(`data-state="[a-z]+"\\][^{]*\\.${part}[^{]*\\{[^}]*display: none`).test(css) || new RegExp(`\\.${part}[^{]*\\{`).test(css), `the stylesheet says when ${part} shows`);
    }
    check(/\[data-state="auto"\] \.wg-rail-name \{ display: none/.test(css) && /:not\(\[data-state="auto"\]\) \.wg-rail-glyph \{ display: none/.test(css), 'a row shows its glyph while automatic and its name once by hand');
    check(/\[data-state="run"\] \.wg-rail-mode \{ display: none/.test(css) && /:not\(\[data-state="run"\]\) \.wg-rail-run \{ display: none/.test(css), 'and the mode word gives way to the run button only while by hand with work');
  }

  // ---- one type scale, and every size on it ----
  //
  // The two stylesheets are one product and shared thirteen absolute sizes between them, every one declared per block
  // rather than per role, so a container's size only ever reached whatever had no rule of its own and the same role came
  // out at a different number on each surface. Five steps replace them. Nothing but this keeps a fourteenth from being
  // added the next time a block needs to be a little smaller, which is how the thirteen arrived.
  {
    const scale = ['micro', 'meta', 'body', 'head', 'title'].map((k) => `--wg-t-${k}`);
    const canvas = ['micro', 'label', 'title'].map((k) => `--wg-g-${k}`);
    for (const name of scale.concat(canvas)) check(new RegExp(`${name}:`).test(css), `the scale declares ${name}`);
    /* the steps come off one base, so a density mode is one declaration and the ratios survive it */
    check(/--wg-t: \d+px;/.test(css), 'and derives them from one base');
    for (const mode of ['.wgsim.narrow', '.wgsim.wgsim-max']) check(css.indexOf(`${mode} { --wg-t: `) > 0, `${mode} sets the base rather than bumping selectors`);
    /* One tone map for a state: the card reads it as a fill, the log's chip as a colour, and neither writes its own. */
    check(/--wg-state: var\(--wg-accent\)/.test(css) && /\.wg-status-label \{ fill: var\(--wg-state\)/.test(css) && /\.wgsim-log-to \{[^}]*color: var\(--wg-state\)/.test(css), 'the card badge and the log chip read one tone map');
    check(!/\.wg-status-label \{[^}]*fill: var\(--wg-(green|red|amber|dim)\)/.test(css), 'and the badge names no colour of its own');
    /* and a dialog takes the base back, since what it says is read at one size wherever it is opened */
    check(css.indexOf('.wgsim .wgsim-help { --wg-t: ') > 0, 'a dialog is read at one size wherever it is opened');
    /* the canvas is viewBox units, which scale with the picture: an absolute value, and never the chrome's token */
    for (const name of canvas) check(new RegExp(`${name}: [0-9.]+px`).test(css), `${name} is an absolute unit of the picture`);

    const loose = [];
    for (const [file, text] of [['workgraph-sim.css', css], ['playground.css', pgCss]])
      for (const m of text.matchAll(/font-size: *([^;}]+)/g)) {
        const v = m[1].trim();
        /* `inherit` is the one other answer: a nested element that refuses to be sized at all is on the scale of whatever holds it */
        if (v === 'inherit' || /^var\(--wg-[tg]-(micro|meta|body|label|head|title)\)$/.test(v)) continue;
        loose.push(`${file}: ${v}`);
      }
    check(!loose.length, `every font-size is a step of the scale or inherits one: ${loose.join(', ')}`);
  }

  // ---- the picture says why it is not advancing ----
  //
  // One class folded four situations into one and the stylesheet read none of them, so a reader who stopped the model had
  // nothing but the pressed state of a button in the band to tell them time had stopped. The cause is on the root now, and
  // the pair that can drift is the causes the renderer draws a caption for against the ones the stylesheet shows.
  {
    const causes = W.HOLD_WORDS.map(([c]) => c);
    check(causes.join() === 'paused,dialog,help', `a word is drawn for the three a reader can undo: ${causes.join(', ')}`);
    for (const c of causes) check(css.indexOf(`[data-hold="${c}"] .wg-hold-${c}`) > 0, `and the stylesheet shows the caption for ${c}`);
    check(!/data-hold="armed"\] \.wg-hold/.test(css), 'nothing is drawn over a model that has not run yet, where the transport glows instead');
    check(/\.wgsim\[data-hold\] \.wgsim-canvas svg \{[^}]*filter:/.test(css), 'and the canvas loses its colour for every cause, whatever is covering it');
    check(!/classList\.toggle\('paused'/.test(src), 'the renderer writes which cause it is, not that there is one');
    /* the word is the same in all three; only the line under it changes */
    check(W.HOLD_WORDS.every(([, why]) => why && !/paused/.test(why)), `each caption says why without repeating the word: ${W.HOLD_WORDS.map(([, w]) => w).join(' | ')}`);
  }

  // ---- every act the markup offers is an act the widget handles ----
  //
  // `act()` is one long chain of `else if`, and the markup that feeds it is spread over thirty template literals. Nothing but
  // this connects the two, so a renamed act fails silently: the click lands, the chain falls through, and the button is inert.
  {
    const emitted = new Set();
    for (const m of src.matchAll(/data-act="([a-z-]+)"/g)) emitted.add(m[1]);
    for (const m of src.matchAll(/\bact: '([a-z-]+)'/g)) emitted.add(m[1]); /* the machines' operator edges carry theirs as data */
    const handled = new Set();
    const chain = src.slice(src.indexOf('    act(act, el) {'), src.indexOf('    /* Choosing a speed also starts'));
    for (const m of chain.matchAll(/act === '([a-z-]+)'/g)) handled.add(m[1]);
    check(handled.size > 30, `the act chain was found: ${handled.size} acts handled`);
    const unhandled = [...emitted].filter((a) => !handled.has(a)).sort();
    check(!unhandled.length, `every act the markup offers is handled: ${unhandled.join(', ')}`);
    /* The other way round cannot be read off the markup: most acts reach it through a button helper, as `data-act="${act}"`
       with the name passed as an argument, or through a `tabAct`. So the weaker question a grep can answer honestly: the name
       appears somewhere in the file besides the chain that handles it. That still catches a handler for an act nothing names. */
    const outside = src.slice(0, src.indexOf('    act(act, el) {')) + src.slice(src.indexOf('    /* Choosing a speed also starts'));
    const unnamed = [...handled].filter((a) => !outside.includes(`'${a}'`) && !outside.includes(`"${a}"`)).sort();
    check(!unnamed.length, `every act the widget handles is named where the markup is built: ${unnamed.join(', ')}`);

    /* The keys the widget binds are bare ones, and a combination belongs to the browser. A reader opens a card by clicking
       it, which leaves the focus inside the widget for the rest of the page's life, and the chip for chaos was answering
       `Cmd+C` and cancelling the copy with it, reset was answering `Cmd+R` and eating the reload, and `Cmd+3` set the model
       running at eight times. Shift is not one of these: `R`, `C`, `E` and `M` are bound in upper case deliberately. Nothing
       about a handler reading a key it was not offered shows in a diff, and the checks have no DOM to press a key in, so the
       guard is asserted where it has to stand — ahead of everything it guards. */
    const keys = /root\.addEventListener\('keydown', \(ev\) => \{[\s\S]*?\n      \}\);/.exec(src);
    check(keys, 'the widget reads the keyboard in one place');
    if (keys) {
      const guard = keys[0].indexOf('ev.metaKey || ev.ctrlKey || ev.altKey');
      check(guard > 0, 'and leaves a key held with a modifier to the browser');
      check(guard > 0 && guard < keys[0].indexOf('preventDefault') && guard < keys[0].indexOf('this.act('), 'before it acts on anything or prevents anything, the region branch included');
      check(!/shiftKey/.test(keys[0]), 'while Shift passes, since the upper-case keys are bound on purpose');
    }
  }

  // ---- and a dialog's own width is not capped by the one every dialog shares ----
  //
  // Every dialog is .wgsim-help plus a class of its own, and the shared rule caps the width of all of them. That rule is
  // declared after the rules that give one dialog a width, so at equal specificity the cap is the one that applies and the
  // dialog silently keeps the shared width: `.wgsim-panel { width: min(980px, 94vw) }` renders at 760, and nothing says so
  // but a ruler. Naming both classes is what makes a dialog's own width the one that wins.
  {
    /* the comments go first: one before a rule is part of what a selector match would swallow */
    const bare = css.replace(/\/\*[\s\S]*?\*\//g, '');
    const base = bare.indexOf('.wgsim-help {');
    check(base > 0, 'the dialogs share a rule');
    const dialogs = [...new Set([...src.matchAll(/<dialog class="wgsim-help ([a-z-]+)/g)].map((m) => m[1]))];
    check(dialogs.length > 1, `the renderer builds its dialogs on that rule: ${dialogs.join(', ')}`);
    const lost = [];
    for (const rule of bare.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
      if (!/(?:^|;|\s)(?:max-)?width:/.test(rule[2])) continue;
      for (const sel of rule[1].split(',').map((x) => x.trim())) {
        /* one element, so the same element as the shared rule: a descendant selector is a different element and cannot clash */
        if (/[\s>+~]/.test(sel) || sel.includes('.wgsim-help')) continue;
        const hit = dialogs.find((d) => new RegExp(`\\.${d}(?![\\w-])`).test(sel));
        if (hit && rule.index < base) lost.push(`${sel} (before .wgsim-help)`);
      }
    }
    check(!lost.length, `every dialog's own width outranks the shared cap: ${lost.join(', ')}`);
  }

  // ---- a constant that has to agree with one in the other file ----
  //
  // The renderer places what the stylesheet sizes. Neither can see the other, and the eye cannot check it, so the pair is
  // asserted here rather than kept in step by hand.
  {
    const mark = /const MARK = (\d+)/.exec(src);
    const markCss = /\.wgsim-mark \{[^}]*?width: (\d+)px;\s*height: (\d+)px/s.exec(css);
    check(mark && markCss, 'the mark declares its size in both files');
    if (mark && markCss) {
      check(mark[1] === markCss[1] && mark[1] === markCss[2], `the mark straddles its corner: MARK is ${mark[1]} and the stylesheet says ${markCss[1]}x${markCss[2]}`);
    }
    const cap = /const FILES = \{ cap: (\d+)/.exec(src);
    const capCss = /\.wgsim-files-list \{[^}]*max-height: calc\((\d+) \* (\d+)px\)/.exec(css);
    check(cap && capCss, 'the files list declares its length in both files');
    if (cap && capCss) {
      check(cap[1] === capCss[1], `the files list is as tall as it is long: FILES.cap is ${cap[1]} and the stylesheet holds ${capCss[1]}`);
      const rowCss = /\.wgsim \.wgsim-files-list li \{[^}]*height: (\d+)px/.exec(css);
      check(rowCss && rowCss[1] === capCss[2], `and a row is the height the list reserves for it: ${rowCss && rowCss[1]} against ${capCss[2]}`);
    }
    /* the press and the frame loop take the model in the same slices, which is the whole of why a stepped run and a played
       run of one seed are one run. Two loops in two methods with one number between them: a press that took a whole model
       second, or a sixtieth of one, would look right on screen and silently be a different run. The same for where each
       stops — a state added to the end of the workgraph's machine has to reach both, or the press goes on stepping a run
       the frame loop has already left alone. */
    const slice = /const h = Math\.min\(([\d.]+), rem\);/.exec(src);
    check(slice && Number(slice[1]) === W.STEP.slice, `the press takes the slice the frame loop takes: ${W.STEP.slice} against ${slice && slice[1]}`);
    const loop = /if \(w !== 'New' && ([^)]+)\) this\.advance/.exec(src);
    const ends = loop ? [...loop[1].matchAll(/w !== '(\w+)'/g)].map((m) => m[1]).sort() : null;
    check(ends && ends.join(',') === [...W.STEP.over].sort().join(','), `and stops where it stops: ${ends} against ${[...W.STEP.over].sort()}`);

    /* an input state's tone is written on four surfaces and coloured in one stylesheet, and the class is the whole of the
       pair: the counts table's number, the hooks' decision, a lineage row and a row of the list a machine's box opens all
       wear `wg-n-<cls>`, and a selector narrower than the class leaves the rest the colour of the prose around them. That
       is invisible in either file — the class is there, the rule is there, and they simply do not meet. */
    const toned = new Set(W.INPUT_ROWS.map((r) => r.cls));
    const coloured = new Set([...css.matchAll(/\.wg-n-(\w+) \{[^}]*color:/g)].map((m) => m[1]));
    check([...toned].every((c) => coloured.has(c)), `every tone an input row wears is coloured: ${[...toned].filter((c) => !coloured.has(c))} are not`);
    check([...css.matchAll(/([^{}\n]*\.wg-n-\w+)[^{}]*\{[^}]*color:/g)].every((m) => !/\b(td|tr|table)\b/.test(m[1])), 'and coloured by the class rather than by the one element that happens to wear it');

    /* the renderer asks the browser for the layout engine at a URL of its own making, and a build hook in another
       language puts the file there. Move either and the site still builds, the checks still pass, and every model on every
       page sits at its loading bar for ever: the file is fetched at runtime, so nothing fails until a reader opens a page.
       The same for the two environments — both are `no-default-feature`, so each has to name elkjs for itself, and the one
       that forgets is only found by someone running that half. */
    const hook = readText('hooks/elkjs.py');
    const wanted = /new URL\('([^']*elk[^']*)', SELF_SRC\)/.exec(src);
    const vendor = /^VENDOR = Path\("([^"]+)"\)/m.exec(hook);
    const bundle = /^BUNDLE = PACKAGE \/ "([^"]+)"/m.exec(hook);
    check(wanted && vendor && bundle, 'the renderer names the URL it fetches and the hook names where it writes');
    if (wanted && vendor && bundle) {
      /* the renderer's URL is relative to its own script, which the site serves out of assets/js/ */
      const served = `assets/js/${wanted[1]}`;
      const written = `${vendor[1]}/${bundle[1].split('/').pop()}`;
      check(served === written, `the hook writes the file the renderer asks for: ${written} against ${served}`);
    }
    const pixi = readText('pixi.toml');
    for (const feature of ['mkdocs', 'workgraph-sim']) {
      const block = new RegExp('\\[feature\\.' + feature + '\\.dependencies\\]([^\\[]*)').exec(pixi);
      check(block && /^elkjs\s*=/m.test(block[1]), `the ${feature} environment declares elkjs for itself`);
    }
    /* and the runner resolves it out of the environment rather than out of the tree, which is the whole of the change */
    const runner = readText('tests/workgraph-sim/run.sh');
    check(/node_modules\/elkjs\/lib\/elk\.bundled\.js/.test(runner) && /pixi install/.test(runner), 'the runner finds the engine in the environment, and says so when it cannot');
    check(!/docs\/assets\/js\/vendor/.test(readText('tests/workgraph-sim/elk-shim.js')), 'and the shim no longer reaches into the tree for it');

    /* the lineage fits its label by dividing the box by an advance per character, and that advance is right for exactly one
       size and one weight. The size is in the stylesheet, on the class the renderer writes; change the token there and the
       renderer goes on dividing by a number for a face nothing draws any more, cutting names that would have fitted or
       letting through names that will not. */
    const linPx = /\.wg-lin-label \{[^}]*font-size: var\(--wg-g-(\w+)\)/.exec(css);
    const token = linPx && new RegExp(`--wg-g-${linPx[1]}: ([\\d.]+)px`).exec(css);
    check(linPx && token, 'the lineage label names the size it is drawn at');
    check(token && Number(token[1]) === W.LIN.px, `and the renderer measures that size: ${token && token[1]}px against ${W.LIN.px}px`);
    /* the sub under the name is the tag and the status in the same box, and the statuses are a closed set: measured in the
       face the site serves, the longest of them reaches 86 of the box's 112 units, so it is left whole. A state named
       longer than the box holds would leave the box with nothing here to say so, the label being the only line fitted. */
    const room = (W.LIN.box * 2 - 8) / W.LIN.ch;
    const longest = PARCEL_STATES.map((st) => `9a5e · ${st}`).sort((a, b) => b.length - a.length)[0];
    check(longest.length <= room, `every parcel state's sub line fits its box: "${longest}" is ${longest.length} characters against ${room.toFixed(1)}`);

    /* a lineage box says whether its member computed something or moved data, and it says it in shape, every other channel
       on that box being spent — the stroke is the status, the dashes a recovery parcel, the fill and the dimmed label a
       failure. The renderer writes the member's own kind as a class and the stylesheet decides which of them is a stadium,
       so the pair is a class emitted in one file against a rule in another, with nothing on screen to say they have come
       apart: a kind the stylesheet does not name simply draws as compute. */
    /* a transformation's kind, which the engine tests through the spec — `sc.kind` is the scout's ladder and a different word */
    const kinds = new Set(['compute', ...[...engineSrc.matchAll(/\bs(?:pec)?\.kind (?:===|!==) '(\w+)'/g)].map((m) => m[1])]);
    const kindRules = [...new Set([...css.matchAll(/\.wg-kind-(\w+)/g)].map((m) => m[1]))].sort();
    check(kinds.size > 1 && kindRules.length, `the model has kinds and the stylesheet reads some of them: ${[...kinds]} against ${kindRules}`);
    check(kindRules.every((k) => kinds.has(k)), `and every kind it reads is one the model has: ${kindRules.filter((k) => !kinds.has(k))} are not`);
    /* every kind that moves data rather than computing gets the shape, or one of them silently reads as a compute parcel */
    const shaped = [...new Set([...css.matchAll(/\.wg-kind-(\w+) \.wg-lin-box/g)].map((m) => m[1]))].sort();
    check([...kinds].filter((k) => k !== 'compute').every((k) => shaped.includes(k)), `every kind that is not compute draws as a stadium: ${[...kinds].filter((k) => k !== 'compute' && !shaped.includes(k))} do not`);
    check(!shaped.includes('compute'), 'and compute keeps the box it had');
    /* and the two channels stay independent, or the encoding collapses back into one: a status rule that reached for the
       shape would make a failed removal and a finished compute parcel the same box again */
    const shapeRules = [...css.matchAll(/([^{}\n]*)\{[^}]*\brx:/g)].map((m) => m[1].trim());
    check(shapeRules.length, 'the shape is set in the stylesheet, where the kind can reach it');
    check(shapeRules.every((sel) => !/wg-lin-(done|failed|cancelled|partiallydone|reserved|assigned|completing|unassigned|recovery)/.test(sel)), `no status rule reaches for the shape: ${shapeRules.join('; ')}`);
    const kindOnLin = [...css.matchAll(/([^{}\n]*\.wg-kind-\w+[^{}\n]*\.wg-lin-[^{}\n]*)\{([^}]*)\}/g)];
    check(kindOnLin.every((m) => !/\bstroke|\bfill/.test(m[2])), `and the kind does not reach for the stroke or the fill the status is drawn in: ${kindOnLin.map((m) => m[1].trim()).join('; ')}`);

    /* the strip under a machine packs its chips and counts the rest, and the renderer divides rather than measures: it
       claims a line holds `CHIPS.perLine` of them, where the stylesheet is what decides how wide one is and how much room
       the strip has. Widen the chip, or the gap, or narrow the dialog, and the claim quietly becomes false — the cap then
       runs to a fourth line, which is the stacked list creeping back in the shape the packing was meant to replace. */
    const chipCss = /\.wgsim-mlist-chips li[^{]*\{[^}]*min-width: (\d+)px/.exec(css);
    const chipGap = /\.wgsim-mlist-chips,[^{]*\{[^}]*gap: (\d+)px/.exec(css);
    const stripPad = /\.wgsim-machine-list \{[^}]*padding: [\d.]+rem (\d+)rem/.exec(css);
    /* the dialog's own width, not the maximised rule above it, whose selector ends in the same two classes */
    const dlgCss = /\n\.wgsim-help\.wgsim-panel \{[^}]*width: min\((\d+)px/.exec(css);
    /* the strip and its chips each beat Material's list rules by carrying a third class. Dropped, `display: flow-root`
       wins, every chip is a block the width of the pane, and the packing simply does not happen — which is what nothing in
       this file would show, the rules all still being here and reading as though they applied. */
    check(/\.md-typeset \.wgsim \.wgsim-mlist-chips \{/.test(css) && /\.md-typeset \.wgsim \.wgsim-mlist-chips li \{/.test(css), 'the strip and its chips each outrank the theme\'s list rules');
    check(chipCss && chipGap && stripPad && dlgCss, 'the chip, the gap, the strip\'s padding and the dialog\'s width are all declared');
    if (chipCss && chipGap && stripPad && dlgCss) {
      const room = Number(dlgCss[1]) - 2 * Number(stripPad[1]) * 16;
      const takes = W.CHIPS.perLine * (Number(chipCss[1]) + Number(chipGap[1])) - Number(chipGap[1]);
      check(takes <= room, `a line holds the ${W.CHIPS.perLine} chips the renderer counts on: ${takes}px of ${room}px`);
      /* and it is worth the divide: a count so cautious that a line is half empty is the stack again with gaps in it */
      check(takes > room * 0.7, `and is not so cautious that the strip runs half empty: ${takes}px of ${room}px`);
    }

    /* the list a box opens is drawn inside the machine's region, and the stylesheet holds the four tabs to one height by
       capping a pane with exactly that nesting in it. Draw the list as the region's sibling instead and it still renders,
       the rule silently stops matching, and the dialog grows under the tab the reader is pressing — which is the thing the
       height was arranged to prevent, and which neither file shows on its own. */
    check(/\.wgsim-panel-body:has\(> \.wgsim-machine-region > \.wgsim-machine-list\)[^}]*max-height/.test(css), 'the stylesheet caps a pane by the nesting the renderer draws');
    check(/machine-region">\$\{machine\}\$\{list\}<\/div>/.test(src), 'and the renderer draws the list inside that region');
    check(/\.wgsim-mlist-chips \{[^}]*min-height: 0[^}]*overflow-y: auto/.test(css), 'so a strip too long for its slack scrolls inside it rather than taking more');

    /* the feeder block's drain guard adds up what has not settled, naming its keys one by one, and every counts table puts
       that same population above its hairline by asking each row whether it is terminal. Neither reads the other, so an
       input state added to the enum, or a row marked terminal, moves one and leaves the other summing a population it no
       longer draws as open — and a member would read `9 inputs \u00b7 keeping this open` over a table that shows eight. */
    const guard = /const open = ([^;]+);/.exec(src);
    const guarded = guard ? guard[1].split('+').map((x) => x.trim().replace(/^c\./, '')).sort() : null;
    const opens = W.INPUT_ROWS.filter((r) => !r.terminal).map((r) => r.key).sort();
    check(guarded && guarded.join(',') === opens.join(','), `the drain guard counts what the tables draw as open: the guard adds ${guarded} and the rows hold ${opens}`);
    /* and on the parcels machine that same rule reads the rows' own flags, where everything that counts a live parcel reads
       the engine's set. The two say which parcel states have settled, and only one of them is in the engine. */
    const rowsDone = W.PARCEL_ROWS.filter((r) => r.terminal).map((r) => r.name).sort();
    check(rowsDone.join(',') === [...PARCEL_TERMINAL].sort().join(','), `the parcel rows have settled where the engine says they have: ${rowsDone} against ${[...PARCEL_TERMINAL].sort()}`);

    /* the actions panel is one row that drains, and the widget counts the columns that fit rather than measuring them: it
       divides the row's width by a column and a gap it does not draw itself. A stylesheet that widened a column would leave
       the count off by one and the last column half over the edge, which no diff of either file would show. */
    const rowCss = /\.wgsim-lists-body \{[^}]*gap: (\d+)px/.exec(css);
    const colCss = /\.wgsim-lists-entity \{[^}]*min-width: (\d+)px/.exec(css);
    const moreCss = /\.wgsim-lists-more \{[^}]*flex: 0 0 (\d+)px/.exec(css);
    const padCss = /\.wgsim-lists-body \{[^}]*padding: [\d.]+rem (\d+)px/.exec(css);
    check(rowCss && colCss && moreCss && padCss, 'the row declares its column, its gap, its count and its padding in the stylesheet');
    if (rowCss && colCss && moreCss) {
      check(Number(colCss[1]) === W.LIST_ROW.col && Number(rowCss[1]) === W.LIST_ROW.gap, `the widget counts with the floor and the gap the row is drawn with: ${W.LIST_ROW.col}/${W.LIST_ROW.gap} against ${colCss[1]}/${rowCss[1]}`);
      check(Number(moreCss[1]) === W.LIST_ROW.more, `and leaves the count at the right end the width it takes: ${W.LIST_ROW.more} against ${moreCss[1]}`);
      check(padCss && Number(padCss[1]) === W.LIST_ROW.pad, `and takes the row's own padding off the width it measures: ${W.LIST_ROW.pad} against ${padCss && padCss[1]}`);
    }
    /* a column never grows into the space a list leaves, which would move it for a reason the reader cannot see; it is as
       wide as its own list needs, between the floor the widget counts with and a cap, and the row never scrolls sideways */
    check(/\.wgsim-lists-entity \{[^}]*flex: 0 0 /.test(css) && !/\.wgsim-lists-body \{[^}]*overflow-x/.test(css), 'the columns do not grow into the space a list leaves, and the row does not scroll sideways');
    const capCol = /\.wgsim-lists-entity \{[^}]*max-width: (\d+)px/.exec(css);
    check(capCol && Number(capCol[1]) > W.LIST_ROW.col, `and a column is capped above that floor rather than unbounded: ${capCol && capCol[1]} against ${W.LIST_ROW.col}`);
    /* the operator's buttons are the widest thing a column carries and they do not shrink, so the line that holds them has to
       wrap: without that the whole deficit falls on the sentence beside them, which collapses to a word a line while the
       buttons hang over the edge of the column anyway */
    check(/\.wgsim-lists-btns \{[^}]*flex: 0 0 auto/.test(css), 'the operator buttons in a list keep their width');
    check(/\.wgsim-lists-why \{[^}]*flex-wrap: wrap/.test(css), 'and the line they sit on wraps them under the sentence rather than taking its width');

    /* the product has two surfaces that name themselves, the maximised widget's band and the playground's toolbar, and one
       mark between them: shaped once beside the tokens the two stylesheets share, and worn by both. A second copy in the
       playground's own stylesheet is how a mark comes to be 20px on one surface and 18px on the other. */
    check(/\.wg-lockup \{/.test(css) && /\.wg-logo \{[^}]*width: 20px/.test(css), 'the lockup is shaped once, with the tokens the two surfaces share');
    const pgLock = [...pgCss.matchAll(/([^{}]*\.wg-(?:lockup|logo)[^{}]*)\{([^}]*)\}/g)];
    check(pgLock.every((m) => /^\s*display: [^;]+;?\s*$/.test(m[2])), `the playground says where the lockup shows and nothing about its shape: ${pgLock.map((m) => m[1].trim() + ' {' + m[2].trim() + ' }').join('; ')}`);
    check(/class="wg-lockup/.test(src) && /class="wg-lockup/.test(pgSrc), 'both surfaces wear it');
    check(/<img class="wg-logo"/.test(src) && /<img class="wg-logo"/.test(pgSrc), 'and both draw the same mark');

    /* the playground's two reports are named once. The settings note points the reader at the other one by name — what was
       clamped is said in *What was parsed* — and that sentence is prose in a template a rename would walk straight past. */
    const drawers = /const DRAWERS = \{([^}]*)\}/.exec(pgSrc);
    check(drawers, 'the playground names its drawers in one place');
    if (drawers) {
      const labels = [...drawers[1].matchAll(/'([^']+)'/g)].map((m) => m[1]);
      check(labels.length === 2, `both of them: ${labels.join(', ')}`);
      for (const l of labels) check(pgSrc.split(l).length - 1 === 1, `"${l}" is written out once and read from the constant everywhere else`);
      const note = /renderSettings\(\) \{[\s\S]*?\n    \}/.exec(pgSrc);
      check(note && /DRAWERS\.parsed/.test(note[0]), 'and the settings note points at the other drawer through that constant, so a rename carries the sentence with it');
    }
    /* the toolbar's line holds what is true while it is true. The bug this replaced cleared it only when what happened to be
       standing there was loud, so an error count outlived the errors; and what the line no longer says is announced in a
       region that has to be both live and out of sight, since dropping either half leaves a silent element or a second
       status line in the bar, neither of which shows in a diff of the file that did not change. */
    check(!/classList\.contains\('loud'\)/.test(pgSrc), 'a condition is cleared when it ends, not when what stands in its place happens to be loud');
    check(/class="wgpg-announce wgpg-quiet" role="status" aria-live="polite"/.test(pgSrc), 'the routine announcement is made in a live region of its own');
    const quiet = /([^{}]*\.wgpg-quiet[^{}]*)\{([^}]*)\}/.exec(pgCss);
    check(quiet && /position: absolute/.test(quiet[2]) && /clip-path: inset\(50%\)/.test(quiet[2]), `and that region is out of sight: ${quiet ? quiet[1].trim() : 'no rule'}`);

    /* the workgraph is not a tab: the one tablist left on the page is the example families, which are peers of each other */
    check((pgSrc.match(/role="tablist"/g) || []).length === 1 && /wgpg-cats" role="tablist"/.test(pgSrc), 'the pane holds the workgraph rather than a strip of tabs over it');

    /* the chaos chip's rate is one number, said once in the code and twice in prose */
    const rate = /const CHAOS_RATE = ([\d.]+)/.exec(src);
    check(rate, 'the chaos rate is a named constant');
    if (rate) {
      const pct = Math.round(Number(rate[1]) * 100);
      const said = [...src.matchAll(/every parcel fails (\w+) the time/g)].map((m) => m[1]);
      check(said.length >= 2 && said.every((w) => (pct === 50 ? w === 'half' : false)), `the prose says the chaos rate the constant holds: ${rate[1]} against ${said.join(', ')}`);
    }
  }


  // ---- a card is one target, and what nests inside it earns that ----
  //
  // A hit area that outlines itself under the pointer promises something of its own. The pool's did, and opened the dialog
  // the whole card opens, on the same node and the same tab: an outline that says "this part leads somewhere" and then goes
  // where everything else goes. Nothing in either file shows two hit areas carrying one action, so it is asserted here. What
  // nests inside a card and earns it does something the card does not: a parcel, a file, the backlog chip, the rail's rows.
  {
    const sim = new Sim({ name: 'card', transformations: { a: { feeder: { seeds: 6 }, packer: { size: 1 } } } });
    sim.start();
    const L = await W.layout(sim);
    const card = W.cardSvg(L.items.a, 'a', 'compute');
    const acts = [...card.matchAll(/data-act="([a-z-]+)" data-node="([^"]*)"/g)].map((m) => `${m[1]}:${m[2]}`);
    check(acts.filter((a) => a === 'open-details:a').length === 1, `a card opens its dialog from one place, not two: ${acts.join(', ')}`);
    check(!/wg-region/.test(card) && !/wg-region/.test(css), 'and the pool draws no target of its own');
    /* the pool's glow is drawn on the pool's own surface now, which the renderer reaches by toggling the class on the group
       that holds it: a rect moved a level deeper would put the glow out of the stylesheet's reach with nothing to show it */
    check(/\.wg-glow > \.wg-pool \{/.test(css), 'the glow is drawn on the pool itself');
    const pool = /<g>\s*(?:<title[^>]*>[^<]*<\/title>\s*)?<rect[^>]*class="wg-pool"[\s\S]*?<g data-dyn="pool"/.exec(card);
    check(pool, 'and the group the renderer marks holds that rect as its own child');
    check(/classList\.toggle\('wg-glow'/.test(src) && /dyn\['pool:' \+ node\.id\]\.parentNode/.test(src), 'which is the group it marks');
    /* the card answers the pointer with the treatment the log's hover already uses, and never with the stroke, which carries
       the transformation's state: a tinted border would read as the card entering Archiving */
    check(/\.wg-card\[data-act\]:hover \{[^}]*filter: drop-shadow/.test(css) || /\.wg-card\.wg-hi, \.wgsim \.wg-card\[data-act\]:hover \{[^}]*filter: drop-shadow/.test(css), 'the card lights under the pointer');
    check(!/\.wg-card\[data-act\]:hover[^{]*\{[^}]*stroke/.test(css), 'and does not touch the stroke its state is drawn in');
  }

  // ---- the workgraph's list is never one of a queue ----
  //
  // The actions panel gives the workgraph's own list the whole row and each member's list a column that keeps its width,
  // because the two never appear together: `runningLists()` pushes the approving list only while the workgraph is Approving
  // or ApprovingBlocked, and a member in either of those is Active or Paused (DX-ADR-005), neither of which runs a list. If
  // that ever stopped holding, a column that takes the row would be standing beside columns that cannot shrink, and the row
  // clips rather than complains. The rule keys on the name the renderer writes into the column, so the two files have to
  // agree on it: a key the stylesheet does not match puts the band back in a 360px box with nothing in a diff to show it.
  {
    const together = [];
    const models = [
      { name: 'approving beside', workgraph: { scouting: { count: 4 } }, transformations: { simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], finalize: ['no seed used twice'] }, reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, finalize: ['no input used twice'] } } },
      { name: 'reopened', transformations: { a: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], finalize: [{ name: 'check', fail: 'once' }] }, b: { feeder: { from: 'a' }, packer: { size: 2 }, finalize: ['no input used twice'] } } },
    ];
    for (const spec of models) {
      const sim = new Sim(Object.assign({}, spec, { workgraph: Object.assign({}, spec.workgraph || {}, { approval: 'auto' }) }));
      sim.start();
      for (let i = 0; i < HANG && sim.wg.status !== 'Completed' && sim.wg.status !== 'Archived'; i++) {
        sim.step(0.05);
        if (sim.blocked) sim.forceAction();
        for (const n of Object.values(sim.nodes)) if (n.run && n.run.blocked) sim.forceAction(n.id);
        writeOffQuarantine(sim);
        const ids = sim.runningLists().map((l) => l.id);
        if (ids.length > 1 && ids.includes('workgraph')) together.push(`${spec.name} at ${sim.t.toFixed(1)}s: ${ids.join(' + ')}`);
      }
    }
    check(!together.length, `the workgraph's list never runs beside a member's: ${together.slice(0, 2).join('; ')}`);

    const wide = /\.wgsim-lists-entity\[data-list\^="([^"]+)"\]/.exec(css);
    check(wide, 'the stylesheet gives one entity\'s list the row rather than a column of the queue');
    if (wide) {
      const sim = new Sim({ name: 'wide', workgraph: { scouting: { count: 4 }, approval: 'auto' }, transformations: { simulation: { feeder: { seeds: 6 }, packer: { size: 1 } } } });
      sim.start();
      check(until(sim, () => sim.runningLists().some((l) => l.id === 'workgraph')), 'the approving list runs');
      check(W.listsHtml(sim, sim.runningLists()).includes(`data-list="${wide[1]}`), `and the panel writes the key the stylesheet reaches it by: ${wide[1]}`);
    }
  }

  // ---- what a frame is allowed to do ----
  //
  // A frame runs twenty times a second and the engine is not what it spends: the largest model the documentation ships steps
  // in 0.035 ms, where its drawing had four hundred and fifty-six files in flight at once. Three habits made that expensive
  // and none of them shows in a diff of the file they were in: asking a path for a length that the layout settled, writing a
  // region as a string when only its positions changed, and reading the layout back in the middle of the frame's writes.
  {
    const frame = /renderFrame\(\) \{[\s\S]*?\n    \}/.exec(src);
    check(frame, 'the frame is drawn in one method');
    check(!/tokens\.innerHTML/.test(src), 'the token layer is moved rather than written as a string: ninety thousand characters a frame at the peak');
    const point = /tokenPoint\(tk\) \{[\s\S]*?\n    \}/.exec(src);
    check(point && !/getTotalLength/.test(point[0]), "a token's point is taken from the length cached with the path, not asked for again every frame");
    check(/pathCache\[p\.dataset\.edge\] = \{ el: p, len: p\.getTotalLength\(\) \}/.test(src), 'which is measured once, when the scene is built');
    const fit = /listsFit\(keys\) \{[\s\S]*?\n    \}/.exec(src);
    check(fit && !/clientWidth/.test(fit[0]), 'and the row\'s width is watched rather than read back while the frame draws');
    check(/new ResizeObserver\(.*contentRect\.width.*\)\.observe\(this\.listsRow\)/.test(src), 'which is what the observer on the row is for');
  }

  // ---- the engine holds no presentation, and the renderer holds no model ----
  //
  // The engine is the model and knows nothing about pixels; the renderer draws and keeps no simulation of its own. Both drift
  // one convenience at a time — a colour needed in a log line, an escape needed for a tooltip — and each one is easy to argue
  // for alone. The crossings that have been cleared are pinned here so they are not re-argued one at a time.
  {
    check(!/const esc = |&amp;|&lt;/.test(engineSrc), 'the engine escapes no HTML');
    check(!/#[0-9a-f]{6}/i.test(engineSrc), 'and names no colour');
    check(!/typeof document|typeof window|document\./.test(engineSrc.replace(/\(typeof window[^)]*\)/, '')), 'and never asks whether it is in a document');
    check(!/\bclass="|<svg|<div|<span/.test(engineSrc), 'and writes no markup');

    /* the model counts source identities; the renderer gives each one a colour, and the two agree on how many there are */
    check(W.PALETTE.length === E.IDENTITIES, `every source identity the model counts has a colour: ${E.IDENTITIES} against ${W.PALETTE.length}`);

    /* the model's clock holds only what the model measures: a fade and a linger are the drawing's */
    check(!('glow' in E.T) && !('linger' in E.T) && !('retry' in E.T), 'the model keeps no fade of its own');
    check(W.FADE.glow > 0 && W.FADE.linger > 0 && W.FADE.retry > 0, 'and the renderer keeps them all');

    /* a log line's tone is read from the state the engine says it reached, never found in the sentence */
    const line = { kind: 'note', text: 'a → Done and a → Failed', to: 'Failed' };
    check(W.logText(line) === 'a → Done and a → <span class="wgsim-log-bad">Failed</span>', `the tone follows the state the line records: ${W.logText(line)}`);
    /* which only works while every line that ends on a toned state says which state that is */
    const untoned = [];
    for (const sim of sweepSims) {
      for (const e of sim.log) {
        if (e.kind === 'state') continue;
        const m = /→ ([A-Za-z]+)/.exec(e.text);
        if (m && W.LOG_TONE[m[1]] && e.to !== m[1]) untoned.push(e.text.slice(0, 60));
      }
    }
    check(!untoned.length, `every line that ends on a toned state records it: ${untoned.slice(0, 3).join('; ')}`);

    /* the spec's slot limit is one a card can actually draw, which is the reason there is a limit at all */
    const boxes = W.poolCells({ x: 0, y: 0, w: W.BODY.w, h: W.BODY.h });
    check(boxes.length > 0, 'the pool lays its cells out');
    check(E.MAX_SLOTS > 0 && E.MAX_SLOTS <= 9, `the slot limit is one the grid is built for: ${E.MAX_SLOTS}`);

    /* and verification is the caller's to ask for, not something the model infers from its surroundings */
    check(E.DEFAULT_SETTINGS.verify === true, 'the checks ask every model they build to verify itself');
  }
  });
})(globalThis);
