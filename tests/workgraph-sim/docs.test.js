// Every ```workgraph fence in the documentation pages named on the command line runs to Completed, under each of a few seeds.
(function (g) {
  g.WG_TEST.suite('docs', async () => {
  const { out, check, started, complete, readText, fileArgs } = g.WG_TEST;
  const { layout, parseSpec } = g.WorkgraphSim;
  const { INPUT_TERMINAL, PARCEL_TERMINAL, COUNT_KEY } = g.WorkgraphSimEngine;

  /* No page pins a seed, so the widget gives each load a random one and a model that only completes on one run is a model
     readers will meet stuck. One seed here would be that one run. Four of them cost about a second over the whole suite, and
     the count they add is structural: four checks per fence however the data falls. A model that fails under one of them is a
     finding about the model, so the failure carries the end state rather than a raised ceiling. */
  const SEEDS = [1, 2, 3, 4];

  /* The seed the widget hands a seedless fence, and the seed a fence that pins one keeps. The checks parse the fences
     themselves, below, so that the models they drive are the engine's default of 1 and whatever this list says. */
  {
    const text = '{ name: "pinned", seed: 7, transformations: { a: { feeder: { seeds: 4 } } } }';
    check(parseSpec(text).seed === 7, 'a fence that pins a seed keeps it');
    const loads = [];
    for (let i = 0; i < 8; i++) loads.push(parseSpec('{ name: "loose", transformations: { a: { feeder: { seeds: 4 } } } }').seed);
    check(loads.every((s) => Number.isInteger(s) && s >= 1), 'a fence that pins none is given a whole seed of at least 1: ' + loads.join(', '));
    check(new Set(loads).size > 1, 'and a different one on each load, so the failures fall differently: ' + loads.join(', '));
  }

  /* What a stuck run has to say for itself: the members still open, what their inputs and parcels were doing, and what the
     workgraph was waiting on. Reported on a failure so that the finding is in the output rather than in a later bisection. */
  function endState(sim) {
    const parts = [`workgraph ${sim.wg.status}`];
    if (sim.blocked) parts.push(`blocked on ${sim.blocked.name}`);
    for (const n of Object.values(sim.nodes)) {
      if (sim.drained(n)) continue;
      const c = sim.counts(n);
      const tally = Object.entries(COUNT_KEY).filter(([, k]) => c[k]).map(([st, k]) => `${st} ${c[k]}`);
      const live = [...n.inputs.values()].filter((i) => !INPUT_TERMINAL.has(i.status)).length;
      const parcels = n.parcels.filter((p) => !PARCEL_TERMINAL.has(p.status));
      const why = [];
      if (live) why.push(`${live} non-terminal inputs`);
      if (parcels.length) why.push(`parcels ${parcels.map((p) => p.status).join(', ')}`);
      if (sim.feederActive(n)) why.push('feeder still running');
      if (sim.waitsOnOperator(n)) why.push('waiting on an operator for its quarantine');
      parts.push(`${n.id} ${n.status} not drained (${tally.join(', ') || 'no inputs'}${why.length ? '; ' + why.join('; ') : ''})`);
    }
    return parts.join(' | ');
  }

  /* One edge crossing another is the picture asking which line is which, and the layout need not ask: with a card's ports
     free to permute within their side, every model the documentation ships routes without a crossing. Counted so that a
     model, or a layout option, that brings one back says so. A proper intersection between a horizontal run of one edge and
     a vertical run of another; an endpoint that merely meets a line is a corner or a junction rather than a crossing. */
  function crosses(p, q) {
    for (let i = 1; i < p.points.length; i++) {
      const a = p.points[i - 1];
      const b = p.points[i];
      if (Math.abs(a.y - b.y) > 1) continue;
      const y = (a.y + b.y) / 2;
      const x0 = Math.min(a.x, b.x);
      const x1 = Math.max(a.x, b.x);
      for (let k = 1; k < q.points.length; k++) {
        const c = q.points[k - 1];
        const d = q.points[k];
        if (Math.abs(c.x - d.x) > 1) continue;
        const x = (c.x + d.x) / 2;
        const y0 = Math.min(c.y, d.y);
        const y1 = Math.max(c.y, d.y);
        if (x > x0 + 1 && x < x1 - 1 && y > y0 + 1 && y < y1 - 1) return true;
      }
    }
    return false;
  }

  const files = fileArgs();
  check(files.length > 0, 'documentation pages were passed on the command line');
  let fences = 0;
  for (const file of files) {
    const text = readText(file);
    const re = /```workgraph\n([\s\S]*?)```/g;
    let m;
    while ((m = re.exec(text))) {
      fences++;
      let raw;
      try {
        raw = new Function('return (' + m[1] + ')')();
      } catch (e) {
        check(false, `${file}: fence does not parse: ${e.message}`);
        continue;
      }
      /* one block, one model: the renderer refuses a list, so a page that grew a second scenario
         has to give it its own block rather than hiding it behind a tab */
      if (Array.isArray(raw)) {
        check(false, `${file}: fence holds a list of models; give each its own block`);
        continue;
      }
      /* Laid out once per fence: the layout depends on the spec alone (`layout` reads `sim.spec` and nothing the run touches),
         so repeating it per seed would buy nothing and elkjs is the slow half of this suite. */
      const sim = started(raw);
      const L = await layout(sim);
      check(L.width > 0 && L.height > 0, `${file} ${raw.name}: layout has size`);
      /* the routing invariant, over every model the documentation ships: an edge that passes
         beneath a node is an edge that disappears, which is what orthogonal routing buys us */
      const boxes = Object.values(L.items).map((it) => ({ id: it.id, x0: it.x, y0: it.y, x1: it.x + it.w, y1: it.y + it.h }));
      const under = new Set();
      for (const e of L.edges) {
        for (let i = 1; i < e.points.length; i++) {
          const a = e.points[i - 1];
          const b = e.points[i];
          const lo = { x: Math.min(a.x, b.x), y: Math.min(a.y, b.y) };
          const hi = { x: Math.max(a.x, b.x), y: Math.max(a.y, b.y) };
          /* a tolerance of 1px, so a segment leaving a port along the node's own border does not count */
          for (const q of boxes) if (lo.x < q.x1 - 1 && hi.x > q.x0 + 1 && lo.y < q.y1 - 1 && hi.y > q.y0 + 1) under.add(`${e.from}>${e.to} under ${q.id}`);
        }
      }
      check(under.size === 0, `${file} ${raw.name}: no edge passes beneath a node${under.size ? ': ' + [...under].join(', ') : ''}`);
      check(L.edges.every((e) => e.d && e.points.length >= 2), `${file} ${raw.name}: every edge was routed`);
      /* edges that leave the same port are a trunk that branches, so where they lie over one another that is the merge */
      const crossed = [];
      for (let i = 0; i < L.edges.length; i++)
        for (let j = i + 1; j < L.edges.length; j++) {
          const a = L.edges[i];
          const b = L.edges[j];
          if (a.sourcePort && a.sourcePort === b.sourcePort) continue;
          if (crosses(a, b) || crosses(b, a)) crossed.push(`${a.from}>${a.to} x ${b.from}>${b.to}`);
        }
      check(crossed.length === 0, `${file} ${raw.name}: no two edges cross${crossed.length ? ': ' + crossed.join(', ') : ''}`);

      /* One check per seed, naming the seed, so the output says which run failed without a second pass. `complete` stops at
         the harness's hang ceiling, generous enough that only a run going nowhere reaches it. */
      const runs = [];
      for (const seed of SEEDS) {
        const s = started(Object.assign({}, raw, { seed }));
        const secs = complete(s);
        const ok = s.wg.status === 'Completed';
        check(ok, `${file} ${raw.name}: reaches Completed under seed ${seed}`);
        runs.push(`${seed}:${ok ? secs.toFixed(0) + 's/' + s.totals.done : 'STUCK'}`);
        if (!ok) out(`     seed ${seed} ended ${endState(s)}`);
      }
      out(`${runs.some((r) => r.includes('STUCK')) ? 'FAIL' : 'ok  '} ${file.split('/').pop()} ${raw.name}: ${runs.join(' ')}`);
    }
  }
  check(fences > 0, 'at least one workgraph fence was found');
  });
})(globalThis);
