// Joins, lookups, artifacts and finalizing actions.
(function (g) {
  g.WG_TEST.suite('features', async () => {
  const { out, check, until, started, complete } = g.WG_TEST;

  // joining: compare fed by recoA, partner from recoB
  {
    const sim = started({
      name: 'join',
      sources: { query: { files: 40 } },
      transformations: {
        recoA: { feeder: { from: 'query' }, packer: { size: 1 }, run: [1, 2], retries: 0, fail: 0.1 },
        recoB: { feeder: { from: 'query' }, packer: { size: 1 }, run: [1, 2], retries: 0, fail: 0.1 },
        compare: { feeder: { from: 'recoA' }, packer: { size: 1, join: 'recoB' }, run: [1, 2], fail: 0.1, finalize: ['every partner joined'] },
      },
      outputs: { cmp: { from: 'compare' } },
    });
    complete(sim);
    out(`join: ${sim.wg.status}, done ${sim.totals.done}, problematic ${sim.totals.problematic}, comparisons ${sim.outputs.cmp.files.length}`);
    check(sim.wg.status === 'Completed', 'join completes');
    const done = sim.nodes.compare.parcels.filter((p) => p.status === 'Done');
    check(done.every((p) => p.extras.length === 1), 'every compare parcel carries one partner');
    check(done.every((p) => p.extras[0].origin === p.inputs[0].file.origin), 'partner shares the origin');
    check(sim.log.some((e) => /every partner joined → Passed/.test(e.text)), 'finalizing action ran');
  }

  // ancestry lookup with two stagings
  {
    const sim = started({
      name: 'ancestry',
      sources: { rdst: { files: 30, ancestors: 'raw' }, raw: { files: 30 } },
      transformations: {
        stageRdst: { kind: 'replication', feeder: { from: 'rdst' }, packer: { size: 2 } },
        stageRaw: { kind: 'replication', feeder: { from: 'raw' }, packer: { size: 2 } },
        strip: { feeder: { from: 'rdst', after: ['stageRdst', 'stageRaw'] }, packer: { size: 2, lookup: 'raw' }, run: [1, 2] },
        merge: { feeder: { from: 'strip' }, packer: { size: 3 }, run: [1, 2] },
      },
      outputs: { out: { from: 'merge' } },
    });
    complete(sim);
    out(`ancestry: ${sim.wg.status}, replicated ${sim.totals.replicated}, out ${sim.outputs.out.files.length}`);
    check(sim.wg.status === 'Completed', 'ancestry completes');
    const done = sim.nodes.strip.parcels.filter((p) => p.status === 'Done');
    check(done.every((p) => p.extras.length === p.inputs.length), 'each strip input brings its ancestor');
    check(done.every((p) => p.inputs.every((i, k) => i.file.ancestor === p.extras[k])), 'ancestor matches');
  }

  // artifacts: histogram merge step and a finalizing merge action
  {
    const sim = started({
      name: 'artifacts',
      sources: { query: { files: 40 } },
      transformations: {
        reco: { feeder: { from: 'query' }, packer: { size: 2 }, run: [1, 2], artifact: true, finalize: [{ name: 'merge histograms', merge: 'hists' }] },
        merge: { feeder: { from: 'reco' }, packer: { size: 3 }, run: [1, 2] },
        histMerge: { feeder: { from: 'reco', port: 'artifact' }, packer: { size: 4 }, run: [0.5, 1], output: 'square' },
      },
      outputs: { out: { from: 'merge' }, hists: { from: 'reco', port: 'artifact' }, merged: { from: 'histMerge' } },
    });
    complete(sim);
    out(`artifacts: ${sim.wg.status}, out ${sim.outputs.out.files.length}, hists ${sim.outputs.hists.files.length}, merged ${sim.outputs.merged.files.length}`);
    check(sim.wg.status === 'Completed', 'artifacts completes');
    check(sim.outputs.hists.files.length === 1 && sim.outputs.hists.files[0].merged > 1, 'finalizing action merged the histogram box');
    check(sim.outputs.merged.files.length > 0, 'histogram merge step produced output');
    check([...sim.nodes.merge.inputs.values()].every((i) => i.file.port !== 'artifact'), 'main consumer received no artifacts');
    check(Object.values(sim.nodes).every((n) => n.status === 'Completed'), 'members end Completed');
  }

  // a removal writes its input off when the consumer did, so the workgraph still drains; while the consumer's inputs
  // are only Problematic the removal waits, since the operator may yet reset them
  {
    const sim = started({
      name: 'writeoff',
      sources: { query: { files: 40 } },
      transformations: {
        spruce: { feeder: { from: 'query' }, packer: { size: 2 }, run: [0.5, 1], fail: 0.3, retries: 0 },
        removal: { kind: 'removal', feeder: { from: 'query', after: ['spruce'] }, packer: { size: 3 } },
      },
      outputs: { out: { from: 'spruce' } },
    });
    check(until(sim, () => sim.waitsOnOperator(sim.nodes.spruce)), 'the sprucing ends with a quarantine');
    const pb = sim.counts(sim.nodes.spruce).Pb;
    check(pb > 0 && sim.counts(sim.nodes.removal).NP === 0 && sim.counts(sim.nodes.removal).U >= pb, 'the removal holds the quarantined files rather than writing them off');
    complete(sim);
    const written = sim.counts(sim.nodes.spruce).NP;
    const np = sim.counts(sim.nodes.removal).NP;
    out(`writeoff: ${sim.wg.status}, spruce quarantined ${pb} then written off ${written}, removal written off ${np}, deleted ${sim.totals.deleted}`);
    check(sim.wg.status === 'Completed', 'removal drains once the operator has written the quarantine off');
    check(written === pb && np === written, 'the removal wrote off exactly the files the operator gave up on');
    check(sim.totals.deleted + np === 40, 'every file was either removed or written off');
  }

  // provenance: a delivered file leads back to seeds, and the seeds lead to it; failed attempts are part of the history
  {
    const sim = started({
      name: 'lineage',
      transformations: {
        simulation: { feeder: { seeds: 40 }, packer: { size: 1 }, run: [0.5, 1], fail: 0.2 },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1] },
        merge: { feeder: { from: 'reco' }, packer: { size: 2 }, run: [0.5, 1] },
      },
      outputs: { out: { from: 'merge' } },
    });
    complete(sim);
    const delivered = sim.outputs.out.files[0];
    const lin = sim.lineageAround(delivered.id);
    const roots = [...lin.files.values()].filter((x) => x.parents.length === 0);
    check(roots.length >= 4 && roots.every((x) => x.seed != null), `a merged file descends from seeds: ${roots.length} roots`);
    const mismatched = [];
    for (const p of lin.parcels.values()) {
      for (const o of p.outputs) {
        const of = sim.files.get(o);
        const parents = new Set(of.parents);
        const inputs = new Set(p.inputs.map((i) => i.file.id));
        if (!(parents.size === inputs.size && [...inputs].every((x) => parents.has(x)))) mismatched.push(p.tag);
      }
    }
    check(!mismatched.length, `every parcel's outputs have its inputs as parents: ${mismatched.join(', ')}`);
    const back = sim.lineageAround(roots[0].id);
    check(back.files.has(delivered.id), 'the seed leads forward to the delivered file');
    const retried = [...sim.nodes.simulation.inputs.values()].find((i) => i.attempts.length > 1);
    check(!!retried, 'some simulation input was retried');
    if (retried) {
      const l2 = sim.lineageAround(retried.file.id);
      check([...l2.parcels.values()].some((p) => p.status === 'Failed'), 'the failed attempt is part of the lineage');
    }
    out(`lineage: ${lin.files.size} files, ${lin.parcels.size} parcels, ${lin.edges.length} edges, roots ${roots.length}`);
  }

  // a split's children name the sections they cover and the input they were cut from (ParentInputID, DX-ADR-004)
  {
    const { lineageStripSvg } = g.WorkgraphSim;
    const sim = started({
      name: 'masks',
      transformations: { process: { feeder: { seeds: 30 }, packer: { size: 1 }, run: [0.4, 0.8], partial: 0.6, sections: 8 } },
      outputs: { out: { from: 'process' } },
    });
    complete(sim);
    const node = sim.nodes.process;
    const child = [...node.inputs.values()].find((i) => i.parent != null && i.attempts.length);
    check(!!child, 'a split produced a child input');
    const svg = lineageStripSvg(sim, { file: child.file, ancestorDepth: 3, showAttempts: true, interactive: true });
    check(svg.includes(`sections ${child.mask}`), `the lineage names the child's sections: ${child.mask}`);
    check(svg.includes(`of input ${node.inputs.get(child.parent).tag}`), 'and the input it was cut from');
    const record = sim.inputAttempts(node, child);
    check(record.length > 0 && record[0].input.id === child.parent, "the child's record starts with its parent's attempts (ParentInputID)");
    /* the lineage dialog has the entity dialogs' shape: the file's tag as the title, its kind, where it came from on the
       right, the round close, and the explanation folded behind "what is this?" until asked */
    const { lineageHtml } = g.WorkgraphSim;
    const dlg = lineageHtml(sim, child.file);
    check(dlg.includes('wgsim-panel-head') && dlg.includes(`<b class="wgsim-panel-title">${child.file.tag}</b>`) && dlg.includes('· input lineage') && dlg.includes('wgsim-panel-origin') && dlg.includes('wgsim-panel-close') && dlg.includes('data-act="close-lineage"'), 'the lineage dialog has the header band the other dialogs have');
    check(dlg.includes('data-act="lineage-what"') && !dlg.includes('ParcelOutputs (DX-ADR-004)') && lineageHtml(sim, child.file, { what: true }).includes('ParcelOutputs (DX-ADR-004)'), 'its explanation is folded until what is this? is opened');

    /* and the band under the picture: where the file stands as an input, which is the way to that machine and the one place
       an operator can move this input without moving every input in its state with it */
    const { lineageInputsHtml } = g.WorkgraphSim;
    const { OPERATOR_EDGES } = g.WorkgraphSimEngine;
    const made = child.file.consumedBy.filter((c) => sim.nodes[c.node] && sim.nodes[c.node].inputs.get(c.input));
    check(made.length >= 2, `a split's file is more than one input of its member: ${made.length}`);
    const band = lineageInputsHtml(sim, child.file);
    check((band.match(/<li>/g) || []).length === made.length, `the band holds a row per input made from the file: ${made.length}`);
    check(band.includes('data-act="open-machine"') && band.includes('data-tab="inputs"'), 'and each row opens that member\'s input machine');
    /* the buttons are the machine's own operator edges, offered on the states that have them and on no others */
    for (const c of made) {
      const i = sim.nodes[c.node].inputs.get(c.input);
      const offered = [...band.matchAll(/data-input="(\d+)" data-to="(\w+)"/g)].filter((m) => Number(m[1]) === i.id).map((m) => m[2]);
      const legal = OPERATOR_EDGES[i.status] || [];
      check(offered.length === legal.length && legal.every((to) => offered.includes(to)), `input ${i.id} in ${i.status} is offered ${legal.join(', ') || 'nothing'}: ${offered.join(', ') || 'nothing'}`);
    }
    /* and a file no transformation holds as an input is a file, with no band and nothing for an operator to do to it */
    const terminal = [...sim.files.values()].find((f) => !f.consumedBy.length);
    check(!!terminal, 'some file is nobody\'s input');
    check(!lineageInputsHtml(sim, terminal) && lineageHtml(sim, terminal).includes('· file lineage'), 'a file that is nobody\'s input says file lineage and carries no band');
  }

  // the operator's edge taken on one input: that input moves and the ones beside it in the same state do not
  {
    const { OPERATOR_EDGES } = g.WorkgraphSimEngine;
    const sim = started({
      name: 'one input',
      transformations: { process: { feeder: { seeds: 40 }, packer: { size: 1 }, fail: 0.9, retries: 0, run: [0.3, 0.6] } },
      outputs: { out: { from: 'process' } },
    });
    for (let k = 0; k < 400 && sim.counts(sim.nodes.process).Pb < 2; k++) sim.step(0.5);
    const node = sim.nodes.process;
    const quarantined = [...node.inputs.values()].filter((i) => i.status === 'Problematic');
    check(quarantined.length >= 2, `the quarantine holds more than one input: ${quarantined.length}`);
    const one = quarantined[0];
    const rest = quarantined.slice(1);
    check(OPERATOR_EDGES.Problematic.includes('Unassigned') && OPERATOR_EDGES.Problematic.includes('NotProcessed'), 'a quarantined input may be reset or written off');
    check(sim.decideInput('process', one.id, 'Unassigned') && one.status === 'Unassigned' && one.errors === 0, 'a reset moves that input back to the pool and clears its failures');
    check(rest.every((i) => i.status === 'Problematic'), `and leaves the rest quarantined: ${rest.length}`);
    const two = rest[0];
    check(sim.decideInput('process', two.id, 'NotProcessed') && two.status === 'NotProcessed', 'a write-off moves that input to NotProcessed');
    /* the same guard the machines keep: an edge that is not the operator's is not taken */
    check(!sim.decideInput('process', two.id, 'Unassigned'), 'and no edge is taken out of a state the operator has none from');
    check(!sim.decideInput('process', one.id, 'Assigned'), 'nor to a state no operator edge leads to');
    out(`one input: reset ${one.file.tag}, wrote off ${two.file.tag}, ${rest.length - 1} left quarantined`);
  }

  // the failed-input strip: every attempt of the input, numbered, the newest at full contrast, folded past four and unfolded in place
  {
    const { lineageStripSvg } = g.WorkgraphSim;
    const { Sim } = g.WorkgraphSimEngine;
    const sim = new Sim({
      name: 'attempts',
      settings: { hookPeriod: 0 },
      transformations: { simulation: { feeder: { seeds: 2 }, packer: { size: 1 }, run: [0.2, 0.4], fail: 1, retries: 12 } },
    });
    sim.start();
    const node = sim.nodes.simulation;
    check(until(sim, () => sim.counts(node).F > 0), 'an input fails');
    const first = [...node.inputs.values()].find((i) => i.status === 'Failed');
    const strip = (i, expanded) => lineageStripSvg(sim, { file: i.file, input: i, node, ancestorDepth: 1, showAttempts: true, interactive: false, expanded });
    const parcels = (svg) => (svg.match(/wg-lin-parcel/g) || []).length;
    let svg = strip(first, false);
    check(parcels(svg) === 1 && svg.includes('wg-lin-latest') && !svg.includes('wg-lin-earlier') && !svg.includes('wg-lin-fold'), 'one attempt draws one parcel, the latest, with nothing folded');
    check(svg.includes('wg-lin-focus') && !svg.includes('data-act="lineage"'), 'the input is the ringed focus and nothing is clickable');
    sim.setMode('simulation', 'failedInput', 'auto');
    const many = () => [...node.inputs.values()].find((i) => i.attempts.length >= 9);
    check(until(sim, () => !!many()), 'with the hook sweeping an input reaches nine attempts');
    const i9 = many();
    svg = strip(i9, false);
    check(parcels(svg) === 4 && svg.includes(`+${i9.attempts.length - 4} earlier`) && svg.includes('data-act="step-more"'), `nine attempts draw four and fold the rest: ${parcels(svg)} drawn`);
    check((svg.match(/wg-lin-earlier/g) || []).length === 3 && (svg.match(/wg-lin-latest/g) || []).length === 1, 'three earlier, one latest');
    check(svg.includes(`>${i9.attempts.length}<`), 'the newest keeps its true number');
    svg = strip(i9, true);
    check(parcels(svg) === i9.attempts.length && !svg.includes('wg-lin-fold'), 'unfolded, every attempt is drawn');
  }

  // the rails exist only in expert mode: laid out without them a card is its body, with them the body plus the rail, and
  // switching a row never lays the graph out again, since the rail's width is fixed
  {
    const { layout, cardSvg, RAIL, BODY } = g.WorkgraphSim;
    const { Sim } = g.WorkgraphSimEngine;
    const sim = new Sim({ name: 'rail-layout', transformations: { simulation: { feeder: { seeds: 4 }, packer: { size: 1 } }, reco: { feeder: { from: 'simulation' }, packer: { size: 1 } } } });
    const plain = await layout(sim);
    const before = await layout(sim, { rail: true });
    sim.setAllModes('manual');
    const after = await layout(sim, { rail: true });
    const pos = (L) => Object.values(L.items).map((it) => `${it.id}:${it.x},${it.y},${it.w},${it.h}`).join(' ');
    const cards = (L) => Object.values(L.items).filter((it) => it.kind === 'node');
    check(pos(before) === pos(after), 'the layout is the same with every row by hand');
    check(!plain.rail && cards(plain).every((it) => it.w === BODY.w && !it.rail), 'outside expert mode a card is its body alone');
    check(before.rail && cards(before).every((it) => it.w === BODY.w + RAIL.w && it.rail) && BODY.w > RAIL.w, 'in expert mode a card is laid out with its rail');
    check(!cardSvg(plain.items.simulation, 'simulation', 'compute').includes('wg-rail'), 'and outside it no rail is drawn at all');
    check(cardSvg(before.items.simulation, 'simulation', 'compute').includes('wg-rail-row'), 'in it the card carries its rail');
    /* the card is where a rail is drawn, and every row of it starts automatic: the frame sets each row's state from
       `railStates`, which controls.test.js drives, and the stylesheet picks what shows from that one attribute */
    const railed = cardSvg(before.items.simulation, 'simulation', 'compute');
    check((railed.match(/data-state="auto"/g) || []).length === 4 && !railed.includes('data-state="run"'), 'a card draws every row automatic and leaves the state to the frame');
    check(railed.includes('class="wg-rail-glyph">▷▷<') && railed.includes('class="wg-rail-name">feeder<') && railed.includes('wg-rail-run'), 'a row carries its glyph, its name and its run button, and the state picks which shows');
  }

  // routing: a fan-out from one port is one trunk that branches, a fan-in is never merged, and the
  // main and artifact outputs never share a trunk
  {
    const { layout } = g.WorkgraphSim;
    const sim = started({
      name: 'routing',
      sources: { query: { files: 40 } },
      transformations: {
        stage: { kind: 'replication', feeder: { from: 'query' }, packer: { size: 2 } },
        reco: { feeder: { from: 'query', after: ['stage'] }, packer: { size: 2 }, artifact: true },
        merge: { feeder: { from: 'reco' }, packer: { size: 2 } },
        hists: { feeder: { from: 'reco', port: 'artifact' }, packer: { size: 2 } },
      },
      outputs: { out: { from: 'merge' } },
    });
    const L = await layout(sim);
    const edge = (from, to) => L.edges.find((e) => e.from === from && e.to === to);
    /* how far two paths run together, measured along their geometry: a branch that runs straight
       through a point where the other turns is still on the trunk, so counting points would lie */
    const trunk = (a, b) => {
      const on = (pts, q) => {
        for (let i = 1; i < pts.length; i++) {
          const p0 = pts[i - 1];
          const p1 = pts[i];
          const within = (lo, hi, v) => v >= Math.min(lo, hi) - 0.6 && v <= Math.max(lo, hi) + 0.6;
          const straight = Math.abs(p1.x - p0.x) < 0.6 ? Math.abs(q.x - p0.x) < 0.6 : Math.abs(q.y - p0.y) < 0.6;
          if (straight && within(p0.x, p1.x, q.x) && within(p0.y, p1.y, q.y)) return true;
        }
        return false;
      };
      let far = 0;
      for (let t = 1; t <= 40; t++) {
        const q = { x: a.points[0].x + ((a.points[1].x - a.points[0].x) * t) / 40, y: a.points[0].y + ((a.points[1].y - a.points[0].y) * t) / 40 };
        if (!on(b.points, q)) break;
        far = Math.hypot(q.x - a.points[0].x, q.y - a.points[0].y);
      }
      return far;
    };

    /* a fan-out from query's one port: a shared trunk, and a junction dot where it splits */
    const toStage = edge('query', 'stage');
    const toReco = edge('query', 'reco');
    check(trunk(toStage, toReco) > 10, `a fan-out from one port shares a trunk: ${Math.round(trunk(toStage, toReco))}px`);
    check(L.junctions.length > 0, 'and the split is marked with a junction');
    const j = L.junctions[0];
    check(Math.hypot(j.x - toStage.points[0].x, j.y - toStage.points[0].y) > 10, 'the junction is below the port, not on it');

    /* the artifact output leaves its own port, so it can never share the main trunk */
    const main = edge('reco', 'merge');
    const art = edge('reco', 'hists');
    check(art.port === 'artifact' && main.port === 'main', 'the two outputs are on different ports');
    check(trunk(main, art) === 0, 'and share no trunk at all');

    /* fan-in: reco is fed by query and waits on stage; the two arrive on their own ports */
    const wait = L.edges.find((e) => e.kind === 'wait' && e.to === 'reco');
    check(!!wait, 'the wait dependency is routed');
    const endOf = (e) => e.points[e.points.length - 1];
    check(Math.abs(endOf(wait).x - endOf(toReco).x) > 1, 'a fan-in lands on one input port per upstream, never merged');

    /* a skip edge is routed around, never straight through */
    const rows = [...new Set(Object.values(L.items).map((it) => Math.round(it.y)))].sort((a, b) => a - b);
    const rowOf = (id) => rows.indexOf(Math.round(L.items[id].y));
    check(rowOf('reco') - rowOf('query') > 1, 'query to reco is a skip edge');
    check(toReco.points.length > 2, `and it detours rather than cutting across: ${toReco.points.length} points`);
    out(`routing: trunk ${Math.round(trunk(toStage, toReco))}px, ${L.junctions.length} junction(s), skip edge ${toReco.points.length} points`);
  }
  });
})(globalThis);
