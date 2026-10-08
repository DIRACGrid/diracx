// Base scenarios run to completion and keep the exactly-one-parcel invariant.
(function (g) {
  g.WG_TEST.suite('engine', async () => {
  const { out, check, started, complete, writeOffQuarantine } = g.WG_TEST;
  const { layout } = g.WorkgraphSim;

  const scenarios = {
    simple: {
      name: 'simple',
      transformations: {
        simulation: { feeder: { seeds: 60 }, packer: { size: 1 }, output: 'circle', run: [1, 2] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 3 }, run: [1, 2] },
        merge: { feeder: { from: 'reco' }, packer: { size: 4 }, run: [1, 2] },
      },
      outputs: { datasets: { from: 'merge', label: 'datasets' } },
    },
    fanout: {
      name: 'fan-out',
      sources: { query: { files: 80 } },
      transformations: {
        reco1: { feeder: { from: 'query' }, packer: { size: 2 }, emit: 'input', run: [1, 2] },
        filter1: { feeder: { from: 'reco1' }, packer: { size: 3 }, emit: 'input', run: [1, 2] },
        mergeA: { feeder: { from: 'filter1' }, packer: { size: 3, by: 'type' }, run: [1, 2] },
      },
      outputs: { t1: { from: 'mergeA', show: 'histogram' } },
    },
    compare: {
      name: 'compare + removal',
      sources: { query: { files: 60 } },
      transformations: {
        recoA: { feeder: { from: 'query' }, packer: { size: 1 }, run: [1, 2] },
        recoB: { feeder: { from: 'query' }, packer: { size: 1 }, run: [1, 2] },
        compare: { feeder: { from: ['recoA', 'recoB'] }, packer: { size: 2, by: 'origin' }, run: [1, 2] },
        mergeA: { feeder: { from: 'recoA' }, packer: { size: 4 }, run: [1, 2] },
        removal: { kind: 'removal', feeder: { from: 'recoA', after: ['compare', 'mergeA'] }, packer: { size: 4 } },
      },
      outputs: { comparisons: { from: 'compare' }, datasets: { from: 'mergeA' } },
    },
    scouting: {
      name: 'scouting',
      workgraph: { scouting: { count: 10 } },
      transformations: {
        simulation: { feeder: { seeds: 40 }, packer: { size: 1 }, run: [1, 2] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [1, 2] },
        replication: { kind: 'replication', feeder: { from: 'reco' }, packer: { size: 3 }, hold: 'approval' },
      },
    },
    sprucing: {
      name: 'sprucing',
      workgraph: { scouting: { count: 8 } },
      sources: { query: { files: 40 } },
      transformations: {
        staging: { kind: 'replication', feeder: { from: 'query' }, packer: { size: 2 } },
        spruce: { feeder: { from: 'query', after: ['staging'] }, packer: { size: 2 }, run: [1, 2] },
        merge: { feeder: { from: 'spruce' }, packer: { size: 3 }, run: [1, 2] },
        replication: { kind: 'replication', feeder: { from: 'merge' }, packer: { size: 2 }, hold: 'approval' },
        removal: { kind: 'removal', feeder: { from: 'query', after: ['spruce'] }, packer: { size: 3 } },
      },
      outputs: { out: { from: 'merge' } },
    },
    cms: {
      name: 'cms',
      sources: { query: { files: 40 } },
      transformations: {
        process: { feeder: { from: 'query' }, packer: { size: 2 }, fail: 0.05, partial: 0.25, run: [1, 2], emit: 'input' },
        merge: { feeder: { from: 'process' }, packer: { size: 4 }, run: [1, 2] },
      },
      outputs: { out: { from: 'merge' } },
    },
  };

  for (const [key, spec] of Object.entries(scenarios)) {
    const sim = started(spec);
    const L = await layout(sim);
    check(L.width > 0 && L.height > 0, key + ': layout has size');
    const seen = new Set([sim.wg.status]);
    let steps = 0;
    while (sim.wg.status !== 'Completed' && steps < 40000) {
      sim.step(0.05);
      /* a Problematic input holds the drain until an operator moves it (DX-ADR-005), so the operator writes each
         quarantine off as soon as it is all that holds a member, as the other drivers here do */
      writeOffQuarantine(sim);
      seen.add(sim.wg.status);
      steps++;
    }
    out(`${key}: ${sim.wg.status} after ${(steps * 0.05).toFixed(0)}s, states ${[...seen].join('>')}, done ${sim.totals.done}`);
    check(sim.wg.status === 'Completed', key + ': reaches Completed');
    for (const n of Object.values(sim.nodes)) {
      const c = sim.counts(n);
      check(c.U === 0 && c.A === 0 && c.F === 0, `${key}/${n.id}: no live inputs at the end`);
      check(!sim.liveParcels(n), `${key}/${n.id}: no live parcels at the end`);
      const owners = new Map();
      for (const p of n.parcels) if (p.status === 'Done') for (const i of p.inputs) owners.set(i.id, (owners.get(i.id) || 0) + 1);
      /* one check for the whole pool rather than one per input: a check total that moves with the run cannot tell a
         deleted assertion from a drifted one, so every assertion here is emitted a fixed number of times */
      const shared = [];
      for (const i of n.inputs.values()) {
        if (i.status === 'Processed' && n.parcels.some((p) => p.inputs.includes(i)) && owners.get(i.id) !== 1) shared.push(`${i.tag} by ${owners.get(i.id)}`);
      }
      check(!shared.length, `${key}/${n.id}: every Processed input was processed by exactly one parcel: ${shared.join(', ')}`);
    }
    for (const o of Object.values(sim.outputs)) check(o.files.length > 0, `${key}/${o.id}: produced output`);
    check(Object.values(sim.nodes).every((n) => !n.feederEnabled), key + ': every feeder has been disabled by the time the workgraph completes');
    if (spec.workgraph && spec.workgraph.scouting) check(seen.has('Scouting') && seen.has('Approving'), key + ': went through scouting and approving');
    if (key === 'cms') check(sim.totals.split > 0, 'cms: some inputs were split');
    if (key === 'compare' || key === 'sprucing') check(sim.totals.deleted > 0, key + ': removal deleted something');
  }

  // New until started, Completed then Archived, and reset returns to New
  {
    const sim = new g.WorkgraphSimEngine.Sim(scenarios.simple);
    check(sim.wg.status === 'New', 'starts in New');
    for (let i = 0; i < 40; i++) sim.step(0.05);
    check(sim.totals.done === 0, 'nothing runs before start');
    sim.start();
    complete(sim);
    for (let i = 0; i < 400; i++) sim.step(0.05);
    check(sim.wg.status === 'Archived', 'archives after Completed: ' + sim.wg.status);
    check(Object.values(sim.nodes).every((n) => n.status === 'Archived' && n.inputs.size === 0 && n.parcels.length === 0), 'archiving cleaned the bulk rows');
    check(Object.values(sim.outputs).every((o) => o.files.length > 0), 'archiving kept the outputs');
    sim.reset();
    check(sim.wg.status === 'New', 'reset returns to New');
  }
  // the parcel machine of DX-ADR-005: a submission that fails goes back to Unassigned, a backend failure is reported on
  // Assigned without a Completing step, and a cancellation is an outcome recorded while Completing
  {
    const { Sim } = g.WorkgraphSimEngine;
    const seen = { Reserved: 0, Completing: 0, back: 0 };
    const sim = new Sim({
      name: 'parcels',
      workgraph: { approval: 'auto' },
      transformations: { r: { feeder: { seeds: 40 }, packer: { size: 1 }, run: [0.4, 0.8], fail: 0.3, retries: 3, submitFail: 0.3 } },
    });
    sim.start();
    let last = new Map();
    let budget = 60000;
    while (sim.wg.status !== 'Completed' && budget-- > 0) {
      sim.step(0.05);
      g.WG_TEST.writeOffQuarantine(sim);
      for (const p of sim.nodes.r.parcels) {
        const was = last.get(p.id);
        if (was === p.status) continue;
        if (was === 'Reserved' && p.status === 'Unassigned') seen.back += 1;
        if (was === 'Assigned' && p.status === 'Failed') seen.Reserved += 1;
        if (was === 'Completing' && p.status === 'Failed') seen.Completing += 1;
        last.set(p.id, p.status);
      }
    }
    out(`parcels: resubmitted ${seen.back}, Assigned→Failed ${seen.Reserved}, Completing→Failed ${seen.Completing}`);
    check(seen.back > 0, 'a submission that fails returns the parcel to Unassigned');
    check(seen.Reserved > 0 && seen.Completing === 0, 'a backend failure is reported on Assigned, never through Completing');
    check(sim.parcelCounts(sim.nodes.r).Failed === seen.Reserved, 'and the counters agree');
  }

  // HandleFailedInput can subdivide instead of retrying, and the children's masks stay inside the parent's sections
  {
    const { Sim } = g.WorkgraphSimEngine;
    const sim = new Sim({
      name: 'subdivide',
      workgraph: { approval: 'auto' },
      transformations: { r: { feeder: { seeds: 30 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0.5, subdivide: 0.7, retries: 1, sections: 8 } },
    });
    sim.start();
    complete(sim);
    const node = sim.nodes.r;
    const t = node.transitions;
    out(`subdivide: Failed→Split ${t['Failed>Split']}, Failed→Unassigned ${t['Failed>Unassigned']}, Failed→Problematic ${t['Failed>Problematic'] || 0}`);
    check(t['Failed>Split'] > 0 && t['Failed>Unassigned'] > 0, 'the failure hook both retries and subdivides');
    check(node.decisions.subdivided === t['Failed>Split'], 'the hooks panel counts what it decided');
    check(sim.verifyTransitions(node).length === 0, 'and the counters still reconcile with the occupancy');
    const bad = [...node.inputs.values()].filter((i) => i.lo < 1 || i.hi > 8 || i.lo > i.hi);
    check(bad.length === 0, `every input covers a real range of sections: ${bad.length} bad`);
    const children = [...node.inputs.values()].filter((i) => i.parent != null);
    check(children.length > 0 && children.every((i) => i.mask === `${i.lo}-${i.hi}`), 'a child carries the mask of its sections');
  }

  // a joining packer of more than one input per parcel takes one partner per input
  {
    const sim = started({
      name: 'join2',
      sources: { query: { files: 40 } },
      transformations: {
        a: { feeder: { from: 'query' }, packer: { size: 1 }, run: [0.5, 1], emit: 'input', partial: 0.6, sections: 4 },
        b: { feeder: { from: 'query' }, packer: { size: 1 }, run: [0.5, 1], emit: 'input', partial: 0.6, sections: 4 },
        cmp: { feeder: { from: 'a' }, packer: { size: 2, by: 'origin', join: 'b' }, run: [0.5, 1] },
      },
      outputs: { out: { from: 'cmp' } },
    });
    complete(sim);
    const done = sim.nodes.cmp.parcels.filter((p) => p.status === 'Done' && !p.recovery);
    const partners = done.flatMap((p) => p.extras.map((e) => e.id));
    out(`join2: ${sim.wg.status}, ${done.length} parcels, ${partners.length} partners`);
    check(sim.wg.status === 'Completed', 'the joined workgraph completes');
    check(done.some((p) => p.inputs.length > 1), `some parcel joined more than one input of an origin: ${Math.max(...done.map((p) => p.inputs.length))} at most`);
    check(done.every((p) => p.extras.length === p.inputs.length), 'every input in a parcel brought its own partner');
    check(new Set(partners).size === partners.length, 'and no partner was handed to two inputs');
  }

  // slots beyond what a card can draw are clamped, and the terminal counters outlive the pruning of the parcel list
  {
    const { Sim, MAX_SLOTS } = g.WorkgraphSimEngine;
    const sim = new Sim({
      name: 'slots',
      workgraph: { approval: 'auto' },
      transformations: { r: { feeder: { seeds: 500 }, packer: { size: 1 }, run: [0.2, 0.4], slots: 40 } },
    });
    check(sim.nodes.r.spec.slots === MAX_SLOTS, `a capacity past the grid is clamped to ${MAX_SLOTS}: ` + sim.nodes.r.spec.slots);
    sim.start();
    complete(sim, 200000);
    const pc = sim.parcelCounts(sim.nodes.r);
    out(`slots: ${sim.nodes.r.parcels.length} parcels kept, ${pc.Done} counted Done`);
    check(sim.nodes.r.parcels.length < pc.Done, 'the parcel list was pruned');
    check(pc.Done === sim.totals.done, 'and the counters still hold every parcel that finished');
  }

  // the event log: oldest first, one subject per line, three levels of emphasis, capped and dropped from the top
  {
    const { Sim } = g.WorkgraphSimEngine;
    const { logText } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'log',
      workgraph: { approval: 'auto' },
      transformations: {
        simulation: { feeder: { seeds: 200 }, packer: { size: 1 }, run: [0.2, 0.4], fail: 0.3, partial: 0.4, subdivide: 0.5, submitFail: 0.3, retries: 1, emit: 'input' },
        merge: { feeder: { from: 'simulation' }, packer: { size: 4 }, run: [0.2, 0.4] },
      },
      outputs: { datasets: { from: 'merge' } },
    });
    sim.start();
    /* on to Archived, so that the archiving lists are in the log too and a running action is there to read */
    for (let k = 0; k < 200000 && sim.wg.status !== 'Archived'; k++) {
      sim.step(0.05);
      if (sim.blocked) sim.forceAction();
      g.WG_TEST.writeOffQuarantine(sim);
    }
    check(sim.wg.status === 'Archived', 'the noisy model archives');
    const log = sim.log;
    out(`log: ${log.length} lines over ${sim.t.toFixed(0)}s`);
    check(log.length === sim.logSeq, `the log keeps the whole run: ${log.length} of ${sim.logSeq}`);
    check(log.every((e, k) => e.seq === k + 1), "so a line's number is the event's number");
    check(log.every((e, k) => k === 0 || e.t >= log[k - 1].t), 'the log is in order, oldest first');
    const subjects = new Set(['workgraph'].concat(Object.keys(sim.nodes)));
    check(log.every((e) => subjects.has(e.subject)), 'every line names an entity, and nothing else');
    check(log.every((e) => !/:/.test(e.subject) && !e.text.startsWith(e.subject)), 'the subject is the column, not a prefix on the message');
    check(log.every((e) => ['state', 'note', 'running'].includes(e.kind)), 'every line carries one of the three levels');
    const states = log.filter((e) => e.kind === 'state');
    check(states.length > 0 && states.every((e) => e.text === `→ ${e.to}`), 'a transition says the state it reached and nothing more');
    /* the cause is a field, not a clause the renderer has to find again in the sentence */
    check(states.some((e) => e.why), 'a transition that has a cause records it separately: ' + (states.find((e) => e.why) || {}).why);
    check(states.every((e) => !e.text.includes('(')), 'and never folds it into the text');
    check(log.some((e) => e.kind === 'running' && /^running /.test(e.text)), 'a running action is the level its own result supersedes');
    /* the state a line ends on carries that state's colour; a transition is the state itself, so it is drawn as the strip
       and the machines draw one rather than as a word the renderer has coloured in */
    check(/→ <span class="wgsim-log-ok">Done<\/span>$/.test(logText({ kind: 'note', text: 'clean intermediates → Done', to: 'Done' })), 'a finished action ends in the success colour: ' + logText({ kind: 'note', text: 'clean intermediates → Done', to: 'Done' }));
    check(logText({ kind: 'state', to: 'Archived' }) === '→ <span class="wgsim-log-to" data-status="Archived">Archived</span>', 'a transition names the state and lets the one tone map colour it: ' + logText({ kind: 'state', to: 'Archived' }));
    check(/data-blocked="wait"/.test(logText({ kind: 'state', to: 'ApprovingBlocked', manual: true })), 'and a block on a sign-off nobody has given is a wait, as it is on the card: ' + logText({ kind: 'state', to: 'ApprovingBlocked', manual: true }));
    check(!/data-blocked/.test(logText({ kind: 'state', to: 'ApprovingBlocked' })), 'where any other failed action is a problem');
    check(logText({ kind: 'state', to: 'Paused', why: 'held <back>' }).endsWith('<span class="wgsim-log-why">held &lt;back&gt;</span>'), 'and its cause follows it, muted and escaped: ' + logText({ kind: 'state', to: 'Paused', why: 'held <back>' }));
    check(logText({ kind: 'note', text: 'input <a> → Problematic', to: 'Problematic' }) === 'input &lt;a&gt; → <span class="wgsim-log-bad">Problematic</span>', 'a message is escaped before it is coloured');
  }

  });
})(globalThis);
