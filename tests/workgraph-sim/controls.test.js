// Operator controls and actions: start, blocked actions, sign-off, the Active hook, feeder disable, flush, cancel.
(function (g) {
  g.WG_TEST.suite('controls', async () => {
  const { check, until, drain, complete, started, writeOffQuarantine, readText, HANG } = g.WG_TEST;
  const { Sim } = g.WorkgraphSimEngine;

  // approving actions with a failure, rerun, and manual sign-off; then the Active hook raises the feeder in batches
  {
    const sim = new Sim({
      name: 'blocked',
      workgraph: { scouting: { count: 6 }, approving: [{ name: 'check success rate', check: 'success rate' }, { name: 'estimate resource usage', fail: 'once' }, { name: 'manual approval', manual: true }] },
      transformations: {
        simulation: { feeder: { seeds: 30, batch: 10 }, packer: { size: 1 }, run: [0.5, 1], fail: 0 },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1], fail: 0 },
      },
    });
    sim.start();
    check(until(sim, () => sim.wg.status === 'ApprovingBlocked'), 'a failing action blocks approval');
    check(sim.blocked && sim.blocked.name === 'estimate resource usage', 'the failed action is the blocked one');
    check(sim.wgList[1].result === 'Failed', 'the list records the failure');
    sim.rerunAction();
    check(sim.wg.status === 'Approving', 'rerun returns to Approving');
    check(until(sim, () => sim.blocked && sim.blocked.manual), 'reaches the manual sign-off');
    check(sim.wg.status === 'ApprovingBlocked', 'a sign-off nobody has given blocks the workgraph');
    sim.forceAction();
    check(sim.wg.status === 'Active', 'forcing it passed is the sign-off, and moves to Active');
    check(until(sim, () => sim.nodes.simulation.seedRequested === 16), 'the Active hook raised the limit by one batch');
    check(until(sim, () => sim.nodes.simulation.seedNext === 16), 'the feeder fed up to the new limit at once');
    check(until(sim, () => sim.nodes.simulation.seedRequested === 30), 'the Active hook reached the requested total');
    check(drain(sim, () => sim.wg.status === 'Completed'), 'completes');
  }

  // the Active hook raises one batch at a time, and stops at the output target
  {
    const sim = new Sim({
      name: 'target',
      workgraph: { scouting: { count: 15 }, approval: 'auto', target: { output: 'datasets', files: 20 } },
      transformations: {
        simulation: { feeder: { seeds: 600, batch: 60 }, packer: { size: 1 }, run: [1.4, 2.6] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 3 } },
        merge: { feeder: { from: 'reco' }, packer: { size: 4 } },
      },
      outputs: { datasets: { from: 'merge' } },
    });
    sim.start();
    const limits = [];
    let disabledAt = null;
    let steps = 0;
    /* The harness's hang ceiling rather than a budget fitted to this run: the check below is that the workgraph terminates. */
    while (sim.wg.status !== 'Completed' && steps++ < HANG) {
      sim.step(0.05);
      writeOffQuarantine(sim);
      const l = sim.nodes.simulation.seedRequested;
      if (limits[limits.length - 1] !== l) limits.push(l);
      if (disabledAt == null && !sim.nodes.simulation.feederEnabled) disabledAt = sim.outputs.datasets.files.length;
    }
    check(sim.wg.status === 'Completed', 'target run completes');
    check(limits.every((l, k) => k === 0 || l - limits[k - 1] === 60), 'the limit rises one batch at a time: ' + limits.join(' > '));
    check(disabledAt === 20, 'the Active hook disabled the feeder at the target: ' + disabledAt);
    check(sim.nodes.simulation.seedNext < 600, 'the request was only an upper bound');
  }

  // a feeder whose limit is reached is still running until the Active hook disables it, so the cascade waits
  {
    const sim = new Sim({
      name: 'waiting',
      workgraph: { target: { output: 'out', files: 1000 } },
      transformations: {
        simulation: { feeder: { seeds: 600, batch: 20, inflight: 1 }, packer: { size: 1 }, run: [0.5, 1] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1] },
      },
      outputs: { out: { from: 'reco' } },
    });
    sim.start();
    check(until(sim, () => sim.nodes.simulation.seedNext === 20 && !sim.liveInputs(sim.nodes.simulation) && !sim.liveParcels(sim.nodes.simulation), 8000), 'first batch fully processed before the Active hook raises again');
    check(sim.feederActive(sim.nodes.simulation), 'the feeder still counts as running while the Active hook may raise it');
    check(sim.feederInfo(sim.nodes.simulation).state === 'waiting on the hook', 'the strip says it waits on the hook, which is what raises the limit: ' + sim.feederInfo(sim.nodes.simulation).state);
    check(sim.nodes.reco.feederEnabled && sim.wg.status === 'Active', 'the downstream feeder has not been stopped');
  }

  // force instead of rerun
  {
    const sim = new Sim({
      name: 'force',
      workgraph: { scouting: { count: 4 }, approval: 'auto', approving: [{ name: 'always fails', fail: true }] },
      transformations: { simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.5, 1] } },
    });
    sim.start();
    check(until(sim, () => sim.wg.status === 'ApprovingBlocked'), 'blocked');
    sim.rerunAction();
    check(until(sim, () => sim.wg.status === 'ApprovingBlocked'), 'blocked again after rerun');
    sim.forceAction();
    check(sim.wg.status === 'Active', 'forced past the failing action');
    check(until(sim, () => sim.wg.status === 'Completed'), 'completes');
  }

  // a disabled feeder keeps the workgraph in Scouting
  {
    const sim = new Sim({
      name: 'nofeed',
      workgraph: { scouting: { count: 6 }, approval: 'auto' },
      transformations: { simulation: { feeder: { seeds: 30 }, packer: { size: 1 }, run: [0.5, 1] } },
    });
    sim.setFeederEnabled('simulation', false);
    sim.start();
    for (let i = 0; i < 400; i++) sim.step(0.05);
    check(sim.wg.status === 'Scouting' && sim.totals.done === 0, 'stays in Scouting with nothing processed');
    sim.setFeederEnabled('simulation', true);
    check(sim.nodes.simulation.feederEnabled, 'a disabled feeder can be re-enabled for recovery');
  }

  // disabling an edge feeder stops its pool filling; the producer's outputs wait unfed
  {
    const sim = new Sim({
      name: 'edge-off',
      transformations: {
        simulation: { feeder: { seeds: 60 }, packer: { size: 1 }, run: [0.5, 1] },
        merge: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1] },
      },
    });
    sim.start();
    check(until(sim, () => sim.nodes.merge.inputs.size >= 4), 'the merge is being fed');
    sim.setFeederEnabled('merge', false);
    const fed = sim.nodes.merge.inputs.size;
    check(until(sim, () => sim.wg.status === 'Completed'), 'the workgraph still completes');
    check(sim.nodes.merge.inputs.size === fed, `no input reached the merge after its feeder was disabled: ${fed} → ${sim.nodes.merge.inputs.size}`);
    check(sim.nodes.merge.unfed.length > 0, `the later outputs wait unfed: ${sim.nodes.merge.unfed.length}`);
  }

  // feeder disabled mid-way drains the workgraph
  {
    const sim = new Sim({ name: 'disable', workgraph: { target: { output: 'out', files: 1000 } }, transformations: { simulation: { feeder: { seeds: 400, batch: 50 }, packer: { size: 1 }, run: [0.5, 1] } }, outputs: { out: { from: 'simulation' } } });
    sim.start();
    check(until(sim, () => sim.nodes.simulation.seedNext === 50), 'the first batch is issued in one sweep');
    sim.setFeederEnabled('simulation', false);
    check(drain(sim, () => sim.wg.status === 'Completed'), 'disabling the feeder lets the workgraph drain');
    check(sim.nodes.simulation.seedNext === 50, 'no seeds issued after the feeder was disabled');
  }

  // sweeps: manual mode runs nothing until asked; the failure multiplier scales every rate
  {
    const sim = new Sim({
      name: 'manual',
      settings: { feederPeriod: 0, packerPeriod: 0, hookPeriod: 0 },
      transformations: {
        simulation: { feeder: { seeds: 20 }, packer: { size: 1 }, run: [0.5, 1] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1] },
      },
    });
    sim.start();
    for (let i = 0; i < 100; i++) sim.step(0.05);
    check(sim.nodes.simulation.seedNext === 0, 'manual feeder issues nothing on its own');
    sim.runFeeder('simulation');
    check(sim.nodes.simulation.seedNext === 20, 'one feeder sweep issues everything owed');
    for (let i = 0; i < 100; i++) sim.step(0.05);
    check(sim.nodes.simulation.parcels.length === 0, 'manual packer packs nothing on its own');
    sim.runPacker('simulation');
    check(sim.nodes.simulation.parcels.length === 20, 'one packer sweep packs every ready input');
    const held = new Sim({
      name: 'held',
      settings: { hookPeriod: 0 },
      workgraph: { scouting: { count: 6 }, approval: 'auto' },
      transformations: { simulation: { feeder: { seeds: 20 }, packer: { size: 1 }, run: [0.5, 1], fail: 0 } },
    });
    held.start();
    check(until(held, () => held.scoutDone()), 'the scouting sample drains with the actions manual');
    for (let i = 0; i < 100; i++) held.step(0.05);
    check(held.wg.status === 'Scouting', 'manual actions hold the workgraph in Scouting');
    held.sweepHooks();
    check(held.wg.status === 'Approving', 'one hook sweep accepts the scout');
    check(until(held, () => held.wg.status === 'Completed'), 'the model runs on once the hooks are run by hand');
    const noFail = new Sim({ name: 'nofail', settings: { failScale: 0 }, transformations: { simulation: { feeder: { seeds: 40 }, packer: { size: 1 }, run: [0.5, 1], fail: 0.5 } } });
    noFail.start();
    check(until(noFail, () => noFail.wg.status === 'Completed'), 'completes with failures off');
    check(noFail.totals.failed === 0, 'a failure multiplier of zero means no failures');
    const allFail = new Sim({ name: 'allfail', settings: { failScale: 3 }, transformations: { simulation: { feeder: { seeds: 20 }, packer: { size: 1 }, run: [0.5, 1], fail: 0.4, retries: 0 } } });
    allFail.start();
    check(until(allFail, () => allFail.waitsOnOperator(allFail.nodes.simulation)), 'with failures forced everything ends in the quarantine');
    check(allFail.totals.done === 0 && allFail.totals.problematic === 20, 'a multiplier past one saturates the failure rate');
  }

  // flush, and a finalizing action that fails
  {
    const sim = new Sim({
      name: 'ops',
      transformations: {
        simulation: { feeder: { seeds: 40 }, packer: { size: 1 }, run: [0.5, 1] },
        merge: { feeder: { from: 'simulation' }, packer: { size: 6 }, run: [0.5, 1], finalize: [{ name: 'check', fail: 'once' }] },
      },
    });
    sim.start();
    check(until(sim, () => sim.nodes.simulation.seedNext === 40), 'seed feeder yields its whole batch');
    check(until(sim, () => sim.counts(sim.nodes.merge).U >= 2 && sim.counts(sim.nodes.merge).U < 6 && sim.feederActive(sim.nodes.merge)), 'merge holds a group below size');
    const before = sim.nodes.merge.parcels.length;
    sim.flush('merge');
    sim.step(0.05);
    check(sim.nodes.merge.parcels.length > before, 'flush packed the remainder');
    check(drain(sim, () => sim.nodes.merge.status === 'FinalizingBlocked'), 'finalizing action failure blocks the member');
    check(sim.wg.status === 'Finalizing', 'workgraph stays Finalizing while a member is blocked');
    sim.forceAction();
    check(drain(sim, () => sim.wg.status === 'Completed'), 'completes after forcing');
  }

  // a stopped downstream member keeps the work in flight, so the Active hook does not keep raising the feeder
  {
    const sim = new Sim({
      name: 'inflight',
      workgraph: { target: { output: 'out', files: 1000 } },
      transformations: {
        simulation: { feeder: { seeds: 600, batch: 20, inflight: 10 }, packer: { size: 1 }, run: [0.5, 1] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1] },
      },
      outputs: { out: { from: 'reco' } },
    });
    sim.start();
    check(until(sim, () => sim.nodes.simulation.seedRequested >= 40), 'the Active hook raises while the chain flows');
    sim.toggleNode('reco');
    check(until(sim, () => !sim.liveInputs(sim.nodes.simulation) && !sim.liveParcels(sim.nodes.simulation)), 'the simulation finishes what it was asked for');
    const limit = sim.nodes.simulation.seedRequested;
    for (let i = 0; i < 400; i++) sim.step(0.05);
    check(sim.nodes.simulation.seedRequested === limit, `the Active hook stops raising while reco is stopped: ${limit} → ${sim.nodes.simulation.seedRequested}`);
    check(sim.counts(sim.nodes.reco).U + sim.nodes.reco.unfed.length > 0, 'the outputs wait in the stopped reco');
    sim.toggleNode('reco');
    check(until(sim, () => sim.nodes.simulation.seedRequested > limit), 'the Active hook raises again once reco drains the backlog');
  }

  // an operator stops and restarts a member from its status badge
  {
    const sim = new Sim({
      name: 'stop',
      transformations: {
        simulation: { feeder: { seeds: 60 }, packer: { size: 1 }, run: [0.5, 1] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1] },
      },
    });
    sim.start();
    check(until(sim, () => sim.nodes.reco.parcels.length >= 2), 'reco has started work');
    sim.toggleNode('reco');
    check(sim.nodes.reco.status === 'Paused', 'clicking Active pauses the member');
    const parcels = sim.nodes.reco.parcels.length;
    for (let i = 0; i < 200; i++) sim.step(0.05);
    check(sim.nodes.reco.parcels.length === parcels, 'no new parcels while Paused');
    check(!sim.liveParcels(sim.nodes.reco), 'parcels already in flight completed');
    check(sim.wg.status === 'Active', 'the workgraph waits for the stopped member');
    sim.toggleNode('reco');
    check(sim.nodes.reco.status === 'Active', 'clicking Paused resumes it');
    check(until(sim, () => sim.wg.status === 'Completed'), 'completes after resuming');
  }

  // Problematic inputs hold the drain (DX-ADR-005): the workgraph stays Active until the operator resets them to the pool or writes them off
  {
    const sim = new Sim({
      name: 'problematic',
      settings: { failScale: 3 },
      transformations: { simulation: { feeder: { seeds: 30 }, packer: { size: 1 }, run: [0.5, 1], fail: 0.4, retries: 0 } },
    });
    sim.start();
    check(until(sim, () => sim.waitsOnOperator(sim.nodes.simulation)), 'everything ends Problematic and waits for an operator');
    check(sim.counts(sim.nodes.simulation).Pb === 30, 'thirty Problematic inputs');
    for (let i = 0; i < 200; i++) sim.step(0.05);
    check(sim.wg.status === 'Active' && !sim.drained(sim.nodes.simulation), 'the quarantine keeps the member from draining and the workgraph Active');
    sim.writeOffProblematic('simulation');
    check(sim.counts(sim.nodes.simulation).NP === 30 && sim.counts(sim.nodes.simulation).Pb === 0, 'write-off moves them to NotProcessed');
    check(until(sim, () => sim.wg.status === 'Completed'), 'and the workgraph drains');
    const sim2 = new Sim({
      name: 'reset',
      transformations: { simulation: { feeder: { seeds: 30 }, packer: { size: 1 }, run: [0.5, 1], fail: 0.5, retries: 0 } },
    });
    sim2.start();
    check(until(sim2, () => sim2.waitsOnOperator(sim2.nodes.simulation)), 'the first pass ends with some Problematic inputs waiting');
    const pb = sim2.counts(sim2.nodes.simulation).Pb;
    check(pb > 0 && sim2.wg.status === 'Active', 'some inputs are Problematic and the workgraph is still Active');
    sim2.settings.failScale = 0;
    sim2.resetProblematic('simulation');
    check(sim2.counts(sim2.nodes.simulation).Pb === 0 && sim2.counts(sim2.nodes.simulation).U === pb, 'reset returns them to the pool');
    check(until(sim2, () => sim2.wg.status === 'Completed'), 'and with the cause fixed the workgraph completes');
    check(sim2.counts(sim2.nodes.simulation).P === 30, 'every input was processed in the end');
  }

  // a workgraph that has stopped for a person says so at the top: `feeders done, draining` is what a healthy drain says, so the
  // state line names the member holding it instead, and that member's strip glows (DX-ADR-005)
  {
    const { stateCard } = g.WorkgraphSim;

    // a workgraph that needs nobody holds nobody, from Active to Completed
    const fine = new Sim({
      name: 'held-none',
      workgraph: { approval: 'auto' },
      transformations: {
        simulation: { feeder: { seeds: 20 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0 },
        merge: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.3, 0.6], fail: 0 },
      },
    });
    fine.start();
    check(until(fine, () => fine.nodes.merge.parcels.length > 0), 'the plain workgraph is Active with work downstream');
    check(fine.drain() && stateCard(fine).text === 'draining' && !fine.heldMembers().length, 'a drained workgraph says draining, with no member held: ' + stateCard(fine).text);
    /* one check over the whole run rather than one per step, so the suite's total does not follow the data */
    const holds = [];
    let steps = 0;
    while (fine.wg.status !== 'Completed' && steps++ < HANG) {
      fine.step(0.05);
      for (const n of fine.heldMembers()) holds.push(`${n.id} at ${fine.wg.status}`);
    }
    check(fine.wg.status === 'Completed' && !holds.length, 'and nothing is held at any point in a run nobody has to touch: ' + holds.slice(0, 3).join(', '));

    // a quarantine nobody has decided, which is all that holds the member (DX-ADR-005)
    const sim = new Sim({
      name: 'held-quarantine',
      settings: { failScale: 3 },
      workgraph: { approval: 'auto' },
      transformations: { spruce: { label: 'sprucing', feeder: { seeds: 24 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0.4, retries: 0 } },
    });
    sim.start();
    check(until(sim, () => sim.waitsOnOperator(sim.nodes.spruce)), 'the sprucing settles with its quarantine and nothing else live');
    const pb = sim.counts(sim.nodes.spruce).Pb;
    const held = sim.heldMembers();
    check(sim.wg.status === 'Active' && pb > 0 && held.length === 1 && held[0].id === 'spruce', `the quarantined member is what holds the Active workgraph: ${held.map((n) => n.id).join(', ') || 'nothing'}`);
    const line = stateCard(sim);
    check(line.text === `held by sprucing · ${pb} quarantined` && line.cls === 'pending', 'the line names it and what holds it, as a wait: ' + line.text);
    check(!/draining|requesting/.test(line.text), 'and says nothing about a workgraph that is finishing: ' + line.text);
    check(line.tip.startsWith(`sprucing has ${pb} quarantined input`) && line.tip.endsWith('the drain until an operator resets or writes them off (DX-ADR-005)'), 'the tooltip says what holds the drain and what moves it: ' + line.tip);
    check(!/click|press|button|toolbar|dialog|below|above|beside/.test(line.tip), 'and carries no direction that would need editing when something moves');
    /* the strip's glow is a class the widget toggles on a node the layout has placed, which these checks have no document for:
       the renderer's own source is the surface, and what it says is that a held member reuses the wg-glow a quarantine and a
       failed action already carry, off one set per frame rather than one per card */
    const src = readText('docs/assets/js/workgraph-sim.js');
    check(/const held = new Set\(sim\.heldMembers\(\)/.test(src) && /function poolWants\([^)]*held\)[\s\S]{0,900}?held\.has\(node\.id\)/.test(src), "a held member's pool wants a person off one set per frame, not one call per card");
    check(/classList\.toggle\('wg-glow', !!wants\)/.test(src), 'and the pulse is that one condition rather than a second copy of it');
    /* The pulse says which of its three reasons it is, and the tab that answers it glows. The enumeration is the renderer's
       and the three conditions are the engine's, so a fourth reason added to the pulse and not to the sentence would leave
       a member pulsing with nothing to say — the check drives the pair rather than reading either side. */
    {
      const W = g.WorkgraphSim;
      const c = sim.counts(sim.nodes.spruce);
      const held = new Set(sim.heldMembers().map((n) => n.id));
      for (const n of Object.values(sim.nodes)) {
        const cn = sim.counts(n);
        const wants = W.poolWants(sim, n, cn, held);
        const glows = cn.Pb > 0 || !!n.run.blocked || held.has(n.id);
        check(!!wants === glows, `${n.id}: the pulse and the sentence are one condition`);
        if (wants) check(wants.why.length > 10 && !/^\s*$/.test(wants.why), `${n.id}: and the sentence says which of the three it is: ${wants.why}`);
      }
      const wants = W.poolWants(sim, sim.nodes.spruce, c, held);
      check(wants && /Problematic/.test(wants.why) && wants.tab === 'inputs', `a quarantine sends the reader to the inputs machine: ${wants && wants.why}`);
      /* a member paused with work left is the hold the transformation machine answers, and the one place the chain runs
         all the way through: the pool, then that tab, then resume itself */
      sim.toggleNode('spruce');
      const paused = sim.nodes.spruce;
      if (paused.status === 'Paused') {
        const held2 = new Set(sim.heldMembers().map((n) => n.id));
        const w2 = W.poolWants(sim, paused, sim.counts(paused), held2);
        check(w2 && /paused/.test(w2.why) && w2.tab === 'transformation', `a pause sends the reader to the transformation machine: ${w2 && w2.why}`);
        const d = W.detailsHtml(sim, paused, { machine: 'status' });
        check(/data-tab="transformation"[^>]*title="[^"]*paused/.test(d) || /class="wgsim-mtab wgsim-glow"[^>]*data-tab="transformation"/.test(d), 'the transformation tab glows and says what waits');
        check(W.nodeMachineHtml(sim, paused).includes('wg-m-wants'), 'and resume pulses on the machine it sends them to');
        /* the third reason is deliberately left without a tab: its answer docks itself over the picture */
        check(!/data-tab="parcels"[^>]*wgsim-glow/.test(d), 'and no tab glows for an action, whose answer is the actions panel');
        sim.toggleNode('spruce');
      }
      /* a member the workgraph does not wait on has a resume with no pulse on it: the mark is the hold, not the state */
      const other = Object.values(sim.nodes).find((n) => n.status === 'Paused' && !sim.heldMembers().some((h) => h.id === n.id));
      if (other) check(!W.nodeMachineHtml(sim, other).includes('wg-m-wants'), 'a paused member nothing waits on does not pulse');
    }
    sim.writeOffProblematic('spruce');
    check(!sim.heldMembers().length && stateCard(sim).text.startsWith('feeders done, draining'), 'written off, nobody holds it and the line drains again: ' + stateCard(sim).text);
    check(until(sim, () => sim.wg.status === 'Completed'), 'and the workgraph completes');

    // a pause with work left, which no count of Problematic inputs would show
    const hand = new Sim({
      name: 'held-paused',
      workgraph: { approval: 'auto' },
      transformations: { removal: { label: 'buffer removal', feeder: { seeds: 30 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0 } },
    });
    hand.start();
    check(until(hand, () => hand.counts(hand.nodes.removal).A > 0), 'the member is working through its pool');
    hand.toggleNode('removal');
    check(hand.wg.status === 'Active' && hand.nodes.removal.status === 'Paused' && hand.counts(hand.nodes.removal).Pb === 0 && hand.heldMembers().map((n) => n.id).join(',') === 'removal', 'a pause with work left holds the workgraph with nothing quarantined at all');
    const paused = stateCard(hand);
    check(paused.text === 'held by buffer removal · paused' && paused.tip === 'buffer removal is paused with work left, which holds the workgraph until an operator resumes it (DX-ADR-005)', 'the line names the member and the pause: ' + paused.text);
    hand.toggleNode('removal');
    check(!hand.heldMembers().length && !stateCard(hand).text.startsWith('held by'), 'resumed, the hold goes from the line: ' + stateCard(hand).text);
    check(until(hand, () => hand.wg.status === 'Completed'), 'and the workgraph completes');

    // the member's own Active hook pauses it, and the top of the widget reads the same way
    const hook = new Sim({
      name: 'held-hook',
      workgraph: { approval: 'auto' },
      transformations: { simulation: { feeder: { seeds: 30 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0.9, retries: 0, pauseAbove: 0.5 } },
    });
    hook.start();
    check(until(hook, () => hook.nodes.simulation.status === 'Paused', 8000), "the member's own Active hook pauses it once too much has failed");
    check(hook.wg.status === 'Active' && stateCard(hook).text === 'held by simulation · paused', "and the hook's pause is a hold like any other: " + stateCard(hook).text);
  }

  // cancel runs the cleaning lists and removes the outputs
  {
    const sim = new Sim({
      name: 'cancel',
      transformations: {
        simulation: { feeder: { seeds: 60 }, packer: { size: 1 }, run: [0.5, 1] },
        merge: { feeder: { from: 'simulation' }, packer: { size: 3 }, run: [0.5, 1] },
      },
      outputs: { out: { from: 'merge' } },
    });
    sim.start();
    check(until(sim, () => sim.outputs.out.files.length >= 2), 'some output produced');
    sim.cancel();
    check(sim.wg.status === 'Cancelling', 'cancel moves to Cancelling');
    check(until(sim, () => sim.wg.status === 'Cleaned'), 'reaches Cleaned');
    check(sim.outputs.out.files.length === 0, 'outputs removed');
    check(Object.values(sim.nodes).every((n) => n.status === 'Cleaned' && n.inputs.size === 0), 'members cleaned');
    check(sim.nodes.simulation.lists.clean.every((a) => a.result === 'Done'), 'cleaning actions recorded Done');
  }

  // the simple view shows a list only while a state runs it: the approving list above the first row, a card's own list in a wider card
  {
    const { layout, stateCard, workgraphHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'state-card',
      workgraph: { scouting: { count: 4 } },
      transformations: {
        simulation: { feeder: { seeds: 20 }, packer: { size: 1 }, run: [0.5, 1], finalize: ['merge histograms'] },
        merge: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1] },
      },
    });
    check(sim.wg.status === 'New' && stateCard(sim).text === '', 'in New the line has nothing to say');
    sim.start();
    const L = await layout(sim);
    const sizes = (l) => Object.values(l.items).filter((it) => it.kind === 'node').map((it) => `${it.w}x${it.h}`);
    check(sizes(L).every((w) => w === sizes(L)[0]), 'every card has one size');
    check(sim.wg.status === 'Scouting' && workgraphHtml(sim).includes('none applicable while Scouting') && workgraphHtml(sim).includes('ScoutingToApproving'), 'while scouting the line waits on the sample and the dialog shows the hook with no list applicable');
    check(until(sim, () => sim.wg.status === 'Approving'), 'reaches Approving');
    const L2 = await layout(sim);
    check(L2.width === L.width && L2.height === L.height && sizes(L2).every((w) => w === sizes(L)[0]), 'approving changes nothing in the picture: the state line lives outside it');
    check(until(sim, () => sim.blocked && sim.blocked.manual), 'reaches the manual sign-off');
    const card = stateCard(sim);
    check((sim.wg.status === 'Approving' || sim.wg.status === 'ApprovingBlocked') && card.text === '', 'while the approving list runs the line says nothing: the actions dialog carries it');
    const lists = g.WorkgraphSim.listsHtml(sim, sim.runningLists());
    check(lists.includes('manual approval waits for a sign-off') && lists.includes('data-act="force"') && lists.includes('data-act="rerun"') && lists.includes('data-act="extend-scout"'), 'the actions panel names the blocked sign-off as a wait with force passed and reset, and offers to scout further');
    /* a sign-off nobody has given is the Blocked state as a wait: amber everywhere the state shows, never red */
    check(g.WorkgraphSim.blockedKind(sim, null) === 'wait' && card.cls === 'pending' && g.WorkgraphSim.statesHtml(sim).includes('wgsim-pill on waiting') && !g.WorkgraphSim.statesHtml(sim).includes(' blocked'), 'blocked on a sign-off is a wait: the line and the strip show it amber');
    check(g.WorkgraphSim.wgMachineHtml(sim).includes('wg-m-on wg-m-waiting') && g.WorkgraphSim.wgMachineHtml(sim).includes('ApprovingBlocked: now, waiting for a sign-off') && lists.includes('not signed off') && lists.includes('data-blocked="wait"'), 'so do the machine and the panel');
    const dialog = workgraphHtml(sim);
    check((dialog.match(/wgsim-state-passed/g) || []).length >= 2 && dialog.includes('data-act="force"') && dialog.includes('approving · 2/3'), 'the dialog opens the approving tab with its progress, the passed checks and the blocked one with its buttons');
    sim.forceAction();
    check(sim.wg.status === 'Active' && stateCard(sim).text.startsWith('requesting'), 'once approved the line moves to Active and says the workgraph is requesting');
    check(drain(sim, () => sim.wg.status === 'Completed'), 'completes');
  }
  // the card's panels and footer: the actions panel carries the blocked action's buttons, the configuration panel the Problematic controls
  {
    const { eligibleAction, inputsHtml, feederHtml, hooksHtml, actionsHtml, parcelsHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'panels',
      workgraph: { approval: 'auto' },
      transformations: {
        simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.5, 1], finalize: [{ name: 'check', fail: 'once' }, 'merge histograms', 'compare means'] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1], retries: 0, fail: 1 },
      },
    });
    sim.start();
    const node = sim.nodes.simulation;
    check(eligibleAction(node.lists, node.status) === null, 'an Active card shows its input bar, no action being eligible');
    check(actionsHtml(sim, node).includes('none applicable while Active') && !actionsHtml(sim, node).includes('wgsim-atab on'), 'while Active no tab is selected and the body says so');
    check(until(sim, () => sim.counts(node).U + sim.counts(node).A > 0), 'the feeder yields');
    check(!inputsHtml(sim, node).includes('>Processed<'), 'the inputs section leaves zero states out');
    /* the drain guard is the feeder's first row, beside the feeder's own state: a member is drained when the feeder is
       off and nothing is non-terminal, and the two halves of that belong in one place */
    check(feederHtml(sim, node).includes('keeping this open') && feederHtml(sim, node).indexOf('keeping this open') < feederHtml(sim, node).indexOf('kind'), 'the feeder block opens with what keeps the member open');
    check(!inputsHtml(sim, node).includes('keeping this open'), 'and the inputs section no longer carries it');
    check(until(sim, () => sim.counts(sim.nodes.reco).Pb > 0), 'reco quarantines its failed inputs');
    check(until(sim, () => sim.waitsOnOperator(sim.nodes.reco)), 'and once nothing else is left the quarantine alone holds it');
    check(sim.wg.status === 'Active' && sim.nodes.simulation.status === 'Active', 'so the workgraph cannot finalise: Problematic is not terminal');
    check(sim.nodes.reco.decisions.quarantined === sim.counts(sim.nodes.reco).Pb && hooksHtml(sim, sim.nodes.reco).includes(`${sim.nodes.reco.decisions.quarantined}</span> → Problematic`) && !hooksHtml(sim, node).includes('0 →') && hooksHtml(sim, node).includes('s ago · no change'), 'the hooks section counts what HandleFailedInput decided');
    const reco = sim.nodes.reco;
    check(reco.transitions['Failed>Problematic'] === reco.decisions.quarantined && reco.transitions['Assigned>Failed'] >= reco.transitions['Failed>Problematic'], 'the transition counters key on the pair: Assigned to Failed, then Failed to Problematic');
    check(sim.verifyTransitions(reco).length === 0 && sim.verifyTransitions(node).length === 0, 'inbound minus outbound equals the occupancy of every state');
    const { machineHtml } = g.WorkgraphSim;
    const svg = machineHtml(sim, reco);
    check((svg.split('</svg>')[0].match(/wg-m-op/g) || []).length === 3 && svg.includes('data-act="reset-problematic"') && svg.includes('data-act="writeoff-problematic"'), 'the machine draws the three operator edges dashed, with buttons where the source holds inputs');
    check(!svg.includes('data-act="writeoff-unassigned"') || sim.counts(reco).U > 0, 'no button on an operator edge whose source is empty');
    /* The flush is the fourth button and not a fourth operator edge: the packer takes that edge, and an operator asks it
       to run now. Offered only where a sweep would not do the same thing a moment later, and counting what the packer is
       holding short of a group rather than everything the pool holds — the two are different numbers. */
    {
      const flush = { name: 'flush', transformations: { a: { feeder: { seeds: 40, batch: 6 }, packer: { size: 5 }, run: [3, 4] } }, outputs: { o: { from: 'a' } } };
      const fs = started(flush);
      let html = '';
      check(until(fs, () => (html = machineHtml(fs, fs.nodes.a)).includes('data-act="ask-flush"')), 'the packer holding a short group offers a flush on its own edge');
      check((html.split('</svg>')[0].match(/wg-m-op/g) || []).length === 3, 'and the edge keeps the packer\'s stroke: still three dashed operator edges');
      const held = Number(/flush · (\d+)/.exec(html)[1]);
      const pool = fs.counts(fs.nodes.a).U;
      check(held > 0 && held <= pool, `the button counts what only a flush would move, not the pool: ${held} of ${pool} Unassigned`);
      check(held % 5 !== 0 || held < 5, `and never a full group, which the packer takes itself: ${held} against a group of 5`);
    }
    check(!inputsHtml(sim, reco).includes('data-act=') && feederHtml(sim, reco).includes(`${sim.counts(reco).Pb} Problematic wait for an operator`) && !inputsHtml(sim, reco).includes('wgsim-terminal'), 'the inputs table is no link, and the guard counts the quarantine as what keeps the member open');
    const details = g.WorkgraphSim.detailsHtml(sim, reco, { machine: 'inputs' });
    check(details.includes('class="wgsim-mtab on wgsim-glow"') && details.includes('data-tab="inputs"') && details.includes('data-act="reset-problematic"'), 'the inputs tab glows while the quarantine waits, and shows the input machine with its buttons');
    /* The step on the transport: one press stops the model and takes it on to the next thing that happens. Two things a
       diff would not show, and the press is a function of its own so that both can be driven without a DOM. */
    {
      const W = g.WorkgraphSim;
      const spec = { name: 'step', transformations: { a: { feeder: { seeds: 40 }, packer: { size: 2 }, run: [1, 2], fail: 0.3, retries: 1 }, b: { feeder: { from: 'a' }, packer: { size: 2 }, run: [1, 2] } }, outputs: { o: { from: 'b' } } };
      /* everything a press could move, so that "the same run" is the whole model and not the one number a check happened to read */
      const shot = (s2) => JSON.stringify([s2.t.toFixed(4), s2.wg.status, s2.logSeq, Object.values(s2.nodes).map((n) => [n.id, n.status, s2.counts(n), s2.parcelCounts(n)])]);
      const stepped = started(spec);
      const played = started(spec);
      check(shot(stepped) === shot(played), 'two runs of one seed start alike, which is what makes the rest of this a comparison');
      let slices = 0;
      let silent = 0;
      let capped = 0;
      for (let i = 0; i < 60; i++) {
        const mark = W.stepMark(stepped);
        const took = W.stepModel(stepped, false);
        slices += Math.round(took / W.STEP.slice);
        /* a press the reader cannot tell happened: nothing moved and the cap was not spent either */
        if (W.stepMark(stepped) === mark) { capped += 1; if (Math.abs(took - W.STEP.cap) > W.STEP.slice) silent += 1; }
      }
      check(!silent, `every press either lands on something or spends its cap: ${silent} of 60 did neither`);
      /* and it is the landing that ends a press, not the cap. On the log alone two presses in three ran out the cap
         instead, which is the fixed slice the cap was meant to be a backstop for. */
      check(slices * W.STEP.slice < 60 * W.STEP.cap / 4, `sixty presses land on things rather than running out: ${(slices * W.STEP.slice).toFixed(1)}s of a possible ${60 * W.STEP.cap}s, ${capped} capped`);
      /* the point of keeping the slices the frame loop's: play the same model time and land in the same place */
      for (let i = 0; i < slices; i++) played.step(W.STEP.slice);
      check(shot(stepped) === shot(played), 'a stepped run and a played run of one seed are the same run');
      /* the number the press watches and the counters it reports from are two readings of one set. A counter one of them
         sees and the other does not leaves a press that stops on something it cannot name, or names something it did not
         stop for, and neither file shows which of the two is short. */
      {
        const w2 = started(spec);
        let agreed = 0;
        for (let i = 0; i < 40; i++) {
          const mark = W.stepMark(w2);
          const snap = W.stepSnap(w2);
          const seq = w2.logSeq;
          W.stepModel(w2, false);
          const after = W.stepSnap(w2);
          let moved = w2.logSeq - seq;
          for (const k in after) moved += after[k] - (snap[k] || 0);
          if (W.stepMark(w2) - mark === moved) agreed += 1;
        }
        check(agreed === 40, `the mark the press watches counts what the snapshot it reports from holds: ${agreed} of 40 agreed`);
      }
      /* and the line names every edge the press took, so that a press cannot move something the foot is silent about */
      {
        const w3 = started(spec);
        let named = 0;
        let capped = 0;
        for (let i = 0; i < 40; i++) {
          const snap = W.stepSnap(w3);
          const took = W.stepModel(w3, false);
          const after = W.stepSnap(w3);
          const said = W.stepSaid(w3, snap, after, took);
          const rows = Object.keys(after).filter((k) => after[k] > (snap[k] || 0));
          if (!rows.length) { capped += 1; if (said.text === 'nothing moved') named += 1; continue; }
          const shown = Math.min(rows.length, W.SAID.rows);
          const rest = rows.length - shown;
          /* every row it shows, and a count of the ones it does not */
          const counted = (said.text.match(/\d+ (?:input|parcel)s?/g) || []).length;
          if (counted === shown && (rest === 0) === !/ \+\d+$/.test(said.text)) named += 1;
        }
        check(named === 40, `the foot names what the press moved, or counts what it leaves out: ${named} of 40 (${capped} moved nothing)`);
      }
      /* the log is the model's record and the foot is the widget's: a press writes nothing into it, which is what keeps
         two readers of one seed holding the same log of it */
      {
        const w4 = started(spec);
        const seq = w4.logSeq;
        const lines = w4.log.length;
        for (let i = 0; i < 20; i++) W.stepModel(w4, false);
        const played = started(spec);
        let rem = w4.t;
        while (rem > 1e-9) { played.step(W.STEP.slice); rem -= W.STEP.slice; }
        check(w4.log.length - lines === played.log.length - lines && w4.logSeq !== seq + 20, 'pressing step writes nothing of its own into the log');
      }
      /* the model holds still behind a dialog, and the press is one of the things that would move it */
      const frozen = shot(stepped);
      let moved = 0;
      for (let i = 0; i < 5; i++) if (W.stepModel(stepped, true) !== 0) moved += 1;
      check(!moved && shot(stepped) === frozen, 'a press while a dialog is open takes no time and moves nothing');
      /* and a run that is over has nothing left to step: the press stops where the frame loop stops */
      const done = started(spec);
      complete(done);
      for (const st of W.STEP.over) {
        done.wg.status = st;
        check(W.stepModel(done, false) === 0, `a press does nothing once the run is ${st}`);
      }
    }

    /* A machine's box lists what is in it. The dialog said how many inputs and parcels were in each state and never which,
       and the box is the selector because it already holds the vocabulary and the count, so the states, the counts and the
       choice are one thing. The rows are for seeing: the machine acts over a whole state and the lineage dialog on one
       input, and the same verbs on a row would be a third scale between the two. */
    {
      const W = g.WorkgraphSim;
      const c = sim.counts(reco);
      const open = c.U + c.A + c.F + c.Pb;
      const m = W.machineHtml(sim, reco);
      const holds = W.INPUT_ROWS.filter((r) => c[r.key]).map((r) => r.name);
      const empty = W.INPUT_ROWS.filter((r) => !c[r.key]).map((r) => r.name);
      check(holds.length && holds.every((n) => m.includes(`data-state="${n}"`)), `every box with something in it opens it: ${holds}`);
      check(empty.every((n) => !m.includes(`data-state="${n}"`)), `and a box drawn zero opens nothing, having nothing to list: ${empty}`);
      /* the state each chip says it is in, read off its title, which is where everything but the shape and the tag went
         when the strip was packed */
      const titles = (html) => [...html.matchAll(/<li class="wgsim-mchip[^"]*"[^>]*title="([^"]*)"/g)].map((x) => x[1]);
      const states = (html) => titles(html).map((t) => (/ · ([A-Za-z]+):/.exec(t) || [])[1]).filter(Boolean);
      const chips = (html) => (html.match(/<li class="wgsim-mchip/g) || []).length;
      const all = W.INPUT_ROWS.reduce((n, r) => n + c[r.key], 0);
      const cap = W.CHIPS.lines * W.CHIPS.perLine;
      const list = W.machineListHtml(sim, reco, 'inputs');
      /* the strip opens on everything, which is what packing the items pays for: there is no set to name and none for the
         reader to work out the shape of before they can trust what is missing */
      check(list.includes('>all<') && list.includes(`>${all} input`), `a tab opens on every input it has, and says so: ${all}`);
      check(states(list).length && new Set(states(list)).size >= 1, `and the chips say which state each is in: ${[...new Set(states(list))]}`);
      /* the ordering is what the filtering used to do: what needs a reader is in the first line and the count swallows the tail */
      check(c.Pb ? states(list)[0] === 'Problematic' : true, 'the quarantine leads, which is what a reader following the tab\'s glow came for');
      check(c.F ? states(list).indexOf('Failed') <= c.Pb : true, 'and the failures follow it, before anything that has settled');
      check(chips(list) === Math.min(all, cap) && (all > cap) === list.includes('data-act="machine-list-more"'), `three lines of chips, the count at the end taking one of the places: ${chips(list)} of ${all}`);
      check(chips(W.machineListHtml(sim, reco, 'inputs', { more: true })) === all, 'which opens every one of them');
      check(list.includes('data-act="lineage"') && !/data-act="(input-decide|reset-problematic|writeoff-\w+)"/.test(list), 'a chip opens its file\'s lineage and carries no operator\'s button');
      /* the state's own colour, out of the map the counts table reads, so a strip holding several states reads at a glance */
      check(W.INPUT_ROWS.filter((r) => c[r.key]).every((r) => !c[r.key] || list.includes(`wgsim-mchip wg-n-${r.cls}`) || !states(list).includes(r.name)), 'every chip carries its state\'s tone');
      const pick = W.machineListHtml(sim, reco, 'inputs', { state: 'Problematic' });
      check(c.Pb && states(pick).length === Math.min(c.Pb, cap) && states(pick).every((st) => st === 'Problematic'), `a box chosen shows what is in that box alone: ${c.Pb} Problematic`);
      check(pick.includes('>Problematic<') && pick.includes(`>${c.Pb} input`), 'and the heading states the choice rather than justifying it');
      check(W.machineHtml(sim, reco, 'Problematic').includes(' wg-m-picked'), 'and the box it was chosen on says it is the one');
      /* the two machines are different vocabularies with states in common, so a choice made on one is no choice on the other */
      check(W.machineListHtml(sim, reco, 'parcels', { state: 'Problematic' }).includes('>all<'), 'a state the machine does not draw is no choice at all');
      const pl = W.machineListHtml(sim, reco, 'parcels');
      check(pl.includes('wgsim-mlist-swatch') && pl.includes('wg-slot-'), 'a parcel\'s chip leads with the swatch its state\'s box wears, as the parcels table does');
      const pc = sim.parcelCounts(reco);
      const parcels = W.PARCEL_ROWS.reduce((n, r) => n + pc[r.name], 0);
      check(pl.includes(`>${parcels} parcel`), `and the parcels tab opens on every parcel it has made: ${parcels}`);
      check(!/data-act="(input-decide|force|rerun|lineage)"/.test(pl), 'a parcel\'s chip carries no operator\'s button, and no click: a parcel is a surface nowhere');
    }
    /* The panel keeps one rule for its sections, and the only way `section + section` reaches the left column is for the
       section that counts what a machine holds to be the control itself rather than sit inside one. */
    {
      const status = g.WorkgraphSim.detailsHtml(sim, reco, { machine: 'status' });
      check((status.match(/<section class="wgsim-jump"/g) || []).length === 2, 'both counted sections are the way to their machine');
      check(!status.includes('<div class="wgsim-jump"'), 'and are the control themselves, so the sections of both columns are adjacent siblings');
      check(status.includes('data-region="inputs"') && status.includes('data-region="parcels"'), 'and each keeps the region help mode aims at');
    }
    sim.writeOffUnassigned('reco');
    sim.writeOffProblematic('reco');
    check(sim.counts(reco).Pb === 0 && sim.counts(reco).U === 0, 'the operator writes the quarantine off');
    check(until(sim, () => sim.nodes.simulation.status === 'FinalizingBlocked'), 'the finalizing check blocks');
    check(eligibleAction(node.lists, node.status).text === 'check failed', 'the strip names the failed action: ' + eligibleAction(node.lists, node.status).text);
    check(actionsHtml(sim, node).includes('data-act="force"') && actionsHtml(sim, node).includes('data-act="rerun"') && actionsHtml(sim, node).includes('wgsim-atab on relevant') && actionsHtml(sim, node).includes('finalizing · 0/3'), 'the finalizing tab is selected with its progress, and the failed row carries force passed and reset');
    check(!actionsHtml(sim, node).includes('clean intermediates') && actionsHtml(sim, node, 'archive').includes('clean intermediates') && !actionsHtml(sim, node, 'archive').includes('wgsim-atab-status'), 'another tab shows its names alone, without a standing');
    sim.forceAction();
    check(until(sim, () => eligibleAction(node.lists, node.status) && eligibleAction(node.lists, node.status).text.startsWith('merge histograms · 2/3')), 'the strip then follows the running action with its place in the list');
    check(until(sim, () => sim.nodes.simulation.status === 'Completed'), 'simulation completes');
    check(eligibleAction(node.lists, node.status).text === '3 finalizing actions passed', 'and sums the list up once it has run: ' + eligibleAction(node.lists, node.status).text);
    const pc = sim.parcelCounts(sim.nodes.reco);
    check(pc.Failed > 0 && parcelsHtml(pc).includes('>Failed<'), 'the parcels panel counts the failures: ' + pc.Failed);
  }
  // births count as inbound edges, so a model with splits and retries still reconciles
  {
    const sim = new Sim({
      name: 'reconcile',
      workgraph: { approval: 'auto' },
      transformations: { reco: { feeder: { seeds: 12 }, packer: { size: 2 }, run: [0.3, 0.6], fail: 0.4, partial: 0.4, retries: 1 } },
    });
    sim.start();
    complete(sim);
    const t = sim.nodes.reco.transitions;
    check(t['born>Unassigned'] > 12 && t['born>Processed'] > 0 && t['Assigned>Split'] > 0, 'split children are born Unassigned and Processed: ' + JSON.stringify(t));
    check(sim.verifyTransitions(sim.nodes.reco).length === 0, 'and the counters still reconcile with the occupancy');
    sim.reset();
    check(Object.keys(sim.nodes.reco.transitions).length === 0, 'reset zeroes the counters');
  }

  // with every sweep manual the workgraph's hooks row waits for the reader, and offers a run once the sample has drained
  {
    const { railStates, stateCard } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'manual-hook',
      workgraph: { scouting: { count: 4 }, approval: 'auto' },
      settings: { feederPeriod: 0, packerPeriod: 0, hookPeriod: 0 },
      transformations: { simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0 } },
    });
    sim.start();
    check(sim.modeSummary() === 'mixed' && sim.modeOf('workgraph', 'hooks') === 'manual' && sim.modeOf('workgraph', 'actions') === 'auto', 'a period of 0 starts that row by hand everywhere: ' + sim.modeSummary());
    check(railStates(sim, 'workgraph').hooks.state === 'manual', "the workgraph's hooks row is by hand with nothing to do: " + JSON.stringify(railStates(sim, 'workgraph')));
    check(railStates(sim, 'simulation').feeder.state === 'run', 'the feeder row has work: ' + JSON.stringify(railStates(sim, 'simulation')));
    sim.runRow('simulation', 'feeder');
    check(railStates(sim, 'simulation').feeder.state === 'manual' && railStates(sim, 'simulation').packer.state === 'run', 'fed, the feeder rests and the packer has work');
    sim.runRow('simulation', 'packer');
    check(until(sim, () => sim.scoutDone()), 'the sample drains');
    check(sim.wg.status === 'Scouting', 'nothing accepts it on its own');
    check(railStates(sim, 'workgraph').hooks.state === 'run', 'the hooks row offers a run once the sample has drained');
    check(sim.runRow('workgraph', 'hooks') && sim.wg.status !== 'Scouting', 'the hook run by hand accepts it');
  }

  // an edge's backlog counts what the producer made and the consumer's feeder has not swept, on the edge or in the queue
  {
    const { edgeBacklog } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'edge-backlog',
      workgraph: { approval: 'auto' },
      settings: { feederPeriod: 0 },
      transformations: {
        simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.5, 1] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.5, 1] },
      },
    });
    sim.start();
    sim.runFeeder('simulation');
    check(until(sim, () => sim.nodes.reco.unfed.length + sim.tokens.filter((tk) => tk.to === 'reco').length === 6), 'every output is on the edge or in the queue');
    check(edgeBacklog(sim, sim.nodes.reco, 'simulation') === 6, 'the edge backlog counts them all: ' + edgeBacklog(sim, sim.nodes.reco, 'simulation'));
    sim.runFeeder('reco');
    check(edgeBacklog(sim, sim.nodes.reco, 'simulation') === sim.tokens.filter((tk) => tk.to === 'reco').length, 'a feeder sweep takes the queue and leaves what is still travelling');
  }
  // the rail's rows: the mode word in auto and in manual, the run button only while by hand with work; a blocked action is not sweep work
  {
    const { railStates, fitTitle } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'head',
      workgraph: { approval: 'auto' },
      transformations: { simulation: { feeder: { seeds: 4 }, packer: { size: 1 }, run: [0.5, 1], finalize: [{ name: 'check', fail: 'once' }, 'merge histograms'] } },
    });
    const rows = () => railStates(sim, 'simulation');
    check(Object.values(rows()).every((r) => r.state === 'auto'), 'a New transformation reads auto on every row');
    sim.setAllModes('manual');
    check(Object.values(rows()).every((r) => r.state === 'manual'), 'by hand, a New transformation has nothing to run, so no row shows a button');
    sim.start();
    check(rows().feeder.state === 'run' && rows().packer.state === 'manual', 'started, the feeder has work and the packer none');
    sim.setMode('simulation', 'feeder', 'auto');
    check(rows().feeder.state === 'auto', 'an automatic row reads auto whatever its work');
    sim.setMode('simulation', 'feeder', 'manual');
    sim.runRow('simulation', 'feeder');
    check(rows().feeder.state === 'manual' && rows().feeder.tip.includes('nothing to feed') && rows().packer.state === 'run', 'fed to the limit, the feeder rests and the packer has something to pack');
    sim.setAllModes('auto');
    sim.setMode('simulation', 'actions', 'manual');
    check(until(sim, () => sim.nodes.simulation.status === 'Finalizing' && !!sim.nextAction('simulation')), 'reaches the finalizing list');
    check(rows().actions.state === 'run' && rows().actions.tip.includes('check is next'), 'the actions row offers the first action: ' + rows().actions.tip);
    check(sim.runRow('simulation', 'actions') && sim.nodes.simulation.status === 'FinalizingBlocked', 'run, the check fails and blocks');
    check(rows().actions.state === 'manual' && rows().actions.tip.includes('force it or reset it'), 'a blocked action is not sweep work, so the row shows no button: ' + rows().actions.tip);
    sim.rerunAction('simulation');
    check(rows().actions.state === 'run', 'reset, it can be run again');
    check(sim.runRow('simulation', 'actions') && sim.nextAction('simulation').action.name === 'merge histograms', 'the row runs it and the next one heads the queue');
    check(sim.runRow('simulation', 'actions') && sim.nodes.simulation.status === 'Finalized' && rows().actions.state === 'manual', 'the row runs the last and the list is done');
    check(fitTitle('MCSimulation', 96).text === 'MCSimulation' && fitTitle('MCReconstructionOfTheWholeRun', 96).text.endsWith('…'), 'a long title is cut with an ellipsis');
    /* and it measures with the advance it is given rather than the card heading's: the lineage label is a smaller face, and
       assuming the heading's would cut a name several characters before it needed to */
    check(fitTitle('MCSimulationRemoval', 96, 4.8).text === 'MCSimulationRemoval' && fitTitle('MCSimulationRemoval', 96).cut, 'and by the advance the caller passes, the heading\'s only when none is');
  }
  // the two backward edges of DX-ADR-005 an operator can take, and a cancel from New
  {
    const { stateCard, statesHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'backward',
      workgraph: { scouting: { count: 6 }, approval: 'auto' },
      transformations: { simulation: { feeder: { seeds: 40, batch: 10 }, packer: { size: 1 }, run: [0.3, 0.6] } },
    });
    sim.start();
    check(until(sim, () => sim.wg.status === 'Approving'), 'reaches Approving');
    check(g.WorkgraphSim.wgEdges(sim).length === 0, 'approving by itself, the dialog offers no edge');
    sim.wg.run.action && sim.block(sim.wg.run.action, 'for the check');
    check(sim.wg.status === 'ApprovingBlocked' && g.WorkgraphSim.wgEdges(sim).some((e) => e.act === 'extend-scout') && g.WorkgraphSim.listsHtml(sim, sim.runningLists()).includes('data-act="extend-scout"'), 'blocked, the actions dialog offers to scout further');
    const sample = sim.scoutSample();
    const fed = sim.nodes.simulation.seedNext;
    sim.extendScout();
    check(sim.wg.status === 'Scouting' && sim.wg.transitions['ApprovingBlocked>Scouting'] === 1, 'the operator sends it back to Scouting');
    check(g.WorkgraphSim.wgMachineHtml(sim).includes('Approving → Scouting: the operator scouts further · 1 so far'), "and the workgraph's machine counts it on the Approving edge, blocked being its counterpart");
    check(sim.scoutSample() > sample && sim.spec.workgraph.scouting.stages.length === 1, `the sample grew without touching the spec: ${sample} → ${sim.scoutSample()}`);
    check(sim.nodes.simulation.seedNext === fed && sim.totals.done > 0, 'and the scout keeps everything it had already produced');
    check(until(sim, () => sim.wg.status === 'Active'), 'a second approval carries on to Active');
    check(sim.nodes.simulation.seedNext > fed, 'the extended scout fed further');
    check(drain(sim, () => sim.wg.status === 'Completed'), 'and the workgraph completes');

    const back = new Sim({
      name: 'reopen',
      workgraph: { approval: 'auto' },
      transformations: { simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], finalize: [{ name: 'check', fail: true }] } },
    });
    back.start();
    check(until(back, () => back.nodes.simulation.status === 'FinalizingBlocked'), 'the finalizing check blocks');
    check(g.WorkgraphSim.wgEdges(back).some((e) => e.act === 'resume-active') && g.WorkgraphSim.listsHtml(back, back.runningLists()).includes('data-act="resume-active"'), 'the actions dialog offers to reopen the workgraph');
    back.resumeFromFinalizing();
    check(back.wg.status === 'Active' && back.nodes.simulation.status === 'Active' && back.wg.transitions['Finalizing>Active'] === 1, 'Finalizing back to Active returns every member to Active');
    check(back.nodes.simulation.lists.finalize.every((a) => a.result === null), 'and the finalizing results are reset for the next attempt');
    check(drain(back, () => back.wg.status === 'Finalizing'), 'the drained workgraph finalises again');

    const fresh = new Sim({ name: 'cancel-new', transformations: { simulation: { feeder: { seeds: 8 }, packer: { size: 1 } } } });
    check(!statesHtml(fresh).includes('data-act') && g.WorkgraphSim.wgMachineHtml(fresh).includes('data-act="cancel"'), 'the strip carries no control; the machine offers cancel before the model starts');
    fresh.cancel();
    check(fresh.wg.status === 'Cancelling' && fresh.wg.transitions['New>Cancelling'] === 1, 'a workgraph can be cancelled before it starts');
    check(until(fresh, () => fresh.wg.status === 'Cleaned'), 'and reaches Cleaned');
    /* The strip is the path this run can have taken, and grey on it means not yet: a workgraph with no scout never
       reaches these two, so drawing them would promise a phase that is not coming. A cancelled one has to keep that,
       since the cancelling branch slices the path rather than the full list. */
    check(!statesHtml(fresh).includes('>Scouting<') && !statesHtml(fresh).includes('>Approving<'), 'a workgraph with no scout leaves the states it cannot reach out of the strip, cancelled as well');
    check(g.WorkgraphSim.wgMachineHtml(fresh).includes('>Scouting<'), 'and the machine keeps them, being the machine of DX-ADR-005 rather than this run');
    const scouted = new Sim({ name: 'scouted', workgraph: { scouting: { count: 3 } }, transformations: { simulation: { feeder: { seeds: 8 }, packer: { size: 1 } } } });
    check(statesHtml(scouted).includes('>Scouting<') && statesHtml(scouted).includes('>Approving<'), 'a workgraph that has a scout and has not started keeps both, since they are still to come');
  }

  // a cleaning action waiting on the backends records Pending, which leaves the workgraph where it is (DX-ADR-006)
  {
    const { actionsHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'pending',
      transformations: { simulation: { feeder: { seeds: 40 }, packer: { size: 1 }, run: [1.5, 3] } },
      outputs: { out: { from: 'simulation' } },
    });
    sim.start();
    check(until(sim, () => sim.nodes.simulation.parcels.some((p) => p.status === 'Assigned')), 'parcels are running on the backend');
    sim.cancel();
    const cancelAction = sim.nodes.simulation.lists.clean[0];
    check(until(sim, () => cancelAction.result === 'Pending', 400), 'the cancelling action records Pending while the backends work');
    check(sim.wg.status === 'Cancelling' && sim.nodes.simulation.status === 'Cancelling', 'Pending leaves both where they are');
    check(actionsHtml(sim, sim.nodes.simulation).includes('pending'), 'and the dialog says so');
    check(sim.nodes.simulation.parcels.some((p) => p.status === 'Completing'), 'the parcels asked to stop are Completing, not already Cancelled');
    check(until(sim, () => cancelAction.result === 'Done'), 'once the backends confirm, the action is Done');
    check(sim.parcelCounts(sim.nodes.simulation).Cancelled > 0, 'and the parcels reached Cancelled');
    check(until(sim, () => sim.wg.status === 'Cleaned'), 'the cleaning list then runs to the end');
  }

  // a transformation's own Active hook pauses it once its failure rate is excessive (DX-ADR-005, DX-ADR-006)
  {
    const { hooksHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'pause-hook',
      workgraph: { approval: 'auto' },
      transformations: { simulation: { feeder: { seeds: 30 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0.9, retries: 0, pauseAbove: 0.5 } },
    });
    sim.start();
    const node = sim.nodes.simulation;
    check(until(sim, () => node.status === 'Paused', 8000), 'the hook pauses a transformation that keeps failing');
    check(/paused it/.test(node.lastActive.text), 'and says why: ' + node.lastActive.text);
    check(hooksHtml(sim, node).includes('Active (workgraph)') && hooksHtml(sim, node).includes('paused it'), "the dialog lists the transformation's hook apart from the workgraph's");
    const parcels = node.parcels.length;
    for (let i = 0; i < 200; i++) sim.step(0.05);
    check(node.parcels.length === parcels && sim.wg.status === 'Active', 'nothing new runs, and the workgraph waits');
    const { feederHtml } = g.WorkgraphSim;
    const open = sim.counts(node).U + sim.counts(node).A + sim.counts(node).F;
    check(open > 0 && feederHtml(sim, node).includes('this member is Paused, so nothing will move them'), `the guard says why the workgraph is not finishing: ${open} non-terminal`);
    sim.settings.failScale = 0;
    sim.toggleNode('simulation');
    check(node.status === 'Active' && node.hookBase === null, 'resuming it gives the hook a fresh window rather than the record that paused it');
    check(until(sim, () => sim.waitsOnOperator(node)), 'the pool drains, and what the hook had given up on still waits');
    check(sim.wg.status === 'Active' && sim.counts(node).Pb > 0, 'the quarantine holds the workgraph open: Problematic is not terminal');
    sim.resetProblematic('simulation');
    check(until(sim, () => sim.wg.status === 'Completed'), 'the operator fixes the cause, resets the quarantine, and the workgraph drains');
    check(node.status === 'Completed' && sim.counts(node).P === 30, 'and every input it had was processed');
  }

  // the spec options that no documentation page uses: an auto start, a fractional scout, the two ways a member is held back,
  // grouping by colour, and a seed that changes the run
  {
    const sim = new Sim({
      name: 'options',
      seed: 7,
      start: 'auto',
      workgraph: { scouting: { fraction: 0.25 }, approval: 'auto' },
      sources: { query: { files: 40, colours: 2 } },
      transformations: {
        reco: { feeder: { from: 'query' }, packer: { size: 2, by: 'colour' }, run: [0.3, 0.6] },
        held: { feeder: { from: 'reco' }, packer: { size: 2 }, run: [0.3, 0.6], hold: 'operator' },
        later: { feeder: { from: 'reco' }, packer: { size: 2 }, run: [0.3, 0.6], hold: 'approval' },
      },
      outputs: { out: { from: 'held' } },
    });
    check(sim.wg.status === 'Scouting', 'start: auto leaves New without an operator');
    check(sim.scoutLimit(sim.sources.query) === 10, 'a fractional scout takes its share of the query: ' + sim.scoutLimit(sim.sources.query));
    check(sim.nodes.held.status === 'Paused' && sim.nodes.later.status === 'Paused', 'both held members wait');
    check(until(sim, () => sim.wg.status === 'Active'), 'the scout is approved');
    check(sim.nodes.held.status === 'Paused', 'the operator-held one still waits');
    sim.startNode('held');
    check(sim.nodes.held.status === 'Active', 'and starts when the operator says so');
    check(until(sim, () => sim.wg.status === 'Completed'), 'the workgraph completes');
    check(sim.scoutLimit(sim.sources.query) === 40, 'the full query is read once the scout is over');
    const grouped = sim.nodes.reco.parcels.filter((p) => p.inputs.length > 1);
    check(grouped.length > 0 && grouped.every((p) => p.inputs.every((i) => i.file.colour === p.inputs[0].file.colour)), 'the packer grouped by colour');

    const other = new Sim(Object.assign({}, { name: 'options', seed: 1, start: 'auto', transformations: { reco: { feeder: { seeds: 10 }, packer: { size: 1 }, run: [0.3, 0.6] } } }));
    const again = new Sim(Object.assign({}, { name: 'options', seed: 2, start: 'auto', transformations: { reco: { feeder: { seeds: 10 }, packer: { size: 1 }, run: [0.3, 0.6] } } }));
    const durations = (x) => x.nodes.reco.parcels.map((p) => p.dur.toFixed(3)).join(',');
    check(until(other, () => other.nodes.reco.parcels.length === 10) && until(again, () => again.nodes.reco.parcels.length === 10), 'both seeds pack their inputs');
    check(durations(other) !== durations(again), 'a different seed is a different run');
    const first = durations(other);
    other.reset();
    other.start();
    check(until(other, () => other.nodes.reco.parcels.length === 10), 'the model runs again after a reset');
    check(durations(other) !== first, 'and each run advances the seed by one, so the second run differs');
  }

  // a member held back for an operator waits for a person, and nothing in the model releases it: a Paused member drains for
  // nobody, so the workgraph stays Active until its start button in the toolbar is pressed
  {
    const { controlsHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'operator hold',
      workgraph: { scouting: { count: 6 }, approval: 'auto' },
      sources: { query: { files: 24 } },
      transformations: {
        spruce: { label: 'sprucing', feeder: { from: 'query' }, packer: { size: 2 }, run: [0.3, 0.6], fail: 0 },
        removal: { kind: 'removal', label: 'buffer removal', feeder: { from: 'query', after: ['spruce'] }, packer: { size: 4 }, run: [0.2, 0.4], fail: 0, hold: 'operator' },
      },
      outputs: { datasets: { from: 'spruce' } },
    });
    check(sim.wg.status === 'New' && controlsHtml(sim) === '', 'a workgraph that has not started offers no start button');
    sim.start();
    check(sim.wg.status === 'Scouting' && sim.nodes.removal.status === 'Paused' && controlsHtml(sim) === '', 'the held member waits through the scout, where it cannot be started');
    check(until(sim, () => sim.wg.status === 'Active'), 'the scout is approved');
    const html = controlsHtml(sim);
    check(/data-act="start-node"[^>]*data-node="removal"/.test(html), 'Active offers the held member a start button in the toolbar: ' + html);
    check(html.includes('wgsim-glow'), 'which glows while it waits, with help mode off');
    check(!drain(sim, () => sim.wg.status === 'Completed', 4000), 'the workgraph does not complete on its own');
    check(sim.wg.status === 'Active' && sim.nodes.removal.status === 'Paused' && sim.drained(sim.nodes.spruce), 'it stays Active on the hold alone, everything else drained');
    sim.startNode(/data-node="([^"]+)"/.exec(controlsHtml(sim))[1]);
    check(sim.nodes.removal.status === 'Active' && controlsHtml(sim) === '', 'the button starts the member, and goes with the hold');
    complete(sim);
    check(sim.wg.status === 'Completed' && sim.counts(sim.nodes.removal).P === 24, 'and the workgraph drains, every buffer copy removed');
  }

  // the operator's edges live on the workgraph's machine as buttons, and only where DX-ADR-005 allows the transition; the strip carries none
  {
    const { statesHtml, wgOps, wgMachineHtml } = g.WorkgraphSim;
    const offered = (sim) => wgOps(sim).some((o) => o.act === 'cancel') && wgMachineHtml(sim).includes('data-act="cancel"') && !statesHtml(sim).includes('data-act');
    const lit = (sim) => (statesHtml(sim).match(/wgsim-pill on/g) || []).length;
    const sim = new Sim({
      name: 'cancel-pill',
      workgraph: { approval: 'auto', archiveAfter: 0.5 },
      transformations: { simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6] } },
    });
    check(offered(sim) && lit(sim) === 1, 'New offers cancel, on the one lit pill');
    check((wgMachineHtml(sim).match(/data-act="cancel"/g) || []).length === 1 && !wgMachineHtml(sim).includes('data-act="drain"'), 'on one edge of the machine; New has no drain');
    sim.start();
    check(offered(sim) && wgOps(sim).map((o) => o.act).join('+') === 'drain+halt+cancel', 'Active offers drain, halt and cancel: ' + wgOps(sim).map((o) => o.act).join('+'));
    check(wgOps(sim).every((o) => /in flight|in-flight/.test(o.tip)), "every operator button's tooltip says what becomes of the work in flight");
    check(until(sim, () => sim.wg.status === 'Finalizing'), 'reaches Finalizing');
    check(offered(sim) && wgOps(sim).length === 1, 'Finalizing offers cancel alone');
    check(until(sim, () => sim.wg.status === 'Completed'), 'reaches Completed');
    check(!offered(sim) && wgOps(sim).length === 0 && !wgMachineHtml(sim).includes('wg-m-btn'), 'Completed offers nothing: no button on the machine');
    check(until(sim, () => sim.wg.status === 'Archived'), 'reaches Archived');
    check(!offered(sim), 'nor does Archived');
    const done = new Sim({ name: 'cancelled', transformations: { simulation: { feeder: { seeds: 4 }, packer: { size: 1 } } } });
    done.cancel();
    check(!offered(done), 'a workgraph already cancelling does not offer it again');
    check(statesHtml(done).includes('Cancelling'), 'and the strip shows the cancelling tail');
  }

  // a member's list waits for the lists of the members it depends on, and members with no dependency between them run theirs at once (DX-ADR-005)
  {
    const { railStates } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'top-down',
      workgraph: { approval: 'auto', archiveAfter: 0.2 },
      transformations: {
        simulation: { feeder: { seeds: 4 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: [{ name: 'check', fail: 'once' }, 'merge'] },
        other: { feeder: { seeds: 4 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: ['check other'] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: ['check reco'] },
      },
    });
    sim.start();
    check(until(sim, () => sim.wg.status === 'Finalizing'), 'reaches Finalizing');
    const reco = sim.nextAction('reco');
    check(reco && reco.why === 'waiting' && reco.on.length === 1 && reco.on[0].id === 'simulation', 'reco waits for simulation: ' + JSON.stringify(reco && reco.why));
    sim.setMode('reco', 'actions', 'manual');
    check(railStates(sim, 'reco').actions.state === 'manual' && railStates(sim, 'reco').actions.tip.includes('waiting for simulation'), 'its actions row has no button and says why: ' + railStates(sim, 'reco').actions.tip);
    sim.setMode('reco', 'actions', 'auto');
    check(!sim.runAction('reco'), 'and it cannot be run by hand');
    check(until(sim, () => sim.nodes.simulation.status === 'FinalizingBlocked'), 'the upstream check blocks');
    check(sim.nodes.other.status === 'Finalized' || sim.nodes.other.run.action || sim.nodes.other.lists.finalize[0].result, 'a member with no dependency runs its list meanwhile');
    check(sim.nodes.simulation.run.blocked && sim.blocked === sim.nodes.simulation.run.blocked && sim.nodes.reco.status === 'Finalizing' && sim.nextAction('reco').why === 'waiting', 'the blocked member holds reco back');
    sim.forceAction('reco');
    check(sim.nodes.simulation.status === 'FinalizingBlocked', 'forcing reco does nothing: it has nothing blocked');
    sim.forceAction('simulation');
    check(until(sim, () => sim.nodes.simulation.status === 'Finalized'), 'forced, simulation finishes its list');
    check(until(sim, () => sim.nodes.reco.run.action || sim.nodes.reco.status === 'Finalized'), 'then reco starts');
    check(until(sim, () => sim.wg.status === 'Completed'), 'and the workgraph completes');
    check(until(sim, () => sim.wg.status === 'Archived'), 'archiving runs the same way to Archived');
  }

  // with the actions by hand nothing runs until asked: the state line runs an approving check, the bolt a member's, and the pending list names each
  {
    const { stateCard, listsHtml, railStates } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'manual-actions',
      settings: { actionPeriod: 0 },
      workgraph: { scouting: { count: 4 }, approving: ['check success rate', { name: 'manual approval', manual: true }] },
      transformations: { simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: ['check', { name: 'cancel parcels', effect: 'cancel' }] } },
    });
    sim.start();
    check(until(sim, () => sim.wg.status === 'Approving'), 'reaches Approving');
    for (let i = 0; i < 100; i++) sim.step(0.05);
    check(sim.wg.status === 'Approving' && !sim.wg.run.action && sim.wgList[0].result === null, 'no check starts on its own');
    check(railStates(sim, 'workgraph').actions.state === 'run' && railStates(sim, 'workgraph').actions.tip.includes('check success rate is next'), 'the rail offers the check by hand: ' + JSON.stringify(railStates(sim, 'workgraph')));
    let steps = sim.pending();
    check(steps.length === 1 && steps[0].kind === 'action' && steps[0].node === null && steps[0].why === 'queued', 'the pending list holds the queued check');
    check(listsHtml(sim, sim.runningLists()).includes('data-act="run-row" data-node="workgraph" data-row="actions"') && listsHtml(sim, sim.runningLists()).includes('run check success rate'), 'the actions dialog offers to run it');
    check(sim.runAction('workgraph') && sim.wgList[0].result === 'Passed', 'run by hand, it passes');
    check(sim.wgList[1].result === null, 'the next one does not start on its own');
    check(sim.runAction() && sim.wg.status === 'ApprovingBlocked' && sim.wgList[1].result === 'Failed', 'the sign-off, run when asked, fails as a sweep would: nobody has given it');
    sim.rerunAction();
    for (let i = 0; i < 100; i++) sim.step(0.05);
    check(sim.wg.status === 'Approving' && sim.wg.run.action === sim.wgList[1], 'reset, it waits unblocked while nothing sweeps');
    check(listsHtml(sim, sim.runningLists()).includes('force passed (sign off)') && !listsHtml(sim, sim.runningLists()).includes('data-act="extend-scout"') && railStates(sim, 'workgraph').actions.state === 'run', 'the actions dialog offers to force it, which is the sign-off, and the rail to run it; nothing is blocked, so not to scout further');
    check(listsHtml(sim, sim.runningLists()).includes('sign-off') && listsHtml(sim, sim.runningLists()).includes('force passed (sign off)'), 'the actions dialog says what a sign-off is and offers it');
    sim.runAction();
    check(sim.wg.status === 'ApprovingBlocked' && sim.pending()[0].why === 'blocked', 'run again, it fails again');
    sim.forceAction('workgraph');
    check(until(sim, () => sim.wg.status === 'Active', 5), 'forced, the workgraph is Active');
    check(until(sim, () => sim.wg.status === 'Finalizing'), 'reaches Finalizing');
    for (let i = 0; i < 100; i++) sim.step(0.05);
    const node = sim.nodes.simulation;
    check(node.status === 'Finalizing' && node.lists.finalize[0].result === null, 'the finalizing list waits too');
    check(railStates(sim, 'simulation').actions.state === 'run', 'the actions row offers the run');
    check(sim.runRow('simulation', 'actions') && node.lists.finalize[0].result === 'Passed' && node.lists.finalize[1].result === null, 'the row runs one action');
    check(sim.runAction('simulation') && node.status === 'Finalized', 'and the next');
    check(railStates(sim, 'simulation').actions.state === 'manual', 'with the list done the row has no button');
  }

  // an action that cancels parcels still in flight records Pending when run by hand, as it does in a sweep
  {
    const sim = new Sim({
      name: 'pending-by-hand',
      settings: { actionPeriod: 0 },
      transformations: { simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [2, 3], fail: 0 } },
    });
    sim.start();
    check(until(sim, () => sim.nodes.simulation.parcels.some((p) => p.status === 'Assigned')), 'parcels run');
    sim.cancel();
    const node = sim.nodes.simulation;
    check(sim.runAction('simulation') && node.lists.clean[0].result === 'Pending' && node.status === 'Cancelling', 'the cancel action records Pending: ' + node.lists.clean[0].result);
    check(until(sim, () => !sim.liveParcels(node)), 'the backends confirm');
    check(sim.runAction('simulation') && node.lists.clean[0].result === 'Done', 'run again, it is Done');
  }

  // with the hooks by hand a failed input waits in Failed for HandleFailedInput, whose decision the walkthrough shows and the operator may take in its place
  {
    const { stepsHtml, machineHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'failed-by-hand',
      settings: { hookPeriod: 0 },
      transformations: { simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 1, retries: 1, sections: 4 } },
    });
    sim.start();
    const node = sim.nodes.simulation;
    check(until(sim, () => sim.counts(node).F > 0), 'an input lands in Failed');
    for (let i = 0; i < 40; i++) sim.step(0.05);
    const c = sim.counts(node);
    check(c.F > 0 && node.decisions.retried === 0, 'and stays there while nothing sweeps: ' + JSON.stringify(c));
    const steps = sim.pending();
    check(steps.length === c.F && steps[0].kind === 'input' && steps[0].input.decision === 'Unassigned', 'the pending list holds each, with the hook\'s decision drawn: ' + JSON.stringify(steps.map((s) => s.input.decision)));
    const html = stepsHtml(sim, steps);
    const picked = (h) => (h.match(/aria-checked="true"[^>]*data-to="(\w+)"/) || [])[1];
    check(picked(html) === 'Unassigned' && html.includes('data-to="Split"') && html.includes('data-to="Problematic"') && html.includes('>default<'), "the walkthrough preselects the hook's decision and offers the other two: " + picked(html));
    check(html.includes('1 of ' + steps.length + ' waiting') && html.includes('data-act="step-later"') && html.includes('data-act="step-apply"') && !/<p[\s>]/.test(html), 'it counts the queue, offers to decide later, and holds no prose until asked');
    check(stepsHtml(sim, steps, { what: true }).includes('<p>'), 'what is this? unfolds the explanation');
    check(stepsHtml(sim, steps, { shown: 1 }).includes('2 of ' + steps.length + ' waiting'), 'the step shown is counted');
    const applyTo = (h) => (h.match(/data-act="step-apply"[^>]*data-to="(\w+)"/) || [])[1];
    for (const to of ['Unassigned', 'Split', 'Problematic']) check(picked(stepsHtml(sim, steps, { to })) === to && applyTo(stepsHtml(sim, steps, { to })) === to, 'the reader can pick ' + to + ' and apply carries it');
    check(html.includes('wg-lin-latest') && html.includes('data-act="lineage"'), 'the strip shows the attempt and opens the full lineage');
    check(machineHtml(sim, node).includes('data-act="run-failed"'), 'the state machine puts the hook\'s button on the Failed box');
    const first = steps[0].input;
    check(sim.decideFailedInput('simulation', first.id, 'Problematic') && first.status === 'Problematic' && node.decisions.quarantined === 1, 'the operator sets one aside in the hook\'s place');
    check(sim.log[sim.log.length - 1].text.includes("in the hook's place"), 'and the log says so');
    const rest = sim.pending();
    if (rest.length) {
      const i = rest[0].input;
      check(sim.decideFailedInput('simulation', i.id) && i.status === 'Unassigned' && node.decisions.retried === 1, 'the hook run by hand retries the next');
    }
    check(sim.runFailedInputs('simulation', 'hand') === sim.pending().length + rest.length - (rest.length ? 1 : 0) - sim.pending().length, 'the hook runs over the rest');
    check(sim.counts(node).F === 0, 'nothing waits in Failed');
    const split = sim.pending();
    check(split.length === 0, 'the pending list is empty');
    check(until(sim, () => sim.counts(node).F > 0), 'the next failure waits again');
    const i2 = sim.pending()[0].input;
    check(sim.decideFailedInput('simulation', i2.id, 'Split') && i2.status === 'Split' && sim.counts(node).U >= 2, 'the operator splits one in the hook\'s place');
    sim.setMode('simulation', 'failedInput', 'auto');
    check(until(sim, () => sim.waitsOnOperator(node)), 'with the hooks sweeping again the run drains down to the one set aside');
    sim.writeOffProblematic('simulation');
    check(until(sim, () => sim.wg.status === 'Completed'), 'and once it is written off the run completes: ' + sim.wg.status);
  }

  // the header: the strip once, its lit pill the one way into the dialog and no arrow after the last state; the line only
  // while there is something to say; the workgraph's pills only in expert mode, in the line or, without one, in the strip
  {
    const { headHtml, stateCard, filesHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'header',
      workgraph: { target: { output: 'datasets', files: 14 } },
      transformations: { simulation: { feeder: { seeds: 40, batch: 4 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0 } },
      outputs: { datasets: { from: 'simulation' } },
    });
    const fresh = headHtml(sim, false);
    const lit = (h) => (h.match(/wgsim-pill on/g) || []).length;
    check(stateCard(sim).text === '' && !fresh.includes('wgsim-state-line') && !fresh.includes('wgsim-wgpill'), 'in New the header is the strip alone, one row: nothing to say and no pill');
    check(lit(fresh) === 1 && !fresh.includes('data-act="open-workgraph"') && fresh.includes('wgsim-head-hint'), 'the strip itself opens nothing: the band around it does, and says so on hover');
    check((fresh.match(/class="wgsim-states"/g) || []).length === 1 && !/wgsim-arrow[^<]*<\/span><\/div>/.test(fresh) && fresh.indexOf('Archived') > fresh.lastIndexOf('wgsim-arrow'), 'the strip is drawn once and no arrow follows Archived');
    const expertNew = headHtml(sim, true);
    check((expertNew.match(/wgsim-wgpill"/g) || []).length === 2 && !expertNew.includes('wgsim-state-line') && expertNew.indexOf('wgsim-wgrail') > expertNew.lastIndexOf('wgsim-pill "'), 'in expert mode with the line suppressed the two pills sit at the end of the strip');
    check(/wgsim-wgpill-glyph[^<]*<\/span><span class="wgsim-wgpill-name">hooks<\/span><span class="wgsim-wgpill-mode">auto</.test(expertNew), 'an automatic pill shows its glyph and the mode word');
    sim.start();
    check(until(sim, () => sim.wg.status === 'Active'), 'the model runs');
    const active = headHtml(sim, true);
    check(active.includes('wgsim-state-line') && active.includes(stateCard(sim).text.slice(0, 20)) && active.lastIndexOf('wgsim-wgrail') > active.indexOf('wgsim-state-line'), 'with something to say the line holds it, with the pills at its right end');
    check(!headHtml(sim, false).includes('wgsim-wgpill') && !headHtml(sim, false).includes('wgsim-wgrail'), 'outside expert mode there is no pill in the header');
    sim.setMode('workgraph', 'hooks', 'manual');
    const byHand = headHtml(sim, true);
    check(/data-state="(manual|run)" data-act="toggle-mode" data-node="workgraph" data-row="hooks"/.test(byHand) && byHand.includes('wgsim-wgpill-name">hooks<'), 'a pill by hand names its sweep');
    sim.setAllModes('auto');

    // the files list: newest first, the producer column only where the rows differ, the two oldest dimmed, and ten rows before the rest fold
    check(until(sim, () => sim.outputs.datasets.files.length >= 3), 'files reach the datasets box');
    const out = sim.outputs.datasets;
    const rows = (h) => (h.match(/data-act="lineage" data-file="(\d+)"/g) || []).map((m) => Number(m.match(/data-file="(\d+)"/)[1]));
    let html = filesHtml(sim, out);
    check(html.includes(`<b class="wgsim-files-n">${out.files.length}</b>`) && html.includes('newest first'), 'the header counts the files and says the order');
    check(rows(html)[0] === out.files[out.files.length - 1].id && rows(html)[rows(html).length - 1] === out.files[0].id, 'the newest row is at the top');
    check(!html.includes('wgsim-file-from') && (html.match(/class="wgsim-file old"/g) || []).length === 2, 'one producer, no producer column; the two oldest rows are dimmed');
    check(html.includes('just now') || /\d+s ago/.test(html), 'each row carries its age');
    const mixed = filesHtml(sim, { spec: { label: 'mixed' }, files: [Object.assign({}, out.files[0], { producer: 'other' }), out.files[1]] });
    check((mixed.match(/wgsim-file-from/g) || []).length === 2 && mixed.includes('>other<'), 'with two producers the column is rendered on every row');
    check(complete(sim) >= 0 && sim.wg.status === 'Completed' && out.files.length > 10, 'the run completes with more than ten files: ' + out.files.length);
    html = filesHtml(sim, out);
    check(rows(html).length === 10 && html.includes(`+${out.files.length - 10} older`) && html.includes('data-act="files-more"'), 'ten rows and a real expander for the rest');
    html = filesHtml(sim, out, { more: true });
    check(rows(html).length === out.files.length && !html.includes('older'), 'unfolded, every file is listed and the expander is gone');
    check(!filesHtml(sim, { spec: { label: 'empty' }, files: [] }).includes('older'), 'with nothing older the expander is not there');
  }

  // the pool shows what waits before the slots: Problematic inputs first, then the parcels the packer has made that wait for a
  // slot, each in its state's border with its inputs, then the loose inputs; the rest is counted in its corner
  {
    const { poolSvg, poolCells, slotLegendHtml, parcelsHtml, DISCLOSURES, BODY } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'pool',
      slots: 2,
      transformations: { simulation: { feeder: { seeds: 40 }, packer: { size: 1 }, run: [1, 2], fail: 0 } },
    });
    sim.start();
    const node = sim.nodes.simulation;
    check(until(sim, () => node.parcels.filter((p) => p.status === 'Unassigned').length > 25), 'with two slots the packer queues parcels');
    const it = { id: 'simulation', x: 0, y: 0, w: BODY.w, h: BODY.h };
    const cells = poolCells(it);
    const queued = node.parcels.filter((p) => p.status === 'Unassigned');
    const entries = queued.map((p) => ({ kind: 'parcel', parcel: p }));
    const pool = poolSvg(it, entries, 'simulation');
    const drawn = (html) => (html.match(/class="wg-pool-parcel"/g) || []).length;
    /* the cells are measured from the region, so the count is what the cells leave over rather than a number written here;
       what the corner takes for the count is cells the shapes do not get, which is why fewer are drawn than there are cells */
    check(cells.length > 12 && drawn(pool.svg) < cells.length && pool.more === `+${queued.length - drawn(pool.svg)}`, `the queue is drawn in the pool's cells and the rest counted exactly: ${drawn(pool.svg)} of ${cells.length} cells, ${pool.more}`);
    const exact = poolSvg(it, entries.slice(0, cells.length), 'simulation');
    check(exact.more === '' && drawn(exact.svg) === cells.length, `with nothing hidden every cell holds a shape and the corner is given back: ${drawn(exact.svg)} of ${cells.length}`);
    check(pool.svg.includes('wg-slot wg-slot-unassigned') && pool.svg.includes('data-act="open-parcels"') && pool.svg.includes('waiting for a slot'), 'a queued parcel wears the Unassigned border and opens the parcel counts');
    const few = poolSvg(it, [{ kind: 'input', input: { file: { id: 1, tag: 'f1', shape: 'circle', colour: 0 } }, cls: 'wg-problematic' }].concat(entries.slice(0, 2)), 'simulation');
    check(few.more === '' && few.svg.indexOf('wg-problematic') < few.svg.indexOf('wg-pool-parcel'), 'nothing hidden, nothing counted; a Problematic input leads');
    /* the slot encoding has a legend: a swatch of each state's box leads its row in the parcels table, and the grid's own
       disclosure carries the same swatches, generated from the same map (tests/workgraph-sim/help.test.js checks that) */
    const table = parcelsHtml(sim.parcelCounts(node));
    check(table.includes('wgsim-swatch') && table.includes('wg-slot-unassigned') && table.includes('wg-slot-reserved') && table.includes('dashed = waiting on the backend'), 'the parcels table carries a swatch per state and the channels');
    const legend = slotLegendHtml();
    check(['unassigned', 'reserved', 'assigned', 'completing', 'done', 'partiallydone', 'failed', 'cancelled'].every((s) => legend.includes(`wg-slot-${s}`)) && !legend.includes('recovery'), 'the slot legend names every treatment in use and no other');
    check(DISCLOSURES.slots.legend({}) === legend && DISCLOSURES.pool.legend({}).includes('wg-pool-parcel'), 'the grid and the pool each disclose their own encoding');
  }

  // the scouting ladder: the ScoutingToApproving hook climbs the stages as each drains and accepts after the last, the
  // success-rate check judges the same threshold, and an operator is needed only once the workgraph is blocked
  {
    const ladder = (extra) => new Sim(Object.assign({
      name: 'ladder',
      workgraph: { scouting: { stages: [4, 8, 16], failAbove: 0.3, minSeen: 6 }, approving: [{ name: 'check success rate', check: 'success rate' }], target: { output: 'datasets', files: 40 } },
      transformations: { simulation: { feeder: { seeds: 200, batch: 20 }, packer: { size: 1 }, run: [0.2, 0.4], fail: 0, retries: 1 } },
      outputs: { datasets: { from: 'simulation' } },
    }, extra || {}));
    const sim = ladder();
    check(sim.spec.workgraph.scouting.stages.join(',') === '4,8,16' && sim.spec.workgraph.scouting.kind === 'count', 'the spec keeps the ladder');
    sim.start();
    check(sim.scoutSample() === 4 && sim.seedTarget(sim.nodes.simulation) === 4, 'the first stage is the sample');
    check(until(sim, () => sim.wg.scout.stage === 1), 'the hook raises the scout to the second stage once the first has drained');
    check(sim.wg.status === 'Scouting' && sim.seedTarget(sim.nodes.simulation) === 8 && sim.log.some((e) => e.text.includes('raised the scout to stage 2 of 3')), 'inside Scouting, and the log says so: ' + sim.wg.status);
    check(sim.wgHooks()[0].text.startsWith('stage 2 of 3'), 'the line says where the scout is: ' + sim.wgHooks()[0].text);
    check(until(sim, () => sim.wg.status === 'Approving'), 'after the last stage the hook accepts');
    check(sim.wg.scout.stage === 2 && sim.nodes.simulation.seedNext === 16, 'having climbed every stage');
    check(until(sim, () => sim.wg.status === 'Active'), 'the success-rate check passes on its own');
    check(sim.wgList[0].result === 'Passed' && sim.log.some((e) => e.text.includes('check success rate → Passed (0 of')), 'and records the rate it judged');
    check(g.WorkgraphSim.stateCard(sim).text.startsWith('requesting'), 'active, the line says the feeders are requesting: ' + g.WorkgraphSim.stateCard(sim).text);
    check(until(sim, () => !sim.feedersActive()), 'the target disables the feeders');
    check(g.WorkgraphSim.stateCard(sim).text.startsWith('feeders done, draining'), 'and the line says the workgraph drains: ' + g.WorkgraphSim.stateCard(sim).text);
    sim.reset();
    check(sim.wg.scout.stage === 0 && sim.wg.scout.stages.join(',') === '4,8,16', 'a reset starts the ladder again');

    const bad = ladder({ transformations: { simulation: { feeder: { seeds: 200, batch: 20 }, packer: { size: 1 }, run: [0.2, 0.4], fail: 1, retries: 1 } } });
    bad.start();
    check(until(bad, () => bad.wg.status !== 'Scouting'), 'with every parcel failing the scout does not run on');
    check(bad.wg.status === 'Approving' && bad.scoutRate().failed >= 6 && bad.scoutRate().processed === 0 && bad.wg.transitions['Scouting>Approving'] === 1, 'the hook accepted early, on inputs that ended Problematic: ' + bad.wg.status);
    check(until(bad, () => bad.wg.status === 'ApprovingBlocked'), 'and the check failed, with no operator involved: ' + bad.wg.status);
    check(bad.wgList[0].result === 'Failed', 'the list records it');
    check(bad.log.some((e) => e.text.includes('accepted early') && e.text.includes('so the success-rate check will fail')), 'the log says the accept was deliberate');
    check(bad.wg.scout.stage === 2 && bad.scoutRate().stage.seen >= 6, 'a stage whose inputs are all set aside drains quarantine aside, and the ladder climbs until one stage alone has settled enough to judge: stage ' + (bad.wg.scout.stage + 1));
    const before = bad.wgList[0].attempts;
    bad.settings.failScale = 0;
    bad.rerunAction('workgraph');
    check(bad.wg.status === 'Approving' && until(bad, () => bad.wg.status === 'ApprovingBlocked') && bad.wgList[0].attempts === before + 1, 'reset re-runs the check against the same numbers, which still fail');
    bad.extendScout();
    check(bad.wg.status === 'Scouting' && bad.wg.scout.stages.length === 4 && bad.wg.scout.stage === 3, 'scout further adds a stage to the ladder');
    /* the first stage's quarantined inputs hold the scout open and the rate up until an operator writes them off, which drain does; written off they no longer count */
    check(drain(bad, () => bad.wg.status === 'Active', 40000), 'with the failures gone the larger sample brings the rate within the threshold and the check passes: ' + bad.wg.status);
  }

  // the sample stands until the workgraph is approved: Approving is where a person decides whether the full query runs, so a
  // feeder that read past the sample while a sign-off waited would answer the question the phase exists to ask. A query feeder
  // is what catches it — a seed feeder carries seedRequested, which holds the line on its own, and a query has only its cursor.
  {
    const spec = (extra) => ({
      name: 'held sample',
      workgraph: Object.assign({ scouting: { count: 6 }, approving: [{ name: 'manual approval', manual: true }] }, extra || {}),
      sources: { query: { files: 60 } },
      transformations: {
        read: { feeder: { from: 'query' }, packer: { size: 1 }, run: [0.2, 0.4], fail: 0 },
        seeded: { feeder: { seeds: 60 }, packer: { size: 1 }, run: [0.2, 0.4], fail: 0 },
      },
      outputs: { datasets: { from: 'read' } },
    });
    const sim = new Sim(spec());
    sim.start();
    check(until(sim, () => sim.wg.status === 'ApprovingBlocked'), 'the sign-off nobody gave blocks the workgraph: ' + sim.wg.status);
    check(sim.scoutLimit(sim.sources.query) === 6 && sim.seedTarget(sim.nodes.seeded) === 6, 'both limits are still the sample while it is blocked');
    const read = sim.nodes.read.inputs.size;
    const seeded = sim.nodes.seeded.inputs.size;
    for (let i = 0; i < 4000; i++) sim.step(0.05);
    check(sim.wg.status === 'ApprovingBlocked' && sim.nodes.read.inputs.size === read && sim.nodes.seeded.inputs.size === seeded, `neither feeder reads on while it waits: query ${read}→${sim.nodes.read.inputs.size}, seeds ${seeded}→${sim.nodes.seeded.inputs.size}`);
    check(read === 6 && seeded === 6, 'and what they took is the sample, not the query: ' + read + ', ' + seeded);
    check(sim.feederInfo(sim.nodes.read).text === '6/6 files', 'the card says the query feeder is at the sample: ' + sim.feederInfo(sim.nodes.read).text);
    sim.forceAction('workgraph');
    check(until(sim, () => sim.wg.status === 'Active'), 'the sign-off releases it: ' + sim.wg.status);
    check(until(sim, () => sim.nodes.read.inputs.size > 6), 'and the rest of the query is read only then');
    check(drain(sim, () => sim.wg.status === 'Completed', 60000), 'the workgraph finishes on the whole query: ' + sim.wg.status);
    check(sim.nodes.read.inputs.size === 60 && sim.nodes.seeded.inputs.size === 60, 'every file and every seed: ' + sim.nodes.read.inputs.size + ', ' + sim.nodes.seeded.inputs.size);

    /* a scout sent further is still inside the sample, so the ladder's new stage is the limit and not the query */
    const again = new Sim(spec({ approving: [{ name: 'chk', fail: true }] }));
    again.start();
    check(until(again, () => again.wg.status === 'ApprovingBlocked'), 'the failing check blocks it: ' + again.wg.status);
    again.extendScout();
    check(again.wg.status === 'Scouting' && again.scoutLimit(again.sources.query) === again.scoutSample(), 'scout further raises the limit to the new stage, not to the query: ' + again.scoutLimit(again.sources.query) + ' of 60');
  }

  // the actions dialog: the lists running now, the workgraph's first and the members' upstream first, each action's result
  // as it lands, the buttons only where the state allows them, and a finished list kept in view with its outcome
  {
    const { listsHtml, stepsHtml, stateCard } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'lists',
      workgraph: { scouting: { count: 4 }, approval: 'auto', approving: [{ name: 'check success rate', check: 'success rate' }, { name: 'estimate resource usage', fail: 'once' }, { name: 'manual approval', manual: true }] },
      transformations: {
        simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: ['no seed used twice'] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.3, 0.6], fail: 0, finalize: ['no input used twice', 'merge histograms'] },
      },
    });
    sim.start();
    check(sim.runningLists().length === 0, 'nothing runs a list while scouting');
    check(until(sim, () => sim.wg.status === 'Approving'), 'reaches Approving');
    let lists = sim.runningLists();
    check(lists.length === 1 && lists[0].id === 'workgraph' && lists[0].key === 'approving' && lists[0].items.length === 3, 'the approving list runs: ' + JSON.stringify(lists.map((l) => l.id)));
    let html = listsHtml(sim, lists);
    /* the column says which entity its list is about, and for this one that is the workgraph: the spec's name is the
       document's title, a line of prose, and the log and the mode matrix both call this entity `workgraph` */
    check(html.includes('<h4><span>workgraph</span>') && !html.includes('>lists<'), 'the workgraph\'s column is titled workgraph rather than the model\'s name: ' + (/<h4><span>([^<]*)</.exec(html) || [])[1]);
    check(html.includes('approving list') && html.includes('check success rate') && !html.includes('data-act="force"') && !html.includes('data-act="run-row"'), 'the dialog shows the list with no action button while it runs by itself');
    check(html.includes('data-act="lists-toggle"') && html.includes('wgsim-panel-head') && !html.includes('close-lists') && !html.includes('data-act="extend-scout"') && !html.includes('wgsim-lists-foot'), 'it collapses rather than closes, has the dialogs\' header, and offers nothing while the list runs by itself');
    const collapsed = listsHtml(sim, lists, { collapsed: true });
    check(collapsed.includes('aria-expanded="false"') && !collapsed.includes('wgsim-lists-body') && collapsed.includes('approving list'), 'collapsed, only the header row is left, still naming what runs');
    check(stateCard(sim).text === '', 'and the header line says nothing while the list runs');
    check(until(sim, () => sim.wg.status === 'ApprovingBlocked'), 'the resource estimate blocks it');
    html = listsHtml(sim, sim.runningLists());
    check(html.includes('estimate resource usage failed') && html.includes('data-act="force"') && html.includes('data-act="rerun"') && html.includes('data-status="ApprovingBlocked"'), 'a failed action carries force passed and reset');
    check(g.WorkgraphSim.blockedKind(sim, null) === 'problem' && html.includes('data-blocked="problem"') && g.WorkgraphSim.statesHtml(sim).includes('wgsim-pill on blocked') && g.WorkgraphSim.wgMachineHtml(sim).includes('wg-m-on wg-m-blocked'), 'a failed check is a problem: red on the strip, the machine and the panel');
    check(html.includes('data-act="extend-scout"') && !html.includes('data-act="resume-active"'), 'and blocked, the footer offers to scout further');
    sim.forceAction();
    check(until(sim, () => sim.wg.status === 'Active'), 'signed off, the workgraph is Active');
    check(sim.runningLists().length === 0, 'and no list runs');
    const kept = listsHtml(sim, [{ id: 'workgraph', key: 'approving', label: 'approving', items: sim.wgList }]);
    check(!kept.includes('every action') && !kept.includes('wgsim-lists-why') && kept.includes('class="wgsim-lists-entity done"'), 'an entry kept past its run says nothing under its ticks: the ticks say it');
    check(until(sim, () => sim.wg.status === 'Finalizing'), 'reaches Finalizing');
    lists = sim.runningLists();
    check(lists.map((l) => l.id).join('+') === 'simulation+reco' && lists[1].next.why === 'waiting', 'both finalizing lists run, upstream first, with reco waiting: ' + JSON.stringify(lists.map((l) => l.id + ':' + (l.next && l.next.why))));
    html = listsHtml(sim, lists);
    check(html.includes('waiting for simulation') && (html.match(/class="wgsim-lists-entity live"/g) || []).length === 2, 'the dialog says what reco waits for');
    /* no string twice with identical wording: each line under a list names its own action or what it waits for */
    const whys = [...html.matchAll(/<div class="wgsim-lists-why"><span>([^<]*)<\/span>/g)].map((m) => m[1]).filter(Boolean);
    check(new Set(whys).size === whys.length, 'no two lists carry the same line: ' + JSON.stringify(whys));
    check(!html.includes('data-act="resume-active"') && !html.includes('data-act="extend-scout"'), 'finalizing by itself, the footer offers nothing');
    check(!stepsHtml(sim, sim.pending()), 'the walkthrough has nothing for actions');
    sim.watchActions = true;
    const t0 = sim.t;
    check(until(sim, () => sim.nodes.simulation.status === 'Finalized'), 'watched, the list still runs');
    check(until(sim, () => sim.wg.status === 'Completed'), 'and completes');
    check(sim.t - t0 >= 1.5, 'no faster than one action a beat: ' + (sim.t - t0).toFixed(2) + 's');
  }

  // what the panel keeps once a list stops running, and for how long. A cancel out of Finalizing is the one phase change with
  // no idle gap for the panel to empty in, so it is the one that can leave a list nothing is running beside the list that
  // replaced it; and a finished list is kept on its own beat rather than the panel's, since the row is the work left to do.
  {
    const { listsHtml, keptLists, orderLists } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'cancel-lists',
      workgraph: { approval: 'auto' },
      transformations: {
        simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: ['no seed used twice'] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.3, 0.6], fail: 0, finalize: ['no input used twice', 'merge histograms'] },
      },
    });
    sim.start();
    /* the panel's own map, kept frame by frame as the widget keeps it: a list is added when it starts, and every frame prunes
       what the panel no longer holds, which is what dates a finished list's beat from the frame that first saw it finish */
    const shown = new Map();
    const watch = () => {
      for (const l of sim.runningLists()) if (!shown.has(`${l.id}:${l.key}`)) shown.set(`${l.id}:${l.key}`, { id: l.id, key: l.key, label: l.label, items: l.items, done: null });
      const kept = keptLists([...shown.values()], sim.runningLists(), sim.t);
      if (kept.length < shown.size) {
        shown.clear();
        for (const e of kept) shown.set(`${e.id}:${e.key}`, e);
      }
    };
    const row = () => orderLists(sim, [...shown.values()]);
    const keys = () => row().map((e) => `${e.id}:${e.key}`).join('+');
    const run = (pred, limit) => { let n = 0; while (!pred() && n++ < (limit || 20000)) { sim.step(0.05); watch(); } return pred(); };
    const beat = (seconds) => { const t = sim.t; while (sim.t - t < seconds) { sim.step(0.05); watch(); } };
    const reco = sim.nodes.reco;
    check(run(() => sim.nodes.simulation.status === 'Finalized' && reco.lists.finalize.some((a) => a.result === 'Running' || a.result === 'Pending')), "reco is midway through its finalizing list while simulation's has finished");
    check(keys() === 'simulation:finalize+reco:finalize', 'both are on the row, the finished one kept past its run: ' + keys());
    sim.cancel();
    beat(0.3);
    check(keys() === 'simulation:finalize+simulation:clean+reco:clean', 'the cancel drops the list it abandoned and keeps the one that finished: ' + keys());
    const html = listsHtml(sim, row());
    check(!html.includes('merge histograms') && (html.match(/<h4>/g) || []).length === 3, 'so the row shows no action nothing is running, and one column per member but for the one kept: ' + (html.match(/<h4>/g) || []).length + ' columns');
    check(html.includes('<span class="wgsim-dim">finalizing</span><span class="wgsim-lists-status" data-status="Finalized"'), 'and the kept column says where its own list got to rather than where the member has gone since');
    beat(2.1);
    check(!keys().includes('simulation:finalize'), 'the finished list leaves a beat later, while the cleaning lists run on: ' + keys());
    check(keys().includes('clean'), 'which is its own beat rather than the panel\'s: ' + keys());
    check(run(() => sim.wg.status === 'Cleaned'), 'the cancelled workgraph still reaches Cleaned');
    beat(2.1);
    check(keys() === '', 'and the panel goes once the row is empty: ' + keys());
  }

  // the row holds the lists in the order the run takes them, which is the engine's own order: the workgraph first, then the
  // members upstream first. A list kept past its run has dropped out of `runningLists()`, so the two are worked out
  // separately and would part company in silence — leaving the row's leftward drain saying nothing about what is left to do.
  {
    const { orderLists } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'row-order',
      workgraph: { approval: 'auto', approving: [{ name: 'check success rate', check: 'success rate' }] },
      transformations: {
        merge: { feeder: { from: 'reco' }, packer: { size: 3 }, run: [0.3, 0.6], fail: 0, finalize: ['no input used twice'] },
        simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: ['no seed used twice'] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.3, 0.6], fail: 0, finalize: ['no input used twice'] },
      },
    });
    sim.start();
    const seen = [];
    const same = () => {
      const running = sim.runningLists();
      if (running.length < 2) return true;
      seen.push(running.map((l) => l.id).join('+'));
      const entries = running.map((l) => ({ id: l.id, key: l.key, label: l.label, items: l.items }));
      /* shuffled by the key, since insertion order is what the panel would otherwise hold them in */
      const shuffled = entries.slice().reverse();
      return orderLists(sim, shuffled).map((e) => `${e.id}:${e.key}`).join('+') === entries.map((e) => `${e.id}:${e.key}`).join('+');
    };
    let agree = true;
    let steps = 0;
    while (sim.wg.status !== 'Completed' && steps++ < 20000) {
      sim.step(0.05);
      if (sim.blocked) sim.forceAction();
      if (!same()) agree = false;
    }
    check(sim.wg.status === 'Completed', 'the run completes');
    check(seen.length > 0, 'and ran more than one list at a time: ' + seen.length + ' frames');
    check(agree, 'the row orders the lists as the engine runs them, whatever order they arrived in: ' + [...new Set(seen)].join(', '));
  }

  // the row is one row: what it has no room for is counted at its right end rather than clipped, since a panel that grew with
  // the run would shove the picture it is docked above
  {
    const { listsHtml, listsRow } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'row-fit',
      workgraph: { approval: 'auto' },
      transformations: {
        simulation: { feeder: { seeds: 8 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: ['no seed used twice'] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.3, 0.6], fail: 0, finalize: ['no input used twice'] },
        merge: { feeder: { from: 'reco' }, packer: { size: 3 }, run: [0.3, 0.6], fail: 0, finalize: ['no input used twice'] },
      },
    });
    sim.start();
    check(until(sim, () => sim.runningLists().length === 3, 40000), 'three lists run at once');
    const all = sim.runningLists();
    check(listsRow(sim, all, 3).includes('wgsim-lists-more') === false, 'with room for all three the row says nothing about a rest');
    const tight = listsRow(sim, all, 2);
    check((tight.match(/wgsim-lists-entity/g) || []).length === 2 && tight.includes('>+1</div>'), 'with room for two, two columns and a count of one: ' + (tight.match(/wgsim-lists-entity/g) || []).length);
    check(tight.indexOf('wgsim-lists-more') > tight.lastIndexOf('wgsim-lists-entity'), 'the count is at the right end of the row');
    check(listsHtml(sim, all, { fit: 1 }).includes('· 3 finalizing lists'), 'and the header counts every list, those past the end included, and names the kind they share');
    /* the heading holds what is left once the panel has said the rest: the entity, and the state it is in */
    const alike = listsRow(sim, all);
    check(!alike.includes('wgsim-dim">finalizing'), 'while every list is of one kind, no column repeats it under the header that says it');
    const mixed = [all[0], { id: all[1].id, key: 'clean', label: 'cleaning', items: sim.nodes[all[1].id].lists.clean }];
    check((listsRow(sim, mixed).match(/wgsim-dim">(finalizing|cleaning)/g) || []).length === 2, 'where two kinds run together, each column says its own');
    check(listsHtml(sim, mixed).includes('· 2 lists') && !listsHtml(sim, mixed).includes('2 finalizing lists'), 'and the header counts them without naming a kind they do not share');
  }

  // the retention rule rests on the engine calling a blocked list running: a blocked list has an action that never landed, so
  // a `runningList` that stopped mapping a blocked state to its list's key would take the failed action off the panel at the
  // moment the reader is being shown it
  {
    const { keptLists } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'blocked-list-kept',
      workgraph: { approval: 'auto' },
      transformations: { simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: [{ name: 'check', fail: 'once' }] } },
    });
    sim.start();
    check(until(sim, () => sim.nodes.simulation.status === 'FinalizingBlocked'), 'the finalizing check blocks the member');
    const shown = sim.runningLists().map((l) => ({ id: l.id, key: l.key, label: l.label, items: l.items }));
    check(shown.length === 1 && !shown[0].items.every((a) => a.result && a.result !== 'Failed'), 'its list has an action that did not land');
    check(keptLists(shown, sim.runningLists()).length === 1, 'and the panel keeps it all the same');
  }

  // a member's state machine: its states so far and every transition counted, the blocked counterpart as a box beside its state
  {
    const { nodeMachineHtml } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'member-machine',
      transformations: { simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0, finalize: [{ name: 'check', fail: 'once' }] } },
    });
    const node = sim.nodes.simulation;
    check(node.visited.join(',') === 'New' && Object.keys(node.statusTransitions).length === 0, 'a member starts in New with no transition');
    sim.start();
    sim.toggleNode('simulation');
    sim.toggleNode('simulation');
    check(node.statusTransitions['New>Active'] === 1 && node.statusTransitions['Active>Paused'] === 1 && node.statusTransitions['Paused>Active'] === 1, 'each transition is counted: ' + JSON.stringify(node.statusTransitions));
    check(until(sim, () => node.status === 'FinalizingBlocked'), 'the finalizing check blocks the member');
    let html = nodeMachineHtml(sim, node);
    check(html.includes('FinalizingBlocked: now') && html.includes('wg-m-on wg-m-blocked') && html.includes('>FinalizingBlocked<'), 'the blocked counterpart is lit, under Finalizing, with its full name');
    check(html.includes('Active → Paused: the operator, or the Active hook · 1 so far') && html.includes('Cleaned: not reached'), 'edges carry their counts and unreached states are faded');
    sim.forceAction('simulation');
    check(until(sim, () => node.status === 'Archived'), 'the member archives');
    html = nodeMachineHtml(sim, node);
    check(html.includes('Archived: now') && html.includes('FinalizingBlocked: passed') && node.statusTransitions['FinalizingBlocked>Finalizing'] === 1, 'the record of the run is on the picture');
  }

  // termination is the workgraph's: drain stops the outermost feeders and lets the rest finish, halt stops everything and
  // cancels what is in flight; both close through Finalizing to Completed, and a member has no such edge of its own
  {
    const { wgMachineHtml, nodeMachineHtml, parcelMachineHtml, detailsHtml, stateCard } = g.WorkgraphSim;
    const spec = {
      name: 'ending',
      transformations: {
        simulation: { feeder: { seeds: 60, batch: 20 }, packer: { size: 1 }, run: [0.6, 1.2], fail: 0, finalize: ['no seed used twice'] },
        reco: { feeder: { from: 'simulation' }, packer: { size: 2 }, run: [0.6, 1.2], fail: 0 },
      },
      outputs: { datasets: { from: 'reco' } },
    };
    const sim = new Sim(spec);
    check(!sim.drain() && !sim.halt(), 'neither is legal before the workgraph is Active');
    sim.start();
    check(until(sim, () => sim.nodes.simulation.parcels.some((p) => p.status === 'Assigned') && sim.nodes.reco.inputs.size > 0), 'work is in flight');
    let m = wgMachineHtml(sim);
    check(m.includes('>drain<') && m.includes('>halt<') && m.includes('data-act="drain"') && m.includes('data-act="halt"') && m.includes('data-act="cancel"') && m.includes('wg-m-btn danger'), 'the workgraph machine carries drain | halt on the edge to Finalizing and a danger cancel on the edge to Cancelling');
    const nm = nodeMachineHtml(sim, sim.nodes.simulation);
    check(!nm.includes('data-act="cancel"') && !nm.includes('data-act="drain"') && !nm.includes('data-act="halt"') && nm.includes('>pause<') && nm.includes('data-act="toggle-node"'), 'a member machine carries pause and no termination edge of its own');
    const nmOps = (nm.split('</svg>')[0].match(/wg-m-op/g) || []).length;
    check(nmOps === 6 && /<g class="wg-m-edge( zero)?"><title>Active → Cancelling/.test(nm) && /<g class="wg-m-edge( zero)?"><title>Paused → Cancelling/.test(nm), 'its cancelling edges are not operator edges any more; pause, resume, reopening and the three blocked edges are: ' + nmOps + ' dashed');
    sim.toggleNode('simulation');
    check(nodeMachineHtml(sim, sim.nodes.simulation).includes('>resume<') && !nodeMachineHtml(sim, sim.nodes.simulation).includes('>pause<'), 'paused, the button is resume');
    sim.toggleNode('simulation');
    const fed = sim.nodes.simulation.seedNext;
    const live = sim.nodes.simulation.parcels.filter((p) => !['Done', 'Failed', 'Cancelled', 'PartiallyDone'].includes(p.status)).length;
    check(sim.drain() && sim.wg.ending === 'drain' && !sim.nodes.simulation.feederEnabled && sim.nodes.reco.feederEnabled, 'drain stops the outermost feeder and leaves the edge feeder to finish');
    check(stateCard(sim).text.startsWith('draining') && !wgMachineHtml(sim).includes('data-act="drain"'), 'the line says draining and the machine offers it no more: ' + stateCard(sim).text);
    check(until(sim, () => sim.wg.status === 'Completed'), 'the workgraph completes');
    check(sim.nodes.simulation.seedNext === fed && sim.nodes.simulation.parcelTerminal.Done >= live && !sim.nodes.simulation.parcelTerminal.Cancelled && sim.outputs.datasets.files.length > 0, 'nothing more was fed, everything in flight finished, nothing was cancelled');

    const ab = new Sim(spec);
    ab.start();
    check(until(ab, () => ab.nodes.reco.parcels.some((p) => p.status === 'Assigned')), 'work is in flight downstream too');
    const running = Object.values(ab.nodes).reduce((n, x) => n + x.parcels.filter((p) => !['Done', 'Failed', 'Cancelled', 'PartiallyDone'].includes(p.status)).length, 0);
    const pool = ab.counts(ab.nodes.simulation).U;
    check(ab.halt() && ab.wg.ending === 'halt' && Object.values(ab.nodes).every((n) => n.halted && !n.feederEnabled), 'halt stops every feeder and marks every member');
    check(stateCard(ab).text.startsWith('halting') && ab.log.some((e) => e.text.includes(`${running} parcels in flight cancelled`)), 'the line says halting and the log counts the parcels cancelled: ' + stateCard(ab).text);
    check(until(ab, () => ab.wg.status === 'Completed'), 'the halted workgraph closes as its slots empty and completes');
    const t = Object.values(ab.nodes).reduce((acc, n) => ({ c: acc.c + (n.parcelTerminal.Cancelled || 0), d: acc.d + (n.parcelTerminal.Done || 0) }), { c: 0, d: 0 });
    check(t.c === running && ab.counts(ab.nodes.simulation).NP >= pool && ab.counts(ab.nodes.simulation).U === 0, `every parcel in flight was cancelled and the pool written off: ${t.c} cancelled`);
    check(ab.nodes.simulation.parcelTransitions['Completing>Cancelled'] >= 1 && ab.nodes.simulation.parcelTransitions['born>Unassigned'] > 0, 'the parcel machine counted the cancellations and the births');
    const pm = parcelMachineHtml(ab, ab.nodes.simulation);
    check(pm.includes('Completing → Cancelled') && pm.includes(`born ${ab.nodes.simulation.parcelTransitions['born>Unassigned']}`) && !pm.includes('wg-m-op') && pm.includes('boxes hold the occupancy'), 'the parcel machine draws the transitions, the births, and no operator edge');
    check(!pm.includes('born 0') && !g.WorkgraphSim.machineHtml(ab, ab.nodes.simulation).includes('born 0'), 'a zero is suppressed on the birth edges too');
    /* one scale across the three machines: every box the same size, every name the same class, on one canvas */
    const { MV, MB } = g.WorkgraphSim;
    const boxes = (h) => [...h.matchAll(/<rect x="[\d.]+" y="[\d.]+" width="([\d.]+)" height="([\d.]+)" rx="8" class="wg-m-box-bg"/g)].map((mm) => `${mm[1]}x${mm[2]}`);
    const one = `${MB.w}x${MB.h}`;
    for (const [name, h] of [['workgraph', m], ['member', nm], ['inputs', g.WorkgraphSim.machineHtml(ab, ab.nodes.simulation)], ['parcels', pm]]) {
      const b = boxes(h);
      check(b.length > 0 && b.every((x) => x === one) && h.includes(`viewBox="0 0 ${MV.w} ${MV.h}"`) && !/font-size/.test(h), `the ${name} machine draws every box at ${one} on the shared canvas: ${[...new Set(b)].join(',')}`);
    }
    check(m.includes('>blocked — a check failed, or a sign-off waits<') && nm.includes('>paused, or blocked on an action<') && nm.includes('>cancelled — the workgraph ended this early<'), 'each band carries its caption');
    /* the captions sit over their bands' leftmost boxes rather than at the picture's edge */
    const capX = (h, text) => Number(h.match(new RegExp(`<text x="([\\d.]+)" y="[\\d.]+" class="wg-m-band">${text}`))[1]);
    const boxX = (h, name) => Number(h.match(new RegExp(`<rect x="([\\d.]+)" y="[\\d.]+" width="[\\d.]+" height="[\\d.]+" rx="8" class="wg-m-box-bg"/><text x="[\\d.]+" y="[\\d.]+" text-anchor="middle" class="wg-m-name">${name}<`))[1]);
    check(capX(nm, 'paused') === boxX(nm, 'Paused') && capX(nm, 'cancelled') === boxX(nm, 'Cancelling') && capX(m, 'blocked') === boxX(m, 'ApprovingBlocked'), 'anchored to the leftmost box of the band');

    /* one dialog per entity: a strip of four panes, and the one showing has the body to itself */
    const d = detailsHtml(ab, ab.nodes.simulation);
    check(d.includes('data-tab="status"') && d.includes('data-tab="transformation"') && d.includes('data-tab="inputs"') && d.includes('data-tab="parcels"'), 'the dialog is a strip over status and the three machines');
    check(/class="wgsim-mtab on"[^>]*data-tab="status"/.test(d), 'and status is the one it opens on');
    check(d.includes('wgsim-cols-node') && !d.includes('wgsim-machine-region'), 'status is the two columns and no machine');
    const dm = detailsHtml(ab, ab.nodes.simulation, { machine: 'transformation' });
    check(dm.includes('wgsim-machine-region') && !dm.includes('wgsim-cols-node'), 'and a machine pane is the machine and no columns');
    check(/data-tab="inputs" role="button"/.test(d) && /data-tab="parcels" role="button"/.test(d), 'the sections that count what a machine holds are the way to that machine');
    check(/inputs <span class="wgsim-mtab-n">· \d+<\/span>/.test(d) && /parcels <span class="wgsim-mtab-n">· \d+<\/span>/.test(d) && !/transformation <span/.test(d) && !/status <span/.test(d), 'the tabs carry their counts, and a zero is suppressed');
    check(detailsHtml(ab, ab.nodes.simulation, { machine: 'parcels' }).includes('boxes hold the occupancy') && detailsHtml(ab, ab.nodes.simulation, { machine: 'inputs' }).includes('an edge an operator takes'), 'each tab shows its machine with its own legend');
    check(!d.includes('>disable<') && !d.includes('>flush<') && !d.includes('toggle-feeder') && !d.includes('data-act="flush"'), 'disable and flush are gone from the dialog');
    check(!d.includes('status report') && !d.includes('nothing yet'), 'a hook that never fired has no row');
    const wg = g.WorkgraphSim.workgraphHtml(ab);
    check(wg.includes('wgsim-machine-region') && wg.includes('wgsim-cols-wg') && wg.includes('wgsim-machine-legend') && !wg.includes('open-wgmachine') && !wg.includes('data-act="open-'), "the workgraph's dialog has the same shape, and its members table is no link");
  }

  // the rail: each row of each entity auto or manual on its own, mixed once one differs, work per row, and modes kept across a reset
  {
    const sim = new Sim({
      name: 'rails',
      transformations: {
        simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0 },
        reco: { feeder: { from: 'simulation' }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0 },
      },
    });
    check(sim.modeSummary() === 'auto', 'every row starts automatic');
    check(sim.setMode('reco', 'feeder', 'manual') && sim.modeSummary() === 'mixed' && sim.modeOf('reco', 'feeder') === 'manual' && sim.modeOf('simulation', 'feeder') === 'auto', 'one row by hand leaves the rest automatic');
    check(!sim.setMode('reco', 'feeder', 'manual') && !sim.setMode('reco', 'nothing', 'manual') && !sim.setMode('nobody', 'feeder', 'manual'), 'setting a row to what it is, or a row that does not exist, does nothing');
    sim.start();
    check(until(sim, () => sim.nodes.reco.unfed.length > 0), "files reach reco's edge");
    for (let i = 0; i < 40; i++) sim.step(0.05);
    check(sim.nodes.reco.inputs.size === 0 && sim.rowPending('reco', 'feeder') && !sim.rowPending('reco', 'packer'), "reco's manual feeder sweeps nothing on its own and has work; its packer has none yet");
    check(sim.pending().length === 0, 'a feeder by hand is not a step of the walkthrough');
    sim.runRow('reco', 'feeder');
    check(sim.nodes.reco.inputs.size > 0, 'run by hand, it feeds');
    sim.setAllModes('manual');
    check(sim.modeSummary() === 'manual' && sim.modeOf('workgraph', 'actions') === 'manual', 'the chip sets every row by hand');
    sim.setAllModes('auto');
    sim.setMode('simulation', 'packer', 'manual');
    sim.reset();
    check(sim.modeOf('simulation', 'packer') === 'manual' && sim.nodes.simulation.modes.packer === 'manual', 'a row keeps its mode across a reset');
    sim.setMode('simulation', 'packer', 'auto');
    sim.start();
    check(until(sim, () => sim.wg.status === 'Completed'), 'with every row automatic the run completes');
    check(sim.log.some((e) => e.subject === 'simulation' && e.text === 'packer automatic'), 'the log records the switch');
  }

  // the two hooks are two rows in the engine and one on the rail: a member whose HandleFailedInput and Active hook disagree
  // makes that rail row mixed, a click on it makes the row uniform, and the ▶ still appears while a part of it has work
  {
    const { railStates, railCells, RAIL_ORDER } = g.WorkgraphSim;
    const { MODE_ROWS } = g.WorkgraphSimEngine;
    const sim = new Sim({
      name: 'mixed-hooks',
      settings: { hookPeriod: 0 },
      transformations: { simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 1, retries: 4 } },
    });
    check(MODE_ROWS.node.join() === 'feeder,packer,failedInput,hooks,actions' && MODE_ROWS.workgraph.join() === 'hooks,actions', 'the engine sets five rows on a member and two on the workgraph: ' + MODE_ROWS.node.join());
    check(RAIL_ORDER.node.length === 4 && railCells('simulation', 'hooks').map((c) => c.row).join() === 'failedInput,hooks', 'the rail draws four, its hooks row covering both');
    check(railCells('workgraph', 'hooks').map((c) => c.row).join() === 'hooks', 'the workgraph has no failed inputs, so its pill covers its status hooks alone');
    check(sim.modeOf('simulation', 'failedInput') === 'manual' && sim.modeOf('simulation', 'hooks') === 'manual', 'a hook period of 0 starts both hook rows by hand');
    sim.setMode('simulation', 'hooks', 'auto');
    check(railStates(sim, 'simulation').hooks.mode === 'mixed' && railStates(sim, 'simulation').hooks.state === 'mixed', 'with one of the two automatic the rail row is mixed');
    check(railStates(sim, 'simulation').hooks.tip.includes('disagree'), 'and its tooltip says so: ' + railStates(sim, 'simulation').hooks.tip);
    sim.start();
    check(until(sim, () => sim.counts(sim.nodes.simulation).F > 0), 'an input fails and waits for HandleFailedInput');
    check(railStates(sim, 'simulation').hooks.state === 'run', 'the mixed row still offers the run while the part by hand has work');
    check(sim.rowPending('simulation', 'failedInput') && !sim.rowPending('simulation', 'hooks'), 'the work is HandleFailedInput\'s, not the Active hook\'s');
    check(sim.runRow('simulation', 'failedInput') && sim.counts(sim.nodes.simulation).F === 0, 'the row run by hand decides what waited');
    /* the row's click is one predictable move: mixed goes automatic, and automatic goes by hand */
    sim.setCellsUniform(railCells('simulation', 'hooks'));
    check(sim.modeOf('simulation', 'failedInput') === 'auto' && sim.modeOf('simulation', 'hooks') === 'auto', 'from mixed, the first click makes the row automatic');
    sim.setCellsUniform(railCells('simulation', 'hooks'));
    check(sim.modeOf('simulation', 'failedInput') === 'manual' && sim.modeOf('simulation', 'hooks') === 'manual', 'and the second takes the whole row by hand');
    check(sim.pending().length === 0 || sim.pending().every((s) => s.kind !== 'input') || sim.modeOf('simulation', 'failedInput') === 'manual', "the walkthrough follows HandleFailedInput's own row");
  }

  // the matrix of what runs by hand: a column per sweep, a row per entity, the workgraph first, a dash where it has no such
  // sweep, an aggregate on every label, and a count in the header band suppressed while nothing is by hand
  {
    const { modesHtml, scopeCells, MATRIX_SWEEPS, MODE_DOTS } = g.WorkgraphSim;
    const sim = new Sim({
      name: 'matrix',
      transformations: {
        simulation: { feeder: { seeds: 6 }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0 },
        reco: { feeder: { from: 'simulation' }, packer: { size: 1 }, run: [0.3, 0.6], fail: 0 },
      },
    });
    check(MATRIX_SWEEPS.map((c) => c.row).join() === 'feeder,packer,failedInput,hooks,actions', "HandleFailedInput comes before the status hooks, so the workgraph's dashes are one block");
    const html = () => modesHtml(sim);
    const rowOf = (h, id) => new RegExp(`<tr data-entity="${id}">[^]*?</tr>`).exec(h)[0];
    const wg = rowOf(html(), 'workgraph');
    check((wg.match(/wgsim-mx-dash/g) || []).length === 3 && /feeder"[^>]*wgsim-mx-dash|wgsim-mx-dash" data-sweep="feeder"/.test(wg.replace(/\n/g, '')), 'the workgraph has no feeder, no packer and no failed inputs');
    check(/data-sweep="feeder"[^]*data-sweep="packer"[^]*data-sweep="failedInput"[^]*data-sweep="hooks"/.test(wg) && wg.indexOf('wgsim-mx-mode') > wg.lastIndexOf('wgsim-mx-dash'), "the workgraph's three dashes are contiguous and come before its cells");
    check((html().match(/wgsim-mx-dash/g) || []).length === 3, 'one dash each and no more: every other cell is a mode');
    check(!/wgsim-mx-mode[^>]*>mixed</.test(html()) && (html().match(/class="wgsim-mx-mode" data-mode="(auto|manual)"/g) || []).length === 12, 'no cell shows a ternary: twelve cells, each auto or manual');
    check(html().indexOf('data-entity="workgraph"') < html().indexOf('data-entity="simulation"') && html().includes('wgsim-mx-gutter'), 'the workgraph is the first row, a gutter between it and the members');
    check(!/wgsim-modes-count/.test(html()), 'with nothing by hand the count in the header band is suppressed');
    sim.setMode('reco', 'packer', 'manual');
    check(/wgsim-modes-count[^>]*>1 of 12 manual</.test(html()), 'one row by hand is counted in the header band: ' + (/>(\d+ of \d+ manual)</.exec(html()) || [])[1]);
    /* the aggregate on each label: one dot, its fill the scale, and the swap that says what the click would do */
    const label = (h, attr) => new RegExp(`<button[^>]*data-act="mode-scope"[^>]*${attr}[^>]*>[^]*?</button>`).exec(h)[0];
    check(label(html(), 'data-row="packer"').includes('data-mode="mixed"') && label(html(), 'data-row="feeder"').includes('data-mode="auto"'), 'the packers column is half filled and the feeders column hollow');
    check(label(html(), 'data-scope="all"').includes('data-mode="mixed"') && label(html(), 'data-scope="all"').includes('→ all auto') && label(html(), 'data-scope="all"').includes('everything'), 'the corner is the global aggregate, named everything, and offers the move away from mixed');
    check(label(html(), 'data-row="feeder"').includes('→ all manual') && label(html(), 'data-row="feeder"').includes('wgsim-mx-shown'), 'a sweep that is wholly automatic offers to take it by hand, and carries both forms of its label');
    check(MODE_DOTS.map((d) => d[0]).join() === 'auto,mixed,manual' && MODE_DOTS.every(([m]) => html().includes(`>${m}</span>`) || html().includes(`${m}</span>`)), 'the footer reads the three fills');
    check(!/data-act="modes-auto"/.test(html()), 'and offers nothing that sets a mode: the corner is where the whole matrix is taken at once');
    check(scopeCells(sim, { kind: 'sweep', sweep: 'hooks' }).length === 3 && scopeCells(sim, { kind: 'entity', entity: 'workgraph' }).length === 2 && scopeCells(sim, { kind: 'all' }).length === 12, 'a sweep reaches the workgraph where it has that sweep, an entity every sweep of one');
    /* one predictable click: all automatic goes by hand, and anything else goes automatic */
    check(sim.setCellsUniform(scopeCells(sim, { kind: 'sweep', sweep: 'packer' })) === 'auto' && sim.modeOf('reco', 'packer') === 'auto', 'from mixed the first click goes automatic');
    check(sim.setCellsUniform(scopeCells(sim, { kind: 'sweep', sweep: 'packer' })) === 'manual' && sim.modeOf('simulation', 'packer') === 'manual', 'and the second by hand');
    check(sim.setCellsUniform(scopeCells(sim, { kind: 'sweep', sweep: 'packer' })) === 'auto' && sim.modeSummary() === 'auto', 'a scope that began uniform is where it started after two clicks');
    check(sim.setCellsUniform(scopeCells(sim, { kind: 'entity', entity: 'workgraph' })) === 'manual' && sim.modeTally(scopeCells(sim, { kind: 'entity', entity: 'workgraph' })).mode === 'manual' && sim.modeOf('simulation', 'hooks') === 'auto', 'an entity reaches the workgraph alone');
    check(sim.setCellsUniform(scopeCells(sim, { kind: 'all' })) === 'auto' && sim.modeSummary() === 'auto', 'and the corner reaches everything');
    /* the log says it once for the scope, not once a row */
    const before = sim.log.length;
    sim.setCellsUniform(scopeCells(sim, { kind: 'all' }));
    check(sim.log.length - before === 1 && sim.log[sim.log.length - 1].text === '12 rows by hand', 'a scope set at once is one line of the log, not one a row: ' + sim.log[sim.log.length - 1].text);
    sim.setAllModes('auto');
    check(sim.setMode('reco', 'failedInput', 'manual') && sim.log[sim.log.length - 1].text === 'HandleFailedInput by hand', 'the log calls a row by the name the reader knows it by: ' + sim.log[sim.log.length - 1].text);
    /* the matrix's own clicks say nothing: the reader is arranging the controls, not moving the model */
    const quiet = sim.log.length;
    sim.toggleMode('reco', 'feeder', true);
    sim.setCellsUniform(scopeCells(sim, { kind: 'sweep', sweep: 'packer' }), true);
    sim.setAllModes('auto', true);
    check(sim.log.length === quiet && sim.modeSummary() === 'auto', 'nothing the matrix sets reaches the log');
  }

  });
})(globalThis);
