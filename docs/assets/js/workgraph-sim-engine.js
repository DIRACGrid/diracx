/*
 * Workgraph simulator: the engine.
 *
 * A DOM-free model of the DiracX Transformation System as described in
 * DX-ADR-002 to DX-ADR-006: workgraphs of transformations, input pools,
 * feeders, packers, parcels, parcel outputs, failure handling, action lists
 * and the workgraph state machine. The renderer in workgraph-sim.js draws it;
 * the checks in tests/workgraph-sim drive it headlessly.
 */
(function (root) {
  'use strict';

  /* How many source identities a file can carry. The renderer gives each one a colour; the model only counts them, and the
     checks hold the two to the same number. */
  const IDENTITIES = 8;
  /* The file types a query returns, which the spec names and the renderer draws as shapes. */
  const SHAPES = ['circle', 'triangle', 'diamond', 'square'];
  /* Model seconds. `cancel` outlasts `action` on purpose: asking a backend to kill a job is slow, so the cleaning action that
     waits for it records Pending at least once before it is Done (DX-ADR-005, DX-ADR-006). */
  const T = { reserve: 0.25, completing: 0.35, cancel: 1.4, travel: 1.1, approve: 1.6, finalize: 1.2, watch: 1.5 }; /* watch: the action period while a list is watched */
  const LISTS = {
    finalize: { state: 'Finalizing', blocked: 'FinalizingBlocked', done: 'Finalized', label: 'finalizing', result: 'Passed' },
    archive: { state: 'Archiving', blocked: 'ArchivingBlocked', done: 'Archived', label: 'archiving', result: 'Done' },
    clean: { state: 'Cancelling', blocked: 'CancellingBlocked', done: 'Cleaned', label: 'cleaning', result: 'Done' },
  };
  const DEFAULT_ARCHIVE = ['clean intermediates', { name: 'clean database entries', effect: 'clean' }];
  const DEFAULT_CLEAN = [{ name: 'cancel in-flight parcels', effect: 'cancel' }, { name: 'remove output files', effect: 'removeOutputs' }, 'remove intermediate files', { name: 'clean database entries', effect: 'clean' }];
  /* Problematic is not terminal (DX-ADR-005): it waits for an operator, so a quarantine holds its member open until someone resets or writes it off. */
  const INPUT_TERMINAL = new Set(['Processed', 'Split', 'NotProcessed']);
  /* Every state a parcel can hold, in the order the dispatcher takes them, the terminal four last (DX-ADR-005). The renderer's
     slot styling and its legend are both driven by this list, so a state added here appears in the legend or fails the checks. */
  const PARCEL_STATES = ['Unassigned', 'Reserved', 'Assigned', 'Completing', 'Done', 'PartiallyDone', 'Failed', 'Cancelled'];
  const PARCEL_TERMINAL = new Set(['Done', 'PartiallyDone', 'Failed', 'Cancelled']);
  /* The most parcels a transformation may run at once. A limit of the spec rather than of the model, so that a card can always
     draw one box per slot and the picture never understates the capacity; the checks hold the drawing to it. */
  const MAX_SLOTS = 9;
  /* Where the operator's cancel is offered, and where a member may be paused or resumed (DX-ADR-005). */
  const CANCELLABLE = new Set(['New', 'Scouting', 'Approving', 'ApprovingBlocked', 'Active', 'Finalizing']);
  const RUNNING = new Set(['Scouting', 'Approving', 'ApprovingBlocked', 'Active']);
  /* Where the scout's sample is still the limit on what a feeder may yield: every state before the workgraph has been approved.
     Approving is inside it because approval is what releases the full query (DX-ADR-005) — a sign-off nobody has given must not,
     and the phase exists to be the place a person decides. `scout further` comes back to Scouting, so the ladder stays capped;
     from Active on the cap is gone for good, which is why this is a set of states and not a flag. */
  const BEFORE_APPROVAL = new Set(['New', 'Scouting', 'Approving', 'ApprovingBlocked']);
  /* The workgraph's happy path, which the state strip draws; WG_ALL_STATES adds the blocked counterpart and the cancelled tail,
     which only its machine draws. A transformation's states are its own (DX-ADR-005): no scouting, no approving. */
  const WG_STATES = ['New', 'Scouting', 'Approving', 'Active', 'Finalizing', 'Completed', 'Archiving', 'Archived'];
  const WG_ALL_STATES = ['New', 'Scouting', 'Approving', 'ApprovingBlocked', 'Active', 'Finalizing', 'Completed', 'Archiving', 'Archived', 'Cancelling', 'Cleaned'];
  const NODE_STATES = ['New', 'Active', 'Paused', 'Finalizing', 'FinalizingBlocked', 'Finalized', 'Completed', 'Archiving', 'ArchivingBlocked', 'Archived', 'Cancelling', 'CancellingBlocked', 'Cleaned'];
  /* Every result an action can carry, null for one that has not run. The renderer adds no state of its own: `Unsigned` is how it
     draws a Failed sign-off, a wait rather than a problem, and it is a treatment of Failed, not a result the engine records. */
  const ACTION_RESULTS = [null, 'Running', 'Pending', 'Passed', 'Done', 'Failed'];
  const DEFAULT_APPROVING = ['check success rate', 'estimate resource usage', { name: 'manual approval', manual: true }];
  /* Sweep periods in model seconds and the failure-rate multiplier; DX-ADR-001 tasks run on such periods. The hook period covers
     HandleFailedInput too, the action period every list. A period of 0 starts every row of that kind manual, and the default
     period is the cadence once a row is set back to auto. */
  /* verify: after every step, check that each input state's inbound minus outbound transitions equal its occupancy. Off by
     default, because it costs a pass over every input; the checks turn it on for every model they build. */
  const DEFAULT_SETTINGS = { feederPeriod: 1, packerPeriod: 0.5, hookPeriod: 1, actionPeriod: 1, failScale: 1, failOverride: null, verify: false };
  /* The sweeps a mode can be set on, in taxonomy order, each auto or manual on its own: a transformation has its feeder, its
     packer, its two hooks and its action lists; the workgraph has no feeder, no packer and no failed inputs, only its status
     hooks and its approving list. Manual rows sweep only by hand. HandleFailedInput and the status hooks are two rows because
     they are two hooks with nothing in common but their period: a reader who takes the failures by hand is rarely asking for
     the Active hook as well. The rail draws them as one row and reports `mixed` where they disagree. */
  const MODE_ROWS = { node: ['feeder', 'packer', 'failedInput', 'hooks', 'actions'], workgraph: ['hooks', 'actions'] };
  const ROW_PERIOD = { feeder: 'feederPeriod', packer: 'packerPeriod', failedInput: 'hookPeriod', hooks: 'hookPeriod', actions: 'actionPeriod' };
  /* What the log calls a row, which is the hook's own name where the key is not it. */
  const ROW_SAID = { feeder: 'feeder', packer: 'packer', failedInput: 'HandleFailedInput', hooks: 'hooks', actions: 'actions' };
  const INPUT_STATES = ['Unassigned', 'Assigned', 'Failed', 'Processed', 'Split', 'Problematic', 'NotProcessed'];
  const COUNT_KEY = { Unassigned: 'U', Assigned: 'A', Failed: 'F', Processed: 'P', Split: 'S', Problematic: 'Pb', NotProcessed: 'NP' };
  /* What HandleFailedInput may decide for an input (DX-ADR-006). */
  const DECISIONS = ['Unassigned', 'Split', 'Problematic'];
  /* The operator's own edges of the input machine (DX-ADR-005): which states an operator may move an input out of, and where
     to. The actions over a whole state, the one over a single input and every button that offers one read this, so none of
     them can come to offer an edge another does not. */
  const OPERATOR_EDGES = { Problematic: ['Unassigned', 'NotProcessed'], Unassigned: ['NotProcessed'] };
  /* The list a member runs while in a status, blocked or not; none while it is elsewhere. */
  const runningList = (status) => Object.keys(LISTS).find((k) => status === LISTS[k].state || status === LISTS[k].blocked) || null;
  /* One entity's action runner: its queue, the action that runs, and the one that failed (DX-ADR-006). */
  const runner = () => ({ queue: [], action: null, blocked: null });

  /* The entity an action belongs to, as the log's subject column names it. */
  const owner = (a) => (a.owner === 'wg' ? 'workgraph' : a.node.id);

  function mulberry32(seed) {
    let a = (seed >>> 0) || 1;
    return function () {
      a = (a + 0x6d2b79f5) | 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  const asArray = (v) => (v == null ? [] : Array.isArray(v) ? v : [v]);
  const hex4 = (rand) => Math.floor(rand() * 65536).toString(16).padStart(4, '0');
  const actionSpec = (a) => (typeof a === 'string' ? { name: a } : Object.assign({ name: 'action' }, a));
  /* What a seed feeder is asked for before anyone extends it: the scouting sample, or the first batch. */
  function initialRequest(spec, feeder) {
    if (!feeder.seeds) return 0;
    const sc = spec.workgraph.scouting;
    if (sc && feeder.batch) return Math.min(feeder.seeds, sc.kind === 'count' ? sc.stages[0] : 20);
    return feeder.batch ? Math.min(feeder.seeds, feeder.batch) : feeder.seeds;
  }

  /* ------------------------------------------------------------------ */
  /* Spec                                                                */
  /* ------------------------------------------------------------------ */

  function normalise(raw) {
    const spec = Object.assign({ name: 'workgraph', seed: 1, start: 'manual', slots: 4, workgraph: {} }, raw);
    spec.settings = Object.assign({}, DEFAULT_SETTINGS, raw.settings || {});
    spec.sources = {};
    for (const [id, s] of Object.entries(raw.sources || {})) {
      spec.sources[id] = Object.assign({ id, label: 'input query', files: 200, types: SHAPES.slice(0, 3), colours: 3, ancestors: null }, s);
    }
    spec.transformations = {};
    for (const [id, n] of Object.entries(raw.transformations || {})) {
      const kind = n.kind || 'compute';
      const node = Object.assign(
        {
          id,
          label: id,
          doc: null, /* one line about what the transformation runs, in the card's tooltip */
          kind,
          run: kind === 'compute' ? [1.6, 3.2] : [0.7, 1.4],
          fail: kind === 'compute' ? 0.08 : 0.02,
          partial: 0,
          submitFail: 0,
          subdivide: 0,
          pauseAbove: null,
          sections: 10,
          retries: 2,
          emit: 'parcel',
          output: null,
          hold: null,
          artifact: false,
          slots: spec.slots,
        },
        n
      );
      node.slots = Math.max(1, Math.min(MAX_SLOTS, node.slots));
      const f = n.feeder || {};
      node.feeder = { from: asArray(f.from), seeds: f.seeds || 0, batch: f.batch || null, inflight: f.inflight != null ? f.inflight : f.batch ? Math.ceil(f.batch / 2) : null, after: asArray(f.after), type: f.type || null, port: f.port || 'main' };
      node.packer = Object.assign({ size: 1, by: null, join: null, lookup: null }, n.packer || {});
      node.finalize = asArray(n.finalize).map(actionSpec);
      node.archive = asArray(n.archive != null ? n.archive : DEFAULT_ARCHIVE).map(actionSpec);
      node.clean = asArray(n.clean != null ? n.clean : DEFAULT_CLEAN).map(actionSpec);
      spec.transformations[id] = node;
    }
    spec.outputs = {};
    for (const [id, o] of Object.entries(raw.outputs || {})) {
      spec.outputs[id] = Object.assign({ id, label: id, show: 'files', port: 'main' }, o);
    }
    const wg = spec.workgraph;
    /* Scouting is a ladder: `stages` (or one `count`) of the sample, or one `fraction` of the query; `failAbove` and `minSeen`
       are the one failure threshold the ScoutingToApproving hook and the success-rate check both read. */
    if (wg.scouting) {
      const sc = wg.scouting;
      const stages = sc.stages ? asArray(sc.stages) : sc.fraction ? [sc.fraction] : [sc.count || 20];
      wg.scouting = { kind: sc.fraction ? 'fraction' : 'count', stages, failAbove: sc.failAbove != null ? sc.failAbove : 0.1, minSeen: sc.minSeen != null ? sc.minSeen : 10 };
    } else wg.scouting = null;
    wg.approval = wg.approval || 'manual';
    wg.archiveAfter = wg.archiveAfter != null ? wg.archiveAfter : 4;
    wg.target = wg.target && wg.target.output ? { output: wg.target.output, files: wg.target.files || 1 } : null;
    wg.approving = asArray(wg.approving != null ? wg.approving : wg.scouting ? DEFAULT_APPROVING : []).map(actionSpec);
    return spec;
  }

  /* ------------------------------------------------------------------ */
  /* Engine                                                              */
  /* ------------------------------------------------------------------ */

  class Sim {
    constructor(raw) {
      this.spec = normalise(raw);
      this.settings = Object.assign({}, this.spec.settings);
      /* Each row's mode outlives a reset, as the settings do: a reader who set a rail keeps it across runs. A period of 0 in the spec starts that row manual everywhere. */
      const initial = (rows) => Object.fromEntries(rows.map((row) => [row, this.spec.settings[ROW_PERIOD[row]] === 0 ? 'manual' : 'auto']));
      this.modes = { workgraph: initial(MODE_ROWS.workgraph), nodes: {} };
      for (const id of Object.keys(this.spec.transformations)) this.modes.nodes[id] = initial(MODE_ROWS.node);
      this.runs = 0;
      this.reset();
    }

    /* The mode of one row of an entity's rail, `workgraph` or a transformation id. */
    modeOf(id, row) {
      const m = id === 'workgraph' ? this.modes.workgraph : this.modes.nodes[id];
      return m ? m[row] : undefined;
    }

    /* `quiet` is for a caller that says one line for the whole scope it set, rather than one per row. */
    setMode(id, row, mode, quiet) {
      const m = id === 'workgraph' ? this.modes.workgraph : this.modes.nodes[id];
      if (!m || !(row in m) || (mode !== 'auto' && mode !== 'manual') || m[row] === mode) return false;
      m[row] = mode;
      if (!quiet) this.say(id, `${ROW_SAID[row]} ${mode === 'manual' ? 'by hand' : 'automatic'}`);
      return true;
    }

    toggleMode(id, row, quiet) {
      return this.setMode(id, row, this.modeOf(id, row) === 'manual' ? 'auto' : 'manual', quiet);
    }

    /* Every row of every entity at once: what leaving expert mode sets, so nothing is left stalled behind a hidden switch. */
    setAllModes(mode, quiet) {
      return this.setCells(this.modeCells(), mode, quiet);
    }

    /* A whole scope to one mode, with one line in the log for the scope rather than one per row: a rail row covers two where
       it covers the hooks, and a dozen lines would bury whatever the model said next. The subject is the entity where the
       scope has one, so the log's filter still reaches it. `quiet` says nothing at all, which is what the matrix of what runs
       by hand does: its clicks are the reader arranging the controls rather than the model doing anything. */
    setCells(cells, mode, quiet) {
      const changed = [];
      for (const c of cells) if (this.setMode(c.id, c.row, mode, true)) changed.push(c);
      if (quiet || !changed.length) return changed.length;
      const said = mode === 'manual' ? 'by hand' : 'automatic';
      if (changed.length === 1) this.say(changed[0].id, `${ROW_SAID[changed[0].row]} ${said}`);
      else this.say(changed.every((c) => c.id === changed[0].id) ? changed[0].id : 'workgraph', `${changed.length} rows ${said}`);
      return changed.length;
    }

    /* Every row of every entity as a flat list, the workgraph first and then the members in the order the graph declares
       them: the cells the matrix of what runs by hand is drawn from, and what every scope in it is a subset of. */
    modeCells() {
      const out = MODE_ROWS.workgraph.map((row) => ({ id: 'workgraph', row }));
      for (const id of Object.keys(this.modes.nodes)) for (const row of MODE_ROWS.node) out.push({ id, row });
      return out;
    }

    /* What a set of cells is as one thing: how many are by hand, out of how many, and `auto` when none is, `manual` when
       every one is, `mixed` between. An empty scope is `auto`, which is what a workgraph column of rows it does not have
       would otherwise have to be a case about. */
    modeTally(cells) {
      const manual = cells.reduce((n, c) => n + (this.modeOf(c.id, c.row) === 'manual' ? 1 : 0), 0);
      return { manual, total: cells.length, mode: manual === 0 ? 'auto' : manual === cells.length ? 'manual' : 'mixed' };
    }

    /* One predictable click on any aggregate: a scope that is entirely automatic goes by hand, and anything else — all by
       hand, or mixed — goes automatic. Never a cycle through three states, which would make the reader click once to find
       out where they are. `mixed` is reported and never set. */
    setCellsUniform(cells, quiet) {
      const mode = this.modeTally(cells).mode === 'auto' ? 'manual' : 'auto';
      this.setCells(cells, mode, quiet);
      return mode;
    }

    /* `manual` when every row is, `auto` when none is, else `mixed`. */
    modeSummary() {
      return this.modeTally(this.modeCells()).mode;
    }

    /* The cadence of a sweep: the spec's period, or the default where the spec said 0 to start the rows manual. */
    period(key) {
      return this.settings[key] > 0 ? this.settings[key] : DEFAULT_SETTINGS[key];
    }

    /* Whether a row's sweep would do anything now, which is what the rail's run button stands for: a feeder with
       something to feed, a packer with a ready input, a hook with an input in Failed or a decision to take, a list with
       an action next. A blocked action is not sweep work, and a list held back by an upstream member's is not runnable. */
    rowPending(id, row) {
      if (id === 'workgraph') {
        const w = this.wg.status;
        if (row === 'hooks') return w === 'Scouting' ? !!this.scoutPlan() : w === 'Active' ? !!this.wgActivePlan() : false;
        if (row === 'actions') {
          const next = w === 'Approving' || w === 'ApprovingBlocked' ? this.nextAction() : null;
          return !!next && next.why !== 'blocked' && next.why !== 'waiting';
        }
        return false;
      }
      const node = this.nodes[id];
      if (!node) return false;
      if (row === 'feeder') return this.feedable(node);
      if (row === 'packer') return this.packable(node);
      if (row === 'failedInput') {
        for (const i of node.inputs.values()) if (i.status === 'Failed') return true;
        return false;
      }
      if (row === 'hooks') return !!this.nodeActivePlan(node);
      if (row === 'actions') {
        if (!runningList(node.status)) return false;
        const next = this.nextAction(id);
        return !!next && next.why !== 'blocked' && next.why !== 'waiting';
      }
      return false;
    }

    /* A feeder with something to yield: files waiting on its edge, or seeds or query files below its limit. */
    feedable(node) {
      if (node.status !== 'Active' || !node.feederEnabled) return false;
      const f = node.spec.feeder;
      if (f.seeds) return node.seedNext < this.seedTarget(node);
      const srcs = f.from.filter((x) => this.sources[x]);
      if (srcs.length) return srcs.some((x) => (node.cursor[x] || 0) < this.scoutLimit(this.sources[x]));
      return node.unfed.length > 0;
    }

    /* A packer with a ready input in the pool. */
    packable(node) {
      if (node.status !== 'Active' || node.halted) return false;
      for (const i of node.inputs.values()) if (i.status === 'Unassigned' && this.ready(node, i)) return true;
      return false;
    }

    /* One sweep of one row by hand. */
    runRow(id, row) {
      if (row === 'feeder') return this.runFeeder(id);
      if (row === 'packer') return this.runPacker(id);
      if (row === 'failedInput') return this.runFailedInputs(id, 'hand');
      if (row === 'hooks') return this.runHooks(id);
      if (row === 'actions') return this.runAction(id === 'workgraph' ? undefined : id);
      return false;
    }

    reset() {
      const spec = this.spec;
      this.rand = mulberry32(spec.seed + this.runs);
      this.runs += 1;
      this.t = 0;
      this.seq = 0;
      this.logSeq = 0; /* bumped by every event, so a renderer knows how much of the log it has drawn */
      this.files = new Map();
      this.parcelIndex = new Map();
      this.tokens = [];
      /* The whole run, so that a line's number is the event's number and stays that: a run ends, and the
         noisiest model the documentation ships makes a couple of hundred lines. The renderer appends the
         lines it has not drawn rather than redrawing the log, so its length costs nothing per event. */
      this.log = [];
      this.totals = { done: 0, failed: 0, retries: 0, split: 0, deleted: 0, replicated: 0, problematic: 0 };
      this.sources = {};
      Object.values(spec.sources).forEach((s, si) => {
        const files = [];
        for (let i = 0; i < s.files; i++) {
          const f = this.newFile({
            colour: (si * 3 + Math.floor(this.rand() * s.colours)) % IDENTITIES,
            shape: s.types[Math.floor(this.rand() * s.types.length)],
            size: 1 + this.rand() * 4,
          });
          f.origin = f.id;
          files.push(f);
        }
        this.sources[s.id] = { id: s.id, spec: s, files };
      });
      for (const src of Object.values(this.sources)) {
        const and = src.spec.ancestors && this.sources[src.spec.ancestors];
        if (and) src.files.forEach((f, i) => (f.ancestor = and.files[i % and.files.length]));
      }
      this.nodes = {};
      Object.values(spec.transformations).forEach((n, ni) => {
        this.nodes[n.id] = {
          id: n.id,
          spec: n,
          index: ni,
          status: 'New',
          inputs: new Map(),
          byFile: new Map(),
          byOrigin: new Map(),
          joinRows: new Map(),
          unfed: [],
          parcels: [],
          cursor: {},
          nextFeed: 0,
          nextPack: 0,
          seedNext: 0,
          seedRequested: initialRequest(spec, n.feeder),
          decisions: { retried: 0, subdivided: 0, quarantined: 0, split: 0, success: 0, failure: 0 }, /* what the failure hook and the status report decided */
          transitions: {}, /* cumulative input transitions by "from>to" pair; births are "born>state", the cleaning list's removals "state>removed" */
          visited: ['New'], /* the member's own states so far, and its transitions by "from>to" pair, for its state machine */
          statusTransitions: {},
          parcelTransitions: {}, /* cumulative parcel transitions by "from>to" pair, births as "born>state", for the parcel machine */
          halted: false, /* set by the workgraph's halt: nothing is fed, packed or decided for it any more */
          parcelTerminal: {}, /* parcels that have reached each terminal state, kept when the parcel list is pruned */
          lastActive: null, /* the transformation's own Active hook: its last sweep, and what it changed */
          lastWgActive: null, /* what the workgraph's Active hook last did to this transformation */
          feederEnabled: true,
          hookBase: null, /* what the Active hook had already seen when this transformation last became Active */
          flushRequested: false,
          lastRetryAt: -9,
          lists: {},
          run: runner(), /* its action lists run here, one list at a time */
          modes: this.modes.nodes[n.id], /* the rows of its rail, shared with the sim so they outlive a reset */
        };
        const node = this.nodes[n.id];
        for (const key of Object.keys(LISTS)) node.lists[key] = n[key].map((a) => this.record('node', node, key, a));
      });
      this.wgList = spec.workgraph.approving.map((a) => this.record('wg', null, 'approving', a));
      this.outputs = {};
      for (const o of Object.values(spec.outputs)) this.outputs[o.id] = { id: o.id, spec: o, files: [] };
      this.consumers = {};
      for (const n of Object.values(this.nodes)) {
        for (const from of n.spec.feeder.from) {
          if (this.nodes[from]) (this.consumers[from] = this.consumers[from] || []).push(n.id);
        }
      }
      this.sinks = {};
      for (const o of Object.values(this.outputs)) (this.sinks[o.spec.from] = this.sinks[o.spec.from] || []).push(o.id);
      this.joiners = {};
      for (const n of Object.values(this.nodes)) {
        const j = n.spec.packer.join;
        if (j && this.nodes[j]) (this.joiners[j] = this.joiners[j] || []).push(n.id);
      }
      this.nextActionSweep = 0;
      this.watchActions = false; /* set by a renderer while a list is watched: the action sweep then runs no faster than T.watch, so each result can be read as it lands */
      const sc = spec.workgraph.scouting;
      this.wg = { status: 'New', since: 0, nextHook: 0, visited: ['New'], transitions: {}, hooks: {}, run: runner(), modes: this.modes.workgraph, scout: sc ? { stage: 0, stages: sc.stages.slice(), base: { processed: 0, failed: 0 } } : null, ending: null }; /* ending: how an operator is closing the workgraph, drain or halt, or null */ /* visited and transitions feed the state strip and the workgraph's machine; hooks holds each hook's last sweep */
      this.say('workgraph', '→ New', 'state', 'New');
      if (spec.start === 'auto') this.start();
    }

    /* Every file carries its provenance: the parcel that produced it, its parent files, and the inputs it became; and the
       model time it was made at, which is what a list of files sorts and ages by. */
    newFile(p) {
      const f = Object.assign({ id: ++this.seq, tag: hex4(this.rand), t: this.t, parents: [], producedBy: null, consumedBy: [] }, p);
      this.files.set(f.id, f);
      return f;
    }

    record(owner, node, list, a) {
      return { owner, node, list, name: a.name, manual: !!a.manual, check: a.check || null, fail: a.fail || false, merge: a.merge || null, effect: a.effect || null, result: null, attempts: 0, since: 0 };
    }

    /* One line of the event log: who it is about, what happened, and how loudly to say it. `state` is a
       transition of the workgraph or a member, which is the skeleton of a run; `running` is a moment its
       own result supersedes a beat later; everything else is ordinary work. `to` is the state the line
       reached, where it reached one, and `why` what brought it there, both as fields of their own so that
       a renderer draws them rather than reading them back out of the sentence. */
    say(subject, text, kind, to, why) {
      this.logSeq += 1;
      this.log.push({ seq: this.logSeq, t: this.t, subject, text, kind: kind || 'note', to: to || null, why: why || null });
    }

    setWg(status) {
      const key = `${this.wg.status}>${status}`;
      this.wg.transitions[key] = (this.wg.transitions[key] || 0) + 1;
      this.wg.status = status;
      this.wg.since = this.t;
      if (!this.wg.visited.includes(status)) this.wg.visited.push(status);
      /* `nextHook` only advances while the workgraph is in a state the hooks run in, so it sits in the past all through
         Approving and the first Active sweep would otherwise fire on the tick of the transition. A hook sweeps a period
         after the workgraph enters Active, like every sweep after it. */
      if (status === 'Active') this.wg.nextHook = this.t + this.period('hookPeriod');
      this.say('workgraph', `→ ${status}`, 'state', status);
    }

    setNode(node, status, why) {
      if (node.status !== status) {
        const key = `${node.status}>${status}`;
        node.statusTransitions[key] = (node.statusTransitions[key] || 0) + 1;
        if (!node.visited.includes(status)) node.visited.push(status);
      }
      node.status = status;
      this.say(node.id, `→ ${status}`, 'state', status, why);
    }

    /* Operator actions */

    start() {
      if (this.wg.status !== 'New') return;
      const scouting = !!this.spec.workgraph.scouting;
      this.setWg(scouting ? 'Scouting' : 'Active');
      for (const n of Object.values(this.nodes)) {
        const h = n.spec.hold;
        const held = h === 'operator' || (scouting && h === 'approval');
        this.setNode(n, held ? 'Paused' : 'Active', held ? 'held back' : 'workgraph started');
      }
    }

    /* The runner of an entity: the workgraph's for no id, a member's for its id. */
    runnerOf(id) {
      if (id == null || id === 'workgraph') return this.wg.run;
      const n = this.nodes[id];
      return n ? n.run : null;
    }

    runnerFor(a) {
      return a.owner === 'wg' ? this.wg.run : a.node.run;
    }

    /* The first blocked action anywhere: the workgraph's, then a member's in DAG order. */
    get blocked() {
      if (this.wg.run.blocked) return this.wg.run.blocked;
      for (const n of this.topoOrder()) if (n.run.blocked) return n.run.blocked;
      return null;
    }

    blockedOf(id) {
      if (id === undefined) return this.blocked;
      const r = this.runnerOf(id);
      return r ? r.blocked : null;
    }

    forceAction(id) {
      const a = this.blockedOf(id);
      if (!a) return;
      this.say(owner(a), `${a.name} forced to Passed by the operator`);
      this.unblock(a);
      this.finishAction(a, 'Passed');
      this.listsDone();
    }

    rerunAction(id) {
      const a = this.blockedOf(id);
      if (!a) return;
      this.say(owner(a), `${a.name} reset to run again`);
      this.unblock(a);
      a.since = this.t;
      a.attempts += 1;
      this.runnerFor(a).action = a;
    }

    /* The action an entity could run by hand: its blocked one, its running one, or the next in its queue, unless an
       upstream member's list still runs, which holds this member's back (DX-ADR-005). */
    nextAction(id) {
      const r = this.runnerOf(id);
      if (!r) return null;
      const node = r === this.wg.run ? null : this.nodes[id];
      if (r.blocked) return { action: r.blocked, why: 'blocked' };
      if (r.action) return { action: r.action, why: 'running' };
      if (!r.queue.length) return null;
      const on = node ? this.upstreamWaiting(node, r.queue[0].list) : [];
      return on.length ? { action: r.queue[0], why: 'waiting', on } : { action: r.queue[0], why: 'queued' };
    }

    /* Run that action to its end now: a blocked one is reset and tried again, a queued one started first. It is settled as a
       sweep would settle it, so a cancellation with parcels still in flight records Pending rather than passing. */
    runAction(id) {
      const next = this.nextAction(id);
      if (!next || next.why === 'waiting') return false;
      const r = this.runnerOf(id);
      const a = next.action;
      if (next.why === 'blocked') {
        this.say(owner(a), `${a.name} reset to run again by the operator`);
        this.unblock(a);
        a.attempts += 1;
        r.action = a;
      } else if (next.why === 'queued') this.startNext(r);
      this.settle(r, 'run by the operator');
      return true;
    }

    /* The operator closes an Active workgraph, at the workgraph level only (DX-ADR-005: there is no forced transition to
       Finalizing; the operator changes what the members do and the core takes the transition once the guard is met).
       Drain stops the outermost feeders and lets everything already in flight finish. */
    drain() {
      if (this.wg.status !== 'Active') return false;
      this.wg.ending = 'drain';
      const stopped = [];
      for (const n of Object.values(this.nodes)) {
        if (!n.feederEnabled || !this.externalFeeder(n)) continue;
        n.feederEnabled = false;
        stopped.push(n.id);
      }
      this.say('workgraph', `drained by the operator: ${stopped.length ? `the feeder of ${stopped.join(', ')} stopped` : 'no feeder left to stop'}, everything in flight finishes`);
      return true;
    }

    /* A halt stops every feeder, packer and hook, cancels the parcels in flight and writes off what was waiting, the quarantine
       included, so each member closes as its slots empty; the workgraph then finalises and completes as a drained one would. */
    halt() {
      if (this.wg.status !== 'Active') return false;
      this.wg.ending = 'halt';
      let cancelled = 0;
      for (const n of Object.values(this.nodes)) {
        n.feederEnabled = false;
        n.halted = true;
        n.unfed = [];
        cancelled += n.parcels.filter((p) => !PARCEL_TERMINAL.has(p.status)).length;
        this.cancelParcels(n);
        for (const i of n.inputs.values()) if (i.status === 'Failed' || i.status === 'Problematic') this.transition(n, i, 'NotProcessed');
      }
      this.tokens = this.tokens.filter((tk) => !this.nodes[tk.to] || !this.nodes[tk.to].halted);
      this.say('workgraph', `halted by the operator: every feeder, packer and hook stopped, ${cancelled} parcel${cancelled === 1 ? '' : 's'} in flight cancelled`);
      return true;
    }

    /* DX-ADR-005: the operator cancels from New and from every running state. */
    cancel() {
      if (!CANCELLABLE.has(this.wg.status)) return;
      this.clearRunner(this.wg.run);
      this.say('workgraph', 'cancelled by the operator');
      this.setWg('Cancelling');
      this.startLists('clean');
    }

    /* Approving back to Scouting (DX-ADR-005): scout further, never scout again, so the sample grows and everything it produced is kept. */
    extendScout() {
      const w = this.wg.status;
      const sc = this.spec.workgraph.scouting;
      if (!sc || (w !== 'Approving' && w !== 'ApprovingBlocked')) return;
      this.clearRunner(this.wg.run);
      for (const a of this.wgList) {
        a.result = null;
        a.attempts = 0;
      }
      const st = this.wg.scout;
      const last = st.stages[st.stages.length - 1];
      st.stages.push(sc.kind === 'fraction' ? Math.min(1, last * 2) : last * 2);
      this.startStage(st.stages.length - 1);
      this.setWg('Scouting');
      this.say('workgraph', `operator extended the scout to ${this.sampleText()}`);
    }

    /* Finalizing back to Active (DX-ADR-005): an operator who cannot recover a finalisation in place reopens the workgraph. */
    resumeFromFinalizing() {
      if (this.wg.status !== 'Finalizing') return;
      for (const n of Object.values(this.nodes)) {
        this.clearRunner(n.run);
        for (const a of n.lists.finalize) {
          a.result = null;
          a.attempts = 0;
        }
      }
      this.say('workgraph', 'operator reopened it after a failed finalisation');
      this.wg.ending = null;
      for (const n of Object.values(this.nodes)) n.halted = false;
      this.setWg('Active');
      for (const n of Object.values(this.nodes)) this.setNode(n, 'Active', 'workgraph to Active');
    }

    startNode(id) {
      const n = this.nodes[id];
      if (n && n.status === 'Paused') this.setNode(n, 'Active', 'operator');
    }

    /* An operator pauses or resumes a member: Active and Paused, while the workgraph is running (DX-ADR-005). */
    toggleNode(id) {
      const n = this.nodes[id];
      if (!n || !RUNNING.has(this.wg.status)) return;
      if (n.status === 'Active') this.setNode(n, 'Paused', 'operator');
      else if (n.status === 'Paused') this.setNode(n, 'Active', 'operator');
    }

    /* One sweep of a feeder, a packer, or the workgraph's own hooks (ScoutingToApproving, Active), as the periodic tasks or an operator would run them. */
    runFeeder(id) {
      const n = this.nodes[id];
      if (n && n.status === 'Active') {
        this.feed(n);
        this.say(n.id, 'feeder swept');
      }
    }

    runPacker(id) {
      const n = this.nodes[id];
      if (n && n.status === 'Active') {
        this.pack(n);
        this.say(n.id, 'packer swept');
      }
    }

    /* One sweep of the hooks: HandleFailedInput over each member's Failed inputs, then the hooks that run while the workgraph
       is in its current state, then every member's own Active hook (DX-ADR-006). Gated, a row set to manual is skipped, and
       HandleFailedInput and the status hooks are gated apart: they share a period and nothing else. */
    sweepHooks(gated) {
      if (this.wg.ending === 'halt') return; /* a halted workgraph decides nothing more */
      const on = (id, row) => !gated || this.modeOf(id, row) === 'auto';
      for (const n of Object.values(this.nodes)) if (on(n.id, 'failedInput')) this.runFailedInputs(n.id);
      if (on('workgraph', 'hooks')) this.sweepWgHooks();
      for (const n of Object.values(this.nodes)) if (on(n.id, 'hooks')) this.nodeActiveHook(n);
    }

    /* The workgraph's own hooks: ScoutingToApproving while it scouts, Active while it runs. */
    sweepWgHooks() {
      const w = this.wg.status;
      if (w === 'Scouting') {
        const plan = this.scoutPlan();
        for (const n of Object.values(this.nodes)) n.lastWgActive = { t: this.t, text: 'no change' };
        let text = 'no change';
        if (plan && plan.raise != null) {
          const st = this.wg.scout;
          this.startStage(plan.raise);
          text = `raised the scout to stage ${st.stage + 1} of ${st.stages.length}, ${this.sampleText()}`;
          this.say('workgraph', `ScoutingToApproving ${text}`);
        } else if (plan && plan.accept === 'failing') {
          text = `accepted early: ${this.rateText(plan.rate, 'this stage')}, so the success-rate check will fail`;
          this.say('workgraph', `ScoutingToApproving ${text}`);
        } else if (plan) text = `accepted: ${this.rateText(plan.rate)}`;
        this.wg.hooks.ScoutingToApproving = { t: this.t, text };
        if (plan && plan.accept) this.startApproving();
      } else if (w === 'Active') {
        this.wg.hooks.Active = { t: this.t, text: this.wgActiveHook() };
      }
    }

    /* The status hooks of one entity run by hand: the workgraph's own hooks, or a member's Active hook. HandleFailedInput is
       its own row and `runFailedInputs` is its sweep. */
    runHooks(id) {
      if (id === 'workgraph') {
        this.sweepWgHooks();
        this.say('workgraph', 'hooks swept');
        return true;
      }
      const n = this.nodes[id];
      if (!n) return false;
      this.nodeActiveHook(n);
      this.say(n.id, 'hooks swept');
      return true;
    }

    /* What a member's Active hook would decide now, without deciding it: `pause`, or nothing. */
    nodeActivePlan(node) {
      if (node.status !== 'Active') return null;
      const d = node.decisions;
      const base = node.hookBase || { success: d.success, failure: d.failure };
      const failure = d.failure - base.failure;
      const seen = d.success - base.success + failure;
      const limit = node.spec.pauseAbove;
      return limit != null && seen >= 4 && failure / seen > limit ? 'pause' : null;
    }

    /* A transformation's own Active hook (DX-ADR-006): it returns one operation for its transformation, here a pause once the
       failure rate is excessive (DX-ADR-005). It judges the transformation on what has happened since it was last started, so
       an operator who resumes one gets a fresh window rather than being overruled by the record that paused it. */
    nodeActiveHook(node) {
      if (node.status !== 'Active') {
        node.lastActive = { t: this.t, text: 'not running' };
        node.hookBase = null;
        return;
      }
      const d = node.decisions;
      if (!node.hookBase) node.hookBase = { success: d.success, failure: d.failure };
      const failure = d.failure - node.hookBase.failure;
      const seen = d.success - node.hookBase.success + failure;
      const limit = node.spec.pauseAbove;
      const pct = seen ? Math.round((100 * failure) / seen) : 0;
      if (limit != null && seen >= 4 && failure / seen > limit) {
        node.lastActive = { t: this.t, text: `paused it: ${pct}% of inputs failed` };
        this.setNode(node, 'Paused', `Active hook: ${pct}% of inputs failed`);
        return;
      }
      node.lastActive = { t: this.t, text: limit == null ? 'no change' : `no change · ${pct}% failed since it started` };
    }

    /* Operator decisions on quarantined inputs (DX-ADR-005): back to the pool, or written off. */
    resetProblematic(id) {
      const n = this.nodes[id];
      if (!n) return;
      let count = 0;
      for (const i of n.inputs.values()) {
        if (i.status !== 'Problematic') continue;
        this.transition(n, i, 'Unassigned');
        i.errors = 0;
        count++;
      }
      if (count) this.say(n.id, `operator reset ${count} Problematic input(s) to Unassigned`);
    }

    /* The third operator edge of the input machine (DX-ADR-005): Unassigned inputs written off as NotProcessed. */
    writeOffUnassigned(id) {
      const n = this.nodes[id];
      if (!n) return;
      let count = 0;
      for (const i of n.inputs.values()) {
        if (i.status !== 'Unassigned') continue;
        this.transition(n, i, 'NotProcessed');
        count++;
      }
      if (count) this.say(n.id, `operator wrote off ${count} Unassigned input(s) as NotProcessed`);
    }

    writeOffProblematic(id) {
      const n = this.nodes[id];
      if (!n) return;
      let count = 0;
      for (const i of n.inputs.values()) {
        if (i.status !== 'Problematic') continue;
        this.transition(n, i, 'NotProcessed');
        count++;
      }
      if (count) this.say(n.id, `operator wrote off ${count} Problematic input(s) as NotProcessed`);
    }

    /* The same edges taken on one input rather than on every input in its state: DX-ADR-005 does not care whether one input
       moves or a hundred, and a reader looking at one file wants that file moved and not its neighbours with it. */
    decideInput(nodeId, inputId, to) {
      const n = this.nodes[nodeId];
      const i = n && n.inputs.get(inputId);
      if (!i || !(OPERATOR_EDGES[i.status] || []).includes(to)) return false;
      this.transition(n, i, to);
      if (to === 'Unassigned') i.errors = 0;
      this.say(n.id, to === 'Unassigned' ? `operator reset input ${i.file.tag} to Unassigned` : `operator wrote off input ${i.file.tag} as NotProcessed`, null, to);
      return true;
    }

    /* FeederEnabled is cleared by the operator, the Active hook or exhaustion (DX-ADR-004), and can be set again for recovery: a workgraph taken from Finalizing back to Active needs its feeder on to pick up what was missed. */
    setFeederEnabled(id, enabled) {
      const n = this.nodes[id];
      if (!n || n.feederEnabled === enabled) return;
      n.feederEnabled = enabled;
      this.say(n.id, enabled ? 'feeder re-enabled by the operator' : 'feeder disabled by the operator');
    }

    /* A flush is an on-demand packer invocation with flush=True (DX-ADR-005). */
    flush(id) {
      const n = this.nodes[id];
      if (!n) return;
      n.flushRequested = true;
      this.say(n.id, 'flush requested');
      if (n.status === 'Active') this.pack(n);
    }

    /* Raised by the workgraph's Active hook on its sweep, or by an operator doing by hand what the hook would. */
    extendFeeder(id, by) {
      const n = this.nodes[id];
      if (!n || !n.spec.feeder.seeds) return;
      const f = n.spec.feeder;
      const before = n.seedRequested;
      n.seedRequested = Math.min(f.seeds, n.seedRequested + (f.batch || f.seeds));
      if (n.seedRequested === before) return;
      if (by === 'operator') this.say(n.id, `operator raised the feeder to ${n.seedRequested} seeds`);
      else this.say('workgraph', `Active hook raised ${n.id}'s feeder to ${n.seedRequested} seeds`);
    }

    /* Limits */

    /* The sample of the current stage: a count, or a fraction of the query. */
    scoutSample() {
      const st = this.wg.scout;
      return st ? st.stages[st.stage] : null;
    }

    sampleText() {
      const sc = this.spec.workgraph.scouting;
      const n = this.scoutSample();
      return sc.kind === 'fraction' ? `${Math.round(n * 100)}% of the query` : `a sample of ${n}`;
    }

    /* How far into a query a feeder may read, and how many seeds a seed feeder may yield: the scout's sample until the workgraph
       is approved, the whole of it after. The two read the sample from different places — a query has only its cursor against
       this limit, where a seed feeder also carries `seedRequested`, which the ScoutingToApproving and Active hooks raise — but
       both are capped by the same set of states, so neither runs past the sample while an approving action waits for a person. */
    scoutLimit(src) {
      const sc = this.spec.workgraph.scouting;
      if (!sc || !BEFORE_APPROVAL.has(this.wg.status)) return src.files.length;
      const n = this.scoutSample();
      if (sc.kind === 'fraction') return Math.min(src.files.length, Math.ceil(n * src.files.length));
      return Math.min(src.files.length, n);
    }

    seedTarget(node) {
      const sc = this.spec.workgraph.scouting;
      const seeds = node.spec.feeder.seeds;
      if (sc && BEFORE_APPROVAL.has(this.wg.status)) return Math.min(seeds, sc.kind === 'count' ? this.scoutSample() : 20);
      return Math.min(seeds, node.seedRequested);
    }

    /* The failure rate by input fate: the inputs set aside as Problematic over the inputs that have settled, Processed or
       Problematic, across every member. A retried input that later succeeds is no failure, so a broken payload trips it and
       a passing hiccup does not; an input still in flight counts neither way. It is what the ScoutingToApproving hook and
       the success-rate check both judge, against the one threshold the spec gives them, so the hook's early accept is a
       guarantee that the check fails. The check judges the whole scout: a Problematic input stays counted until an operator
       resets it or writes it off. The hook's early accept judges what settled in the current stage, under `stage`, so that
       a scout sent further after a block is judged on what the new stage produces rather than accepted again at once. */
    scoutRate() {
      let processed = 0;
      let failed = 0;
      for (const n of Object.values(this.nodes)) {
        const c = this.counts(n);
        processed += c.P;
        failed += c.Pb;
      }
      const seen = processed + failed;
      const b = this.wg.scout ? this.wg.scout.base : { processed: 0, failed: 0 };
      const sp = Math.max(0, processed - b.processed);
      const sf = Math.max(0, failed - b.failed);
      return { seen, failed, processed, rate: seen ? failed / seen : 0, stage: { seen: sp + sf, failed: sf, processed: sp, rate: sp + sf ? sf / (sp + sf) : 0 } };
    }

    /* A new stage judges its own window. */
    startStage(k) {
      const st = this.wg.scout;
      st.stage = k;
      const r = this.scoutRate();
      st.base = { processed: r.processed, failed: r.failed };
    }

    /* Whether the rate is over the threshold. The check judges whatever has settled; the hook's early accept, which cuts a
       scout short, needs the minimum sample as well. */
    rateBad(r) {
      const sc = this.spec.workgraph.scouting;
      return !!sc && r.seen > 0 && r.rate > sc.failAbove;
    }

    rateText(r, scope) {
      const sc = this.spec.workgraph.scouting;
      return `${r.failed} of ${r.seen} settled input${r.seen === 1 ? '' : 's'}${scope ? ` ${scope}` : ''} Problematic, ${this.rateBad(r) ? 'above' : 'within'} ${Math.round(sc.failAbove * 100)}%`;
    }

    /* What the ScoutingToApproving hook would do now, without doing it. The hook climbs the ladder: while the current stage
       still runs, nothing; once it has drained, the next stage, or accept after the last. It also accepts early, knowing the
       success-rate check will fail, once enough of the sample has failed: a scout in trouble then shows as ApprovingBlocked,
       the one place an operator looks, and a scout still climbing is plainly Scouting. */
    scoutPlan() {
      if (this.wg.status !== 'Scouting') return null;
      const r = this.scoutRate();
      if (r.stage.seen >= this.spec.workgraph.scouting.minSeen && this.rateBad(r.stage)) return { accept: 'failing', rate: r.stage };
      if (!this.scoutDone()) return null;
      const st = this.wg.scout;
      if (st.stage < st.stages.length - 1) return { raise: st.stage + 1 };
      return { accept: 'done', rate: r };
    }

    /* Where the scout is inside Scouting, for the line under the strip: the stage, the sample and the parcels so far. */
    scoutStatus() {
      const st = this.wg.scout;
      const r = this.scoutRate();
      const stage = st.stages.length > 1 ? `stage ${st.stage + 1} of ${st.stages.length} · ` : '';
      return `${stage}${this.sampleText()} · ${r.stage.processed} processed, ${r.stage.failed} Problematic`;
    }

    /* Whether any external feeder is still enabled: with none, the workgraph is draining, a condition inside Active rather than a state of its own. */
    feedersActive() {
      return Object.values(this.nodes).some((n) => n.feederEnabled && this.externalFeeder(n));
    }

    /* The feeder still has something to yield. */
    feederActive(node) {
      if (!node.feederEnabled) return false;
      if (node.unfed.length) return true;
      const f = node.spec.feeder;
      if (f.seeds) {
        if (this.wg.status === 'Scouting') {
          if (node.seedNext < this.seedTarget(node)) return true;
        } else if (node.seedNext < f.seeds) return true;
      }
      for (const from of f.from) {
        const src = this.sources[from];
        if (src) {
          if ((node.cursor[from] || 0) < this.scoutLimit(src)) return true;
          continue;
        }
        const up = this.nodes[from];
        if (up && !this.drained(up)) return true;
      }
      if (this.tokens.some((tk) => tk.to === node.id)) return true;
      return false;
    }

    liveInputs(node, quarantineAside) {
      for (const i of node.inputs.values()) if (!INPUT_TERMINAL.has(i.status) && !(quarantineAside && i.status === 'Problematic')) return true;
      return false;
    }

    liveParcels(node) {
      return node.parcels.some((p) => !PARCEL_TERMINAL.has(p.status));
    }

    /* DX-ADR-005: feeder disabled, and nothing non-terminal, Problematic included. Paused does not drain a member: pausing changes nothing about what is ultimately done, so a paused member whose feeder still has work holds the workgraph in Active. */
    drained(node) {
      if (this.liveInputs(node) || this.liveParcels(node)) return false;
      if (this.tokens.some((tk) => tk.to === node.id)) return false;
      if (this.feederActive(node)) return false;
      return true;
    }

    /* Everything but the quarantine has settled: only Problematic inputs keep the member from draining, and only an operator can move them. */
    waitsOnOperator(node) {
      let quarantined = 0;
      for (const i of node.inputs.values()) {
        if (i.status === 'Problematic') quarantined++;
        else if (!INPUT_TERMINAL.has(i.status)) return false;
      }
      if (!quarantined || this.liveParcels(node)) return false;
      if (this.tokens.some((tk) => tk.to === node.id)) return false;
      if (this.feederActive(node)) return false;
      return true;
    }

    /* The members holding an Active workgraph open on something only a person can release (DX-ADR-005): a quarantine nobody has
       decided, or a member paused while its feeder still has work. A member held back for approval is not one of them, since the
       approval releases it, which is why the list is empty outside Active. */
    heldMembers() {
      if (this.wg.status !== 'Active') return [];
      return Object.values(this.nodes).filter((n) => (n.status === 'Paused' ? !this.drained(n) : this.waitsOnOperator(n)));
    }

    /* The scout's stage has drained: every active member has nothing live, quarantine aside, since a Problematic input waits
       for an operator and would otherwise hold the ladder for ever, and the sample has produced something. */
    scoutDone() {
      if (this.t - this.wg.since < 1) return false;
      let processed = 0;
      for (const n of Object.values(this.nodes)) {
        if (n.status === 'Active') {
          if (this.liveInputs(n, true) || this.liveParcels(n) || n.unfed.length || this.tokens.some((tk) => tk.to === n.id)) return false;
          if (n.feederEnabled && this.feederActive(n)) return false;
        }
        const c = this.counts(n);
        processed += c.P + c.Pb;
      }
      /* something settled: processed, or set aside, which is also an answer about the payload */
      return processed > 0;
    }

    externalFeeder(node) {
      const f = node.spec.feeder;
      return !!f.seeds || f.from.some((x) => this.sources[x]);
    }

    allDrained() {
      for (const n of Object.values(this.nodes)) if (!this.drained(n)) return false;
      return true;
    }

    /* The workgraph's Active hook (DX-ADR-006): disable the external feeders once the target output is reached, otherwise raise a batched feeder by one batch whenever its work in flight has dropped. */
    wgActiveHook() {
      const plan = this.wgActivePlan();
      for (const n of Object.values(this.nodes)) n.lastWgActive = { t: this.t, text: 'no change' };
      if (!plan) return 'no change';
      if (plan.disable) {
        for (const id of plan.disable) {
          this.nodes[id].feederEnabled = false;
          this.nodes[id].lastWgActive.text = 'disabled the feeder';
        }
        this.say('workgraph', `Active hook disabled the feeder of ${plan.disable.join(', ')}: ${plan.out.files.length} files in ${plan.target.output} reached the target of ${plan.target.files}`);
        return `disabled the feeder of ${plan.disable.join(', ')}`;
      }
      const raised = [];
      for (const id of plan.raise) {
        const n = this.nodes[id];
        const before = n.seedRequested;
        this.extendFeeder(n.id);
        if (n.seedRequested !== before) {
          n.lastWgActive.text = `raised the limit to ${n.seedRequested}`;
          raised.push(`${n.id} to ${n.seedRequested}`);
        }
      }
      return raised.length ? `raised ${raised.join(', ')}` : 'no change';
    }

    /* What the workgraph's Active hook would do now, without doing it: the feeders it would disable once the target is
       reached, or the batched feeders it would raise; nothing when a sweep would change nothing. */
    wgActivePlan() {
      const tg = this.spec.workgraph.target;
      const out = tg && this.outputs[tg.output];
      const nodes = Object.values(this.nodes);
      if (out && out.files.length >= tg.files) {
        const disable = nodes.filter((n) => n.feederEnabled && this.externalFeeder(n)).map((n) => n.id);
        return disable.length ? { disable, out, target: tg } : null;
      }
      const raise = nodes
        .filter((n) => {
          const f = n.spec.feeder;
          return n.status === 'Active' && n.feederEnabled && f.seeds && f.batch && n.seedRequested < f.seeds && this.inFlight(n) < f.inflight;
        })
        .map((n) => n.id);
      return raise.length ? { raise } : null;
    }

    /* Work in flight below a feeder: what it has been asked for but not issued, plus every live input of the compute members downstream of it, so a stopped member keeps the count up. */
    inFlight(node) {
      let total = Math.max(0, this.seedTarget(node) - node.seedNext);
      const seen = new Set();
      const stack = [node.id];
      while (stack.length) {
        const id = stack.pop();
        if (seen.has(id)) continue;
        seen.add(id);
        const n = this.nodes[id];
        if (n.spec.kind !== 'compute') continue;
        const c = this.counts(n);
        total += c.U + c.A + c.F + n.unfed.length;
        for (const consumer of this.consumers[id] || []) stack.push(consumer);
      }
      total += this.tokens.filter((tk) => seen.has(tk.to)).length;
      return total;
    }

    /* Actions (DX-ADR-006): each entity's lists run in order, one action per sweep of the action task, and a member's
       list waits for the lists of the members it depends on (DX-ADR-005). */

    queueActions(r, list) {
      for (const a of list) {
        a.result = null;
        a.attempts = 0;
        r.queue.push(a);
      }
    }

    clearRunner(r) {
      r.queue = [];
      r.action = null;
      r.blocked = null;
    }

    /* Every member enters the list's state in one transaction; one with nothing to run is done at once. */
    startLists(key) {
      for (const n of this.topoOrder()) {
        this.setNode(n, LISTS[key].state);
        this.clearRunner(n.run);
        this.queueActions(n.run, n.lists[key]);
        if (n.lists[key].length === 0) this.setNode(n, LISTS[key].done);
      }
    }

    /* The members a member depends on: its producers, the ones it waits for, and its joining partner. */
    depsOf(node) {
      return [...node.spec.feeder.from, ...node.spec.feeder.after, node.spec.packer.join].filter((d) => d && this.nodes[d]).map((d) => this.nodes[d]);
    }

    /* The upstream members whose list of this kind has not finished, which hold this member's back. */
    upstreamWaiting(node, key) {
      return this.depsOf(node).filter((d) => d.status !== LISTS[key].done);
    }

    startNext(r) {
      const next = r.queue.shift();
      next.since = this.t;
      next.attempts += 1;
      next.result = 'Running';
      r.action = next;
      if (next.effect === 'cancel') this.cancelParcels(next.node);
      this.say(owner(next), `running ${next.name}`, 'running');
    }

    /* Settle the running action. A sign-off nobody has given fails, so the workgraph blocks and an operator forces it
       passed, which is the sign-off, or resets it. DX-ADR-006: an action whose answer is not knowable yet records
       Pending, leaves the entity where it is, and is tried again. */
    settle(r, why) {
      const a = r.action;
      if (!a) return;
      if (a.manual) {
        if (this.spec.workgraph.approval !== 'auto') this.block(a, 'no sign-off yet');
        return;
      }
      if (a.effect === 'cancel' && this.liveParcels(a.node)) {
        if (a.result !== 'Pending') {
          a.result = 'Pending';
          this.say(a.node.id, `${a.name} → Pending (waiting for the backends)`, null, 'Pending');
        }
        return;
      }
      if (a.check === 'success rate' && this.spec.workgraph.scouting) {
        const r = this.scoutRate();
        if (this.rateBad(r)) this.block(a, this.rateText(r));
        else this.finishAction(a, 'Passed', this.rateText(r));
        return;
      }
      const fails = a.fail === true || (a.fail === 'once' && a.attempts === 1);
      if (fails) this.block(a);
      else this.finishAction(a, 'Passed', why);
    }

    /* One sweep of the action task: every entity with a list to run settles its running action and starts its next,
       upstream members first, so a list that finishes in this sweep lets the lists below it start in the same one. */
    sweepActions(gated) {
      const on = (id) => !gated || this.modeOf(id, 'actions') === 'auto';
      const w = this.wg.status;
      if ((w === 'Approving' || w === 'ApprovingBlocked') && on('workgraph')) this.sweepRunner(this.wg.run);
      for (const n of this.topoOrder()) {
        const key = runningList(n.status);
        if (key && on(n.id)) this.sweepRunner(n.run, n, key);
      }
    }

    sweepRunner(r, node, key) {
      if (r.blocked) return;
      if (r.action) this.settle(r);
      if (!r.action && !r.blocked && r.queue.length && (!node || !this.upstreamWaiting(node, key).length)) this.startNext(r);
    }

    /* The checks' stand-in for the person who signs off: with approval "auto" a manual action passes by itself after a moment,
       whether or not the action task sweeps. No model the documentation ships carries it — a sign-off there waits for its
       reader, and `parseSpec` refuses the key — so the only specs it is set on are the ones the checks build. */
    signOffs() {
      const a = this.wg.run.action;
      if (a && a.manual && this.spec.workgraph.approval === 'auto' && this.t - a.since > T.approve) this.finishAction(a, 'Passed', 'signed off');
    }

    /* The action lists running now, the workgraph's first and then the members' upstream first: each with its items, the
       entity's status and what its runner is at. A list with nothing in it is not running. */
    runningLists() {
      const out = [];
      const w = this.wg.status;
      if ((w === 'Approving' || w === 'ApprovingBlocked') && this.wgList.length) out.push({ id: 'workgraph', key: 'approving', label: 'approving', items: this.wgList, status: w, next: this.nextAction() });
      for (const n of this.topoOrder()) {
        const key = runningList(n.status);
        if (!key || !n.lists[key].length) continue;
        out.push({ id: n.id, key, label: LISTS[key].label, items: n.lists[key], status: n.status, next: this.nextAction(n.id) });
      }
      return out;
    }

    /* What waits for the reader while rows are by hand, in the order a walk through it takes: the inputs in Failed of
       each member whose HandleFailedInput is by hand, then the action each entity whose actions are by hand could run. */
    pending() {
      const out = [];
      for (const n of Object.values(this.nodes)) {
        if (this.modeOf(n.id, 'failedInput') !== 'manual') continue;
        for (const i of n.inputs.values()) if (i.status === 'Failed') out.push({ kind: 'input', node: n, input: i });
      }
      const w = this.wg.status;
      if ((w === 'Approving' || w === 'ApprovingBlocked') && this.modeOf('workgraph', 'actions') === 'manual') {
        const next = this.nextAction();
        if (next) out.push(Object.assign({ kind: 'action', node: null }, next));
      }
      for (const n of this.topoOrder()) {
        if (!runningList(n.status) || this.modeOf(n.id, 'actions') !== 'manual') continue;
        const next = this.nextAction(n.id);
        if (next && next.why !== 'waiting') out.push(Object.assign({ kind: 'action', node: n }, next));
      }
      return out;
    }

    /* DX-ADR-005: a parcel the backend has cannot simply be dropped, so cancellation goes through Completing; one that was never
       submitted has nothing to ask and goes straight to Cancelled. */
    cancelParcels(node) {
      for (const p of node.parcels) {
        if (PARCEL_TERMINAL.has(p.status)) continue;
        p.cancelling = true;
        if (p.status === 'Unassigned') this.endParcel(node, p, 'Cancelled');
        else if (p.status !== 'Completing') this.setParcel(node, p, 'Completing');
      }
      for (const i of node.inputs.values()) if (i.status === 'Unassigned') this.transition(node, i, 'NotProcessed');
    }

    /* Every change of a parcel's status, counted by "from>to" pair for the parcel machine. */
    setParcel(node, p, status) {
      const key = `${p.status}>${status}`;
      node.parcelTransitions[key] = (node.parcelTransitions[key] || 0) + 1;
      p.status = status;
      p.since = this.t;
    }

    /* A parcel reaches a terminal state: its inputs are released and the transformation's cumulative counter is kept, so pruning the list never loses the history. */
    endParcel(node, p, status) {
      this.setParcel(node, p, status);
      p.ended = this.t;
      node.parcelTerminal[status] = (node.parcelTerminal[status] || 0) + 1;
      if (status === 'Cancelled') for (const i of p.inputs) if (i.status === 'Assigned') this.transition(node, i, 'NotProcessed');
    }

    block(a, why) {
      const r = this.runnerFor(a);
      r.blocked = a;
      r.action = null;
      a.result = 'Failed';
      this.say(owner(a), `${a.name} → Failed${why ? ` (${why})` : ''}`, null, 'Failed');
      if (a.owner === 'wg') this.setWg('ApprovingBlocked');
      else this.setNode(a.node, LISTS[a.list].blocked, 'action failed');
      /* A blocked state covers two situations, and which one it is cannot be read back later: the run has moved on by the
         time a line is. A sign-off nobody has given is a wait; any other failed action is a problem (DX-ADR-005). */
      this.log[this.log.length - 1].manual = !!a.manual;
    }

    unblock(a) {
      this.runnerFor(a).blocked = null;
      a.result = 'Running';
      if (a.owner === 'wg') this.setWg('Approving');
      else this.setNode(a.node, LISTS[a.list].state);
    }

    finishAction(a, result, why) {
      const r = this.runnerFor(a);
      r.action = null;
      if (a.owner === 'node' && result === 'Passed') result = LISTS[a.list].result;
      a.result = result;
      const who = owner(a);
      this.say(who, `${a.name} → ${result}${why ? ' (' + why + ')' : ''}`, null, result);
      if (a.effect === 'clean' && a.node) {
        for (const p of this.parcelIndex.values()) if (p.node === a.node.id) this.parcelIndex.delete(p.id);
        for (const i of a.node.inputs.values()) this.transition(a.node, i, 'removed');
        /* the rows go and the history stays: a removal is recorded as an outbound transition, for parcels as for inputs, so the
           machines' edges still hold what happened while their boxes hold nothing. Nothing draws a `removed` box, and no edge
           reads the key; it is what keeps the counters reconcilable with the occupancy once a list has emptied a member. */
        const removed = this.parcelCounts(a.node);
        for (const st of PARCEL_STATES) if (removed[st]) a.node.parcelTransitions[`${st}>removed`] = (a.node.parcelTransitions[`${st}>removed`] || 0) + removed[st];
        a.node.inputs.clear();
        a.node.parcels = [];
        a.node.parcelTerminal = {};
        a.node.joinRows.clear();
      }
      if (a.effect === 'removeOutputs' && a.node) for (const o of this.sinks[a.node.id] || []) this.outputs[o].files = [];
      const out = a.merge && this.outputs[a.merge];
      if (out && out.files.length > 1) {
        const f = out.files[0];
        const size = Math.min(5, out.files.reduce((acc, x) => acc + x.size, 0));
        out.files = [this.newFile({ colour: f.colour, shape: f.shape, size, origin: f.origin, producer: who, port: f.port, merged: out.files.length })];
        this.say(who, `merged ${out.files[0].merged} files into ${out.files[0].tag}`);
      }
      if (a.owner === 'node' && !r.queue.length) this.setNode(a.node, LISTS[a.list].done);
    }

    /* The workgraph moves on once every list of the state has run: a cheap check, made every step. */
    listsDone() {
      const w = this.wg.status;
      const r = this.wg.run;
      const nodes = Object.values(this.nodes);
      const settled = this.t - this.wg.since > T.finalize;
      if (w === 'Approving') {
        if (!r.action && !r.blocked && !r.queue.length) this.activate();
      } else if (w === 'Finalizing' && settled && nodes.every((n) => n.status === 'Finalized')) {
        this.setWg('Completed');
        for (const n of nodes) this.setNode(n, 'Completed');
      } else if (w === 'Archiving' && settled && nodes.every((n) => n.status === 'Archived')) {
        this.setWg('Archived');
      } else if (w === 'Cancelling' && settled && nodes.every((n) => n.status === 'Cleaned')) {
        this.setWg('Cleaned');
      }
    }

    startApproving() {
      this.setWg('Approving');
      this.clearRunner(this.wg.run);
      this.queueActions(this.wg.run, this.wgList);
    }

    startArchiving() {
      this.setWg('Archiving');
      this.startLists('archive');
    }

    activate() {
      this.setWg('Active');
      for (const n of Object.values(this.nodes)) {
        if (n.status === 'Paused' && n.spec.hold === 'approval') this.setNode(n, 'Active', 'approved');
      }
    }

    /* Members in a linearised order of the DAG, upstream first. */
    topoOrder() {
      const order = [];
      const placed = new Set();
      const ids = Object.keys(this.nodes);
      let guard = 0;
      while (order.length < ids.length && guard++ < 200) {
        for (const id of ids) {
          if (placed.has(id)) continue;
          const n = this.nodes[id];
          if (this.depsOf(n).every((d) => placed.has(d.id))) {
            placed.add(id);
            order.push(n);
          }
        }
      }
      for (const id of ids) if (!placed.has(id)) order.push(this.nodes[id]);
      return order;
    }

    startFinalizing() {
      this.setWg('Finalizing');
      this.startLists('finalize');
    }

    /* Time */

    step(dt) {
      this.t += dt;
      const arrived = [];
      this.tokens = this.tokens.filter((tk) => {
        if (this.t >= tk.t1) {
          arrived.push(tk);
          return false;
        }
        return true;
      });
      for (const tk of arrived) if (tk.arrive) tk.arrive();

      const w = this.wg;
      if ((w.status === 'Scouting' || w.status === 'Active') && this.t >= w.nextHook) {
        w.nextHook = this.t + this.period('hookPeriod');
        this.sweepHooks(true);
      }
      if (w.status === 'Approving' || w.status === 'ApprovingBlocked' || w.status === 'Finalizing' || w.status === 'Archiving' || w.status === 'Cancelling') {
        if (this.t >= this.nextActionSweep) {
          this.nextActionSweep = this.t + (this.watchActions ? Math.max(this.period('actionPeriod'), T.watch) : this.period('actionPeriod'));
          this.sweepActions(true);
        }
        this.signOffs();
        this.listsDone();
      } else if (w.status === 'Active') {
        if (this.t - w.since > 1 && this.allDrained()) this.startFinalizing();
      } else if (w.status === 'Completed') {
        if (this.t - w.since > this.spec.workgraph.archiveAfter) this.startArchiving();
      }

      for (const node of Object.values(this.nodes)) {
        if (node.status === 'Active') {
          if (this.t >= node.nextFeed) {
            node.nextFeed = this.t + this.period('feederPeriod');
            if (node.modes.feeder === 'auto' && !node.halted) this.feed(node);
          }
          if (this.t >= node.nextPack) {
            node.nextPack = this.t + this.period('packerPeriod');
            if (node.modes.packer === 'auto' && !node.halted) this.pack(node);
          }
          this.dispatch(node);
          /* No source in the model is open-ended, so a feeder with nothing left to yield has nothing more coming and the core clears FeederEnabled here. A real feeder says so itself, by yielding NoMoreInputs (DX-ADR-006). */
          if (node.feederEnabled && w.status === 'Active' && !this.feederActive(node)) {
            node.feederEnabled = false;
            this.say(node.id, 'feeder reported exhaustion, disabled');
          }
        }
        this.advanceParcels(node);
      }
      if (this.settings.verify) {
        for (const node of Object.values(this.nodes)) {
          const bad = this.verifyTransitions(node);
          if (bad.length) throw new Error(`input transitions do not reconcile: ${bad.join('; ')}`);
        }
      }
    }

    /* Every input state change passes through here, so the (from, to) counters reconcile with the occupancy (DX-ADR-005). */
    transition(node, input, to) {
      const from = input.status;
      if (from === to) return;
      input.status = to;
      const key = `${from}>${to}`;
      node.transitions[key] = (node.transitions[key] || 0) + 1;
    }

    /* Inbound minus outbound against the occupancy, per state; empty when they reconcile. */
    verifyTransitions(node) {
      const c = this.counts(node);
      const bad = [];
      for (const st of INPUT_STATES) {
        let net = 0;
        for (const [key, n] of Object.entries(node.transitions)) {
          const [from, to] = key.split('>');
          if (to === st) net += n;
          if (from === st) net -= n;
        }
        if (net !== c[COUNT_KEY[st]]) bad.push(`${node.id}: ${st} has ${c[COUNT_KEY[st]]} but the transitions net ${net}`);
      }
      return bad;
    }

    /* Sections an input covers, and the mask that names them; a whole file carries no mask, which is what the pictures and the feeder's tally read. */
    sections(input) {
      return input.hi - input.lo + 1;
    }

    addInput(node, file, extra) {
      if (node.halted) return null; /* nothing is fed to a halted member: a file arriving late is left where it is */
      const all = node.spec.sections;
      const input = Object.assign({ id: ++this.seq, tag: hex4(this.rand), file, status: 'Unassigned', errors: 0, lo: 1, hi: all, parent: null, attempts: [] }, extra || {});
      input.portion = this.sections(input) / all;
      input.mask = input.lo === 1 && input.hi === all ? null : `${input.lo}-${input.hi}`;
      node.inputs.set(input.id, input);
      const born = `born>${input.status}`;
      node.transitions[born] = (node.transitions[born] || 0) + 1;
      file.consumedBy.push({ node: node.id, input: input.id });
      if (!node.byFile.has(file.id)) node.byFile.set(file.id, input);
      if (!node.byOrigin.has(file.origin)) node.byOrigin.set(file.origin, input);
      return input;
    }

    /* A feeder invocation yields everything it owes, in one burst. */
    feed(node) {
      if (!node.feederEnabled) return;
      const f = node.spec.feeder;
      /* the edge feeder: rows recorded by the producers since the last sweep */
      while (node.unfed.length) this.addInput(node, node.unfed.shift());
      if (f.seeds) {
        const target = this.seedTarget(node);
        while (node.seedNext < target) {
          const i = node.seedNext++;
          const file = this.newFile({ colour: (node.index * 3 + Math.floor(this.rand() * 3)) % IDENTITIES, shape: SHAPES[Math.floor(this.rand() * 3)], size: 1 + this.rand() * 4, seed: i, producer: node.id });
          file.origin = file.id;
          this.addInput(node, file, { seed: i });
        }
      }
      for (const from of f.from) {
        const src = this.sources[from];
        if (!src) continue;
        const limit = this.scoutLimit(src);
        while ((node.cursor[from] || 0) < limit) {
          const idx = node.cursor[from] || 0;
          node.cursor[from] = idx + 1;
          const file = src.files[idx];
          this.travel(from, node.id, file, () => this.addInput(node, file));
        }
      }
    }

    /* The partner a joining or lookup packer adds to the parcel, if it exists yet. `taken` counts the rows this sweep has already
       promised per origin, so two inputs of one origin never claim the same joined row. */
    partner(node, input, taken) {
      const pk = node.spec.packer;
      if (pk.lookup) return input.file.ancestor || null;
      if (pk.join) {
        const rows = node.joinRows.get(input.file.origin) || [];
        const k = taken ? taken.get(input.file.origin) || 0 : 0;
        return rows.length > k ? rows[k] : null;
      }
      return null;
    }

    /* The joined edge will never produce a partner for this input: the row it would have come from was written off
       (DX-ADR-004), or the joined transformation has drained with nothing left to send. A Problematic row is not lost
       yet: the operator may still reset it, so the input waits. `taken` is what this
       packer sweep has already promised, so the second input of an origin with one row left is lost while the first is not. */
    partnerLost(node, input, taken) {
      const b = node.spec.packer.join && this.nodes[node.spec.packer.join];
      if (!b) return false;
      const rows = node.joinRows.get(input.file.origin) || [];
      if (rows.length > (taken ? taken.get(input.file.origin) || 0 : 0)) return false;
      const bi = b.byOrigin.get(input.file.origin);
      if (bi && bi.status === 'NotProcessed') return true;
      return this.drained(b) && !this.tokens.some((tk) => tk.to === node.id && tk.kind === 'join');
    }

    /* Packer readiness (DelayedUntil): Unassigned, the partner exists, and every `after` transformation that receives the file has processed it. */
    ready(node, input, taken) {
      if (input.status !== 'Unassigned') return false;
      const files = [input.file];
      if (node.spec.packer.join || node.spec.packer.lookup) {
        const partner = this.partner(node, input, taken);
        if (!partner) return false;
        files.push(partner);
      }
      const after = node.spec.feeder.after;
      if (after.length === 0) return true;
      let seen = 0;
      for (const f of files) {
        for (const a of after) {
          const other = this.nodes[a];
          const oi = other && other.byFile.get(f.id);
          if (!oi) continue;
          seen++;
          if (oi.status !== 'Processed') return false;
        }
      }
      return seen > 0;
    }

    delayed(node, input) {
      return input.status === 'Unassigned' && !this.ready(node, input);
    }

    /* The packer packs every ready group at once; parcels then queue as Unassigned for the dispatcher. */
    pack(node) {
      const size = node.spec.packer.size;
      const by = node.spec.packer.by;
      const flush = node.flushRequested || !this.feederActive(node);
      node.flushRequested = false;
      const groups = new Map();
      const taken = new Map(); /* joined rows this sweep has promised, by origin */
      for (const i of node.inputs.values()) {
        if (i.status === 'Unassigned' && this.partnerLost(node, i, taken)) {
          this.transition(node, i, 'Problematic');
          this.totals.problematic += 1;
          this.say(node.id, `input ${i.file.tag} → Problematic (partner will never be produced)`, null, 'Problematic');
          continue;
        }
        const lost = i.status === 'Unassigned' && this.afterLost(node, i);
        if (lost) {
          this.transition(node, i, 'Problematic');
          this.totals.problematic += 1;
          this.say(node.id, `input ${i.file.tag} → Problematic (its consumer left it ${lost})`, null, 'Problematic');
          continue;
        }
        if (!this.ready(node, i, taken)) continue;
        if (node.spec.packer.join) taken.set(i.file.origin, (taken.get(i.file.origin) || 0) + 1);
        const k = by === 'type' ? i.file.shape : by === 'colour' ? i.file.colour : by === 'origin' ? i.file.origin : '*';
        if (!groups.has(k)) groups.set(k, []);
        groups.get(k).push(i);
      }
      for (const list of groups.values()) {
        while (list.length >= size || (flush && list.length > 0)) this.createParcel(node, list.splice(0, size));
      }
    }

    /* The dispatcher claims queued parcels while the backend has capacity (DX-ADR-003). */
    dispatch(node) {
      let running = node.parcels.filter((p) => p.status === 'Reserved' || p.status === 'Assigned' || p.status === 'Completing').length;
      for (const p of node.parcels) {
        if (running >= node.spec.slots) break;
        if (p.status !== 'Unassigned') continue;
        this.setParcel(node, p, 'Reserved');
        running++;
      }
    }

    createParcel(node, inputs) {
      const [lo, hi] = node.spec.run;
      const extras = [];
      for (const i of inputs) {
        /* a joining packer consumes one row per input, so two inputs of one origin take two different partners */
        const partner = node.spec.packer.join ? (node.joinRows.get(i.file.origin) || []).shift() : this.partner(node, i);
        if (partner) extras.push(partner);
      }
      const parcel = { id: ++this.seq, tag: hex4(this.rand), node: node.id, inputs, extras, outputs: [], status: 'Unassigned', since: this.t, dur: lo + this.rand() * (hi - lo), ended: null, recovery: false, cancelling: false, outcome: null };
      this.parcelIndex.set(parcel.id, parcel);
      node.parcelTransitions['born>Unassigned'] = (node.parcelTransitions['born>Unassigned'] || 0) + 1;
      for (const i of inputs) {
        this.transition(node, i, 'Assigned');
        i.parcel = parcel.id;
        i.attempts.push(parcel.id);
      }
      node.parcels.push(parcel);
      if (node.parcels.length > 400) {
        node.parcels = node.parcels.filter((p, idx) => idx >= node.parcels.length - 40 || !PARCEL_TERMINAL.has(p.status));
      }
      return parcel;
    }

    /* The parcel machine of DX-ADR-005. A submission that fails returns the parcel to Unassigned for the dispatcher to try
       again; a backend failure is reported on Assigned and needs no Completing step; everything else, cancellation included,
       records its outcome while Completing. The list is walked by index because a recovery parcel is appended as we go. */
    advanceParcels(node) {
      const list = node.parcels;
      for (let k = 0, n = list.length; k < n; k++) {
        const p = list[k];
        if (p.status === 'Reserved' && this.t - p.since >= T.reserve) {
          if (this.rand() < node.spec.submitFail) {
            this.setParcel(node, p, 'Unassigned');
            this.say(node.id, `parcel ${p.tag} was not accepted, back to Unassigned for the dispatcher`);
          } else this.setParcel(node, p, 'Assigned');
        } else if (p.status === 'Assigned' && this.t - p.since >= p.dur) {
          const outcome = this.outcomeOf(node);
          if (outcome === 'Failed') {
            this.endParcel(node, p, 'Failed');
            this.failParcel(node, p);
          } else {
            p.outcome = outcome;
            this.setParcel(node, p, 'Completing');
          }
        } else if (p.status === 'Completing' && this.t - p.since >= (p.cancelling ? T.cancel : T.completing)) {
          if (p.cancelling) this.endParcel(node, p, 'Cancelled');
          else this.finish(node, p);
        }
      }
    }

    outcomeOf(node) {
      const s = node.spec;
      const r = this.rand();
      const fail = this.settings.failOverride != null ? this.settings.failOverride : Math.min(1, s.fail * this.settings.failScale);
      return r < fail ? 'Failed' : r < fail + s.partial ? 'PartiallyDone' : 'Done';
    }

    /* A failed parcel returns its partners to the pool of joined rows; each input goes to Failed, where HandleFailedInput
       finds it. The hook's decision is drawn now and applied when the hook runs, so a run is the same whether the hook
       sweeps or is run by hand. */
    failParcel(node, p) {
      const s = node.spec;
      this.totals.failed += 1;
      node.decisions.failure += p.inputs.length;
      if (s.packer.join) {
        /* the joined rows are still there for the next parcel to find */
        for (const e of p.extras) {
          const rows = node.joinRows.get(e.origin) || [];
          rows.unshift(e);
          node.joinRows.set(e.origin, rows);
        }
      }
      for (const i of p.inputs) {
        this.transition(node, i, 'Failed');
        i.errors += 1;
        i.decision = this.hookDecision(node, i);
      }
    }

    /* The HandleFailedInput hook (DX-ADR-006) decides one input: back to the pool, replaced by finer-grained children, or quarantined. */
    hookDecision(node, i) {
      const s = node.spec;
      if (s.subdivide && this.rand() < s.subdivide && this.sections(i) >= 2) return 'Split';
      return i.errors <= s.retries ? 'Unassigned' : 'Problematic';
    }

    /* The hook's reason, in words. */
    decisionWhy(node, i, to) {
      const s = node.spec;
      const n = `${i.errors} failure${i.errors === 1 ? '' : 's'}`;
      if (to === 'Split') return `${n}; replaced by two smaller inputs rather than retried, which the hook does with probability ${s.subdivide}`;
      if (to === 'Unassigned') return `${n}, of the ${s.retries} the hook retries`;
      return `${n}, more than the ${s.retries} the hook retries`;
    }

    /* Apply a decision to an input in Failed: the hook's own, or one an operator takes in the hook's place. */
    applyDecision(node, i, to, who) {
      if (i.status !== 'Failed' || !DECISIONS.includes(to)) return false;
      if (to === 'Split' && this.sections(i) < 2) return false;
      i.decision = null;
      const by = who === 'operator' ? " (the operator, in the hook's place)" : '';
      if (to === 'Split') {
        this.subdivide(node, i, by);
      } else if (to === 'Unassigned') {
        this.transition(node, i, 'Unassigned');
        this.totals.retries += 1;
        node.decisions.retried += 1;
        node.lastRetryAt = this.t;
        if (who) this.say(node.id, `input ${i.file.tag} → Unassigned, retry ${i.errors} of ${node.spec.retries}${by}`, null, 'Unassigned');
      } else {
        this.transition(node, i, 'Problematic');
        this.totals.problematic += 1;
        node.decisions.quarantined += 1;
        this.say(node.id, `input ${i.file.tag} → Problematic after ${i.errors} failures${by}`, null, 'Problematic');
      }
      return true;
    }

    /* The hook over every input in Failed, of one member or of all: its sweep, or an operator running it by hand. */
    runFailedInputs(id, who) {
      const nodes = id != null ? [this.nodes[id]].filter(Boolean) : Object.values(this.nodes);
      let n = 0;
      for (const node of nodes) {
        for (const i of [...node.inputs.values()]) {
          if (i.status !== 'Failed') continue;
          if (this.applyDecision(node, i, i.decision || this.hookDecision(node, i), who)) n += 1;
        }
      }
      return n;
    }

    /* One input in Failed: the hook's decision, or the operator's choice in its place. */
    decideFailedInput(id, inputId, choice) {
      const node = this.nodes[id];
      const i = node && node.inputs.get(inputId);
      if (!i) return false;
      return this.applyDecision(node, i, choice || i.decision || this.hookDecision(node, i), choice ? 'operator' : 'hand');
    }

    /* Failed → Split: the hook replaces the input with two smaller ones that carry on in its place, each with its own error count. */
    subdivide(node, i, by) {
      this.transition(node, i, 'Split');
      const mid = i.lo + Math.floor(this.sections(i) / 2) - 1;
      this.addInput(node, i.file, { lo: i.lo, hi: mid, parent: i.id, status: 'Unassigned' });
      this.addInput(node, i.file, { lo: mid + 1, hi: i.hi, parent: i.id, status: 'Unassigned' });
      this.totals.split += 1;
      node.decisions.subdivided += 1;
      this.say(node.id, `input ${i.file.tag} subdivided into ${i.lo}-${mid} and ${mid + 1}-${i.hi} after ${i.errors} failures${by || ''}`);
    }

    finish(node, p) {
      const s = node.spec;
      const outcome = p.outcome || 'Done';
      this.endParcel(node, p, outcome);
      if (outcome === 'Done') {
        this.totals.done += 1;
        node.decisions.success += p.inputs.length;
        for (const i of p.inputs) this.transition(node, i, 'Processed');
        if (s.kind === 'removal') this.totals.deleted += p.inputs.length;
        if (s.kind === 'replication') this.totals.replicated += p.inputs.length;
        this.emit(node, p.inputs, p);
        return;
      }
      /* PartiallyDone: the status report says which sections finished, so the input is replaced by a Processed child and an Unassigned remainder. */
      this.totals.split += p.inputs.length;
      node.decisions.split += p.inputs.length;
      const recovered = [];
      for (const i of p.inputs) {
        this.transition(node, i, 'Split');
        const n = this.sections(i);
        const k = 1 + Math.floor(this.rand() * Math.max(1, n - 1));
        const a = this.addInput(node, i.file, { lo: i.lo, hi: i.lo + k - 1, parent: i.id, status: 'Processed' });
        if (k < n) this.addInput(node, i.file, { lo: i.lo + k, hi: i.hi, parent: i.id, status: 'Unassigned', errors: i.errors });
        recovered.push(a);
      }
      const rec = { id: ++this.seq, tag: hex4(this.rand), node: node.id, inputs: recovered, extras: [], outputs: [], status: 'Done', since: this.t, dur: 0, ended: this.t, recovery: true, cancelling: false, outcome: 'Done' };
      node.parcelTransitions['born>Done'] = (node.parcelTransitions['born>Done'] || 0) + 1;
      this.parcelIndex.set(rec.id, rec);
      node.parcelTerminal.Done = (node.parcelTerminal.Done || 0) + 1;
      for (const i of recovered) {
        i.parcel = rec.id;
        i.attempts.push(rec.id);
      }
      node.parcels.push(rec);
      this.say(node.id, `parcel ${p.tag} PartiallyDone, ${p.inputs.length} input(s) split`);
      this.emit(node, recovered, rec);
    }

    /* Done parcels of compute transformations record their outputs; every consumer of an output picks them up (DX-ADR-004). */
    emit(node, inputs, parcel) {
      const s = node.spec;
      if (s.kind !== 'compute' || inputs.length === 0) return;
      const files = [];
      const provenance = (group) => ({ parents: group.map((i) => i.file.id), producedBy: parcel ? parcel.id : null });
      const mk = (group) => {
        const first = group[0].file;
        const size = group.reduce((acc, i) => acc + i.file.size * i.portion, 0) * 0.8;
        files.push(this.newFile(Object.assign({ colour: first.colour, shape: s.output || first.shape, size: Math.max(0.3, Math.min(5, size)), origin: first.origin, producer: node.id }, provenance(group))));
      };
      if (s.emit === 'input') inputs.forEach((i) => mk([i]));
      else mk(inputs);
      if (s.artifact) {
        const first = inputs[0].file;
        files.push(this.newFile(Object.assign({ colour: first.colour, shape: 'square', size: 0.4, origin: first.origin, producer: node.id, port: 'artifact' }, provenance(inputs))));
      }
      if (parcel) for (const f of files) parcel.outputs.push(f.id);
      for (const file of files) {
        const port = file.port || 'main';
        for (const c of this.consumers[node.id] || []) {
          const consumer = this.nodes[c];
          if (consumer.spec.feeder.port !== port) continue;
          if (consumer.spec.feeder.type && consumer.spec.feeder.type !== file.shape) continue;
          this.travel(node.id, c, file, () => consumer.unfed.push(file));
        }
        if (port === 'main') {
          for (const j of this.joiners[node.id] || []) {
            const joiner = this.nodes[j];
            this.travel(node.id, j, file, () => {
              const rows = joiner.joinRows.get(file.origin) || [];
              rows.push(file);
              joiner.joinRows.set(file.origin, rows);
            }, 'join');
          }
        }
        for (const o of this.sinks[node.id] || []) {
          const out = this.outputs[o];
          if (out.spec.port !== port) continue;
          this.travel(node.id, o, file, () => out.files.push(file));
        }
      }
    }

    travel(from, to, file, arrive, kind) {
      this.tokens.push({ from, to, file, t0: this.t, t1: this.t + T.travel, arrive, kind: kind || 'edge' });
    }

    /* Provenance around a focus, by generation: the parcel that produced the file is one generation up and its inputs'
       files two, a parcel that consumed the file one generation down and its outputs two. Provenance is walked up only
       and consumers down only, so what is left of the focus is where it came from and what is right of it is what it
       became. Each side is walked to a depth in generations of provenance, a parcel and its files, or as far as it goes,
       up to a cap on the nodes. */
    lineageAround(fileId, opts) {
      const o = Object.assign({ ancestorDepth: Infinity, descendantDepth: Infinity, cap: 90 }, opts || {});
      const files = new Map();
      const parcels = new Map();
      const gen = new Map();
      const edges = [];
      const edgeKeys = new Set();
      const edge = (from, to, kind) => {
        const k = `${from}>${to}>${kind}`;
        if (edgeKeys.has(k)) return;
        edgeKeys.add(k);
        edges.push({ from, to, kind });
      };
      let truncated = false;
      const full = () => files.size + parcels.size >= o.cap;
      const place = (key, g) => {
        if (!gen.has(key)) gen.set(key, g);
      };
      const addParcel = (pid, g) => {
        const p = this.parcelIndex.get(pid);
        if (!p) return null;
        if (!parcels.has(pid)) parcels.set(pid, p);
        place(`p:${pid}`, g);
        return p;
      };
      const addFile = (f, g) => {
        if (!files.has(f.id)) files.set(f.id, f);
        place(`f:${f.id}`, g);
      };
      const queue = [{ id: fileId, g: 0, dir: 0 }];
      const seen = new Set();
      while (queue.length) {
        const { id, g, dir } = queue.shift();
        if (seen.has(id)) continue;
        seen.add(id);
        const f = this.files.get(id);
        if (!f) continue;
        if (full()) {
          truncated = true;
          break;
        }
        addFile(f, g);
        /* up: the producing parcel and its inputs' files */
        if (dir <= 0 && -g < 2 * o.ancestorDepth) {
          const prod = f.producedBy && addParcel(f.producedBy, g - 1);
          if (prod) {
            edge(`p:${prod.id}`, `f:${id}`, 'out');
            for (const i of prod.inputs) {
              edge(`f:${i.file.id}`, `p:${prod.id}`, 'in');
              queue.push({ id: i.file.id, g: g - 2, dir: -1 });
            }
            for (const x of prod.extras) {
              addFile(x, g - 2);
              edge(`f:${x.id}`, `p:${prod.id}`, 'partner');
            }
          }
        }
        /* down: every parcel this file's inputs were in, failed attempts included, and their outputs */
        if (dir >= 0 && g < 2 * o.descendantDepth) {
          for (const c of f.consumedBy) {
            const node = this.nodes[c.node];
            const input = node && node.inputs.get(c.input);
            const attempts = input ? input.attempts : [];
            for (const pid of attempts) {
              const p = addParcel(pid, g + 1);
              if (!p) continue;
              edge(`f:${id}`, `p:${pid}`, 'in');
              for (const oid of p.outputs) {
                edge(`p:${pid}`, `f:${oid}`, 'out');
                queue.push({ id: oid, g: g + 2, dir: 1 });
              }
              for (const x of p.extras) {
                addFile(x, g);
                edge(`f:${x.id}`, `p:${pid}`, 'partner');
              }
            }
          }
        }
      }
      return { focus: fileId, files, parcels, edges, gen, truncated };
    }

    /* The parcels an input has been in, oldest first, its parents' before its own (ParentInputID, DX-ADR-004): what its
       failure count rests on. A recovery parcel, born Done from a status report, ran nothing and is left out. */
    inputAttempts(node, i) {
      const chain = [];
      const seen = new Set();
      for (let cur = i; cur && !seen.has(cur.id); cur = cur.parent != null ? node.inputs.get(cur.parent) : null) {
        seen.add(cur.id);
        chain.unshift(cur);
      }
      const out = [];
      for (const x of chain) {
        for (const pid of x.attempts) {
          const p = this.parcelIndex.get(pid);
          if (p && !p.recovery) out.push({ parcel: p, input: x });
        }
      }
      return out;
    }

    /* Summaries */

    /* The inputs by state, under the short keys COUNT_KEY gives them, and how many of the Unassigned are not ready yet.
       COUNT_KEY is the one place a state's key is decided, so a state added to the enum is counted here by itself. */
    counts(node) {
      const c = Object.assign(Object.fromEntries(Object.values(COUNT_KEY).map((k) => [k, 0])), { delayed: 0 });
      for (const i of node.inputs.values()) {
        const key = COUNT_KEY[i.status];
        if (!key) continue;
        c[key] += 1;
        if (i.status === 'Unassigned' && !this.ready(node, i)) c.delayed += 1;
      }
      return c;
    }

    parcelCounts(node) {
      /* live parcels are counted from the list; terminal ones from the cumulative counters, which survive its pruning */
      const c = Object.assign(Object.fromEntries(PARCEL_STATES.map((k) => [k, 0])), node.parcelTerminal);
      for (const p of node.parcels) if (!PARCEL_TERMINAL.has(p.status)) c[p.status] += 1;
      return c;
    }

    wgHooks() {
      const out = [];
      const w = this.wg.status;
      const sc = this.spec.workgraph.scouting;
      if (sc) {
        const st = this.wg.scout;
        const ladder = st.stages.length > 1 ? `climb the stages ${st.stages.map((x) => (sc.kind === 'fraction' ? `${Math.round(x * 100)}%` : x)).join(', ')} as each drains` : 'accept once the sample has drained';
        out.push({ on: w === 'Scouting', name: 'ScoutingToApproving', text: w === 'Scouting' ? this.scoutStatus() : ladder, full: `ScoutingToApproving hook: ${ladder}, accept after the last; accept early once more than ${Math.round(sc.failAbove * 100)}% of at least ${sc.minSeen} settled inputs ended Problematic, so that the success-rate check blocks the workgraph` });
      }
      const batched = Object.values(this.nodes).filter((n) => n.spec.feeder.seeds && n.spec.feeder.batch);
      const tg = this.spec.workgraph.target;
      if (batched.length || tg) {
        const parts = [];
        const fulls = [];
        for (const n of batched) {
          parts.push(`raise ${n.spec.label} by ${n.spec.feeder.batch} below ${n.spec.feeder.inflight} in flight`.replace(' in flight', ''));
          fulls.push(`raise ${n.spec.label}'s feeder by ${n.spec.feeder.batch} seeds whenever fewer than ${n.spec.feeder.inflight} are in flight`);
        }
        if (tg) {
          parts.push(`stop at ${tg.files} in ${tg.output}`);
          fulls.push(`disable the external feeders once ${tg.output} holds ${tg.files} files`);
        }
        out.push({ on: w === 'Active', name: 'Active', text: parts.join(' · '), full: `Active hook: ${fulls.join('; ')}` });
      }
      return out;
    }

    feederInfo(node) {
      const f = node.spec.feeder;
      let info;
      if (f.seeds) {
        const scouting = !!this.spec.workgraph.scouting && BEFORE_APPROVAL.has(this.wg.status);
        const limit = this.seedTarget(node);
        info = { kind: 'seed feeder', text: `${node.seedNext}/${limit} seeds${scouting ? ' (scout)' : ''}`, extendable: !!f.batch && node.seedRequested < f.seeds && this.wg.status === 'Active', step: f.batch };
      } else {
        const srcs = f.from.filter((x) => this.sources[x]);
        if (srcs.length) info = { kind: 'query feeder', text: srcs.map((x) => `${node.cursor[x] || 0}/${this.scoutLimit(this.sources[x])} files`).join(', '), extendable: false };
        else info = { kind: 'edge feeder', text: `${f.from.join(', ')}${node.unfed.length ? ` · ${node.unfed.length} waiting` : ''}`, extendable: false };
      }
      if (!node.feederEnabled) info.state = 'disabled';
      else if (node.status !== 'Active') info.state = 'not running';
      else if (!this.feederActive(node)) info.state = 'off';
      else if (f.seeds && node.seedNext >= this.seedTarget(node)) info.state = 'waiting on the hook';
      else info.state = 'running';
      return info;
    }

    /* A file the consumer wrote off will never be ready, so the waiting transformation quarantines its own input for an
       operator: what the waiting work should do differs by what the wait was for, and the core cannot tell (DX-ADR-005).
       One the consumer set aside as Problematic is still undecided, so the waiting input keeps waiting. */
    afterLost(node, input) {
      for (const a of node.spec.feeder.after) {
        const other = this.nodes[a];
        const oi = other && other.byFile.get(input.file.id);
        if (oi && oi.status === 'NotProcessed') return oi.status;
      }
      return null;
    }
  }

  root.WorkgraphSimEngine = { Sim, normalise, IDENTITIES, SHAPES, T, LISTS, WG_STATES, WG_ALL_STATES, NODE_STATES, PARCEL_STATES, ACTION_RESULTS, DEFAULT_SETTINGS, MODE_ROWS, DECISIONS, OPERATOR_EDGES, runningList, INPUT_STATES, INPUT_TERMINAL, PARCEL_TERMINAL, COUNT_KEY, CANCELLABLE, RUNNING, MAX_SLOTS, asArray };
})(typeof window !== 'undefined' ? window : globalThis);
