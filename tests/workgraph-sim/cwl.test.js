// The CWL compiler (docs/assets/js/workgraph-cwl.js): the documents the playground ships, the clamps it has to state,
// and one fixture per diagnostic, each beside a document that proves the diagnostic does not fire on a good one.
(function (g) {
  g.WG_TEST.suite('cwl', async () => {
  const { out, check, started, complete, readText } = g.WG_TEST;
  const { compile, parseYaml, locate, idmap, hintMap, DEFAULT_RUN_SETTINGS } = g.WorkgraphCwl;
  const { layout } = g.WorkgraphSim;
  const { INPUT_TERMINAL, PARCEL_TERMINAL, COUNT_KEY } = g.WorkgraphSimEngine;

  /* The workgraphs live under docs/, not beside these fixtures: the playground's example picker fetches them from the
     built site, and mkdocs ships what is in docs/ and nothing else. They are fixtures all the same. */
  const EXAMPLES = 'docs/assets/examples/';
  const FIXTURES = 'tests/workgraph-sim/fixtures/';
  const WORKGRAPHS = ['simulation', 'simulation-scouted', 'mc-simulation', 'analysis-production', 'sprucing', 'rdst-stripping', 'joining', 'histograms'];
  /* Three seeds a workgraph, for the reason docs.test.js runs four over a fence: a compiled spec that completes only
     under the seed a check happened to take is one a reader would meet stuck. */
  const SEEDS = [1, 2, 3];

  const readDoc = (path) => parseYaml(readText(path));
  const example = (name) => readDoc(EXAMPLES + name + '.cwl');
  const fixture = (name) => readDoc(FIXTURES + name + '.cwl');
  const errors = (r) => r.problems.filter((p) => p.severity === 'error');
  const warnings = (r) => r.problems.filter((p) => p.severity === 'warning');
  const rules = (list) => list.map((p) => p.rule).join(', ');
  /* Key order is not part of a spec's identity, so the two forms of one document are compared over a canonical form. */
  const canon = (v) => JSON.stringify(v, (k, x) => (x && typeof x === 'object' && !Array.isArray(x) ? Object.keys(x).sort().reduce((o, kk) => ((o[kk] = x[kk]), o), {}) : x));

  /* What a stuck run has to say for itself, as docs.test.js reports it: a failure is a finding about the compiled
     model, so it carries the end state rather than a raised ceiling. */
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

  /* ---------------------------------------------------------------- */
  /* The reader                                                        */
  /* ---------------------------------------------------------------- */

  {
    const doc = example('analysis-production');
    check(doc.class === 'Workflow' && doc.cwlVersion === 'v1.2', 'the reader takes a plain mapping');
    check(doc['$namespaces'].dirac === 'https://diracgrid.org/cwl#', 'a URL is a scalar, and the # in it is not a comment');
    check(doc.doc.indexOf('LHCb Analysis Production.\n  Event type') === 0, 'a |- block scalar keeps its own indentation and loses its trailing newline');
    check(doc.inputs['output-prefix'].default === '00012345_00006789', "a quoted number stays a string");
    check(doc.hints['dirac:Workgraph'].schema_version === '1.0', 'a key may hold a colon, so dirac:Workgraph is one key');
    check(doc.steps.transformation_1.hints['dirac:Transformation'].packer.args.group_size === 2, 'a flow mapping nests');
    check(Array.isArray(doc.steps.transformation_2.out) && doc.steps.transformation_2.out[0] === 'LB2DDSTP_ROOT', 'a flow sequence of bare strings');
    check(doc.hints['dirac:Workgraph'].output_sandbox.length === 5, 'a block sequence under a key at the same indent');
    const mc = example('mc-simulation');
    check(mc.hints['dirac:Workgraph'].data_management[0].actions.Finalizing.length === 2, 'a sequence item that opens a mapping carries the rest of it');
    check(mc.steps.MCReconstruction.hints['dirac:Transformation'].packer.args.keep_storage_together === true, 'true is a boolean');
    check(mc.inputs.events['dirac:Feeder'].args.target_events === 1000000000, 'a large integer is a number');
    check(readText(EXAMPLES + 'analysis-production.cwl').indexOf('#!/usr/bin/env') === 0 && doc.cwlVersion === 'v1.2', 'a shebang line is a comment');
  }

  /* ---------------------------------------------------------------- */
  /* The shapes every idmap field arrives in                           */
  /* ---------------------------------------------------------------- */

  {
    const ids = (v, scalarField) => idmap(v, scalarField).map((e) => e.id).join(',');
    check(ids({ reco: { packer: 1 }, merge: {} }) === 'reco,merge', 'a mapping keyed by id, in document order');
    check(ids([{ id: 'reco' }, { id: 'merge' }]) === 'reco,merge', 'a list of objects carrying id');
    check(ids(['merged']) === 'merged', 'a list of bare strings');
    check(idmap({ 'input-data': 'up/out' }, 'source')[0].source === 'up/out', 'in: a mapping whose value is the source');
    check(idmap({ x: { source: 'a', valueFrom: 'b' } }, 'source')[0].valueFrom === 'b', 'in: a mapping whose value is a body');
    check(idmap(null).length === 0 && idmap(undefined).length === 0, 'a field that is not there is no entries');
    check(hintMap({ 'dirac:Workgraph': { type: 'MC' } })['dirac:Workgraph'].type === 'MC', 'hints as a map keyed by class');
    check(hintMap([{ class: 'dirac:Workgraph', type: 'MC' }])['dirac:Workgraph'].type === 'MC', 'hints as a list of class objects');
    check(hintMap([{ class: 'dirac:Workgraph' }])['dirac:Workgraph'].class === undefined, 'the class is the key, not a field of the body');
    check(hintMap({ 'https://diracgrid.org/cwl#Workgraph': {} })['dirac:Workgraph'] !== undefined, 'a hint class written as a full URI is the same class');
  }

  /* ---------------------------------------------------------------- */
  /* The workgraphs                                                     */
  /* ---------------------------------------------------------------- */

  for (const name of WORKGRAPHS) {
    const r = compile(example(name), {});
    check(errors(r).length === 0, `${name}: compiles with no errors${errors(r).length ? ': ' + errors(r).map((p) => p.message).join(' | ') : ''}`);
    check(warnings(r).length === 0, `${name}: compiles with no warnings${warnings(r).length ? ': ' + rules(warnings(r)) : ''}`);
    check(Object.keys(r.spec.transformations).length > 0, `${name}: has transformations`);

    const sim = started(r.spec);
    const L = await layout(sim);
    check(L.width > 0 && L.height > 0, `${name}: the compiled spec lays out`);
    const boxes = Object.values(L.items).map((it) => ({ id: it.id, x0: it.x, y0: it.y, x1: it.x + it.w, y1: it.y + it.h }));
    const under = new Set();
    for (const e of L.edges) {
      for (let i = 1; i < e.points.length; i++) {
        const a = e.points[i - 1];
        const b = e.points[i];
        const lo = { x: Math.min(a.x, b.x), y: Math.min(a.y, b.y) };
        const hi = { x: Math.max(a.x, b.x), y: Math.max(a.y, b.y) };
        for (const q of boxes) if (lo.x < q.x1 - 1 && hi.x > q.x0 + 1 && lo.y < q.y1 - 1 && hi.y > q.y0 + 1) under.add(`${e.from}>${e.to} under ${q.id}`);
      }
    }
    check(under.size === 0, `${name}: no edge passes beneath a node${under.size ? ': ' + [...under].join(', ') : ''}`);

    const runs = [];
    for (const seed of SEEDS) {
      const s = started(Object.assign({}, r.spec, { seed }));
      const secs = complete(s);
      const ok = s.wg.status === 'Completed';
      check(ok, `${name}: runs to Completed under seed ${seed}`);
      runs.push(`${seed}:${ok ? secs.toFixed(0) + 's/' + s.totals.done : 'STUCK'}`);
      if (!ok) out(`     seed ${seed} ended ${endState(s)}`);
    }
    out(`${runs.some((r2) => r2.includes('STUCK')) ? 'FAIL' : 'ok  '} ${name}: ${runs.join(' ')}`);
  }

  /* ---------------------------------------------------------------- */
  /* The two forms of one document                                     */
  /* ---------------------------------------------------------------- */

  {
    const mapping = compile(example('analysis-production'), {});
    const list = compile(fixture('analysis-production-list-form'), {});
    check(errors(list).length === 0, 'the list form compiles with no errors');
    check(canon(mapping.spec) === canon(list.spec), 'the list form and the mapping form of one document produce an identical spec');
    check(mapping.ignored === list.ignored, 'and the same account of what was dropped');
  }

  /* ---------------------------------------------------------------- */
  /* What the document says, and what the settings say                 */
  /* ---------------------------------------------------------------- */

  {
    const mc = example('mc-simulation');
    const r = compile(mc, {});
    const sim = r.spec.transformations.MCSimulation;
    check(sim.feeder.seeds === DEFAULT_RUN_SETTINGS.maxPool, `a billion events is clamped to the pool ceiling, not run: ${sim.feeder.seeds} seeds`);
    check(r.notes.indexOf('1,000,000,000 events') >= 0 && r.notes.indexOf('300 seeds') >= 0, `the clamp is stated: ${r.notes.split('\n')[0]}`);
    check(r.spec.workgraph.scouting.count <= DEFAULT_RUN_SETTINGS.scoutSample, 'the scouting sample is clamped too');
    check(r.notes.indexOf('100,000 events at 1,000 a seed is a sample of 100 seeds, clamped to') >= 0, `and that clamp is stated as well, in the unit the feeder states it in: ${r.notes}`);
    check(r.spec.workgraph.target.files < 200 && r.notes.indexOf('a target of 200 files clamped to') >= 0, 'a target the clamped pools could never reach is clamped with it, so the hook that disables the feeders still runs');
    check(r.spec.workgraph.target.output === 'datasets', 'the target names the output box');

    /* The clamp moves with the setting, which is the point of it being a setting. */
    const big = compile(mc, { maxPool: 60 });
    check(big.spec.transformations.MCSimulation.feeder.seeds === 60, 'the pool ceiling is the caller of compile, not the document');

    /* Nothing a document says may set a run setting: a document that could would no longer be one that could submit. */
    const smuggled = parseYaml(readText(EXAMPLES + 'mc-simulation.cwl'));
    smuggled.hints['dirac:Workgraph'].seed = 42;
    smuggled.hints['dirac:Workgraph'].slots = 9;
    smuggled.hints['dirac:Workgraph'].settings = { failScale: 4 };
    smuggled.steps.MCSimulation.hints['dirac:Transformation'].fail = 0.5;
    smuggled.steps.MCSimulation.hints['dirac:Transformation'].run = [40, 50];
    const after = compile(smuggled, {});
    check(canon(after.spec) === canon(r.spec), 'a dirac: field naming a run setting changes nothing about the compiled spec');

    const settings = compile(mc, { seed: 7, slots: 2, fail: 0.3, jobMin: 0.4, jobMax: 0.5, partial: 0.25 });
    const node = settings.spec.transformations.MCReconstruction;
    check(settings.spec.seed === 7 && settings.spec.slots === 2, 'the seed and the parcel concurrency come from the settings');
    check(node.fail === 0.3 && node.partial === 0.25 && node.run[0] === 0.4, 'and so do the durations and the outcome rates');
  }

  {
    /* The three action lists, the hold a scout implies, and the account of everything left on the floor. */
    const r = compile(example('analysis-production'), {});
    check(r.spec.transformations.transformation_2.finalize.length === 1, 'hooks.Finalizing is the finalizing list');
    check(r.ignored.indexOf('run body transformations/transformation-1.cwl (not loaded)') >= 0, 'a run body is named and not followed');
    check(r.ignored.indexOf('output-prefix: a job parameter, not a pool') >= 0, 'an input with no feeder is a job parameter');
    check(r.ignored.indexOf('InlineJavascriptRequirement') >= 0, "and the workflow's requirements are dropped in the open");
    check(r.sandbox.indexOf('prodConf_*.json') >= 0 && r.sandbox.indexOf('*.log') >= 0, 'the sandbox patterns are listed');
    /* Sandbox capture is a side channel: a log is never an output, never an edge and never a box. */
    const spec = JSON.stringify(r.spec);
    check(spec.indexOf('.log') < 0 && spec.indexOf('prodConf') < 0 && spec.indexOf('summary') < 0, 'and no glob of it reaches the picture');
    check(Object.keys(r.spec.outputs).length === 1, 'one deliverable, which is what the document declares');

    const mc = compile(example('mc-simulation'), {});
    check(mc.spec.transformations.MCSimulation.hold === undefined, 'a compute step runs during the scout');
    check(mc.spec.transformations.OutputReplication.hold === 'approval', 'a replication of a workgraph output sits only downstream, so it is held until the workgraph is approved');
    check(mc.spec.transformations.MCSimulationRemoval.hold === 'approval', 'and so does a removal of an intermediate, which the document need not say twice');
    check(mc.spec.transformations.MCSimulationRemoval.feeder.after.join() === 'MCReconstruction', "a removal's after is a wait on the sibling that reads the same files");
    check(mc.spec.workgraph.approving.filter((a) => a.manual).length === 1, 'ManualApproval is the one action that waits for a person');
    check(mc.spec.workgraph.approving[0].check === 'success rate', 'CheckSuccessRate is the check that judges the scout');
    check(mc.spec.workgraph.approving[2].name === 'PPG approval', 'a hook name is read as a sentence, and an acronym keeps its case');
    check(mc.spec.transformations.MCReconstruction.packer.by === 'colour', 'a packer that keeps a run or a storage together groups by the source identity');
    check(mc.ignored.indexOf('CERN-DST') >= 0, 'a destination is recorded rather than drawn: the model moves files without naming a storage');

    const sp = compile(example('sprucing'), {});
    check(sp.spec.transformations.StagingReplication.hold === undefined, 'a staging a step waits for runs during the scout, or the sample never drains');
    check(sp.spec.transformations.sprucing.feeder.from.join() === 'raw-data', 'the sprucing shares the query rather than being fed from the staging');
    check(sp.spec.transformations.BufferRemoval.feeder.from.join() === 'raw-data', 'and so does the removal declared against the same input');
    check(sp.spec.transformations.BufferRemoval.feeder.after.join() === 'sprucing', 'whose after points back at the step that reads it');
    check(sp.spec.transformations.BufferRemoval.hold === 'approval', 'while the removal downstream of it is held, which only its after tells apart from the staging');
    check(sp.spec.transformations.OutputReplication.hold === 'approval', 'and so is the replication of the output');
    check(sp.notes.indexOf('LHCbBookkeeping') >= 0 && sp.notes.indexOf('cannot size') >= 0, `a feeder that samples in a vocabulary of its own says what the scout was drawn as instead: ${sp.notes}`);

    /* `initial_state` overrides the rule in both directions, and says the third thing a boolean could not. */
    const over = JSON.parse(JSON.stringify(example('sprucing')));
    over.hints['dirac:Workgraph'].data_management[1].initial_state = 'Active';
    over.steps.merge.hints['dirac:Transformation'].initial_state = 'Paused';
    const ov = compile(over, {});
    check(ov.spec.transformations.BufferRemoval.hold === undefined, 'a downstream removal asking to start Active is in the scout after all');
    check(ov.spec.transformations.merge.hold === 'operator', 'and a step asking to start Paused waits for an operator, which no boolean could have said');

    /* Whether the workgraph scouts at all is derived: something to judge, and something to judge it on. */
    const sc = compile(example('simulation-scouted'), {});
    check(sc.spec.workgraph.scouting.stages.join() === '8,12', `the ladder comes from the hook that climbs it, in the feeder's unit: ${JSON.stringify(sc.spec.workgraph.scouting)}`);
    check(sc.notes.indexOf('a ladder of 8,000, 16,000 events at 1,000 a seed is 8, 16 seeds, clamped to 8, 12') >= 0, `and its clamp is stated in the same unit: ${sc.notes}`);

    const jn = compile(example('joining'), {});
    check(jn.spec.transformations.comparison.packer.join === 'reconstruction-b', 'a second pool-driving input from another step is the packer\'s join');
    check(jn.spec.transformations.comparison.feeder.from.join() === 'reconstruction-a', 'and driving_input says which of the two is the pool');

    const hist = compile(example('histograms'), {});
    check(hist.spec.transformations['histogram-merge'].feeder.port === 'artifact', "a step's second output is the artifact port");
    check(hist.spec.transformations.sprucing.artifact === true, 'and the step that declares it is marked as emitting one');
    check(hist.spec.transformations.merge.feeder.port === undefined, 'while the first output is the transformation\'s own');

    const st = compile(example('rdst-stripping'), {});
    check(st.spec.workgraph.scouting === undefined, 'a workgraph with no approving list has nothing to judge a scout, so it does not scout');
    check(st.spec.transformations.BufferRemoval.hold === undefined, 'and a member the position rule holds back then waits for nothing, there being no Approving to wait in');
    check(st.spec.transformations.stripping.feeder.from.join() === 'rdst-data', 'driving_input picks which pool-driving input is the pool');
    check(st.spec.transformations.stripping.packer.lookup === 'raw-ancestors', 'and the other becomes the packer partner it looks up');
    check(st.spec.sources['rdst-data'].ancestors === 'raw-ancestors', 'which is what the ancestor query is to the driving one');
  }

  /* ---------------------------------------------------------------- */
  /* The diagnostics                                                   */
  /* ---------------------------------------------------------------- */

  {
    const good = compile(fixture('good'), {});
    check(good.problems.length === 0, `the good document has nothing to report: ${rules(good.problems)}`);

    const cases = [
      ['error-bare-step', 'error', 'bare-step'],
      ['error-orphan-output', 'error', 'orphan-output'],
      ['error-unknown-source', 'error', 'unknown-source'],
      ['error-unknown-files', 'error', 'unknown-source'],
      ['error-no-driving-input', 'error', 'no-driving-input'],
      ['error-ambiguous-driving-input', 'error', 'ambiguous-driving-input'],
      ['error-driving-input', 'error', 'driving-input'],
      ['error-cycle', 'error', 'cycle'],
      ['error-no-workgraph-hint', 'error', 'not-a-workgraph'],
      ['error-wrong-class', 'error', 'not-a-workgraph'],
      ['warn-scatter', 'warning', 'scatter'],
      ['warn-when', 'warning', 'when'],
      ['warn-expression-tool', 'warning', 'expression-tool'],
    ];
    for (const [name, severity, rule] of cases) {
      const r = compile(fixture(name), {});
      const hit = r.problems.filter((p) => p.rule === rule && p.severity === severity);
      check(hit.length === 1, `${name}: one ${severity}, ${rule}${hit.length === 1 ? '' : `, got ${rules(r.problems) || 'nothing'}`}`);
      check(r.problems.length === 1, `${name}: and nothing else${r.problems.length === 1 ? '' : `, got ${rules(r.problems)}`}`);
      check(hit.length === 1 && !!hit[0].path, `${name}: the problem carries a path (${hit.length === 1 ? hit[0].path : '—'})`);
      /* A warning that only names the CWL feature leaves the reader guessing what the picture in front of them means,
         so each one says what is drawn in its place. */
      check(hit.length === 1 && hit[0].message.length > 20 && (severity === 'error' || /drawn as/.test(hit[0].message)), `${name}: and says what the reader is looking at instead`);
      /* the sibling half: the rule that fires here fires on nothing that is well formed */
      check(good.problems.filter((p) => p.rule === rule).length === 0, `${name}: ${rule} does not fire on a good document`);
      for (const workgraph of WORKGRAPHS) check(compile(example(workgraph), {}).problems.filter((p) => p.rule === rule).length === 0, `${name}: nor on ${workgraph}`);
    }
    /* Every rule the compiler can raise has a fixture: a diagnostic nobody exercises is a diagnostic nobody maintains. */
    const covered = new Set(cases.map(([, , rule]) => rule));
    for (const rule of ['bare-step', 'orphan-output', 'unknown-source', 'no-driving-input', 'ambiguous-driving-input', 'driving-input', 'cycle', 'not-a-workgraph', 'scatter', 'when', 'expression-tool']) check(covered.has(rule), `${rule} has a fixture`);
  }

  /* ---------------------------------------------------------------- */
  /* The published hint schema                                         */
  /* ---------------------------------------------------------------- */

  {
    const schema = JSON.parse(readText('docs/schemas/dirac-1.0.json'));
    const { checkSchema, schemaAt, suggestAt, containerAt, pathAt, hintSites } = g.WorkgraphCwl;
    check(schema.$id === 'https://diracx.io/schemas/dirac-1.0.json', 'the schema names itself');
    check(Object.keys(schema.properties).indexOf('dirac:Workgraph') >= 0 && Object.keys(schema.properties).indexOf('dirac:Transformation') >= 0, 'the map form is what the schema is written over: a class is a property, not a member of a discriminated list');

    /* The documents are the vocabulary until DX-ADR-007's schema is settled, so this is what keeps the two in step: a
       field added to an example and not to the schema fails here, and so does the reverse. */
    for (const name of WORKGRAPHS) {
      const problems = checkSchema(example(name), schema);
      check(problems.length === 0, `${name}: every hint it uses is in the schema${problems.length ? ': ' + problems.map((p) => p.message).join(' | ') : ''}`);
    }
    for (const name of ['good', 'analysis-production-list-form']) check(checkSchema(fixture(name), schema).length === 0, `${name}: checks clean against the schema`);
    /* and in the other direction: every field the schema knows is one of these documents' own */
    const used = new Set();
    const walk = (value, at) => {
      if (!value || typeof value !== 'object') return;
      if (Array.isArray(value)) return value.forEach((v) => walk(v, at));
      for (const [k, v] of Object.entries(value)) {
        used.add(k);
        walk(v, k);
      }
    };
    for (const name of WORKGRAPHS) for (const site of hintSites(example(name))) walk(site.body, site.name);
    const defs = ['workgraph', 'transformation', 'feeder', 'dataTransformation', 'packer', 'actionLists'];
    const compiler = readText('docs/assets/js/workgraph-cwl.js');
    const unused = [];
    for (const def of defs) for (const field of Object.keys(schema.$defs[def].properties || {})) if (!used.has(field) && compiler.indexOf(field) < 0) unused.push(`${def}.${field}`);
    check(unused.length === 0, `every field of the schema is one an example uses or the compiler reads${unused.length ? ': unused ' + unused.join(', ') : ''}`);

    /* A field the schema does not know is what a reader wants caught, and the typo is what they want back. */
    const typo = JSON.parse(JSON.stringify(example('mc-simulation')));
    typo.steps.MCSimulation.hints['dirac:Transformation'].packr = typo.steps.MCSimulation.hints['dirac:Transformation'].packer;
    delete typo.steps.MCSimulation.hints['dirac:Transformation'].packer;
    const caught = checkSchema(typo, schema);
    check(caught.length === 1 && caught[0].rule === 'schema' && caught[0].severity === 'warning', `a field the schema has never heard of is one warning: ${caught.map((p) => p.rule).join(', ')}`);
    check(caught.length === 1 && /did you mean packer/.test(caught[0].message), `and it says what was probably meant: ${caught.length ? caught[0].message : '—'}`);
    check(caught.length === 1 && caught[0].path === 'steps.MCSimulation.hints.dirac:Transformation.packr', 'and carries the path to it');

    const wrong = JSON.parse(JSON.stringify(example('sprucing')));
    wrong.hints['dirac:Workgraph'].data_management[0].initial_state = 'Sleeping';
    check(checkSchema(wrong, schema).some((p) => /not one of/.test(p.message)), 'an enum the schema fixes is checked');
    const missing = JSON.parse(JSON.stringify(example('sprucing')));
    delete missing.hints['dirac:Workgraph'].data_management[0].files;
    check(checkSchema(missing, schema).some((p) => /needs files/.test(p.message)), 'and so is a field it requires');

    /* Where the cursor is, which is what the editor asks to offer a completion or a hover. */
    const text = readText(EXAMPLES + 'mc-simulation.cwl');
    const lineOf = (needle) => text.split('\n').findIndex((l) => l.indexOf(needle) >= 0) + 1;
    check(pathAt(text, lineOf('type: MCSimulation')) === 'hints.dirac:Workgraph.type', 'a key inside a hint knows its path');
    check(containerAt(text, lineOf('type: MCSimulation')).path === 'hints.dirac:Workgraph', 'and the mapping that holds it');
    check(pathAt(text, lineOf('files: MCReconstruction/reco-files')) === 'hints.dirac:Workgraph.data_management.1.files', 'an item of a sequence is an index of the path');
    check(pathAt(text, lineOf('packer: {name: PerInput}')) === 'steps.MCSimulation.hints.dirac:Transformation.packer', 'a step hint knows which step it is in');
    check((schemaAt(schema, 'hints.dirac:Workgraph.target') || {}).description.indexOf('Active hook') >= 0, 'a path lands on the schema node that documents it');
    check((schemaAt(schema, 'hints.dirac:Workgraph.data_management.0.operation') || {}).enum.join() === 'replicate,remove,archive', 'through a sequence as well');
    check((schemaAt(schema, 'inputs.events.dirac:Feeder.name') || {}).type === 'string', 'and into a dirac:Feeder, which sits on an input parameter as an extension field');
    check(schemaAt(schema, 'steps.MCSimulation.run') === null, 'a path outside a hint lands nowhere, since the schema is only the hints');
    const offered = suggestAt(schema, text, lineOf('packer: {name: PerInput}')).map((s) => s.key);
    check(offered.indexOf('driving_input') >= 0 && offered.indexOf('after') >= 0 && offered.indexOf('initial_state') >= 0, `the fields left in dirac:Transformation are offered, the one override on where a member starts among them: ${offered.join(', ')}`);
    check(offered.indexOf('actions') < 0, 'and one already written is not');
    check(suggestAt(schema, text, lineOf('packer: {name: PerInput}')).every((s) => s.doc), 'each with the sentence the schema documents it by');
  }

  /* ---------------------------------------------------------------- */
  /* Paths into lines                                                  */
  /* ---------------------------------------------------------------- */

  {
    const text = readText(EXAMPLES + 'mc-simulation.cwl');
    const lines = text.split('\n');
    const at = (path) => lines[locate(text, path) - 1] || '';
    check(/^steps:/.test(at('steps')), 'a top-level key finds its own line');
    check(/MCReconstruction:/.test(at('steps.MCReconstruction')), 'a step finds its own line, not the one that mentions it');
    check(/packer:/.test(at('steps.MCReconstruction.hints.dirac:Transformation.packer')), 'a key with a colon in it is one segment');
    check(/data_management:/.test(at('hints.dirac:Workgraph.data_management')), 'a sequence under a key');
    check(/id: MCReconstructionRemoval/.test(at('hints.dirac:Workgraph.data_management.1')), 'an index picks the item of a sequence');
    check(/files: MCReconstruction\/reco-files/.test(at('hints.dirac:Workgraph.data_management.1.files')), 'and a key inside that item');
    check(locate(text, 'steps.nowhere.in') === locate(text, 'steps'), 'a path that runs out falls back on the deepest line it did find');
    check(locate(text, '') === 1 && locate('', 'a.b') === 1, 'and an empty path is the first line');

    const r = compile(fixture('error-orphan-output'), {});
    const ftext = readText(FIXTURES + 'error-orphan-output.cwl');
    check(/out: \[processed, leftovers\]/.test(ftext.split('\n')[locate(ftext, r.problems[0].path) - 1] || ''), "a problem's path reveals the line the reader has to change");
  }
  });
})(globalThis);
