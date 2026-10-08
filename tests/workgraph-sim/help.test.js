// Help mode: the enums, the generated legends, and what the ? panel is allowed to say.
//
// The point of these checks is recurrence. The old help page rotted because it was a second copy of facts that lived in the
// drawing, and nothing forced anyone to notice when the drawing moved. Here nothing is copied: a legend row is drawn by the
// function that draws the real element, over the state -> style map the drawing itself reads. What is left to check is that
// the maps are complete and that nothing has crept back into the ? panel that a change to the picture could invalidate.
(function (g) {
  g.WG_TEST.suite('help', async () => {
  const { check, readText } = g.WG_TEST;
  const E = g.WorkgraphSimEngine;
  const W = g.WorkgraphSim;
  const { Sim, INPUT_STATES, PARCEL_STATES, NODE_STATES, WG_STATES, WG_ALL_STATES, ACTION_RESULTS } = E;

  const src = readText('docs/assets/js/workgraph-sim.js');
  const css = readText('docs/assets/css/workgraph-sim.css');
  const lineOf = (needle) => src.slice(0, src.indexOf(needle)).split('\n').length;
  const missing = (want, have) => want.filter((x) => !have.includes(x));
  const spare = (have, want) => have.filter((x) => !want.includes(x));

  // ---- every member of every state enum has a style and a label, and every style belongs to a live state ----
  //
  // This is the check that fails the build when a state is added without a legend entry. Each half matters: the first says a
  // new state cannot be drawn with no legend, the second that a legend cannot describe a state nothing reaches any more.
  {
    // parcels: the slot border, the halo, and the row that names the state
    const slotKeys = Object.keys(W.SLOT_CLS);
    const parcelNames = W.PARCEL_ROWS.map((r) => r.name);
    check(!missing(PARCEL_STATES, slotKeys).length, `every parcel state has a slot style: missing ${missing(PARCEL_STATES, slotKeys)}`);
    check(!spare(slotKeys, PARCEL_STATES).length, `every slot style is a live parcel state: spare ${spare(slotKeys, PARCEL_STATES)}`);
    check(!missing(PARCEL_STATES, parcelNames).length, `every parcel state has a label: missing ${missing(PARCEL_STATES, parcelNames)}`);
    check(!spare(parcelNames, PARCEL_STATES).length, `every labelled parcel state is live: spare ${spare(parcelNames, PARCEL_STATES)}`);
    check(!spare(Object.keys(W.SLOT_GLOW), PARCEL_STATES).length, 'every halo belongs to a live parcel state');

    // inputs: the bar's colour, the row that names the state, and the order the bar stacks them in
    const inputNames = W.INPUT_ROWS.map((r) => r.name);
    const inputKeys = W.INPUT_ROWS.map((r) => r.key);
    check(!missing(INPUT_STATES, inputNames).length, `every input state has a row: missing ${missing(INPUT_STATES, inputNames)}`);
    check(!spare(inputNames, INPUT_STATES).length, `every input row is a live state: spare ${spare(inputNames, INPUT_STATES)}`);
    check(W.INPUT_ROWS.every((r) => r.cls && r.what), 'every input row carries a colour and a meaning');
    check(!missing(inputKeys, W.BAR_ORDER).length && !spare(W.BAR_ORDER, inputKeys).length, 'the bar stacks every input state and no other');

    // transformations: the member rows the workgraph's dialog counts by
    const memberKeys = W.MEMBER_ROWS.map((r) => r.key);
    check(!missing(NODE_STATES, memberKeys).length, `every transformation state has a row: missing ${missing(NODE_STATES, memberKeys)}`);
    check(!spare(memberKeys, NODE_STATES).length, `every member row is a live state: spare ${spare(memberKeys, NODE_STATES)}`);
    check(W.MEMBER_ROWS.every((r) => r.cls && r.what), 'every member row carries a colour and a meaning');

    // the workgraph: the strip draws the happy path, the machine every state there is
    check(!missing(WG_STATES, WG_ALL_STATES).length, 'the strip draws no state the machine does not have');
    const sim = new Sim({ name: 'enum', transformations: { a: { feeder: { seeds: 4 }, packer: { size: 1 }, run: [0.2, 0.3] } } });
    const machine = W.wgMachineHtml(sim);
    check(!WG_ALL_STATES.filter((s) => !machine.includes(`>${s}<`)).length, `the workgraph machine draws every state: missing ${WG_ALL_STATES.filter((s) => !machine.includes(`>${s}<`))}`);
    const node = W.nodeMachineHtml(sim, sim.nodes.a);
    check(!NODE_STATES.filter((s) => !node.includes(`>${s}<`)).length, `the member machine draws every state: missing ${NODE_STATES.filter((s) => !node.includes(`>${s}<`))}`);
    const parcels = W.parcelMachineHtml(sim, sim.nodes.a);
    check(!PARCEL_STATES.filter((s) => !parcels.includes(`>${s}<`)).length, `the parcel machine draws every state: missing ${PARCEL_STATES.filter((s) => !parcels.includes(`>${s}<`))}`);

    // action results: the one vocabulary, and nothing outside it
    const resultKeys = Object.keys(W.RESULTS);
    const order = W.RESULT_ORDER.map(String);
    check(!missing(ACTION_RESULTS.map(String), resultKeys).length, `every action result has a marker: missing ${missing(ACTION_RESULTS.map(String), resultKeys)}`);
    check(!spare(resultKeys, order).length, `every marker is a result the vocabulary has: spare ${spare(resultKeys, order)}`);
    check(W.RESULT_ORDER.every((r) => W.RESULTS[String(r)].tick && W.RESULTS[String(r)].dot && W.RESULTS[String(r)].cls), 'every result carries its glyphs and its colour');
  }

  // ---- no legend contains a hand-written swatch: each row is the real drawing, byte for byte ----
  {
    const slots = W.slotLegendHtml();
    check(PARCEL_STATES.every((st) => slots.includes(W.slotSwatch(st))), 'every slot legend row is drawn by the same call the grid makes');
    check(PARCEL_STATES.every((st) => slots.includes(`wg-slot-${W.SLOT_CLS[st]}`)), 'and carries that state’s own class');

    const bar = W.DISCLOSURES.bar.legend({});
    const row = (key) => W.INPUT_ROWS.find((r) => r.key === key);
    check(W.BAR_ORDER.every((k) => bar.includes(W.barSegmentSvg(0, 3, 22, 5, row(k), null, null))), 'every bar legend row is drawn by the same call the strip makes');

    const pool = W.DISCLOSURES.pool.legend({});
    check(Object.values(W.POOL_CLS).every((cls) => (cls ? pool.includes(cls) : true)), 'every pool treatment is in its legend');
    check(pool.includes('wg-pool-parcel'), 'including the parcel box the pool holds');

    const spine = W.machineLegend({ tones: true, op: true });
    check(W.MACHINE_TONES.every((t) => spine.includes(W.machineBoxSvg('', 1, 1, 34, 14, t.lit, t.blocked, ''))), 'every machine tone is drawn by the same call the machine makes');
    check(!W.machineLegend({ occupancy: true }).includes('wg-m-op'), 'a machine with no operator edge says nothing about one');

    const states = W.DISCLOSURES.states.legend({});
    check(W.PILL_TONES.every((t) => states.includes(W.statePill('Active', t.tone))), 'every pill tone is drawn by the same call the strip makes');

    /* the line's legend covers every tone the line can reach, and no tone it cannot */
    const line = W.DISCLOSURES.stateline.legend({});
    check(W.LINE_RESULTS.every((r) => line.includes(`wgsim-state-${W.RESULTS[String(r)].cls}`)), 'every state line tone is in its legend');
    const tones = new Set();
    for (const status of ['New', 'Scouting', 'Approving', 'ApprovingBlocked', 'Active', 'Finalizing', 'Completed', 'Archiving', 'Archived', 'Cancelling', 'Cleaned']) {
      const s2 = new Sim({ name: 'line', transformations: { a: { feeder: { seeds: 2 }, packer: { size: 1 }, run: [0.2, 0.3] } } });
      s2.wg.status = status;
      tones.add(W.stateCard(s2).cls);
    }
    const covered = W.LINE_RESULTS.map((r) => W.RESULTS[String(r)].cls);
    check(![...tones].filter((t) => !covered.includes(t)).length, `the line reaches no tone its legend lacks: ${[...tones].filter((t) => !covered.includes(t))}`);

    const log = W.DISCLOSURES.log.legend({});
    check(W.LOG_KINDS.every((k, i) => log.includes(W.logRowHtml(W.logKindRow(k, i)))), 'every log level is a real line of the log');
  }

  // ---- the ? panel holds only what a change to the picture cannot invalidate ----
  {
    const guide = W.guideHtml();
    /* what the reader sees: the tags gone and the entities back, since a key the panel has to escape to print — `>` — is
       still that key on screen and the checks below look for it by the character */
    const text = guide.replace(/<[^>]+>/g, ' ').replace(/&(gt|lt|amp|quot|#39);/g, (_, e) => ({ gt: '>', lt: '<', amp: '&', quot: '"', '#39': "'" })[e]);
    /* if a sentence would need editing because a node moved, it belongs in that node's disclosure instead */
    const positional = ['left', 'right', 'top', 'bottom', 'above', 'below', 'beneath', 'beside', 'corner', 'upper', 'lower', 'column', 'toolbar', 'header', 'footer'];
    const found = positional.filter((w) => new RegExp(`\\b${w}\\b`, 'i').test(text));
    check(!found.length, `the ? panel names no screen position: found ${found}`);
    check(!/wg-slot|wgsim-swatch|wgsim-legend|colour|shape|dashed|glyph/i.test(guide), 'and describes no encoding');
    check(/talks to nothing|no DIRAC installation/i.test(text) && /model of the DiracX Transformation System/i.test(text), 'it says what the simulator is and that nothing here reaches a real DIRAC');
    check(/mark/i.test(text) && /explains itself/i.test(text), 'and that the marks are showing and each opens in place');
    for (const [key] of W.CONTROLS) check(text.includes(key), `the controls list carries ${key}`);
    check(W.CONTROLS.map(([k]) => k).join(' ') === 'space > 1 2 3 R E C M', 'the controls are space, the step, 1/2/3, R, E, C and M');
    for (const [noun] of W.NOUNS) check(text.includes(noun), `the words list carries ${noun}`);
    check(W.NOUNS.length === 8 && W.NOUNS.every(([, clause]) => clause.split('.').length === 1), 'eight nouns, one clause each');
    check(guide.includes('DX-ADR-005_state_machines') && guide.includes('DX-ADR-004_schema'), 'and links to DX-ADR-005 and DX-ADR-004');
  }

  // ---- every region of both surfaces, each disclosed beside the code that draws it ----
  //
  // Help marks one surface at a time: the picture, or the dialog while it is open, since the dialog covers the picture. Each
  // has an order of its own, and every region of either is anchored and disclosed.
  {
    const keys = Object.keys(W.DISCLOSURES).sort();
    const placed = W.DISCLOSURE_ORDER.concat(W.PANEL_ORDER);
    check(keys.length === 18, `every region is disclosed: ${keys.length}`);
    check(placed.slice().sort().join(',') === keys.join(','), 'and every one of them is placed on a surface');
    check(new Set(placed).size === placed.length, 'and on one surface only');
    check(Object.keys(W.ANCHORS).sort().join(',') === keys.join(','), 'and every one of them is anchored');

    /* the rule that keeps a disclosure honest: it is registered next to the function that draws its region, so the two land
       in one diff. A disclosure that drifts away from its drawing fails here before a reader ever finds it stale. */
    const NEAR = {
      states: 'function statesHtml',
      stateline: 'function headHtml',
      bar: 'function barSegmentSvg',
      pool: 'function poolLegendHtml',
      slots: 'function slotLegendHtml',
      rail: 'function railLegendHtml',
      edge: 'function edgeChipsSvg',
      datasets: 'function filesHtml',
      log: 'function logLegendHtml',
      tabs: 'function detailsHtml',
      machine: 'function machineLegend',
      inputs: 'function inputsHtml',
      parcels: 'function parcelsHtml',
      members: 'function membersHtml',
      feeder: 'function feederHtml',
      packer: 'function packerHtml',
      hooks: 'function hooksSection',
      actions: 'function actionListsHtml',
    };
    for (const key of placed) {
      const d = W.DISCLOSURES[key];
      check(typeof d.body === 'function' && d.title && d.tab !== undefined && d.adr, `${key} has a title, a body, a machine tab and an ADR`);
      const away = Math.abs(lineOf(`disclose('${key}'`) - lineOf(NEAR[key]));
      check(away <= 40, `${key} is registered beside the code it describes: ${away} lines away from ${NEAR[key]}`);
    }

    /* each ends on its machine and its ADR, and explains its own region and nothing else. A region that is itself a machine,
       or the tabs that choose between machines, links to no machine: the link would open what the reader is looking at. */
    const sim = new Sim({ name: 'disc', sources: { q: { files: 6 } }, transformations: { a: { feeder: { from: 'q' }, packer: { size: 1 }, run: [0.2, 0.3] } }, outputs: { out: { from: 'a' } } });
    sim.start();
    for (let i = 0; i < 120; i++) sim.step(0.05);
    const OTHERS = { slots: ['the rail', 'an edge', 'the event log'], rail: ['the pool', 'the slot grid', 'an edge'], pool: ['the rail', 'the event log'], edge: ['the rail', 'the slot grid'], feeder: ['the packer', 'the hooks'], packer: ['the feeder'], members: ['the hooks'] };
    for (const key of placed) {
      const d = W.DISCLOSURES[key];
      const html = W.disclosureHtml(d, { sim, node: 'a' });
      check(/DX-ADR-00\d/.test(html), `${key} links to its ADR`);
      const machine = html.includes('data-act="open-machine"') || html.includes('data-act="open-workgraph"');
      check(d.tab === null ? !machine : machine, `${key} links to its state machine, or to none where it is one`);
      for (const other of OTHERS[key] || []) check(!html.includes(other), `${key} says nothing about ${other}`);
    }

    /* the dialog's regions name themselves, which is what its anchors aim at: a section found by the shape of its markup
       would move the moment the dialog's layout did */
    for (const key of W.PANEL_ORDER) {
      const sel = W.ANCHORS[key];
      check(sel.startsWith('[data-region=') || sel === '.wgsim-mtabs' || sel === '.wgsim-machine-region', `${key} is anchored to a name, not to a position: ${sel}`);
      if (sel.startsWith('[data-region=')) check(src.includes(`data-region="${key}"`), `and the drawing carries it: data-region="${key}"`);
    }
  }

  // ---- turning help on moves nothing, and the chip says it is on ----
  {
    check(/\.wgsim-marks \{[^}]*position: absolute/.test(css) && /\.wgsim-marks \{[^}]*pointer-events: none/.test(css), 'the marks sit on an absolute layer that takes no pointer of its own');
    check(/\.wgsim-mark \{[^}]*position: absolute/.test(css), 'and each mark is absolutely placed over its region');
    check(/\.wgsim \{[\s\S]*?position: relative/.test(css), 'the widget is the layer’s containing block');
    check(/\.wgsim-chip-help\[aria-checked="true"\][^}]*var\(--wg-accent-soft\)/.test(css), 'the ? chip wears the same tint as expert and chaos while it is on');
    check(/role="switch" aria-checked="false" class="wgsim-tb wgsim-round wgsim-chip-help"/.test(src), 'and is a toggle, not a button that opens something');

    /* maximised, the widget fills the window over the page: the same picture at a larger scale, not a second rendering of it */
    check(/class="wgsim-tb wgsim-maxbtn" data-act="maximise" aria-pressed="false"/.test(src), 'the maximise control is a toggle that says whether it is on');
    check(/\.wgsim\.wgsim-max \{[^}]*position: fixed/.test(css) && /\.wgsim\.wgsim-max \{[^}]*inset: 0/.test(css), 'and maximised the widget is fixed over the page rather than in its flow');
    check(/\.wgsim-max \.wgsim-canvas \{[^}]*flex: 1 1 auto/.test(css) && /\.wgsim-max \.wgsim-canvas svg \{[^}]*height: 100%/.test(css), 'the picture takes the height the band and the log leave it');
    check(/setMax\(on\) \{[\s\S]*?this\.spacer = document\.createElement/.test(src), 'a spacer holds its place in the flow, so the page behind neither reflows nor loses where it was');
    check(/setMax\(on\) \{[\s\S]*?document\.body\.appendChild\(this\.root\)/.test(src), 'and the widget moves to the body, where no ancestor of the page can be its containing block');
    check(/setMax\(on\) \{[\s\S]*?html\.style\.overflow = 'hidden'/.test(src) && /dispose\(\) \{[\s\S]*?this\.setMax\(false\)/.test(src), 'the document holds still while it is maximised, and a widget the page replaces gives that back');
    check(/data-act="theme"/.test(src) && /\[data-md-component="palette"\]/.test(src), 'the band carries the scheme switch while it covers the page\u2019s own, and presses that one');
    check(!/data-md-color-scheme'?\s*,/.test(src) && !/__md_set/.test(src), 'and keeps no scheme of its own: what it presses is the page\u2019s, stored where the theme stores it');
    check(/fitCanvas\(\) \{[\s\S]*?maxWidth = 'none'/.test(src) && /rebuildScene\(\) \{[\s\S]*?this\.fitCanvas\(\)/.test(src), 'and the size of the picture is one decision, made in both modes by the same function');

    /* no renderer emits a mark: the layer is the only place they exist, which is why nothing shifts when they appear */
    const sim = new Sim({ name: 'shift', sources: { q: { files: 4 } }, transformations: { a: { feeder: { from: 'q' }, packer: { size: 1 }, run: [0.2, 0.3] } }, outputs: { out: { from: 'a' } } });
    sim.start();
    for (let i = 0; i < 60; i++) sim.step(0.05);
    const it = { id: 'a', x: 0, y: 0, w: W.BODY.w, h: W.BODY.h, kind: 'node' };
    for (const [what, html] of [['the header', W.headHtml(sim, true)], ['a card', W.cardSvg(it, 'a', 'compute')], ['the pool', W.poolSvg(it, W.poolEntries(sim, sim.nodes.a), 'a').svg]]) {
      check(!html.includes('wgsim-mark'), `${what} draws no mark of its own`);
    }
  }

  // ---- the mode ends when the model moves again, and on nothing else that is not asked for ----
  //
  // Help mode holds the model, so starting it again is the way out: the playback ends the mode, as do the ? chip, the ? key
  // and Esc. Nothing ends it on a click. The rule that did ended it on every mark: opening a disclosure draws the marks layer
  // again, which detaches the very button the click is still travelling through, and a rule that asked the document where
  // that button had ended up was told it was nowhere, which it read as a click outside the widget.
  {
    const speed = src.slice(src.indexOf('setSpeed(mult) {'), src.indexOf('playPause() {'));
    check(speed.includes('this.setHelp(false)'), 'choosing a speed ends help mode');
    const play = src.slice(src.indexOf('playPause() {'), src.indexOf('toggleChip(key) {'));
    check(play.includes('this.setHelp(false)'), 'and pausing or resuming ends it');
    check(/act === 'reset'\) \{ this\.setHelp\(false\)/.test(src), 'and so does running the model again');
    check(!src.includes('onDocClick') && !src.includes('addEventListener(\'click\', this.onDoc'), 'no click on the page ends it');
    check(!/helpOn && !ev\.target\.closest/.test(src), 'and no click inside the widget ends it either');
    check(/key === '\?'/.test(src) && /key === 'Escape' && this\.helpOn/.test(src), 'the ? key and Esc still end it');
  }

  // ---- the model holds while the mode is on ----
  //
  // A mark is placed over the region it explains, so a region that has moved is one the mark is no longer over. The reader's
  // own pause is a separate thing and is not touched, which is what lets the mode hand the model back as it found it.
  {
    const held = src.slice(src.indexOf('held() {'), src.indexOf('openLineage(fileId) {'));
    check(/this\.dialogOpen\(\) \|\| this\.helpOn/.test(held), 'the model is held while help mode is on, as it is for a dialog');
    const keys = src.slice(src.indexOf("root.addEventListener('keydown'"), src.indexOf('The log follows its newest line'));
    check(keys.includes('this.dialogOpen()') && !keys.includes('this.held()'), 'and the keys that end the mode still work while it is on');
  }

  // ---- one panel at a time ----
  //
  // The ? panel is anchored under the chip and a disclosure over the region it explains, which are independent places: on a
  // widget the width of a documentation column the two overlap, and whichever is drawn second covers the other. The panel
  // introduces the marks, so a mark's own disclosure takes its place.
  {
    const open = src.slice(src.indexOf('openDisclosure(key) {'), src.indexOf('closeDisclosure() {'));
    check(/this\.guideOn = false/.test(open), 'opening a disclosure puts the ? panel away');
    check(/if \(this\.disc\)/.test(open), 'and only when one is opened, not when the same mark closes it again');
  }

  // ---- the old page is gone, and nothing is left pointing at it ----
  {
    check(!('helpSvg' in W) && !src.includes('helpSvg') && !src.includes('HELP_LEGEND'), 'the annotated screenshot and the central index are gone');
    check(!src.includes('wg-callout') && !css.includes('wg-callout'), 'and so are the callout lines that crossed it');
    check(!/drain stopping the outermost feeders|halt stopping every feeder/.test(src), 'the operator semantics are the machine buttons’ alone');
    check(W.wgOps(Object.assign(Object.create(Sim.prototype), { wg: { status: 'Active', ending: null } })).every((o) => o.tip), 'which is where they still are');
  }
  });
})(globalThis);
