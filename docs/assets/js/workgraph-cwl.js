/*
 * Workgraph playground: the CWL compiler.
 *
 * Turns a CWL v1.2 workgraph document (DX-ADR-007) into a spec the simulator's
 * engine understands (docs/dev/reference/workgraph-sim.md), reporting what it
 * could not carry across on the way. No DOM, no dependencies: the page in
 * playground.js feeds it a parsed document, and tests/workgraph-sim/cwl.test.js
 * drives it headlessly.
 *
 * Two things the compiler is deliberately not. It never follows `run`: the body
 * of a step is exactly what the simulation does not model, and a browser cannot
 * fetch a relative path out of a document it was handed. And it takes no run
 * settings from the document: durations, failure rates, the size of a query and
 * the random seed are the caller's second argument, because a document that
 * carried them would no longer be one that could really be submitted.
 */
(function (root) {
  'use strict';

  /* ------------------------------------------------------------------ */
  /* Small helpers                                                       */
  /* ------------------------------------------------------------------ */

  const isObj = (v) => !!v && typeof v === 'object' && !Array.isArray(v);
  const asList = (v) => (v == null ? [] : Array.isArray(v) ? v : [v]);
  const num = (v) => (typeof v === 'number' && isFinite(v) ? v : typeof v === 'string' && /^[-+]?[0-9.eE+]+$/.test(v) && isFinite(Number(v)) ? Number(v) : null);

  /* Thousands separators without a locale, so the checks read the same under node and the jsc shell. */
  function fmt(n) {
    const s = String(Math.round(n));
    return s.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }

  /* An action or plugin name as a sentence: `CheckSuccessRate` is `check success rate`, `PPGApproval` is `PPG approval`.
     An acronym keeps its case, since lowercasing it would make the action list unreadable. */
  function words(name) {
    return String(name == null ? '' : name)
      .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
      .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
      .split(/[\s_]+/)
      .filter(Boolean)
      .map((w) => (/^[A-Z0-9]{2,}$/.test(w) ? w : w.toLowerCase()))
      .join(' ');
  }

  const firstLine = (s) => (typeof s === 'string' ? s.trim().split('\n')[0].trim() : null);

  /* ------------------------------------------------------------------ */
  /* A small YAML reader                                                 */
  /* ------------------------------------------------------------------ */

  /* js-yaml is what the page uses, from a CDN. This is what it falls back to with the network blocked, and what the
     checks read the fixtures with, since the repo carries no YAML dependency. It covers the subset these documents are
     written in: block mappings and sequences, flow collections, quoted and block scalars, comments. It is lenient
     rather than strict — a malformed document gives a strange object rather than an error — so the page says which
     reader answered. */

  const NUMBER = /^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/;
  const indentOf = (line) => line.length - line.replace(/^ +/, '').length;

  function unquote(s) {
    const q = s[0];
    const body = s.slice(1, -1);
    if (q === "'") return body.replace(/''/g, "'");
    return body.replace(/\\(.)/g, (m, c) => ({ n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '0': '\0' }[c] !== undefined ? { n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '0': '\0' }[c] : c));
  }

  function scalar(text) {
    const s = String(text).trim();
    if (!s.length) return null;
    const q = s[0];
    if ((q === '"' || q === "'") && s.length > 1 && s[s.length - 1] === q) return unquote(s);
    if (s === '~' || s === 'null' || s === 'Null' || s === 'NULL') return null;
    if (s === 'true' || s === 'True' || s === 'TRUE') return true;
    if (s === 'false' || s === 'False' || s === 'FALSE') return false;
    if (NUMBER.test(s)) return Number(s);
    return s;
  }

  /* A `#` starts a comment only where a space precedes it, which is what keeps `cwl#` inside a URL. */
  function strip(line) {
    let q = null;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (q) {
        if (c === '\\' && q === '"') i++;
        else if (c === q) q = null;
        continue;
      }
      if (c === '"' || c === "'") { q = c; continue; }
      if (c === '#' && (i === 0 || /\s/.test(line[i - 1]))) return line.slice(0, i);
    }
    return line;
  }

  /* Where a plain key ends: a colon at the top level of the line, followed by a space or the end of it. */
  function keySplit(body) {
    let q = null;
    let depth = 0;
    for (let i = 0; i < body.length; i++) {
      const c = body[i];
      if (q) {
        if (c === '\\' && q === '"') i++;
        else if (c === q) q = null;
        continue;
      }
      if (c === '"' || c === "'") { q = c; continue; }
      if (c === '{' || c === '[') depth++;
      else if (c === '}' || c === ']') depth--;
      else if (c === ':' && depth === 0 && (i + 1 >= body.length || body[i + 1] === ' ')) return i;
    }
    return -1;
  }

  /* Flow collections: `{a: b, c: [d, e]}`. */
  function parseFlow(text) {
    const st = { s: text, i: 0 };
    return flowValue(st);
  }

  const skipWs = (st) => { while (st.i < st.s.length && /\s/.test(st.s[st.i])) st.i++; };

  function flowValue(st) {
    skipWs(st);
    const c = st.s[st.i];
    if (c === '{') return flowMap(st);
    if (c === '[') return flowSeq(st);
    return scalar(flowPlain(st));
  }

  function flowPlain(st) {
    const start = st.i;
    let q = null;
    while (st.i < st.s.length) {
      const c = st.s[st.i];
      if (q) {
        if (c === '\\' && q === '"') st.i++;
        else if (c === q) q = null;
        st.i++;
        continue;
      }
      if (c === '"' || c === "'") { q = c; st.i++; continue; }
      if (c === ',' || c === ']' || c === '}') break;
      if (c === ':' && (st.i + 1 >= st.s.length || /[\s,\]}]/.test(st.s[st.i + 1]))) break;
      st.i++;
    }
    return st.s.slice(start, st.i);
  }

  function flowMap(st) {
    const map = {};
    st.i++;
    for (;;) {
      skipWs(st);
      if (st.i >= st.s.length || st.s[st.i] === '}') { st.i++; break; }
      const key = String(scalar(flowPlain(st)));
      skipWs(st);
      let value = null;
      if (st.s[st.i] === ':') { st.i++; value = flowValue(st); }
      map[key] = value;
      skipWs(st);
      if (st.s[st.i] === ',') { st.i++; continue; }
    }
    return map;
  }

  function flowSeq(st) {
    const items = [];
    st.i++;
    for (;;) {
      skipWs(st);
      if (st.i >= st.s.length || st.s[st.i] === ']') { st.i++; break; }
      items.push(flowValue(st));
      skipWs(st);
      if (st.s[st.i] === ',') { st.i++; continue; }
    }
    return items;
  }

  function significant(st, from) {
    for (let i = from; i < st.lines.length; i++) if (strip(st.lines[i]).trim()) return i;
    return -1;
  }

  /* The value of a key, on the lines below it: a mapping indented past it, or a sequence which YAML allows at
     the key's own indent. `strict` is for a sequence item with nothing after its dash, where a sibling item at
     the same indent must not be swallowed as its value. */
  function parseValue(st, parent, strict) {
    const i = significant(st, st.i);
    if (i < 0) return null;
    const line = strip(st.lines[i]);
    const ind = indentOf(line);
    const body = line.trim();
    const dash = body[0] === '-' && (body.length === 1 || body[1] === ' ');
    if (dash && (strict ? ind > parent : ind >= parent)) { st.i = i; return parseSeq(st, ind); }
    if (!dash && ind > parent) { st.i = i; return parseMap(st, ind); }
    return null;
  }

  function parseMap(st, ind, seed) {
    const map = {};
    if (seed != null) readPair(st, ind, seed, map);
    for (;;) {
      const i = significant(st, st.i);
      if (i < 0) break;
      const line = strip(st.lines[i]);
      const body = line.trim();
      if (indentOf(line) !== ind) break;
      if (body[0] === '-' && (body.length === 1 || body[1] === ' ')) break;
      st.i = i + 1;
      readPair(st, ind, body, map);
    }
    return map;
  }

  function parseSeq(st, ind) {
    const items = [];
    for (;;) {
      const i = significant(st, st.i);
      if (i < 0) break;
      const line = strip(st.lines[i]);
      const body = line.trim();
      if (indentOf(line) !== ind) break;
      if (!(body[0] === '-' && (body.length === 1 || body[1] === ' '))) break;
      const after = line.slice(ind + 1);
      const col = ind + 1 + (after.length - after.replace(/^ +/, '').length);
      const content = body.slice(1).trim();
      st.i = i + 1;
      if (!content.length) items.push(parseValue(st, ind, true));
      else if (content[0] === '{' || content[0] === '[') items.push(parseFlow(content));
      else if (keySplit(content) >= 0) items.push(parseMap(st, col, content));
      else items.push(scalar(content));
    }
    return items;
  }

  function readPair(st, ind, body, map) {
    const k = keySplit(body);
    if (k < 0) { map[String(scalar(body))] = null; return; }
    const key = String(scalar(body.slice(0, k)));
    const rest = body.slice(k + 1).trim();
    if (!rest.length) map[key] = parseValue(st, ind);
    else if (rest[0] === '|' || rest[0] === '>') map[key] = blockScalar(st, ind, rest);
    else if (rest[0] === '{' || rest[0] === '[') map[key] = parseFlow(rest);
    else map[key] = scalar(rest);
  }

  function blockScalar(st, ind, header) {
    const fold = header[0] === '>';
    const chomp = header.indexOf('-') >= 0 ? 'strip' : header.indexOf('+') >= 0 ? 'keep' : 'clip';
    const out = [];
    let base = null;
    while (st.i < st.lines.length) {
      const raw = st.lines[st.i];
      if (!raw.trim()) { out.push(''); st.i++; continue; }
      const k = indentOf(raw);
      if (k <= ind) break;
      if (base === null) base = k;
      out.push(raw.slice(Math.min(base, k)));
      st.i++;
    }
    while (out.length && !out[out.length - 1]) out.pop();
    let text = fold ? out.reduce((acc, l) => (acc === '' ? l : l === '' ? acc + '\n' : /^\s/.test(l) ? acc + '\n' + l : acc + ' ' + l), '') : out.join('\n');
    if (chomp === 'clip' && text.length) text += '\n';
    return text;
  }

  function parseYaml(text) {
    const st = { lines: String(text == null ? '' : text).replace(/\r\n?/g, '\n').split('\n'), i: 0 };
    for (const line of st.lines) if (/^\t+/.test(line)) throw new Error('the document is indented with tabs, which YAML does not allow');
    for (;;) {
      const i = significant(st, st.i);
      if (i < 0) return null;
      const body = strip(st.lines[i]).trim();
      if (body === '---' || body[0] === '%') { st.i = i + 1; continue; }
      break;
    }
    return parseValue(st, -1);
  }

  /* ------------------------------------------------------------------ */
  /* Paths into lines                                                    */
  /* ------------------------------------------------------------------ */

  /* The line a problem's path names, 1-based, so the page can reveal it. Best effort over the text rather than over
     the parsed object, so it works whichever reader parsed the document, and it degrades to the deepest line it did
     find rather than to nothing. */
  function locate(text, path) {
    if (!path) return 1;
    const lines = String(text == null ? '' : text).replace(/\r\n?/g, '\n').split('\n');
    const segments = String(path).split('.').filter((s) => s.length);
    let start = 0;
    let stop = lines.length;
    let parent = -1;
    let found = 1;
    for (const seg of segments) {
      const wantIndex = /^\d+$/.test(seg);
      let ind = null;
      let hit = -1;
      let seen = 0;
      for (let i = start; i < stop; i++) {
        const line = strip(lines[i]);
        const body = line.trim();
        if (!body) continue;
        const k = indentOf(line);
        const dash = body[0] === '-' && (body.length === 1 || body[1] === ' ');
        if (k < parent || (k === parent && !dash)) break;
        if (ind === null) ind = k;
        if (k !== ind) continue;
        if (wantIndex) {
          if (dash && seen++ === Number(seg)) { hit = i; break; }
          continue;
        }
        if (dash) continue;
        const c = keySplit(body);
        if (c < 0) continue;
        if (String(scalar(body.slice(0, c))) === seg) { hit = i; break; }
      }
      if (hit < 0) return found;
      found = hit + 1;
      parent = ind;
      start = hit + 1;
    }
    return found;
  }

  /* ------------------------------------------------------------------ */
  /* CWL shapes                                                          */
  /* ------------------------------------------------------------------ */

  /* Every CWL idmap field, in one shape: a list of objects carrying `id`, in document order. The forms are a mapping
     keyed by id, a list of objects with `id:`, a list of bare strings, and — in `in:` alone — a mapping whose value is
     the source rather than a body, which is what `scalarField` names. The LHCb converter emits the mapping form
     throughout and the ADRs are written in the list form, so both have to arrive at the same place. */
  function idmap(value, scalarField) {
    const out = [];
    if (value == null) return out;
    if (Array.isArray(value)) {
      for (const item of value) {
        if (item == null) continue;
        if (typeof item === 'string') out.push({ id: item });
        else if (isObj(item)) out.push(Object.assign({}, item, { id: item.id == null ? null : String(item.id) }));
      }
      return out;
    }
    if (!isObj(value)) return out;
    for (const [id, body] of Object.entries(value)) {
      if (isObj(body)) out.push(Object.assign({}, body, { id }));
      else if (scalarField && body != null) out.push({ id, [scalarField]: body });
      else out.push({ id });
    }
    return out;
  }

  /* `hints` keyed by class, from either the map form (canonical) or the list form. A class is matched on its local
     name, so `dirac:Workgraph` and the full URI both land on the same key. */
  const HINT_CLASSES = ['Workgraph', 'Transformation', 'Feeder', 'Job'];

  function hintKey(name) {
    const local = String(name == null ? '' : name).split(/[#/]/).pop().split(':').pop();
    const known = HINT_CLASSES.find((c) => c === local);
    return known ? 'dirac:' + known : String(name);
  }

  function hintMap(value) {
    const map = {};
    if (value == null) return map;
    if (Array.isArray(value)) {
      for (const item of value) {
        if (!isObj(item) || item.class == null) continue;
        const body = Object.assign({}, item);
        delete body.class;
        map[hintKey(item.class)] = body;
      }
      return map;
    }
    if (!isObj(value)) return map;
    for (const [name, body] of Object.entries(value)) map[hintKey(name)] = isObj(body) ? body : {};
    return map;
  }

  /* `dirac:Feeder` sits on an input parameter as an extension field, because CWL input parameters have no hints slot
     (DX-ADR-007). A document that writes one anyway is read the same way. */
  function feederOf(input) {
    if (!isObj(input)) return null;
    for (const key of Object.keys(input)) if (hintKey(key) === 'dirac:Feeder' && isObj(input[key])) return input[key];
    const hints = hintMap(input.hints);
    return isObj(hints['dirac:Feeder']) ? hints['dirac:Feeder'] : null;
  }

  /* A step input's source, as `{step, port}` for another step's output or `{input}` for a workflow input. */
  function refOf(source) {
    if (typeof source !== 'string') return null;
    const s = source.replace(/^#/, '');
    const slash = s.lastIndexOf('/');
    if (slash < 0) return { input: s };
    return { step: s.slice(0, slash), port: s.slice(slash + 1) };
  }

  /* ------------------------------------------------------------------ */
  /* Run settings                                                        */
  /* ------------------------------------------------------------------ */

  /* None of this belongs to a workgraph, which is why none of it is read from the document. How long a job takes, how
     often one fails, how many files a query returns and how many parcels run at once are properties of a site and of a
     workgraph's scale; a `dirac:` field carrying them would break the one property this page exists to demonstrate,
     which is that what you are editing would really submit. */
  const DEFAULT_RUN_SETTINGS = {
    seed: 1,
    slots: 4, /* parcels a transformation runs at once */
    filesPerQuery: 120, /* files a query feeder is given, since no query is evaluated here */
    maxPool: 300, /* the playground's ceiling on any pool: real workgraphs ask for millions */
    scoutSample: 12, /* the scouting sample, and the ceiling on the one a document asks for */
    jobMin: 1.6,
    jobMax: 3.2,
    fail: 0.08,
    partial: 0,
  };

  /* What the settings panel offers, in order: the field, its label, and the bounds a number is held to. */
  const RUN_SETTING_FIELDS = [
    { key: 'slots', label: 'parcels at once', min: 1, max: 9, step: 1, hint: 'slots on every card' },
    { key: 'filesPerQuery', label: 'files per query', min: 10, max: 300, step: 10, hint: 'what a feeder plugin would have returned' },
    { key: 'maxPool', label: 'pool ceiling', min: 20, max: 400, step: 20, hint: 'every pool is clamped to this' },
    { key: 'scoutSample', label: 'scouting sample', min: 2, max: 60, step: 2, hint: 'and the ceiling on the sample a document asks for' },
    { key: 'jobMin', label: 'job seconds, least', min: 0.2, max: 10, step: 0.2, hint: 'model seconds' },
    { key: 'jobMax', label: 'job seconds, most', min: 0.4, max: 20, step: 0.2, hint: 'model seconds' },
    { key: 'fail', label: 'failure rate', min: 0, max: 0.9, step: 0.01, hint: 'per parcel' },
    { key: 'partial', label: 'partial rate', min: 0, max: 0.9, step: 0.01, hint: 'parcels that finish part of each input' },
    { key: 'seed', label: 'random seed', min: 1, max: 999999, step: 1, hint: 'the same seed is the same run' },
  ];

  /* ------------------------------------------------------------------ */
  /* The compiler                                                        */
  /* ------------------------------------------------------------------ */

  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  const safeId = (id) => String(id).replace(/[^A-Za-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '') || 'x';
  const unique = (list) => list.filter((v, i) => list.indexOf(v) === i);
  /* The identities a pool carries, which is what a packer grouping by run or by storage groups by: the model's own
     count, and the `colours` a query source is given below. */
  const IDENTITIES_DRAWN = 3;

  /* A feeder that issues seeds rather than evaluating a query (DX-ADR-006). Named by its plugin, or by asking for a
     number of events, since a seed feeder is the one that counts what it produces rather than what it finds. */
  function isSeedFeeder(f) {
    if (!isObj(f)) return false;
    const args = isObj(f.args) ? f.args : {};
    if (/seed/i.test(String(f.name || ''))) return true;
    return args.target_events != null || args.events_per_seed != null || args.seeds != null;
  }

  /* An action list, as the engine takes it: each entry is an action plugin and its arguments, named as a sentence.
     A bare name is still read, so a document written before the schema required the mapping form still compiles. */
  function actionList(value) {
    return asList(value)
      .map((a) => {
        if (typeof a === 'string') return { name: words(a) };
        if (!isObj(a)) return null;
        if (a.name != null) return Object.assign({}, a, { name: String(a.name) });
        if (a.action != null) return { name: words(a.action) };
        return null;
      })
      .filter(Boolean);
  }

  function compile(doc, settings) {
    const S = Object.assign({}, DEFAULT_RUN_SETTINGS, settings || {});
    const problems = [];
    const ignored = [];
    const sandbox = [];
    const notes = [];
    const err = (path, rule, message) => problems.push({ path, severity: 'error', rule, message });
    const warn = (path, rule, message) => problems.push({ path, severity: 'warning', rule, message });
    const spec = {
      name: 'workgraph',
      seed: Math.max(1, Math.round(num(S.seed) || 1)),
      /* Built is not running: the model waits in New, as every model the documentation embeds does, and the reader
         presses play. A workgraph that started itself would be a page that begins by moving. */
      start: 'manual',
      slots: clamp(Math.round(num(S.slots) || 4), 1, 9),
      workgraph: {},
      sources: {},
      transformations: {},
      outputs: {},
    };
    const done = () => ({ spec, problems, ignored: ignored.join('\n'), sandbox: sandbox.join('\n'), notes: notes.join('\n') });

    if (!isObj(doc)) {
      err('', 'not-a-workgraph', 'the document is not a CWL mapping');
      return done();
    }
    if (doc.class !== 'Workflow') err('class', 'not-a-workgraph', `a workgraph is a CWL Workflow; this document's class is ${doc.class == null ? 'missing' : doc.class}`);
    const hints = hintMap(doc.hints);
    const W = isObj(hints['dirac:Workgraph']) ? hints['dirac:Workgraph'] : null;
    if (!W) err('hints', 'not-a-workgraph', 'no dirac:Workgraph hint: without one this is a plain CWL workflow rather than a workgraph (DX-ADR-007)');
    const wg = W || {};
    spec.name = String(doc.label || doc.id || wg.type || 'workgraph');

    const requirementNames = (value) => (Array.isArray(value) ? value.map((r) => (isObj(r) ? String(r.class) : String(r))) : isObj(value) ? Object.keys(value) : []);
    const reqs = requirementNames(doc.requirements);
    if (isObj(wg.hooks)) ignored.push(`the workgraph's hooks ${Object.keys(wg.hooks).join(', ')} — the model runs its own, so no binding is resolved here; only a ScoutingToApproving's stages are read, for the size of the scout`);
    if (reqs.length) ignored.push(`the workflow's requirements: ${reqs.join(', ')} — what a job asks of a site is the run settings' business here`);

    /* ---- workflow inputs: seeds, queries, and everything that is only a parameter ---- */

    const queries = {};
    const seedFeeders = {};
    for (const input of idmap(doc.inputs)) {
      if (input.id == null) continue;
      const f = feederOf(input);
      if (!f) {
        const dflt = input.default != null ? ` (default ${typeof input.default === 'string' ? `'${input.default}'` : JSON.stringify(input.default)})` : '';
        ignored.push(`${input.id}: a job parameter, not a pool${dflt}`);
        continue;
      }
      if (isSeedFeeder(f)) seedFeeders[input.id] = { feeder: f, input };
      else queries[input.id] = { feeder: f, input };
    }

    const queryFiles = clamp(Math.round(num(S.filesPerQuery) || 120), 1, Math.round(num(S.maxPool) || 300));
    for (const [id, q] of Object.entries(queries)) {
      spec.sources[safeId(id)] = { label: String(q.input.label || id), files: queryFiles, types: ['circle', 'triangle', 'diamond'], colours: 3 };
      notes.push(`${q.feeder.name || 'the feeder'} on ${id}: no query is evaluated here, so the pool is the playground's ${fmt(queryFiles)} files`);
    }
    /* A feeder that derives one input from another's ancestors is what the model's `ancestors` is: the partner file a
       lookup packer adds to each parcel (DX-ADR-006). The ancestor query gets a shape of its own so the two read apart. */
    for (const [id, q] of Object.entries(queries)) {
      const args = isObj(q.feeder.args) ? q.feeder.args : {};
      const of = args.of != null ? String(args.of) : args.ancestors_of != null ? String(args.ancestors_of) : null;
      if (!of || !queries[of]) continue;
      spec.sources[safeId(of)].ancestors = safeId(id);
      spec.sources[safeId(id)].types = ['square'];
    }

    /* ---- steps ---- */

    /* `initial_state` is the one override on where a member starts, and it reads the same on a step as on a
       data-management declaration: `Stopped` holds it back until the workgraph is approved, `Paused` until an operator
       starts it, `Active` puts it in the scout whatever its position would have said. A member that states one is
       recorded here, so that the position rule below leaves it alone. A value that is none of the three is not an
       override, so it falls through to the rule and says so. */
    const stated = new Set();
    function initialState(node, given, path, label) {
      if (given == null) return;
      const state = String(given);
      if (/^stopped$/i.test(state)) node.hold = 'approval';
      else if (/^paused$/i.test(state)) node.hold = 'operator';
      else if (!/^active$/i.test(state)) {
        warn(path, 'initial-state', `${label} asks to start ${state}, which is not a state a member starts in: Active puts it in the scout, Stopped holds it back until the workgraph is approved, Paused until an operator starts it (DX-ADR-005)`);
        return;
      }
      stated.add(node.id);
    }

    const steps = idmap(doc.steps).filter((s) => s.id != null);
    const stepIds = new Set(steps.map((s) => String(s.id)));
    const outsOf = {};
    for (const step of steps) outsOf[String(step.id)] = idmap(step.out).map((o) => String(o.id));

    /* One transformation id per step, kept clear of the source ids so `feeder.from` can never mean two things. */
    const nodeId = {};
    for (const step of steps) {
      let id = safeId(step.id);
      if (spec.sources[id]) id = id + '-step';
      nodeId[String(step.id)] = id;
    }

    const consumed = new Set();
    const dataRefs = [];

    for (const step of steps) {
      const sid = String(step.id);
      const id = nodeId[sid];
      const path = `steps.${sid}`;
      const sh = hintMap(step.hints);
      const T = isObj(sh['dirac:Transformation']) ? sh['dirac:Transformation'] : null;
      if (!T) err(`${path}.hints`, 'bare-step', `${sid} carries no dirac:Transformation hint, and a bare step is a submission-time error: a transformation boundary is marked rather than inferred (DX-ADR-007)`);
      const t = T || {};

      if (step.scatter != null) warn(`${path}.scatter`, 'scatter', `scatter is not modelled: ${sid} is drawn as if it were absent, one pool and one packer over everything the feeder yields`);
      if (step.when != null) warn(`${path}.when`, 'when', `when is not modelled: ${sid} is drawn as if it were absent, as though the condition held for every parcel`);
      if (isObj(step.run) && step.run.class === 'ExpressionTool') warn(`${path}.run`, 'expression-tool', `an ExpressionTool step is not modelled: ${sid} is drawn as an ordinary transformation, though nothing in it would reach a backend`);
      if (typeof step.run === 'string') ignored.push(`${sid}: run body ${step.run} (not loaded)`);
      else if (isObj(step.run)) ignored.push(`${sid}: run body ${step.run.class === 'Workflow' ? 'a nested Workflow' : step.run.class || 'inline'} (not loaded)`);
      const sreqs = requirementNames(step.requirements);
      if (sreqs.length) ignored.push(`${sid}: requirements ${sreqs.join(', ')} (not modelled)`);

      /* Which of the step's inputs drive the pool, in document order. Everything else is a job parameter: a value the
         resolution layer injects per parcel, which says nothing about how the work is divided. */
      const driving = [];
      for (const entry of idmap(step.in, 'source')) {
        const first = asList(entry.source)[0];
        const ref = refOf(first);
        if (!ref) continue;
        if (ref.step != null) {
          if (!stepIds.has(ref.step)) {
            err(`${path}.in`, 'unknown-source', `${sid}.${entry.id} sources ${first}, and no step called ${ref.step} produces it`);
            continue;
          }
          if (outsOf[ref.step].indexOf(ref.port) < 0) {
            err(`${path}.in`, 'unknown-source', `${sid}.${entry.id} sources ${first}, and ${ref.step} declares no output called ${ref.port}`);
            continue;
          }
          consumed.add(`${ref.step}/${ref.port}`);
          driving.push({ id: String(entry.id), step: ref.step, port: ref.port });
          continue;
        }
        if (seedFeeders[ref.input]) { driving.push({ id: String(entry.id), seed: ref.input }); continue; }
        if (queries[ref.input]) { driving.push({ id: String(entry.id), source: ref.input }); continue; }
      }

      let drive = null;
      if (t.driving_input != null) {
        drive = driving.find((d) => d.id === String(t.driving_input)) || null;
        if (!drive) err(`${path}.hints`, 'driving-input', `driving_input names ${t.driving_input}, which is not one of ${sid}'s pool-driving inputs (${driving.map((d) => d.id).join(', ') || 'it has none'})`);
      }
      if (!drive) {
        if (!driving.length) err(`${path}.in`, 'no-driving-input', `${sid} has no input that drives its pool: a transformation is fed either from an upstream step's output or from a workflow input carrying a dirac:Feeder`);
        else if (driving.length > 1) err(`${path}.hints`, 'ambiguous-driving-input', `${sid} has ${driving.length} pool-driving inputs (${driving.map((d) => d.id).join(', ')}); name one in dirac:Transformation.driving_input and the rest become the packer's join`);
        drive = driving[0] || null;
      }

      /* The packer: its group size, and the grouping the model can draw. A run or a storage is not in the model, so
         grouping by one is drawn as grouping by the file's source identity, which is what the note says. */
      const pk = isObj(t.packer) ? t.packer : {};
      const pargs = isObj(pk.args) ? pk.args : {};
      const pname = String(pk.name || '');
      const packer = { size: Math.max(1, Math.round(num(pargs.group_size) || 1)) };
      if (/run|storage/i.test(pname) || pargs.keep_storage_together === true || pargs.keep_run_together === true) {
        packer.by = 'colour';
        notes.push(`${sid}: ${pname || 'the packer'} keeps a run or a storage together, which is drawn as the model's source identity`);
      }

      const node = { id, label: String(step.label || sid), kind: 'compute', packer };
      const tip = firstLine(step.doc);
      if (tip) node.doc = tip;
      node.slots = spec.slots;
      node.run = [num(S.jobMin) || 1.6, Math.max(num(S.jobMin) || 1.6, num(S.jobMax) || 3.2)];
      node.fail = clamp(num(S.fail) != null ? num(S.fail) : 0.08, 0, 1);
      node.partial = clamp(num(S.partial) != null ? num(S.partial) : 0, 0, 1);

      const feeder = { from: [], after: asList(t.after).map(String) };
      if (drive && drive.seed != null) Object.assign(feeder, seedArgs(drive.seed, id));
      else if (drive && drive.source != null) feeder.from = [safeId(drive.source)];
      else if (drive && drive.step != null) {
        feeder.from = [nodeId[drive.step]];
        if (outsOf[drive.step].indexOf(drive.port) > 0) feeder.port = 'artifact';
      }
      node.feeder = feeder;

      /* A second pool-driving input is either a partner the packer looks up, where it comes from a query, or the join
         of a joining packer, where another transformation produces it (DX-ADR-006). */
      for (const extra of driving) {
        if (extra === drive) continue;
        if (extra.step != null) packer.join = nodeId[extra.step];
        else if (extra.source != null && drive && drive.source != null) {
          packer.lookup = safeId(extra.source);
          spec.sources[safeId(drive.source)].ancestors = safeId(extra.source);
          spec.sources[safeId(extra.source)].types = ['square'];
        } else if (extra.source != null) warn(`${path}.in`, 'second-input', `${sid}.${extra.id} is a second pool-driving input the model cannot carry: a lookup adds a partner to each input of a query-fed pool, and ${sid} is not fed from a query`);
      }

      const lists = isObj(t.actions) ? t.actions : {};
      node.finalize = actionList(lists.Finalizing);
      if (lists.Archiving != null) node.archive = actionList(lists.Archiving);
      if (lists.Cleaning != null) node.clean = actionList(lists.Cleaning);
      if (t.output_sandbox != null) sandbox.push(`${sid}: ${asList(t.output_sandbox).join(', ')}`);
      if (isObj(t.hooks)) ignored.push(`${sid}: its hooks ${Object.keys(t.hooks).join(', ')} — the model runs its own, so a binding is not resolved here`);
      if (t.requirements_template != null) ignored.push(`${sid}: its requirements_template — what its parcels ask of a site is the run settings' business here`);

      initialState(node, t.initial_state, `${path}.hints.dirac:Transformation.initial_state`, sid);
      spec.transformations[id] = node;
    }

    /* ---- the seed feeder's arguments, clamped to something a page can draw ---- */

    function seedArgs(inputId, owner) {
      const f = seedFeeders[inputId].feeder;
      const args = isObj(f.args) ? f.args : {};
      const maxPool = clamp(Math.round(num(S.maxPool) || 300), 1, 100000);
      const per = num(args.events_per_seed);
      const events = num(args.target_events);
      let asked = num(args.seeds);
      if (asked == null && events != null) asked = Math.ceil(events / (per || 1));
      if (asked == null) asked = maxPool;
      const seeds = Math.max(1, Math.min(maxPool, asked));
      const ratio = seeds / asked;
      if (seeds < asked) {
        notes.push(
          events != null && per != null
            ? `${owner}: ${fmt(events)} events at ${fmt(per)} a seed clamped to ${fmt(seeds)} seeds`
            : `${owner}: ${fmt(asked)} seeds clamped to ${fmt(seeds)}`
        );
      }
      const out = { from: [], seeds };
      const scale = (v, floor) => Math.max(floor, Math.min(seeds, Math.max(1, Math.round(v * ratio))));
      const batch = num(args.batch);
      const inflight = num(args.max_in_flight != null ? args.max_in_flight : args.inflight);
      /* The ceiling has to sit below the pool or it can never bind, and then it is not a ceiling. `inFlight` counts what
         the feeder has been asked for but not issued plus every live input of the compute members under it, which cannot
         reach the pool's own size, so a ceiling clamped to `seeds` leaves the Active hook's condition true on every sweep
         and the feeder climbs to the whole pool in a run of near-identical steps with nothing happening between them.
         Half a batch is what the engine gives a batched feeder that names no ceiling, so it is the cap here too: the hook
         tops the feeder up once the work outstanding has drained, which is the cycle the batch exists to show. A document
         whose own ceiling is smaller than that keeps it. */
      const cap = (v) => Math.max(1, Math.min(out.batch ? Math.ceil(out.batch / 2) : seeds, Math.round(v)));
      if (batch != null) {
        /* A batch scaled straight down can be a seed or two, which leaves the Active hook raising the feeder a seed at
           a time for the rest of the run. A tenth of the pool is the floor, and the note says both numbers. */
        out.batch = ratio < 1 ? scale(batch, Math.ceil(seeds / 10)) : Math.max(1, Math.min(seeds, Math.round(batch)));
        if (inflight != null) out.inflight = cap(ratio < 1 ? inflight * ratio : inflight);
        if (ratio < 1) notes.push(`${owner}: a batch of ${fmt(batch)} and ${inflight != null ? `${fmt(inflight)} in flight` : 'no ceiling'} scaled to ${fmt(out.batch)}${inflight != null ? ` and ${fmt(out.inflight)}` : ''}`);
      } else if (inflight != null) out.inflight = cap(ratio < 1 ? inflight * ratio : inflight);
      return out;
    }

    /* ---- what the workgraph delivers ---- */

    for (const o of idmap(doc.outputs)) {
      if (o.id == null) continue;
      const oid = String(o.id);
      const refs = asList(o.outputSource).map((s) => refOf(s)).filter(Boolean);
      const good = [];
      for (const r of refs) {
        if (r.step == null || !stepIds.has(r.step) || outsOf[r.step].indexOf(r.port) < 0) {
          err(`outputs.${oid}.outputSource`, 'unknown-source', `the output ${oid} sources ${r.step ? `${r.step}/${r.port}` : r.input}, which no step produces`);
          continue;
        }
        consumed.add(`${r.step}/${r.port}`);
        good.push(r);
      }
      if (!good.length) continue;
      const box = { from: nodeId[good[0].step], label: String(o.label || oid) };
      if (outsOf[good[0].step].indexOf(good[0].port) > 0) box.port = 'artifact';
      spec.outputs[safeId(oid)] = box;
      if (good.length > 1) warn(`outputs.${oid}.outputSource`, 'many-sources', `${oid} collects several transformations; the box is drawn over ${good[0].step} alone, and ${good.slice(1).map((r) => `${r.step}/${r.port}`).join(', ')} is dropped`);
    }

    /* Where a data-management declaration's files come from: a step's output, a workgraph output, or a workgraph input
       whose feeder evaluates a query, as when a file staged to a buffer is taken back once the step has processed it. */
    function filesRef(value) {
      const raw = value == null ? '' : String(value);
      const ref = refOf(raw);
      if (!ref) return null;
      if (ref.step != null) {
        if (!stepIds.has(ref.step) || outsOf[ref.step].indexOf(ref.port) < 0) return null;
        consumed.add(`${ref.step}/${ref.port}`);
        return { from: nodeId[ref.step], port: outsOf[ref.step].indexOf(ref.port) > 0 ? 'artifact' : null };
      }
      const box = spec.outputs[safeId(ref.input)];
      if (box) return { from: box.from, port: box.port || null };
      if (queries[ref.input]) return { from: safeId(ref.input), port: null };
      return null;
    }

    /* ---- data management: nodes of the graph that runs, without being steps of the document ---- */

    for (const [i, dm] of asList(wg.data_management).entries()) {
      if (!isObj(dm)) continue;
      const path = `hints.dirac:Workgraph.data_management.${i}`;
      const given = dm.id != null ? String(dm.id) : `data-transformation-${i + 1}`;
      let id = safeId(given);
      if (spec.sources[id] || spec.transformations[id]) id = id + '-dm';
      const ref = filesRef(dm.files);
      if (!ref) {
        err(`${path}.files`, 'unknown-source', `${given} acts on ${dm.files}, which is neither a step's output, nor a declared output, nor a workflow input with a feeder`);
        continue;
      }
      const pargs = isObj(dm.packer) && isObj(dm.packer.args) ? dm.packer.args : {};
      const pname = isObj(dm.packer) ? String(dm.packer.name || '') : '';
      const packer = { size: Math.max(1, Math.round(num(pargs.group_size) || 1)) };
      if (/run|storage/i.test(pname) || pargs.keep_storage_together === true) packer.by = 'colour';
      const node = {
        id,
        label: given,
        kind: /remov|delete/i.test(String(dm.operation || '')) ? 'removal' : 'replication',
        feeder: { from: [ref.from], after: asList(dm.after).map(String) },
        packer,
        finalize: actionList(isObj(dm.actions) ? dm.actions.Finalizing : null),
        slots: spec.slots,
      };
      if (ref.port) node.feeder.port = ref.port;
      if (isObj(dm.actions) && dm.actions.Archiving != null) node.archive = actionList(dm.actions.Archiving);
      if (isObj(dm.actions) && dm.actions.Cleaning != null) node.clean = actionList(dm.actions.Cleaning);
      initialState(node, dm.initial_state, `${path}.initial_state`, given);
      const where = [];
      if (dm.destination != null) where.push(`to ${asList(dm.destination).join(', ')}`);
      if (dm.storage != null) where.push(`at ${asList(dm.storage).join(', ')}`);
      if (where.length) ignored.push(`${given}: ${where.join(' ')} — the model moves files without naming a storage`);
      if (dm.output_sandbox != null) sandbox.push(`${given}: ${asList(dm.output_sandbox).join(', ')}`);
      spec.transformations[id] = node;
    }

    /* A step's first output is the transformation's own; a second one is the artifact port, which a producer only
       emits on when it is told to (DX-ADR-006's histograms, and the model's `artifact`). Nothing else in the model has
       a second port, so a third declared output that someone consumes is as far as the picture goes. */
    for (const n of Object.values(spec.transformations)) {
      if (n.feeder.port !== 'artifact') continue;
      const producer = spec.transformations[n.feeder.from[0]];
      if (producer) producer.artifact = true;
    }
    for (const box of Object.values(spec.outputs)) {
      if (box.port !== 'artifact') continue;
      const producer = spec.transformations[box.from];
      if (producer) producer.artifact = true;
    }
    for (const step of steps) {
      const extra = outsOf[String(step.id)].slice(2).filter((out) => consumed.has(`${step.id}/${out}`));
      if (extra.length) warn(`steps.${step.id}.out`, 'ports', `${step.id} declares ${outsOf[String(step.id)].length} outputs, and a transformation has two ports: ${extra.join(', ')} is drawn on the artifact port with the second`);
    }

    /* ---- what the document said that the picture has to answer for ---- */

    for (const step of steps) {
      const sid = String(step.id);
      for (const out of outsOf[sid]) {
        if (consumed.has(`${sid}/${out}`)) continue;
        err(`${'steps.' + sid}.out`, 'orphan-output', `${sid}/${out} is neither consumed by another step nor declared as a workgraph output, and a step output that is neither is an error at submission time (DX-ADR-007)`);
      }
    }

    const known = new Set(Object.keys(spec.transformations));
    for (const [id, n] of Object.entries(spec.transformations)) {
      n.feeder.after = n.feeder.after.map((a) => (known.has(a) ? a : nodeId[a] && known.has(nodeId[a]) ? nodeId[a] : a));
      const missing = n.feeder.after.filter((a) => !known.has(a));
      if (missing.length) {
        warn(`steps.${id}`, 'unknown-after', `${n.label} waits for ${missing.join(', ')}, which this document does not declare; the wait is dropped`);
        n.feeder.after = n.feeder.after.filter((a) => known.has(a));
      }
      if (n.packer.join && !known.has(n.packer.join)) delete n.packer.join;
    }

    /* A cycle in the dataflow. `after` is not dataflow — a removal declared against a workgraph input waits for the
       step that reads the same query, which is a wait pointing backwards and perfectly legal — so only the feeders' own
       edges are walked. */
    {
      const state = {};
      const cycles = [];
      const walk = (id, trail) => {
        if (state[id] === 'done') return;
        if (state[id] === 'open') { cycles.push(trail.slice(trail.indexOf(id)).concat(id).join(' → ')); return; }
        state[id] = 'open';
        for (const from of spec.transformations[id].feeder.from) if (spec.transformations[from]) walk(from, trail.concat(id));
        state[id] = 'done';
      };
      for (const id of Object.keys(spec.transformations)) walk(id, []);
      for (const c of unique(cycles)) err('steps', 'cycle', `the dataflow has a cycle: ${c}`);
    }

    if (!Object.keys(spec.transformations).length) err('steps', 'empty-workgraph', 'the workgraph has no transformations: a workgraph is a DAG of steps, each carrying a dirac:Transformation hint');

    /* A feeder's ceiling counts inputs outstanding below it, and a chain of packers always holds some of them back: a
       packer that groups by ten keeps up to nine of each identity waiting for the tenth, and that backlog only clears
       once the feeder is off. A ceiling under it is one the Active hook can never satisfy, so the feeder is never raised
       again and the workgraph stops where it stands rather than climbing. The ceiling is lifted clear of what the chain
       below can park — the packers alone, since work in a slot is moving — and stays under the pool, which is the other
       half of being a ceiling at all. */
    {
      const parked = (id) => {
        let total = 0;
        const seen = new Set();
        const stack = [id];
        while (stack.length) {
          const cur = stack.pop();
          if (seen.has(cur) || !spec.transformations[cur]) continue;
          seen.add(cur);
          const n = spec.transformations[cur];
          if (n.kind && n.kind !== 'compute') continue;
          /* a packer that keeps a run or a storage together waits for a group of each identity the pool carries */
          total += Math.max(0, n.packer.size - 1) * (n.packer.by === 'colour' ? IDENTITIES_DRAWN : 1);
          for (const m of Object.values(spec.transformations)) if (m.feeder.from.indexOf(cur) >= 0) stack.push(m.id);
        }
        return total;
      };
      for (const n of Object.values(spec.transformations)) {
        const f = n.feeder;
        if (!f.seeds || f.inflight == null) continue;
        const lifted = clamp(Math.max(f.inflight, parked(n.id) + 1), 1, f.seeds);
        if (lifted !== f.inflight) notes.push(`${n.label}: a ceiling of ${fmt(f.inflight)} in flight lifted to ${fmt(lifted)}, clear of what the packers below it hold back`);
        f.inflight = lifted;
      }
    }

    /* ---- scouting, its approving list, and the target that ends the workgraph ---- */

    /* Which members run during the scout is the graph's to say, not each declaration's: a member runs if it is a compute
       step or an ancestor of one, so a staging replication a step waits for is in the scout and data management that only
       sits downstream — replicating an output, removing an intermediate — is held until the workgraph is approved. What
       tells upstream from downstream is `after` and the dataflow together, never `files`: in sprucing the staging and the
       buffer removal both act on raw-data, and only `after: [sprucing]` separates them. `initial_state` overrides it, on
       a step and on a data-management declaration alike, and has already spoken where it was given. */
    {
      const down = {};
      for (const n of Object.values(spec.transformations))
        for (const up of unique(n.feeder.from.concat(n.feeder.after))) (down[up] = down[up] || []).push(n.id);
      const reaches = {};
      /* `after` may point backwards, so the walk has to survive a cycle: an id is false while it is open, which is the
         honest answer to "can a compute step be reached by coming back round to where we started". */
      const walk = (id) => {
        if (reaches[id] != null) return reaches[id];
        reaches[id] = false;
        const kind = spec.transformations[id].kind;
        let hit = !kind || kind === 'compute';
        for (const next of down[id] || []) if (walk(next)) hit = true;
        reaches[id] = hit;
        return hit;
      };
      for (const [id, n] of Object.entries(spec.transformations)) if (!stated.has(id) && !walk(id)) n.hold = 'scouting';
    }

    const approving = asList(wg.approving)
      .map((a) => {
        if (typeof a === 'string') return { name: words(a) };
        if (!isObj(a)) return null;
        const plugin = String(a.action != null ? a.action : a.name != null ? a.name : 'action');
        const flat = plugin.replace(/[\s_]/g, '').toLowerCase();
        const act = { name: words(plugin) };
        /* A sign-off is the one action that waits for a person: it is Failed until someone gives it, which is what
           makes the workgraph show ApprovingBlocked with the sign-off offered (DX-ADR-005, DX-ADR-006). */
        if (/manualapproval/.test(flat)) act.manual = true;
        if (/successrate/.test(flat)) {
          act.check = 'success rate';
          const args = isObj(a.args) ? a.args : {};
          act.minRate = num(args.min_success_rate);
          act.minPassed = num(args.min_passed);
        }
        return act;
      })
      .filter(Boolean);

    /* Whether the workgraph scouts at all is derived too: it scouts when there is something to scout — a member the rule
       above leaves running — and something to judge. A scout nobody judges is not a scout, it is the first parcels of the
       run; and a judgement with nothing running under it has nothing to judge. */
    const runsInScout = Object.values(spec.transformations).some((n) => !n.hold);
    const ceiling = clamp(Math.round(num(S.scoutSample) || 12), 1, 1000);
    if (approving.length && runsInScout) spec.workgraph.scouting = scoutSpec(ceiling);
    if (approving.length && !runsInScout)
      warn('hints.dirac:Workgraph.approving', 'nothing-to-scout', 'every member of this workgraph is held back by its initial_state, so there is nothing for a scout to run and the approving list never runs: the workgraph goes straight from New to Active');

    for (const act of approving) {
      const scouting = spec.workgraph.scouting;
      if (act.minRate != null && scouting) scouting.failAbove = Math.round(Math.max(0, Math.min(1, 1 - act.minRate)) * 1000) / 1000;
      if (act.minPassed != null && scouting) {
        const sample = scouting.count != null ? scouting.count : (scouting.stages || [ceiling])[0];
        const kept = Math.max(1, Math.min(Math.round(act.minPassed), sample));
        if (kept < Math.round(act.minPassed)) notes.push(`check success rate: a minimum of ${fmt(act.minPassed)} settled inputs clamped to ${fmt(kept)}, the size of the clamped sample`);
        scouting.minSeen = kept;
      }
      delete act.minRate;
      delete act.minPassed;
    }
    if (approving.length) spec.workgraph.approving = approving;

    /* What a member held back waits for: the approval, where the workgraph has a scout to approve, and nothing at all
       where it has not, since there is then no Approving state for it to wait in. */
    for (const n of Object.values(spec.transformations)) {
      if (n.hold !== 'scouting') continue;
      if (spec.workgraph.scouting) n.hold = 'approval';
      else delete n.hold;
    }

    /* How big the scout is belongs to the feeders, which is where DX-ADR-006 puts it: a seed feeder is given
       `scouting_events`, a catalogue feeder a `scouting_fraction`, and a VO feeder may take a sample of its own such as a
       run range, which is that plugin's vocabulary and not this compiler's. A ladder belongs to the hook that climbs it,
       in the feeder's own unit. What the picture needs is a number of inputs, so each is read in the unit its plugin
       states it in and converted once; where nothing names a size the model draws the playground's own sample and says
       where the sample it drew came from. */
    function scoutSpec(cap) {
      const seed = Object.values(seedFeeders)[0];
      const per = seed ? num((isObj(seed.feeder.args) ? seed.feeder.args : {}).events_per_seed) || 1 : 1;
      const unit = seed && per > 1 ? ` at ${fmt(per)} a seed` : '';
      const inputs = (v) => Math.max(1, Math.round(v / per));

      const hook = isObj(wg.hooks) && isObj(wg.hooks.ScoutingToApproving) ? wg.hooks.ScoutingToApproving : null;
      const hargs = hook && isObj(hook.args) ? hook.args : {};
      const stages = Array.isArray(hargs.stages) ? hargs.stages.map((v) => num(v)).filter((v) => v != null).map(inputs) : null;
      if (stages && stages.length) {
        const kept = stages.map((v) => Math.min(cap, v));
        if (kept.some((v, k) => v !== stages[k]))
          notes.push(
            unit
              ? `scouting: a ladder of ${hargs.stages.map(fmt).join(', ')} events${unit} is ${stages.map(fmt).join(', ')} seeds, clamped to ${kept.map(fmt).join(', ')}`
              : `scouting: a ladder of ${stages.map(fmt).join(', ')} inputs clamped to ${kept.map(fmt).join(', ')}`
          );
        return { stages: kept };
      }

      for (const s of Object.values(seedFeeders)) {
        const events = num((isObj(s.feeder.args) ? s.feeder.args : {}).scouting_events);
        if (events == null) continue;
        const asked = inputs(events);
        const kept = Math.min(cap, asked);
        if (kept < asked) notes.push(`scouting: ${fmt(events)} events${unit} is a sample of ${fmt(asked)} seeds, clamped to ${fmt(kept)}`);
        return { count: kept };
      }
      for (const q of Object.values(queries)) {
        const fraction = num((isObj(q.feeder.args) ? q.feeder.args : {}).scouting_fraction);
        if (fraction != null) return { fraction };
      }

      const named = Object.values(seedFeeders).concat(Object.values(queries)).filter((f) => Object.keys(isObj(f.feeder.args) ? f.feeder.args : {}).some((k) => /^scouting_/.test(k)));
      notes.push(
        named.length
          ? `scouting: ${named.map((f) => f.feeder.name || 'the feeder').join(', ')} samples in a vocabulary of its own, which this playground cannot size, so the scout is drawn as ${fmt(cap)} inputs`
          : `scouting: no feeder names a sample, so the scout is drawn as the playground's own ${fmt(cap)} inputs`
      );
      return { count: cap };
    }

    /* How many files an output box can hold once every pool is clamped: the root pool divided by the group sizes on
       the way down. A target the clamped workgraph could never reach would leave the hook that disables the feeders
       out of the run altogether, which is the part of the model the target exists to show. */
    function capacity(outputId) {
      const box = spec.outputs[outputId];
      if (!box) return null;
      let id = box.from;
      let size = 1;
      for (let guard = 0; guard < 50 && id; guard++) {
        const n = spec.transformations[id];
        if (!n) return null;
        size *= Math.max(1, n.packer.size);
        if (n.feeder.seeds) return Math.floor(n.feeder.seeds / size);
        const from = n.feeder.from[0];
        if (spec.sources[from]) return Math.floor(spec.sources[from].files / size);
        id = from;
      }
      return null;
    }

    if (isObj(wg.target) && wg.target.output != null) {
      const oid = safeId(wg.target.output);
      if (!spec.outputs[oid]) warn('hints.dirac:Workgraph.target', 'unknown-target', `the target names the output ${wg.target.output}, which this document does not declare; nothing will disable the feeders`);
      else {
        let files = Math.max(1, Math.round(num(wg.target.files) || 1));
        const cap = capacity(oid);
        const most = cap == null ? null : Math.max(1, Math.floor(cap * 0.6));
        if (most != null && files > most) {
          notes.push(`${wg.target.output}: a target of ${fmt(files)} files clamped to ${fmt(most)}, which the clamped pools can reach`);
          files = most;
        }
        spec.workgraph.target = { output: oid, files };
      }
    }

    if (wg.output_sandbox != null) sandbox.unshift(`the whole workgraph: ${asList(wg.output_sandbox).join(', ')}`);
    if (wg.schema_version != null && String(wg.schema_version) !== '1.0') warn('hints.dirac:Workgraph.schema_version', 'schema-version', `this playground knows the dirac: hint schema 1.0, and the document asks for ${wg.schema_version}`);

    return done();
  }
  /* ------------------------------------------------------------------ */
  /* The hint schema                                                     */
  /* ------------------------------------------------------------------ */

  /* docs/schemas/dirac-1.0.json is the published vocabulary (DX-ADR-007). It is enough of JSON Schema to say what a hint
     may hold, and these are enough of a validator to hold the documents and the schema to each other: types, enums, the
     required fields, and — the one that earns its keep in an editor — a field the schema does not know.

     Schema problems are warnings rather than errors. The schema version is provisional, it is not what decides whether a
     workgraph can run, and a document a little ahead of it should still build. */

  function deref(node, schema) {
    let seen = 0;
    while (isObj(node) && typeof node.$ref === 'string' && seen++ < 20) {
      const parts = node.$ref.replace(/^#\//, '').split('/');
      let at = schema;
      for (const p of parts) at = isObj(at) ? at[p] : undefined;
      node = at;
    }
    return isObj(node) ? node : null;
  }

  const typeOk = (value, type) => {
    if (type === 'object') return isObj(value);
    if (type === 'array') return Array.isArray(value);
    if (type === 'integer') return typeof value === 'number' && Math.floor(value) === value;
    if (type === 'number') return typeof value === 'number';
    if (type === 'string') return typeof value === 'string';
    if (type === 'boolean') return typeof value === 'boolean';
    return true;
  };

  const nameOf = (node) => (node && (node.title || (node.$ref || '').split('/').pop())) || 'this';

  /* The nearest known field to one the schema does not have, so a typo reads as a typo. */
  function nearest(word, options) {
    let best = null;
    let bestScore = Infinity;
    for (const option of options) {
      const a = word.toLowerCase();
      const b = option.toLowerCase();
      let d = new Array(b.length + 1).fill(0).map((_, i) => i);
      for (let i = 1; i <= a.length; i++) {
        let prev = d[0];
        d[0] = i;
        for (let j = 1; j <= b.length; j++) {
          const t = d[j];
          d[j] = Math.min(d[j] + 1, d[j - 1] + 1, prev + (a[i - 1] === b[j - 1] ? 0 : 1));
          prev = t;
        }
      }
      const score = d[b.length];
      if (score < bestScore) {
        bestScore = score;
        best = option;
      }
    }
    return bestScore <= Math.max(2, Math.floor(word.length / 3)) ? best : null;
  }

  function validateAgainst(value, node, schema, path, out) {
    node = deref(node, schema);
    if (!node) return;
    if (Array.isArray(node.anyOf)) {
      if (!node.anyOf.some((alt) => !validateAgainst(value, alt, schema, path, []).length)) {
        out.push({ path, message: `${path.split('.').pop()} is not any of the shapes ${nameOf(node)} allows here` });
      }
      return out;
    }
    if (node.type && !typeOk(value, node.type)) {
      out.push({ path, message: `${path.split('.').pop()} is ${Array.isArray(value) ? 'a list' : value === null ? 'empty' : typeof value}, and the schema has it as ${node.type}` });
      return out;
    }
    if (Array.isArray(node.enum) && node.enum.indexOf(value) < 0) {
      out.push({ path, message: `${JSON.stringify(value)} is not one of ${node.enum.map((v) => JSON.stringify(v)).join(', ')}` });
      return out;
    }
    if (isObj(value)) {
      for (const key of node.required || []) if (!(key in value)) out.push({ path, message: `${nameOf(node)} needs ${key}` });
      const known = Object.keys(node.properties || {});
      for (const [key, sub] of Object.entries(value)) {
        if (node.properties && node.properties[key]) {
          validateAgainst(sub, node.properties[key], schema, `${path}.${key}`, out);
          continue;
        }
        if (node.additionalProperties === false) {
          const near = nearest(key, known);
          out.push({ path: `${path}.${key}`, message: `${nameOf(node)} has no field ${key}${near ? `; did you mean ${near}?` : ''}` });
        }
      }
    }
    if (Array.isArray(value) && node.items) value.forEach((v, i) => validateAgainst(v, node.items, schema, `${path}.${i}`, out));
    return out;
  }

  /* Every place a hint lives in a document, with the path the editor turns back into a line. */
  function hintSites(doc) {
    const sites = [];
    if (!isObj(doc)) return sites;
    for (const [name, body] of Object.entries(hintMap(doc.hints))) sites.push({ path: `hints.${name}`, name, body });
    for (const step of idmap(doc.steps)) {
      if (step.id == null) continue;
      for (const [name, body] of Object.entries(hintMap(step.hints))) sites.push({ path: `steps.${step.id}.hints.${name}`, name, body });
    }
    for (const input of idmap(doc.inputs)) {
      if (input.id == null) continue;
      const feeder = feederOf(input);
      if (feeder) sites.push({ path: `inputs.${input.id}.dirac:Feeder`, name: 'dirac:Feeder', body: feeder });
    }
    return sites;
  }

  /* The document against the published hint schema. Separate from `compile`, because it answers a different question:
     compile asks what the picture should be, this asks whether the document is written in the vocabulary. */
  function checkSchema(doc, schema) {
    const problems = [];
    if (!isObj(schema)) return problems;
    const version = (/dirac-([0-9.]+)\.json/.exec(String(schema.$id || '')) || [null, '1.0'])[1];
    for (const site of hintSites(doc)) {
      const node = site.name === 'dirac:Feeder' ? schema.$defs && schema.$defs.feeder : (schema.properties || {})[site.name];
      if (!node) {
        if (/^dirac:/.test(site.name)) problems.push({ path: site.path, severity: 'warning', rule: 'schema', message: `${site.name} is not a hint class of schema ${version}` });
        continue;
      }
      for (const p of validateAgainst(site.body, node, schema, site.path, [])) problems.push({ path: p.path, severity: 'warning', rule: 'schema', message: p.message });
    }
    return problems;
  }

  /* ------------------------------------------------------------------ */
  /* Where the cursor is                                                 */
  /* ------------------------------------------------------------------ */

  /* The path of the mapping that contains a line, and the keys already written in it: what an editor needs to offer the
     fields the schema has left, and what it needs to say which field a hover is over. Text, not the parsed object,
     because a document being typed into is usually not parseable yet. */
  function containerAt(text, line) {
    const lines = String(text == null ? '' : text).replace(/\r\n?/g, '\n').split('\n');
    const target = Math.max(1, Math.min(line, lines.length));
    const raw = strip(lines[target - 1] || '');
    const indent = raw.trim() ? indentOf(raw) : (lines[target - 1] || '').length;
    const here = raw.trim();
    const targetIsItem = here[0] === '-' && (here.length === 1 || here[1] === ' ');
    const stack = [];
    const counts = [];
    /* Leave every block deeper than `ind`, and the one at `ind` itself unless a sequence item is about to take its
       place: a block sequence may sit at the indent of the key that owns it, so a dash at the same indent is inside
       what is on the stack rather than beside it. The item counter of a block is dropped with the block. */
    const leave = (ind, forItem) => {
      while (stack.length) {
        const top = stack[stack.length - 1];
        const deeper = top.indent > ind;
        const same = top.indent === ind && (forItem ? top.item : true);
        if (!deeper && !same) break;
        stack.pop();
        if (deeper || !forItem) counts[top.indent] = undefined;
      }
    };
    for (let i = 0; i < target - 1; i++) {
      const text2 = strip(lines[i]);
      const body = text2.trim();
      if (!body) continue;
      const ind = indentOf(text2);
      const dash = body[0] === '-' && (body.length === 1 || body[1] === ' ');
      if (dash) {
        leave(ind, true);
        const index = counts[ind] == null ? 0 : counts[ind];
        counts[ind] = index + 1;
        stack.push({ indent: ind, key: String(index), item: true });
        const content = body.slice(1).trim();
        const col = ind + 1 + (text2.slice(ind + 1).length - text2.slice(ind + 1).replace(/^ +/, '').length);
        const k = keySplit(content);
        if (k >= 0) stack.push({ indent: col, key: String(scalar(content.slice(0, k))) });
        continue;
      }
      leave(ind, false);
      const k = keySplit(body);
      if (k >= 0) stack.push({ indent: ind, key: String(scalar(body.slice(0, k))) });
    }
    leave(indent, targetIsItem);
    const path = stack.map((s) => s.key).join('.');
    /* The keys already written in the same block, so an editor offers only what is left of it. */
    const siblings = [];
    let depth = 0;
    for (let i = 0; i < lines.length; i++) {
      const text2 = strip(lines[i]);
      const body = text2.trim();
      if (!body) continue;
      const ind = indentOf(text2);
      if (ind < indent) {
        depth = i < target - 1 ? 1 : 2; /* the block before the line, then the one after it */
        if (depth === 2) break;
        siblings.length = 0;
        continue;
      }
      if (ind !== indent || i === target - 1) continue;
      const k = keySplit(body);
      if (k >= 0) siblings.push(String(scalar(body.slice(0, k))));
    }
    return { path, indent, siblings };
  }

  /* The path of the key on a line, for a hover. */
  function pathAt(text, line) {
    const lines = String(text == null ? '' : text).replace(/\r\n?/g, '\n').split('\n');
    const body = strip(lines[Math.max(0, line - 1)] || '').trim();
    const container = containerAt(text, line);
    const k = keySplit(body);
    if (k < 0) return container.path;
    const key = String(scalar(body.slice(0, k)));
    return container.path ? `${container.path}.${key}` : key;
  }

  /* The schema node a document path lands on: everything from `hints` or from a `dirac:Feeder` extension field down. */
  function schemaAt(schema, path) {
    if (!isObj(schema) || !path) return null;
    const parts = String(path).split('.').filter(Boolean);
    let node = null;
    let rest = [];
    const hintsAt = parts.lastIndexOf('hints');
    const feederAt = parts.lastIndexOf('dirac:Feeder');
    if (feederAt >= 0 && feederAt > hintsAt) {
      node = schema.$defs && schema.$defs.feeder;
      rest = parts.slice(feederAt + 1);
    } else if (hintsAt >= 0) {
      const cls = parts[hintsAt + 1];
      if (cls == null) return { node: schema, rest: [] };
      node = (schema.properties || {})[cls];
      rest = parts.slice(hintsAt + 2);
    } else return null;
    for (const seg of rest) {
      node = deref(node, schema);
      if (!node) return null;
      if (/^\d+$/.test(seg) && node.items) node = node.items;
      else if (node.properties && node.properties[seg]) node = node.properties[seg];
      else if (Array.isArray(node.anyOf)) {
        const alt = node.anyOf.map((a) => deref(a, schema)).find((a) => a && a.properties && a.properties[seg]);
        node = alt ? alt.properties[seg] : null;
      } else node = null;
      if (!node) return null;
    }
    return deref(node, schema);
  }

  /* What an editor offers where the cursor is: the fields of the containing hint the document has not used yet. */
  function suggestAt(schema, text, line) {
    const at = containerAt(text, line);
    const node = schemaAt(schema, at.path);
    if (!node) return [];
    const target = Array.isArray(node.anyOf) ? deref(node.anyOf.find((a) => deref(a, schema) && deref(a, schema).properties), schema) : node;
    if (!target || !target.properties) return [];
    return Object.entries(target.properties)
      .filter(([key]) => at.siblings.indexOf(key) < 0)
      .map(([key, sub]) => {
        const body = deref(sub, schema) || {};
        return { key, detail: body.type || nameOf(body), doc: body.description || '', required: (target.required || []).indexOf(key) >= 0 };
      });
  }

  root.WorkgraphCwl = { compile, checkSchema, schemaAt, suggestAt, containerAt, pathAt, hintSites, parseYaml, locate, idmap, hintMap, hintKey, feederOf, refOf, isSeedFeeder, actionList, asList, words, fmt, isObj, num, DEFAULT_RUN_SETTINGS, RUN_SETTING_FIELDS };
})(typeof window !== 'undefined' ? window : globalThis);
