/*
 * The workgraph playground (docs/playground.md).
 *
 * A CWL document on the left, the workgraph it describes running on the right, and an explicit build between them.
 * The compiler is docs/assets/js/workgraph-cwl.js and the simulation is the widget every other page embeds; this
 * file is the page around them — the editor, the diagnostics, the three panels, the permalink.
 *
 * The script is on every page of the site, as the simulator's are, so it does nothing at all until it finds its
 * mount div. Everything it needs to validate, build and run is local: js-yaml and Monaco are fetched from a CDN for
 * the editing experience alone, and with the network blocked the page falls back to a plain textarea and the
 * compiler's own YAML reader.
 */
(function (root) {
  'use strict';

  if (typeof document === 'undefined') return;

  /* Where this script was loaded from, captured now because document.currentScript is null once we are in a callback. */
  const SELF_SRC = document.currentScript ? document.currentScript.src : '';
  const asset = (path) => (SELF_SRC ? new URL(path, SELF_SRC).href : path);

  const JS_YAML_URL = 'https://cdn.jsdelivr.net/npm/js-yaml@4.1.0/dist/js-yaml.min.js';

  /* The examples, by family: a workgraph and then what is added to it, so the difference between two of them is one
     thing rather than a document to read. The first of the first group is what the page opens with. */
  const EXAMPLES = [
    {
      key: 'simulation',
      group: 'Simulation',
      items: [
        { file: 'simulation.cwl', name: 'Basic', note: 'a seed feeder, three steps, and a target that ends the workgraph' },
        { file: 'simulation-scouted.cwl', name: 'With a scout', note: 'a ladder of samples, the approving actions, and a sign-off' },
        { file: 'mc-simulation.cwl', name: 'With removals and a replication', note: 'a billion events, two removals of the intermediates, and a replication held until approval' },
      ],
    },
    {
      key: 'shapes',
      group: 'Other shapes',
      items: [
        { file: 'joining.cwl', name: 'Joining two reconstructions', note: 'a packer that waits for the partner of every input' },
        { file: 'histograms.cwl', name: 'Collecting histograms', note: "a second output on the artifact port, merged by a transformation of its own" },
      ],
    },
    {
      key: 'lhcb',
      group: 'LHCb examples',
      items: [
        { file: 'analysis-production.cwl', name: 'Analysis production', note: 'two transformations, one edge, one deliverable' },
        { file: 'sprucing.cwl', name: 'Sprucing, staged from tape', note: 'a staging the step waits for, and a removal that waits for the step' },
        { file: 'rdst-stripping.cwl', name: 'RDST stripping, with RAW ancestors', note: 'two stagings, and a packer that finds the partner by lookup' },
      ],
    },
  ];
  const EXAMPLE_FILES = EXAMPLES.reduce((all, section) => all.concat(section.items), []);
  /* which family a document belongs to, so the picker opens where the reader already is */
  const FAMILY_OF = EXAMPLES.reduce((of, section) => section.items.reduce((o, item) => Object.assign(o, { [item.file]: section.key }), of), {});

  /* The settings in a URL, one short parameter each: a permalink is pasted into a review thread, where a line of
     query string is already more than anyone wants to read. */
  const PARAM = { seed: 's', slots: 'p', filesPerQuery: 'q', maxPool: 'm', scoutSample: 'c', jobMin: 'a', jobMax: 'b', fail: 'f', partial: 'r' };
  /* Past this a URL stops being something a browser, a chat client and a code review will all carry intact. */
  const LINK_MAX = 8192;
  const DEBOUNCE = 250; /* before the document is compiled again */
  const REBUILD = 700; /* and before the workgraph is, which throws a run away */
  const CONFIRM = 6000; /* how long a confirmation stands in the toolbar before it goes by itself */
  /* What the browser remembers, under the theme's own storage scope so it travels with the rest of the site's state. */
  const STORE = '__playground';

  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  /* Drawn the way the simulator draws its own: one stroke weight, one size, currentColor. */
  const ICON = {
    external: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><path d="M15 3h6v6"/><path d="m10 14 11-11"/></svg>',
    notice: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 16v-5"/><path d="M12 8h.01"/></svg>',
    close: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 6 12 12"/><path d="M18 6 6 18"/></svg>',
  };

  function el(tag, attrs, html) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v == null) continue;
      if (k === 'class') node.className = v;
      else node.setAttribute(k, String(v));
    }
    if (html != null) node.innerHTML = html;
    return node;
  }

  /* ------------------------------------------------------------------ */
  /* The document in a URL                                               */
  /* ------------------------------------------------------------------ */

  const b64url = (bytes) => {
    let s = '';
    for (const b of bytes) s += String.fromCharCode(b);
    return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  };

  const unb64url = (text) => {
    const s = atob(text.replace(/-/g, '+').replace(/_/g, '/'));
    const bytes = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) bytes[i] = s.charCodeAt(i);
    return bytes;
  };

  async function squeeze(text) {
    const bytes = new TextEncoder().encode(text);
    if (typeof CompressionStream !== 'function') return 'u' + b64url(bytes);
    const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream('deflate-raw'));
    return 'z' + b64url(new Uint8Array(await new Response(stream).arrayBuffer()));
  }

  async function unsqueeze(text) {
    const kind = text[0];
    const bytes = unb64url(text.slice(1));
    if (kind !== 'z') return new TextDecoder().decode(bytes);
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('deflate-raw'));
    return new TextDecoder().decode(await new Response(stream).arrayBuffer());
  }

  /* ------------------------------------------------------------------ */
  /* What the browser remembers                                          */
  /* ------------------------------------------------------------------ */

  /* The document, the settings and the shape of the page, kept where the reader left them. The theme's own helpers are
     used where they are there, so the entry sits beside `__palette` under the same scope and goes when the site's
     storage goes. */
  function remembered() {
    try {
      if (typeof root.__md_get === 'function') return root.__md_get(STORE);
      return JSON.parse(localStorage.getItem(STORE));
    } catch (e) {
      return null;
    }
  }

  function remember(state) {
    try {
      if (typeof root.__md_set === 'function') root.__md_set(STORE, state);
      else localStorage.setItem(STORE, JSON.stringify(state));
    } catch (e) {
      /* a browser that refuses storage is a browser the page opens fresh in, which is no worse than it was */
    }
  }

  function forget() {
    try {
      if (typeof root.__md_set === 'function') root.__md_set(STORE, null);
      else localStorage.removeItem(STORE);
    } catch (e) {}
  }

  /* ------------------------------------------------------------------ */
  /* The editor                                                          */
  /* ------------------------------------------------------------------ */

  /* One shape, two implementations: a textarea, and Monaco once it has arrived. The compiler, the simulator and the
     diagnostics are all local, so only the editing experience depends on the network. */
  function textareaEditor(host, value, onChange) {
    const area = el('textarea', { class: 'wgpg-area', spellcheck: 'false', autocapitalize: 'off', autocomplete: 'off', 'aria-label': 'the CWL document' });
    area.value = value;
    host.appendChild(area);
    let quiet = false;
    area.addEventListener('input', () => { if (!quiet) onChange(); });
    const offsetOf = (line) => {
      const lines = area.value.split('\n');
      let at = 0;
      for (let i = 0; i < Math.min(line - 1, lines.length); i++) at += lines[i].length + 1;
      return { at, length: (lines[Math.min(line, lines.length) - 1] || '').length };
    };
    return {
      kind: 'textarea',
      getValue: () => area.value,
      setValue(text) {
        quiet = true;
        area.value = text;
        quiet = false;
      },
      focus: () => area.focus(),
      reveal(line) {
        const { at, length } = offsetOf(line);
        area.focus();
        area.setSelectionRange(at, at + length);
        /* No API puts a line in view, so the height of one is measured off the box itself. */
        const rows = area.value.split('\n').length || 1;
        area.scrollTop = Math.max(0, (area.scrollHeight / rows) * (line - 1) - area.clientHeight / 2);
      },
      setProblems() {},
      relayout() {},
      dispose() {
        if (area.parentNode) area.parentNode.removeChild(area);
      },
    };
  }

  /* ------------------------------------------------------------------ */
  /* Monaco                                                              */
  /* ------------------------------------------------------------------ */

  /* Monaco is fetched from a CDN, lazily, only on this page, and only to edit with. The compiler, the simulator, the
     diagnostics and the hint schema are all local, so a reader with no network gets the textarea above and everything
     else works exactly as it does here.

     The schema's completion, hover and validation are wired by hand rather than through monaco-yaml. monaco-yaml is
     ESM-only and its language service runs in a web worker; served straight from a CDN, that worker and the editor end
     up with two separately bundled copies of Monaco's worker protocol and the handshake fails. A bundler would fix it,
     and a bundler is the one thing this repository does not have. Reading the schema here costs a hundred lines, works
     with the network blocked, and is checked by tests/workgraph-sim/cwl.test.js, which monaco-yaml would not be. */
  const MONACO_VERSION = '0.52.2';
  const MONACO_URL = `https://cdn.jsdelivr.net/npm/monaco-editor@${MONACO_VERSION}/+esm`;
  const MONACO_WORKER_URL = `https://cdn.jsdelivr.net/npm/monaco-editor@${MONACO_VERSION}/esm/vs/editor/editor.worker.js/+esm`;
  const MODEL_URI = 'inmemory://playground/workgraph.cwl';

  let monacoLoad = null;

  function loadMonaco() {
    if (monacoLoad) return monacoLoad;
    monacoLoad = (async () => {
      /* A cross-origin script cannot be a worker directly, so the worker is a one-line module that imports it. */
      root.MonacoEnvironment = root.MonacoEnvironment || {
        getWorker() {
          const blob = new Blob([`import ${JSON.stringify(MONACO_WORKER_URL)};`], { type: 'text/javascript' });
          return new Worker(URL.createObjectURL(blob), { type: 'module', name: 'editor' });
        },
      };
      return await import(/* webpackIgnore: true */ MONACO_URL);
    })().catch(() => null);
    return monacoLoad;
  }

  const SEVERITY = { error: 8, warning: 4 }; /* monaco.MarkerSeverity, which is not available until it has loaded */

  function monacoEditor(monaco, host, value, onChange, page) {
    const token = (name) => getComputedStyle(page.root).getPropertyValue(name).trim() || null;
    const themes = { light: 'vs', dark: 'vs-dark' };
    const defineTheme = () => {
      const dark = document.body.getAttribute('data-md-color-scheme') === 'slate';
      const colors = {};
      if (token('--wg-card')) colors['editor.background'] = token('--wg-card');
      if (token('--wg-ink')) colors['editor.foreground'] = token('--wg-ink');
      monaco.editor.defineTheme('wgpg', { base: dark ? 'vs-dark' : 'vs', inherit: true, rules: [], colors });
      monaco.editor.setTheme('wgpg');
      return themes[dark ? 'dark' : 'light'];
    };

    const uri = monaco.Uri.parse(MODEL_URI);
    const existing = monaco.editor.getModel(uri);
    if (existing) existing.dispose();
    const model = monaco.editor.createModel(value, 'yaml', uri);
    const editor = monaco.editor.create(host, {
      model,
      automaticLayout: true,
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      fontSize: 12,
      fontFamily: token('--wg-mono') || undefined,
      lineNumbersMinChars: 3,
      renderLineHighlight: 'line',
      tabSize: 2,
      insertSpaces: true,
      padding: { top: 8, bottom: 8 },
      ariaLabel: 'the CWL document',
    });
    defineTheme();
    /* Material's palette toggle is a body attribute, and the editor is the one thing on the page that does not follow it. */
    const themeWatch = new MutationObserver(() => defineTheme());
    themeWatch.observe(document.body, { attributes: true, attributeFilter: ['data-md-color-scheme'] });

    const changed = model.onDidChangeContent(() => onChange());
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => page.build());

    const ours = (m) => m && String(m.uri) === MODEL_URI;
    const CWL = root.WorkgraphCwl;
    const providers = [
      monaco.languages.registerCompletionItemProvider('yaml', {
        provideCompletionItems(m, position) {
          if (!ours(m) || !page.schema) return { suggestions: [] };
          const line = m.getLineContent(position.lineNumber).slice(0, position.column - 1);
          if (/:\s*\S/.test(line)) return { suggestions: [] };
          const word = m.getWordUntilPosition(position);
          const range = { startLineNumber: position.lineNumber, endLineNumber: position.lineNumber, startColumn: word.startColumn, endColumn: word.endColumn };
          return {
            suggestions: CWL.suggestAt(page.schema, m.getValue(), position.lineNumber).map((s) => ({
              label: s.key,
              kind: monaco.languages.CompletionItemKind.Property,
              detail: s.detail + (s.required ? ' · required' : ''),
              documentation: { value: s.doc },
              insertText: s.key + ': ',
              range,
            })),
          };
        },
      }),
      monaco.languages.registerHoverProvider('yaml', {
        provideHover(m, position) {
          if (!ours(m) || !page.schema) return null;
          const path = CWL.pathAt(m.getValue(), position.lineNumber);
          const node = CWL.schemaAt(page.schema, path);
          if (!node || !node.description) return null;
          const head = `**${node.title || path.split('.').pop()}**`;
          const enums = Array.isArray(node.enum) ? `\n\nOne of: ${node.enum.map((v) => '`' + v + '`').join(', ')}` : '';
          return { contents: [{ value: `${head}\n\n${node.description}${enums}` }] };
        },
      }),
    ];

    return {
      kind: 'monaco',
      getValue: () => model.getValue(),
      relayout: () => editor.layout(),
      setValue(text) {
        if (model.getValue() !== text) model.setValue(text);
      },
      focus: () => editor.focus(),
      reveal(line) {
        editor.revealLineInCenter(line);
        editor.setPosition({ lineNumber: line, column: 1 });
        editor.focus();
      },
      setProblems(problems) {
        monaco.editor.setModelMarkers(model, 'workgraph-cwl', problems.map((p) => {
          const n = Math.max(1, Math.min(p.line || 1, model.getLineCount()));
          return {
            startLineNumber: n,
            endLineNumber: n,
            startColumn: model.getLineFirstNonWhitespaceColumn(n) || 1,
            endColumn: model.getLineMaxColumn(n),
            message: `${p.message} (${p.rule})`,
            severity: SEVERITY[p.severity] || SEVERITY.warning,
          };
        }));
      },
      dispose() {
        themeWatch.disconnect();
        changed.dispose();
        for (const p of providers) p.dispose();
        editor.dispose();
        model.dispose();
      },
    };
  }

  /* ------------------------------------------------------------------ */
  /* The colour scheme                                                   */
  /* ------------------------------------------------------------------ */

  /* The switch is the theme's own markup, rendered by docs/overrides/playground.html from the palette in mkdocs.yml,
     but it is driven here: the theme binds the palette it finds at startup, and a page with no header is not where it
     looks. Driving it costs less than the header would, and the state is still shared — `__md_get` and `__md_set` are
     the theme's, so what is written is the entry the documentation reads on its next load, and the other way round. */
  function mountPalette(form) {
    const inputs = Array.prototype.slice.call(form.querySelectorAll('input[name="__palette"]'));
    if (!inputs.length) return null;
    /* Each option's label follows its input, and points at the *next* option: the switch shows where you are and takes
       you on. Looking a label up by `for` would find the one that arrives at an option rather than the one that owns it. */
    const labelOf = (input) => (input.nextElementSibling && input.nextElementSibling.tagName === 'LABEL' ? input.nextElementSibling : null);
    const stored = typeof root.__md_get === 'function' ? root.__md_get('__palette') : null;
    const media = (input) => {
      const query = input.getAttribute('data-md-color-media');
      return query && typeof matchMedia === 'function' ? matchMedia(query).matches : false;
    };
    /* The automatic entry carries no colours of its own. It borrows them from the light entry or the dark one, whichever
       the reader's system asks for, which is what the theme does, and it is why the media query is kept in what is
       stored: a scheme written down in its place would stop following the system on the next load, here and in the
       documentation both. */
    const colorOf = (input) => {
      const color = {
        media: input.getAttribute('data-md-color-media'),
        scheme: input.getAttribute('data-md-color-scheme'),
        primary: input.getAttribute('data-md-color-primary'),
        accent: input.getAttribute('data-md-color-accent'),
      };
      if (color.media !== '(prefers-color-scheme)') return color;
      const light = typeof matchMedia === 'function' ? matchMedia('(prefers-color-scheme: light)').matches : true;
      const from = form.querySelector(`[data-md-color-media="(prefers-color-scheme: ${light ? 'light' : 'dark'})"]`);
      if (from) for (const key of ['scheme', 'primary', 'accent']) color[key] = from.getAttribute(`data-md-color-${key}`);
      return color;
    };

    let at = 0;
    const apply = (n, save) => {
      const index = ((n % inputs.length) + inputs.length) % inputs.length;
      at = index;
      const input = inputs[index];
      input.checked = true;
      const color = colorOf(input);
      for (const [key, value] of Object.entries(color)) {
        if (key === 'media' || !value) continue;
        document.body.setAttribute(`data-md-color-${key}`, value);
      }
      /* so that a native control, which the page has several of, is drawn for the scheme it is sitting on */
      if (color.scheme) document.documentElement.style.colorScheme = color.scheme === 'slate' ? 'dark' : 'light';
      inputs.forEach((other, i) => {
        const label = labelOf(other);
        if (label) label.hidden = i !== index;
      });
      if (save && typeof root.__md_set === 'function') root.__md_set('__palette', { index, color });
    };

    /* And it follows the system while it is the automatic entry that is chosen. */
    if (typeof matchMedia === 'function') {
      const dark = matchMedia('(prefers-color-scheme: dark)');
      const follow = () => {
        const input = inputs[at];
        if (input && input.getAttribute('data-md-color-media') === '(prefers-color-scheme)') apply(at, false);
      };
      if (dark.addEventListener) dark.addEventListener('change', follow);
    }
    form.addEventListener('change', () => {
      const n = inputs.findIndex((input) => input.checked);
      if (n >= 0) apply(n, true);
    });
    const start = stored && typeof stored.index === 'number' ? stored.index : Math.max(0, inputs.findIndex(media));
    apply(start, false);
    return { apply };
  }

  /* ------------------------------------------------------------------ */
  /* The page                                                            */
  /* ------------------------------------------------------------------ */

  /* The two reports about the page, each named once: the head that opens it and every sentence that points at it read the
     name from here, so one of them cannot come to name a drawer the other has renamed. Neither is a peer of the workgraph —
     one is what the page is for, and these are a report on the document and a form for the run — so neither is a tab. */
  const DRAWERS = { parsed: 'What was parsed', settings: 'Run settings' };

  /* A report beside the thing it is about, closed until it is asked for: the shape the problems strip already had, a head
     that says what it holds and a body that opens under it. */
  const drawer = (key) => `<div class="wgpg-drawer" data-drawer="${key}">
              <button type="button" class="wgpg-drawer-head" data-act="disclose" aria-controls="wgpg-body-${key}" aria-expanded="false"><span class="wgpg-caret" aria-hidden="true">▸</span><span>${esc(DRAWERS[key])}</span></button>
              <div class="wgpg-drawer-body" id="wgpg-body-${key}" hidden></div>
            </div>`;

  class Playground {
    constructor(host) {
      const CWL = root.WorkgraphCwl;
      this.settings = Object.assign({}, CWL.DEFAULT_RUN_SETTINGS);
      this.host = host;
      this.widget = null;
      this.result = null;
      this.blocked = false;
      this.timer = null;
      this.buildTimer = null;
      this.built = null;
      this.split = 46;
      this.example = ''; /* the file the document was opened from, while it is still that document */
      this.exampleText = '';
      this.category = EXAMPLES[0].key;
      this.seen = false; /* whether this browser has been shown what the page is */
      this.yaml = null; /* js-yaml, once it has arrived */
      this.schema = null; /* docs/schemas/dirac-1.0.json, which is served with the site and so arrives offline too */
      this.build = this.build.bind(this);
      this.buildDom();
      this.readUrl().then(() => {
        this.validate();
        if (!this.errors().length) this.build();
        if (!this.fromLink && !this.seen) this.openLibrary();
      });
      /* Both of these can change what the document means, so the workgraph is rebuilt after them; it is only rebuilt if
         the workgraph has actually changed, so the usual answer is that nothing happens. */
      loadSchema().then((s) => {
        if (!s || !this.root) return;
        this.schema = s;
        this.validate();
      });
      loadYaml().then((y) => {
        if (!y || !this.root) return;
        this.yaml = y;
        this.validate();
        this.rebuild();
      });
      loadMonaco().then((monaco) => {
        if (monaco && this.root) this.upgradeEditor(monaco);
      });
    }

    /* The textarea is what the page opens with, so it is usable before anything has been fetched; Monaco replaces it in
       place, carrying whatever has been typed in the meantime, and never at all where the CDN cannot be reached. */
    upgradeEditor(monaco) {
      const text = this.editor.getValue();
      const host = this.root.querySelector('.wgpg-editor');
      this.editor.dispose();
      host.innerHTML = '';
      const pane = el('div', { class: 'wgpg-monaco' });
      host.appendChild(pane);
      try {
        this.editor = monacoEditor(monaco, pane, text, () => this.edited(), this);
      } catch (e) {
        host.innerHTML = '';
        this.editor = textareaEditor(host, text, () => this.edited());
      }
      if (this.result) this.editor.setProblems(this.result.problems);
    }

    buildDom() {
      const root2 = el('div', { class: 'wgpg' });
      root2.innerHTML = `
        <div class="wgpg-bar">
          <span class="wg-lockup wgpg-brand"><img class="wg-logo" src="${esc(asset('../images/logo.svg'))}" alt="" width="20" height="20"><span class="wg-lockup-name">Playground</span></span>
          <span class="wgpg-divider" aria-hidden="true"></span>
          <button type="button" class="wgpg-btn" data-act="reset" aria-haspopup="dialog" title="forget the document, the settings and the layout this browser is holding, and open the page as a first visit finds it">Reset playground</button>
          <button type="button" class="wgpg-btn" data-act="link">Copy sharable link</button>
          <span class="wgpg-status" role="status" aria-live="polite"></span>
          <span class="wgpg-announce wgpg-quiet" role="status" aria-live="polite"></span>
          <span class="wgpg-right-group">
            <span class="wgpg-palette-slot"></span>
            <a class="wgpg-docs" href="${esc(asset('../../'))}" target="_blank" rel="noopener">Documentation ${ICON.external}</a>
          </span>
        </div>
        <div class="wgpg-split">
          <div class="wgpg-pane wgpg-left">
            <div class="wgpg-editor"></div>
            <div class="wgpg-problems">
              <button type="button" class="wgpg-problems-head" data-act="disclose" aria-controls="wgpg-body-problems" aria-expanded="true">
                <span class="wgpg-caret" aria-hidden="true">▾</span><span class="wgpg-problems-label">no problems</span>
              </button>
              <ul class="wgpg-problems-list" id="wgpg-body-problems"></ul>
            </div>
            ${drawer('parsed')}
          </div>
          <div class="wgpg-gutter" role="separator" tabindex="0" aria-orientation="vertical" aria-label="the width of the editor" aria-valuemin="20" aria-valuemax="80" aria-valuenow="46"></div>
          <div class="wgpg-pane wgpg-right">
            <div class="wgpg-workgraph"><div class="wgpg-sim"></div><div class="wgpg-stale" hidden><span class="wgpg-stale-why"></span></div></div>
            ${drawer('settings')}
          </div>
        </div>`;
      this.host.appendChild(root2);
      this.root = root2;
      this.status = root2.querySelector('.wgpg-status');
      this.announcer = root2.querySelector('.wgpg-announce');
      this.problemsList = root2.querySelector('.wgpg-problems-list');
      this.problemsLabel = root2.querySelector('.wgpg-problems-label');
      root2.appendChild(this.libraryDialog());
      this.library = root2.querySelector('.wgpg-library');
      this.bodies = { parsed: root2.querySelector('#wgpg-body-parsed'), settings: root2.querySelector('#wgpg-body-settings') };
      this.workgraph = root2.querySelector('.wgpg-workgraph');
      this.simHost = this.workgraph.querySelector('.wgpg-sim');
      this.staleOverlay = this.workgraph.querySelector('.wgpg-stale');
      this.staleWhy = this.workgraph.querySelector('.wgpg-stale-why');
      this.renderSettings();

      this.editor = textareaEditor(root2.querySelector('.wgpg-editor'), '', () => this.edited());

      /* The theme's palette switch, rendered by docs/overrides/playground.html and moved here. Moving a node keeps the
         listeners Material bound to it, so the switch goes on writing the scheme where the documentation reads it. */
      const palette = document.querySelector('[data-wgpg-palette]');
      const slot = root2.querySelector('.wgpg-palette-slot');
      if (palette && slot) {
        palette.hidden = false;
        slot.appendChild(palette);
        const form = palette.querySelector('form');
        if (form) mountPalette(form);
      }

      /* The simulation is mounted inside this element and names its own controls the same way, so a click on one of them
         arrives here as well: its reset and the page's shared a name, and pressing the model's reset reset the page
         around it. Nothing of the widget's is the page's to handle, whatever it is called. */
      const mine = (el) => el && !el.closest('.wgsim');
      root2.addEventListener('click', (ev) => {
        const target = ev.target.closest('[data-act]');
        if (!target || !root2.contains(target) || !mine(target)) return;
        const act = target.dataset.act;
        if (act === 'build') this.build();
        else if (act === 'reset') this.reset();
        else if (act === 'link') this.copyLink();
        else if (act === 'cat') this.showCategory(target.dataset.cat);
        else if (act === 'pick') this.pick(target.dataset.file);
        else if (act === 'disclose') this.disclose(target);
        else if (act === 'problem') this.editor.reveal(Number(target.dataset.line) || 1);
      });
      root2.addEventListener('change', (ev) => {
        const target = ev.target.closest('[data-act]');
        if (!target || !mine(target)) return;
        if (target.dataset.act === 'setting') this.setSetting(target.dataset.key, target.value);
      });
      /* Building is the one thing a reader does often enough to want a key for, and the one thing that is never
         automatic: a rebuild throws away the run in front of them. */
      root2.addEventListener('keydown', (ev) => {
        if (!(ev.key === 'Enter' && (ev.metaKey || ev.ctrlKey))) return;
        if (!mine(ev.target)) return;
        /* Monaco binds the same keys itself, since a key the editor swallows would otherwise never reach this. */
        if (ev.target.closest && ev.target.closest('.wgpg-monaco')) return;
        ev.preventDefault();
        this.build();
      });
      this.wireGutter(root2.querySelector('.wgpg-gutter'));
      document.body.classList.add('wgpg-page');
      /* The page fills the viewport from wherever it starts. What is above it is the banner, the header, the tabs and
         the page's own title, each of which comes and goes with the theme's settings and with the width, so the top is
         measured off the mount itself rather than added up from a list of things that might be there. */
      this.fit = () => {
        if (!this.root) return;
        this.root.style.setProperty('--wgpg-top', '0px');
        const top = Math.max(0, this.host.getBoundingClientRect().top + (root.scrollY || 0));
        this.root.style.setProperty('--wgpg-top', Math.round(top) + 'px');
      };
      /* An editor measures text in device pixels, so it has to be told to measure again whenever the window changes,
         and a window moved to a display of a different density changes nothing a resize event or a ResizeObserver can
         see: the CSS size of the page is the same on both screens. A media query on the density that is current is the
         one thing that does fire, and it is re-armed each time because the query names the density it was made with. */
      this.refit = () => {
        this.fit();
        if (this.editor && this.editor.relayout) this.editor.relayout();
      };
      this.armDensity = () => {
        if (this.density && this.density.removeEventListener) this.density.removeEventListener('change', this.onDensity);
      if (this.viewport) this.viewport.disconnect();
        if (typeof matchMedia !== 'function') return;
        this.density = matchMedia(`(resolution: ${root.devicePixelRatio || 1}dppx)`);
        if (this.density.addEventListener) this.density.addEventListener('change', this.onDensity);
      };
      this.onDensity = () => {
        this.armDensity();
        this.refit();
      };
      this.fit();
      this.armDensity();
      root.addEventListener('resize', this.refit);
      /* And the viewport's own box, for the changes a resize event does not describe. */
      if (typeof ResizeObserver !== 'undefined') {
        this.viewport = new ResizeObserver(() => this.refit());
        this.viewport.observe(document.documentElement);
      }
    }

    /* One dialog holds both of the things a reader arrives needing, one above the other: what the page is, and the
       examples it ships. The families are down the left and the documents of one down the right, and picking a
       document opens it there and then. It is shown on a first visit and by a reset, which is what a reset is: the
       page as someone who has never been here finds it. */
    libraryDialog() {
      const dialog = el('dialog', { class: 'wgpg-library', 'aria-labelledby': 'wgpg-library-title' });
      dialog.innerHTML = `
        <div class="wgpg-library-head">
          <h2 id="wgpg-library-title"><b>DiracX</b> Playground</h2>
          <form method="dialog"><button type="submit" class="wgpg-close" aria-label="close">${ICON.close}</button></form>
        </div>
        <div class="wgpg-library-body">
          ${this.aboutSection()}
          <h3 class="wgpg-library-h3">Examples</h3>
          <div class="wgpg-picker">
            <div class="wgpg-cats" role="tablist" aria-orientation="vertical" aria-label="the examples, by family">
              ${EXAMPLES.map(
                (family) => `<button type="button" role="tab" class="wgpg-cat" data-act="cat" data-cat="${family.key}" id="wgpg-cat-${family.key}"
                  aria-controls="wgpg-library-panel" aria-selected="false" tabindex="-1">${esc(family.group)}</button>`
              ).join('')}
            </div>
            <div class="wgpg-items" role="tabpanel" id="wgpg-library-panel" tabindex="0"></div>
          </div>
        </div>`;
      dialog.querySelector('.wgpg-cats').addEventListener('keydown', (ev) => {
        const step = ev.key === 'ArrowDown' ? 1 : ev.key === 'ArrowUp' ? -1 : 0;
        if (!step) return;
        ev.preventDefault();
        const at = EXAMPLES.findIndex((family) => family.key === this.category);
        this.showCategory(EXAMPLES[(at + step + EXAMPLES.length) % EXAMPLES.length].key, true);
      });
      return dialog;
    }

    /* What the page is: what it models, that it reaches no real DiracX, and the ADRs it draws. */
    aboutSection() {
      const adr = (slug, text) => `<a href="${esc(asset('../../adr/' + slug + '/'))}">${text}</a>`;
      return `<div class="wgpg-about">
        <p>Welcome to the <b>DiracX Transformation System Playground</b>, a browser-based simulation of DiracX CWL documents.</p>
        <p>Write a document on the left and watch the workgraph it describes run on the right. The run shows how a feeder fills an input pool and a packer groups it into parcels, what becomes of an input whose job fails, and how a scouting phase, the approving actions and the finalizing checks decide what the workgraph does next. The model is ${adr('DX-ADR-002_overview', 'DX-ADR-002')}, the document is ${adr('DX-ADR-007_cwl', 'DX-ADR-007')}, and the statuses are ${adr('DX-ADR-005_state_machines', 'DX-ADR-005')}.</p>
        <p class="wgpg-notice">${ICON.notice}<span>This page does not try to represent the actual work done by the CWL. To run that, use a CWL runner locally: ${adr('DX-ADR-007_cwl', 'DX-ADR-007')} describes <code>dirac-cwl-runner</code> and what stock <code>cwltool</code> is enough for.</span></p>
      </div>`;
    }

    /* A family's documents. The one the reader is on is marked, and stops being marked at the first keystroke of
       their own. */
    familyPanel(family) {
      const current = this.ownDocument() ? null : this.example;
      return `<ul class="wgpg-item-list">
          ${family.items
            .map(
              (item) => `<li><button type="button" class="wgpg-item" data-act="pick" data-file="${esc(item.file)}"${item.file === current ? ' aria-current="true"' : ''}>
                <span class="wgpg-item-name">${esc(item.name)}</span>
                <span class="wgpg-item-note">${esc(item.note)}</span>
              </button></li>`
            )
            .join('')}
        </ul>`;
    }

    showCategory(key, focus) {
      if (!this.library) return;
      const family = EXAMPLES.find((f) => f.key === key);
      if (!family) return;
      this.category = key;
      for (const tab of this.library.querySelectorAll('.wgpg-cat')) {
        const on = tab.dataset.cat === key;
        tab.setAttribute('aria-selected', String(on));
        tab.tabIndex = on ? 0 : -1;
        if (on && focus) tab.focus();
      }
      const panel = this.library.querySelector('.wgpg-items');
      panel.innerHTML = this.familyPanel(family);
      panel.setAttribute('aria-labelledby', 'wgpg-cat-' + key);
      panel.scrollTop = 0;
    }

    /* It opens on the family the reader is already in, since that is where the neighbouring document is. */
    openLibrary(key) {
      if (!this.library || typeof this.library.showModal !== 'function' || this.library.open) return;
      this.seen = true;
      this.save();
      this.showCategory(key || (!this.ownDocument() && FAMILY_OF[this.example]) || EXAMPLES[0].key);
      this.library.showModal();
    }

    async pick(file) {
      if (this.library && this.library.open) this.library.close();
      await this.loadExample(file);
    }

    ownDocument() {
      return !this.example || !this.exampleText || this.editor.getValue().trim() !== this.exampleText.trim();
    }

    /* ---- the split ---- */

    wireGutter(gutter) {
      const setFrom = (clientX) => {
        const box = this.root.querySelector('.wgpg-split').getBoundingClientRect();
        this.setSplit(((clientX - box.left) / box.width) * 100);
      };
      gutter.addEventListener('pointerdown', (ev) => {
        ev.preventDefault();
        gutter.setPointerCapture(ev.pointerId);
        const move = (e) => setFrom(e.clientX);
        const up = () => {
          gutter.removeEventListener('pointermove', move);
          gutter.removeEventListener('pointerup', up);
        };
        gutter.addEventListener('pointermove', move);
        gutter.addEventListener('pointerup', up);
      });
      gutter.addEventListener('keydown', (ev) => {
        const step = ev.key === 'ArrowRight' ? 2 : ev.key === 'ArrowLeft' ? -2 : 0;
        if (!step) return;
        ev.preventDefault();
        this.setSplit(this.split + step);
      });
      this.setSplit(this.split);
    }

    setSplit(pc) {
      this.split = Math.max(20, Math.min(80, pc));
      this.root.style.setProperty('--wgpg-split', this.split.toFixed(1) + '%');
      this.root.querySelector('.wgpg-gutter').setAttribute('aria-valuenow', String(Math.round(this.split)));
      this.save();
    }

    /* ---- the disclosures ---- */

    /* The problems under the editor and the two reports beside what they are about are one shape and one handler: the caret,
       `aria-expanded`, and the body the head names. A drawer marks the strip it is in while it is open, which is how the
       stylesheet lets that strip take the room to be read in. */
    disclose(head, force) {
      const open = force == null ? head.getAttribute('aria-expanded') !== 'true' : force;
      head.setAttribute('aria-expanded', String(open));
      head.querySelector('.wgpg-caret').textContent = open ? '▾' : '▸';
      const body = this.root.querySelector('#' + head.getAttribute('aria-controls'));
      if (body) body.hidden = !open;
      const strip = head.closest('.wgpg-drawer');
      if (strip) strip.classList.toggle('open', open);
    }

    /* A reader lands on the workgraph every time: what the drawers remember is that they were closed. */
    closeDrawers() {
      for (const head of this.root.querySelectorAll('.wgpg-drawer-head')) this.disclose(head, false);
    }

    /* ---- reading and writing the document ---- */

    /* Compiling is quick enough to do while the reader types, and building is quick enough that a button for it was
       only ever in the way. Rebuilding still throws a run away, so it waits for the typing to stop, and then happens
       only if the workgraph it would draw is not the one already running. */
    edited() {
      if (this.sayTimer) this.say(''); /* a confirmation is about the document as it was a moment ago */
      if (this.timer) clearTimeout(this.timer);
      if (this.buildTimer) clearTimeout(this.buildTimer);
      this.timer = setTimeout(() => this.validate(), DEBOUNCE);
      this.buildTimer = setTimeout(() => this.rebuild(), REBUILD);
    }

    /* The compiled spec, canonically, so that a change to a comment, a label or anything else the model does not carry
       leaves the run alone. */
    shape() {
      return this.result && this.result.spec ? JSON.stringify(this.result.spec, (k, v) => (v && typeof v === 'object' && !Array.isArray(v) ? Object.keys(v).sort().reduce((o, kk) => ((o[kk] = v[kk]), o), {}) : v)) : null;
    }

    rebuild() {
      if (this.errors().length) return;
      const shape = this.shape();
      if (shape && shape === this.built) return;
      this.build();
    }

    errors() {
      return this.result ? this.result.problems.filter((p) => p.severity === 'error') : [];
    }

    validate() {
      const CWL = root.WorkgraphCwl;
      const text = this.editor.getValue();
      const reader = this.yaml ? 'js-yaml' : "The compiler's own reader";
      let doc = null;
      let syntax = null;
      try {
        doc = this.yaml ? this.yaml.load(text) : CWL.parseYaml(text);
      } catch (e) {
        syntax = e;
      }
      if (syntax) {
        const line = syntax.mark && syntax.mark.line != null ? syntax.mark.line + 1 : 1;
        this.result = { spec: null, problems: [{ path: '', severity: 'error', rule: 'yaml', message: String(syntax.message || syntax).split('\n')[0], line }], ignored: '', sandbox: '', notes: '' };
      } else {
        this.result = CWL.compile(doc, this.settings);
        /* Two questions, asked separately: the compiler says what the picture should be, and the published hint schema
           says whether the document is written in the vocabulary. The reader sees one list. */
        if (this.schema) this.result.problems = this.result.problems.concat(CWL.checkSchema(doc, this.schema));
        for (const p of this.result.problems) p.line = CWL.locate(text, p.path);
        this.result.problems.sort((a, b) => a.line - b.line || (a.severity === b.severity ? 0 : a.severity === 'error' ? -1 : 1));
      }
      this.result.reader = reader;
      this.renderProblems();
      this.renderParsed();
      this.editor.setProblems(this.result.problems);
      this.writeUrl();
      this.save();
      return this.result;
    }

    renderProblems() {
      const problems = this.result.problems;
      const errors = problems.filter((p) => p.severity === 'error').length;
      const warnings = problems.length - errors;
      const parts = [];
      if (errors) parts.push(`${errors} error${errors === 1 ? '' : 's'}`);
      if (warnings) parts.push(`${warnings} warning${warnings === 1 ? '' : 's'}`);
      this.problemsLabel.textContent = parts.length ? parts.join(', ') : 'no problems';
      this.root.querySelector('.wgpg-problems').classList.toggle('has-errors', errors > 0);
      this.problemsList.innerHTML = problems
        .map(
          (p) => `<li class="wgpg-problem wgpg-${p.severity}"><button type="button" data-act="problem" data-line="${p.line || 1}">
            <span class="wgpg-problem-line">${p.line || 1}</span>
            <span class="wgpg-problem-rule">${esc(p.rule)}</span>
            <span class="wgpg-problem-text">${esc(p.message)}</span></button></li>`
        )
        .join('');
      this.showBlocked(
        errors > 0,
        `${errors} error${errors === 1 ? '' : 's'} on the left. A document with an error would not submit either, so the workgraph beside this is the last one that compiled.`
      );
      if (errors) this.say(`${errors} error${errors === 1 ? '' : 's'}: waiting for a document that compiles`);
      else this.say(''); /* the line said a condition, and the condition is over: what stood there goes with it */
    }

    /* The honest half: what the picture is not, in the order a reader doubts it. */
    renderParsed() {
      const r = this.result;
      const block = (title, body, empty) => `<h2>${esc(title)}</h2>${body ? `<pre>${esc(body)}</pre>` : `<p class="wgpg-empty">${esc(empty)}</p>`}`;
      this.bodies.parsed.innerHTML = `
        <div class="wgpg-parsed">
          <p class="wgpg-note">The spec below is everything the simulation knows. ${esc(r.reader || 'The reader')} ${r.spec ? 'parsed' : 'could not parse'} the document${this.schema ? `, and the hints are checked against <a href="${esc(asset('../../schemas/dirac-1.0.json'))}">dirac-1.0.json</a>` : ', and the hint schema was not loaded'}.</p>
          ${block('The spec the simulation runs', r.spec ? JSON.stringify(r.spec, null, 2) : '', 'nothing compiled: fix the errors on the left')}
          ${block('Dropped on the floor', r.ignored, 'nothing was dropped')}
          <h2>The output sandbox</h2>
          <p class="wgpg-note">Sandbox capture is a job-wrapper side channel driven by these glob patterns. Those files exist, and none of them is dataflow: they never travel an edge and never become an output box (DX-ADR-007).</p>
          ${r.sandbox ? `<pre>${esc(r.sandbox)}</pre>` : '<p class="wgpg-empty">the document asks for no patterns, so the installation default applies</p>'}
          ${block('What the playground had to change', r.notes, 'nothing had to be scaled down')}
        </div>`;
    }

    renderSettings() {
      const CWL = root.WorkgraphCwl;
      this.bodies.settings.innerHTML = `
        <div class="wgpg-settings">
          <p class="wgpg-note">None of this is in the document, and none of it can be. How long a job takes, how often one fails, how many files a query returns and how many parcels run at once belong to a site and to a workgraph's scale, not to a workgraph — a <code>dirac:</code> field carrying them would break the one property this page is for, which is that what you are editing would really submit.</p>
          <div class="wgpg-grid">
            ${CWL.RUN_SETTING_FIELDS.map(
              (f) => `<label class="wgpg-field wgpg-setting">
                <span>${esc(f.label)}</span>
                <input type="number" data-act="setting" data-key="${f.key}" value="${this.settings[f.key]}" min="${f.min}" max="${f.max}" step="${f.step}">
                <small>${esc(f.hint)}</small>
              </label>`
            ).join('')}
          </div>
          <p class="wgpg-note">Every pool is clamped to the ceiling above, and what was clamped is said in <b>${esc(DRAWERS.parsed)}</b>. Durations and rates are given to the compute transformations; a data transformation keeps the model's own, since moving a file is not running a job.</p>
        </div>`;
    }

    setSetting(key, value) {
      const CWL = root.WorkgraphCwl;
      const field = CWL.RUN_SETTING_FIELDS.find((f) => f.key === key);
      if (!field) return;
      const n = Number(value);
      if (!isFinite(n)) return;
      this.settings[key] = Math.max(field.min, Math.min(field.max, n));
      this.edited();
    }

    /* ---- building ---- */

    /* The picture is dimmed for one reason: the document in front of the reader does not compile, so what is drawn is
       the last one that did. Everything else the editor does reaches the picture within the second. */
    showBlocked(blocked, why) {
      this.blocked = blocked;
      this.workgraph.classList.toggle('stale', blocked);
      this.staleOverlay.hidden = !blocked;
      this.staleWhy.textContent = why || '';
      if (blocked) this.simHost.setAttribute('inert', '');
      else this.simHost.removeAttribute('inert');
      if (this.widget && blocked) this.widget.paused = true;
    }

    build() {
      const errors = this.errors();
      if (!this.result || errors.length) {
        this.say(`${errors.length || 'no'} error${errors.length === 1 ? '' : 's'} in the document: ${errors.length ? 'the first is on line ' + errors[0].line + ', ' + errors[0].message : 'nothing has compiled yet'}`, true);
        const first = this.problemsList.querySelector('[data-act="problem"]');
        if (first) first.focus();
        return;
      }
      this.built = this.shape();
      const wasMax = !!(this.widget && this.widget.maxOn); /* the reader maximised the model, not this build of it */
      this.disposeWidget();
      const host = el('div', {});
      this.simHost.appendChild(host);
      this.widget = root.WorkgraphSim.mountSpec(host, this.result.spec);
      if (wasMax) this.widget.setMax(true);
      this.showBlocked(false);
      /* A rebuild is routine and shows nothing: the picture changed under the reader, and a model in `New` glows on its own
         playback control until it is started, which is the affordance, on the button rather than at the far end of the
         toolbar from it. A reader who cannot see either needs the event, and gets it. */
      this.announce('workgraph rebuilt');
    }

    /* Three kinds of message land in the toolbar's line, and each lives as long as it is true. A condition — an error count
       — lasts exactly as long as the condition, and `renderProblems` clears it when the errors go. A confirmation is true
       for a moment: `flash` takes it away after a beat, or the reader's next keystroke does. A routine event is not written
       here at all. The line is not there while there is nothing to say, which is the state line's rule next door. */
    say(text, loud) {
      if (this.sayTimer) clearTimeout(this.sayTimer);
      this.sayTimer = null;
      this.status.textContent = text || '';
      this.status.classList.toggle('loud', !!loud);
    }

    flash(text) {
      this.say(text);
      this.sayTimer = setTimeout(() => this.say(''), CONFIRM);
    }

    /* What the line no longer shows, for a reader who is not watching it: a live region of its own, out of sight, so that
       the toolbar keeps only what earns its space and an announcement is still made. It is cleared first because a reader
       who builds twice hears nothing the second time if the text has not changed. */
    announce(text) {
      if (this.announceTimer) clearTimeout(this.announceTimer);
      this.announcer.textContent = '';
      this.announceTimer = setTimeout(() => {
        if (this.announcer) this.announcer.textContent = text;
      }, 60);
    }

    disposeWidget() {
      if (this.widget && this.widget.dispose) this.widget.dispose();
      this.widget = null;
      this.simHost.innerHTML = '';
    }

    /* ---- what is kept between visits ---- */

    /* Written a moment after the last change rather than on each one, since the gutter alone would write a hundred
       times while it is dragged. */
    save() {
      if (this.saveTimer) clearTimeout(this.saveTimer);
      this.saveTimer = setTimeout(() => {
        if (!this.root) return;
        remember({
          doc: this.editor.getValue(),
          example: this.example,
          settings: this.settings,
          split: Math.round(this.split),
          seen: this.seen,
        });
      }, 300);
    }

    async reset() {
      const CWL = root.WorkgraphCwl;
      if (this.ownDocument() && typeof confirm === 'function' && !confirm('Reset the playground?\n\nThe document in the editor is not one of the examples, and this is the only copy of it.')) return;
      forget();
      /* Everything this browser was holding, including having been told what the page is: a reset leaves the page the
         way a reader who has never been here finds it, dialog and all, which is also how the examples are reached. */
      this.seen = false;
      this.settings = Object.assign({}, CWL.DEFAULT_RUN_SETTINGS);
      this.renderSettings();
      this.setSplit(46);
      this.closeDrawers();
      history.replaceState(null, '', location.pathname);
      this.linkedText = null;
      await this.loadExample(EXAMPLE_FILES[0].file);
      this.openLibrary();
    }

    /* ---- examples and links ---- */

    async loadExample(file) {
      if (!file) return;
      try {
        const res = await fetch(asset('../examples/' + file));
        if (!res.ok) throw new Error(res.status + ' ' + res.statusText);
        const text = await res.text();
        this.example = file;
        this.exampleText = text;
        this.editor.setValue(text);
      } catch (e) {
        this.say(`${file} could not be loaded: ${e.message}`, true);
        return;
      }
      this.validate();
      this.build();
      this.save();
    }

    async readUrl() {
      const saved = remembered() || {};
      this.seen = !!saved.seen;
      if (saved.settings) {
        for (const [key, value] of Object.entries(saved.settings)) {
          if (key in this.settings && typeof value === 'number' && isFinite(value)) this.settings[key] = value;
        }
      }
      if (typeof saved.split === 'number') this.setSplit(saved.split);
      /* A link carries what it was shared with, so its settings are written over the remembered ones. */
      const params = new URLSearchParams(location.search);
      for (const [key, short] of Object.entries(PARAM)) {
        const raw = params.get(short);
        if (raw == null) continue;
        const n = Number(raw);
        if (isFinite(n)) this.settings[key] = n;
      }
      this.renderSettings();
      const hash = new URLSearchParams(location.hash.replace(/^#/, ''));
      const packed = hash.get('doc');
      if (packed) {
        try {
          const text = await unsqueeze(packed);
          this.linkedText = text;
          this.fromLink = true;
          this.editor.setValue(text);
          return;
        } catch (e) {
          this.say('the link carried a document this page could not unpack', true);
        }
      }
      if (typeof saved.doc === 'string' && saved.doc.trim()) {
        this.editor.setValue(saved.doc);
        this.example = saved.example || '';
        if (saved.example) {
          /* what the example says now, so a reset can tell an edited document from one that was only opened */
          try {
            const res = await fetch(asset('../examples/' + saved.example));
            if (res.ok) this.exampleText = await res.text();
          } catch (e) {}
        }
        return;
      }
      const first = EXAMPLE_FILES[0];
      try {
        const res = await fetch(asset('../examples/' + first.file));
        if (res.ok) {
          this.example = first.file;
          this.exampleText = await res.text();
          this.editor.setValue(this.exampleText);
        }
      } catch (e) {
        /* offline and unserved: the page still works, it just opens empty */
      }
    }

    /* The settings go in the query string as they change; the document goes in the hash only when someone asks for a
       link, since squeezing it on every keystroke would be work nobody asked for. A hash that no longer describes what
       is in the editor is worse than none, so the first edit away from a linked document drops it. */
    writeUrl() {
      const CWL = root.WorkgraphCwl;
      const params = new URLSearchParams(location.search);
      for (const [key, short] of Object.entries(PARAM)) {
        if (this.settings[key] === CWL.DEFAULT_RUN_SETTINGS[key]) params.delete(short);
        else params.set(short, String(this.settings[key]));
      }
      const query = params.toString();
      const linked = /(^|[#&])doc=/.test(location.hash) && this.linkedText === this.editor.getValue();
      history.replaceState(null, '', location.pathname + (query ? '?' + query : '') + (linked ? location.hash : ''));
    }

    async copyLink() {
      const text = this.editor.getValue();
      const packed = await squeeze(text);
      const url = location.origin + location.pathname + location.search + '#doc=' + packed;
      if (url.length > LINK_MAX) {
        this.say(`this document is too long to put in a link (${url.length} characters, and about ${LINK_MAX} is what survives being pasted around)`, true);
        return;
      }
      this.linkedText = text;
      history.replaceState(null, '', location.pathname + location.search + '#doc=' + packed);
      try {
        await navigator.clipboard.writeText(url);
        this.flash('a link to this document is on your clipboard');
      } catch (e) {
        this.flash('the link is in the address bar');
      }
    }

    dispose() {
      if (this.timer) clearTimeout(this.timer);
      if (this.buildTimer) clearTimeout(this.buildTimer);
      if (this.saveTimer) clearTimeout(this.saveTimer);
      if (this.sayTimer) clearTimeout(this.sayTimer);
      if (this.announceTimer) clearTimeout(this.announceTimer);
      if (this.refit) root.removeEventListener('resize', this.refit);
      if (this.density && this.density.removeEventListener) this.density.removeEventListener('change', this.onDensity);
      this.disposeWidget();
      if (this.editor) this.editor.dispose();
      if (this.root && this.root.parentNode) this.root.parentNode.removeChild(this.root);
      this.root = null;
      document.body.classList.remove('wgpg-page');
    }
  }

  /* ------------------------------------------------------------------ */
  /* Loading, and Material's instant navigation                          */
  /* ------------------------------------------------------------------ */

  let yamlLoad = null;
  let schemaLoad = null;

  /* The published hint vocabulary (DX-ADR-007). It is part of the site, not of a CDN, so the editor knows the schema
     with the network blocked. */
  function loadSchema() {
    if (schemaLoad) return schemaLoad;
    schemaLoad = fetch(asset('../../schemas/dirac-1.0.json'))
      .then((res) => (res.ok ? res.json() : null))
      .catch(() => null);
    return schemaLoad;
  }

  function loadYaml() {
    if (yamlLoad) return yamlLoad;
    yamlLoad = new Promise((resolve) => {
      if (root.jsyaml) return resolve(root.jsyaml);
      const script = document.createElement('script');
      script.src = JS_YAML_URL;
      script.async = true;
      script.onload = () => resolve(root.jsyaml || null);
      script.onerror = () => resolve(null);
      document.head.appendChild(script);
    });
    return yamlLoad;
  }

  let page = null;

  function mountPlayground() {
    const host = document.querySelector('#wg-playground');
    if (page) {
      /* instant navigation swapped the document out from under it, or replaced the mount div */
      if (host && page.host === host) return;
      page.dispose();
      page = null;
    }
    if (!host) return;
    page = new Playground(host);
  }

  /* `current` is how a driver, or a console, reaches the page that is mounted. */
  root.WorkgraphPlayground = { Playground, mountPlayground, current: () => page, EXAMPLES, EXAMPLE_FILES, PARAM, squeeze, unsqueeze };

  if (root.document$ && typeof root.document$.subscribe === 'function') root.document$.subscribe(() => mountPlayground());
  else if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mountPlayground);
  else mountPlayground();
})(typeof window !== 'undefined' ? window : globalThis);
