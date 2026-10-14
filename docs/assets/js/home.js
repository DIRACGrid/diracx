/*
  The landing page's hero (overrides/home.html, styled by assets/css/home.css).

  It plays three acts in turn, nine seconds each: a workgraph running, data and jobs moving between sites, and the
  communities that run DIRAC. The first two are SVG drawn here; the third is markup in the template. Each picture is
  described once, as data, in two layouts: a wide one, which on a desktop lies under the copy and keeps its subject to
  the right of it, and a tall one for a phone, where the copy sits above the picture instead. The layout follows the
  shape of the stage, so it is redrawn when that shape crosses from one to the other, and not otherwise.

  Everything moving is a CSS animation on a class from home.css, so the stylesheet decides about reduced motion and
  the script only decides what is on screen. An act stays while the reader points at the buttons that pick one or has
  focus in the hero, and everything stops while the hero is scrolled out of view or the tab is in the background; a
  reader who prefers reduced motion gets each act as a still picture, which changes only when they pick another.
*/
(function (root) {
  'use strict';

  const DWELL = 9000;
  /* The stage is drawn wide at this width-to-height ratio and above, tall below it. */
  const WIDE = 1.15;

  /*
    The pictures' colours, one palette per colour scheme of the theme (slate and default); home.css carries the same
    split for the rest of the hero. The brand's green and cyan, and the colours that say which source an item came
    from, are the same in both.
  */
  const PALETTES = {
    slate: {
      card: '#0d1a22', cardLine: '#2a4a57', well: '#060a0f', slotLine: '#2f4a55', track: '#18303a',
      ink: '#e8eef4', dim: '#8fb3c2', faint: '#4f6b77', chip: '#1f3a12', chipInk: '#b9e38a', done: '#12260f',
      green: '#77b52c', greenLight: '#9ed35a', cyan: '#00afca', cyanLight: '#7fdcf0', cyanDeep: '#2fbfd8',
      amber: '#e0a526', violet: '#a99cf0', coral: '#f07c6a', star: '#cfe7f0', file: '#cfdcd3',
      globe: '#08131a', globeLine: '#1d3a46', meridian: '#12303b', transferLight: '#bdf0fb', jobLight: '#d7f0b8',
    },
    default: {
      card: '#ffffff', cardLine: '#bfd0d8', well: '#eef3f6', slotLine: '#c3d3db', track: '#dde7ec',
      ink: '#14222b', dim: '#4b616c', faint: '#8aa0aa', chip: '#e3f1d3', chipInk: '#3b6e12', done: '#eef7e4',
      green: '#77b52c', greenLight: '#5f9e1f', cyan: '#00afca', cyanLight: '#0090a8', cyanDeep: '#0090a8',
      amber: '#e0a526', violet: '#7c6fd6', coral: '#e8604c', star: '#9fb3bd', file: '#5b6b73',
      globe: '#e4edf1', globeLine: '#b4c8d1', meridian: '#cddbe2', transferLight: '#007a8f', jobLight: '#4f8f17',
    },
  };

  /* The palette being drawn with; draw() sets it from the theme's scheme before drawing. */
  let C = PALETTES.slate;
  /* Colour says which source an item descends from, as in the simulator on the Transformation System pages. */
  const sources = () => [C.green, C.cyan, C.amber];

  function scheme() {
    return document.body.getAttribute('data-md-color-scheme') === 'default' ? 'default' : 'slate';
  }

  /* A small seeded generator, so the stars and the stagger of the slots are the same on every load. */
  function random(seed) {
    let s = seed >>> 0;
    return () => {
      s = (s * 1664525 + 1013904223) >>> 0;
      return s / 4294967296;
    };
  }

  const n = (v) => Math.round(v * 10) / 10;

  /* A view is [width, height] from the origin, or [x, y, width, height] for a band cut out of a wide drawing. */
  function svg(view, align, label, body) {
    const role = label ? `role="img" aria-label="${label}"` : 'aria-hidden="true" focusable="false"';
    const box = view.length === 4 ? view.join(' ') : `0 0 ${view[0]} ${view[1]}`;
    return `<svg class="dxh__svg" viewBox="${box}" preserveAspectRatio="${align}" ${role}>${body}</svg>`;
  }

  /*
    A wide drawing serves two layouts. Under the copy on a desktop it fills the stage and keeps its right edge, since the
    copy covers its left. On a tablet the copy is above it instead, so the stage shows only the band the subject is in,
    centred.
  */
  function layout(layouts, mode) {
    if (mode !== 'band') return layouts[mode];
    return { ...layouts.wide, view: layouts.wide.band, align: 'xMidYMid meet' };
  }

  function text(x, y, size, fill, value, anchor) {
    const a = anchor ? ` text-anchor="${anchor}"` : '';
    return `<text x="${n(x)}" y="${n(y)}" font-size="${size}" fill="${fill}"${a}>${value}</text>`;
  }

  /* -- The sky behind every act -------------------------------------------------------------------------------------- */

  function sky() {
    const r = random(7);
    let stars = '';
    for (let i = 0; i < 46; i++) {
      const x = n(r() * 1600), y = n(r() * 620), size = n(0.8 + r() * 0.8), delay = n(-r() * 4.5);
      stars += `<circle class="dxh-twinkle" cx="${x}" cy="${y}" r="${size}" fill="${C.star}" style="animation-delay:${delay}s"></circle>`;
    }
    return svg([1600, 800], 'xMidYMid slice', null, stars);
  }

  /* -- Act one: a workgraph ------------------------------------------------------------------------------------------- */

  const CARD_H = 132;

  /* A source or a sink of the workgraph: a title, and a second line under it when it has one. */
  function box(b) {
    const cx = b.x + b.w / 2;
    const stroke = b.done ? C.green : C.cardLine;
    const fill = b.done ? C.done : C.card;
    const frame = `<rect x="${b.x}" y="${b.y}" width="${b.w}" height="${b.h}" rx="8" fill="${fill}" stroke="${stroke}"></rect>`;
    if (!b.sub) return frame + text(cx, b.y + b.h / 2 + 4, 12, C.ink, b.title, 'middle');
    return frame
      + text(cx, b.y + 21, 11, b.done ? C.chipInk : C.dim, b.title, 'middle')
      + text(cx, b.y + 39, 12, C.ink, b.sub, 'middle');
  }

  function pool(x, y, kind, r) {
    if (kind === 'seeds') {
      return text(x + 38, y + 28, 10, C.dim, 'seeds', 'middle')
        + `<text class="dxh-pool" x="${x + 38}" y="${y + 46}" font-size="12" fill="${C.violet}" text-anchor="middle">#4 096</text>`;
    }
    let out = '';
    for (let i = 0; i < 10; i++) {
      const cx = x + 12 + (i % 4) * 16, cy = y + 13 + Math.floor(i / 4) * 16;
      const colour = kind === 'merged' ? [C.cyan, C.green, C.violet, C.coral][i % 4] : sources()[i % 3];
      const cls = r() < 0.3 ? ' class="dxh-pool"' : '';
      const style = cls ? ` style="animation-delay:${n(-r() * 2.6)}s"` : '';
      if (kind === 'merged' && i % 2) {
        out += `<polygon${cls}${style} points="${cx},${cy - 4.5} ${cx + 4.5},${cy + 3.5} ${cx - 4.5},${cy + 3.5}" fill="${colour}"></polygon>`;
      } else if (kind === 'merged') {
        out += `<rect${cls}${style} x="${cx - 3.5}" y="${cy - 3.5}" width="7" height="7" fill="${colour}"></rect>`;
      } else {
        out += `<circle${cls}${style} cx="${cx}" cy="${cy}" r="3.6" fill="${colour}"></circle>`;
      }
    }
    return out;
  }

  function card(c, w, seed) {
    const r = random(seed);
    const { x, y } = c;
    /* A marked card is one the hero's copy lines up with (see align() below). */
    const mark = c.mark ? ` data-dxh-mark="${c.mark}"` : '';
    let out = `<rect${mark} x="${x}" y="${y}" width="${w}" height="${CARD_H}" rx="10" fill="${C.card}" stroke="${C.cardLine}"></rect>`
      + text(x + 14, y + 24, 13, C.ink, c.name)
      + `<rect x="${x + w - 64}" y="${y + 11}" width="52" height="18" rx="9" fill="${C.chip}"></rect>`
      + text(x + w - 38, y + 24, 10, C.chipInk, 'Active', 'middle')
      + `<rect x="${x + 14}" y="${y + 38}" width="76" height="66" rx="6" fill="${C.well}"></rect>`
      + pool(x + 14, y + 38, c.pool, r);
    /* Six slots, one per job the backend runs at once: each fills as its job runs and turns green when it is done. */
    const sx = x + 102, gap = 6, sw = (w - 102 - 14 - 2 * gap) / 3, sh = 30;
    for (let i = 0; i < 6; i++) {
      const px = n(sx + (i % 3) * (sw + gap)), py = y + 38 + Math.floor(i / 3) * (sh + gap);
      out += `<rect x="${px}" y="${py}" width="${n(sw)}" height="${sh}" rx="4" fill="none" stroke="${C.slotLine}"></rect>`;
      if (i < c.busy) {
        const dur = n(3.6 + r() * 3), delay = n(-r() * dur);
        out += `<rect class="dxh-slot" x="${n(px + 3)}" y="${py + 3}" width="${n(sw - 6)}" height="${sh - 6}" rx="2"`
          + ` style="animation-duration:calc(${dur} * var(--dxh-t));animation-delay:${delay}s"></rect>`;
      }
    }
    /* The feeder's bar: how much of the input it has processed. */
    out += `<rect x="${x + 14}" y="${y + 114}" width="${w - 28}" height="5" rx="2.5" fill="${C.track}"></rect>`
      + `<rect class="dxh-feed" x="${x + 14}" y="${y + 114}" width="${n((w - 28) * c.fed)}" height="5" rx="2.5" fill="${C.green}"`
      + ` style="animation-delay:${n(-r() * 18)}s"></rect>`;
    return out;
  }

  function item(shape, colour, path, delay) {
    const style = `offset-path:path('${path}');animation-delay:${delay}s`;
    switch (shape) {
      case 'square': return `<rect class="dxh-flow" x="-4" y="-4" width="8" height="8" fill="${colour}" style="${style}"></rect>`;
      case 'triangle': return `<polygon class="dxh-flow" points="0,-5 5,4 -5,4" fill="${colour}" style="${style}"></polygon>`;
      case 'file': return `<rect class="dxh-flow" x="-5" y="-5" width="10" height="10" rx="2" fill="${C.file}" style="${style}"></rect>`;
      default: return `<circle class="dxh-flow" r="4" fill="${colour}" style="${style}"></circle>`;
    }
  }

  const WORKGRAPH = {
    wide: {
      /*
        The input sits above its step rather than beside it, so the picture keeps clear of the copy on its left. The
        view is cut close round the drawing and fitted whole, so the hero can be no taller than the drawing needs.
      */
      view: [0, 40, 1600, 600], align: 'xMaxYMid meet', cardW: 240, band: [760, 50, 840, 580],
      input: { x: 840, y: 70, w: 160, h: 52 },
      process: { x: 800, y: 170 },
      simulate: { x: 800, y: 470 },
      merge: { x: 1150, y: 320 },
      output: { x: 1450, y: 360, w: 120, h: 52 },
      edges: [
        'M920 122 L920 170',
        'M1040 236 C1095 236 1095 386 1150 386',
        'M1040 536 C1095 536 1095 386 1150 386',
        'M1390 386 L1450 386',
      ],
    },
    tall: {
      view: [600, 680], align: 'xMidYMid meet', cardW: 260,
      input: { x: 220, y: 20, w: 160, h: 52 },
      process: { x: 30, y: 130 },
      simulate: { x: 310, y: 130 },
      merge: { x: 170, y: 390 },
      output: { x: 220, y: 600, w: 160, h: 52 },
      edges: [
        'M300 72 C300 101 160 101 160 130',
        'M160 262 C160 326 300 326 300 390',
        'M440 262 C440 326 300 326 300 390',
        'M300 522 L300 600',
      ],
    },
  };

  function workgraph(mode) {
    const L = layout(WORKGRAPH, mode);
    const [toProcess, fromProcess, fromSimulate, toOutput] = L.edges;
    let out = `<g style="font-family:var(--md-code-font-family)">`;
    out += `<g fill="none" stroke="${C.slotLine}" stroke-width="1.5">${L.edges.map((d) => `<path d="${d}"></path>`).join('')}</g>`;
    /* What travels an edge looks like what it is: the source's files, then the processed ones, then the merged ones. */
    out += item('circle', C.green, toProcess, 0)
      + item('circle', C.cyan, toProcess, -1.1)
      + item('circle', C.amber, toProcess, -2.3)
      + item('square', C.cyan, fromProcess, -0.6)
      + item('square', C.green, fromProcess, -2.1)
      + item('triangle', C.violet, fromSimulate, -0.3)
      + item('triangle', C.coral, fromSimulate, -1.8)
      + item('file', null, toOutput, -1.2);
    out += box({ ...L.input, title: 'input data' })
      + card({ ...L.process, name: 'process', pool: 'files', busy: 6, fed: 0.75, mark: 'head' }, L.cardW, 11)
      + card({ ...L.simulate, name: 'simulate', pool: 'seeds', busy: 6, fed: 0.85 }, L.cardW, 23)
      + card({ ...L.merge, name: 'merge', pool: 'merged', busy: 3, fed: 0.5 }, L.cardW, 37)
      + box({ ...L.output, title: 'output', sub: 'datasets', done: true });
    out += '</g>';
    return svg(L.view, L.align,
      'A workgraph running: input data feeds a process step, a simulate step runs from seeds, and both feed a merge that writes the output datasets',
      out);
  }

  /* -- Act two: transfers and compute across sites --------------------------------------------------------------------- */

  /*
    No site is the centre. Tier-1s keep the data and pass it between themselves; jobs go out to wherever there is
    capacity. Sites are named by role, so any community can see its own computing in the picture.
  */
  const GRID = {
    /*
      The globe is a horizon across the hero, framed like the workgraph; drawn past the edges of its view, it fills
      whatever width the hero has to spare.
    */
    wide: {
      view: [0, 40, 1600, 600], align: 'xMaxYMid meet', size: 12, band: [700, 130, 900, 500],
      globe: { cx: 1050, cy: 1790, r: 1480, lat: [520, 980], mer: [360, 860, 1300] },
      stores: [[800, 400], [1000, 350], [1240, 360], [1440, 430]],
      computes: [[760, 520, 'Tier-2'], [950, 480, 'university'], [1120, 530, 'Tier-2'], [850, 590, 'cloud'],
        [1520, 550, 'HPC'], [1320, 540, 'Tier-2']],
      transfers: [[0, 1, 900, 250], [1, 2, 1120, 230], [2, 3, 1340, 270], [0, 2, 1020, 160], [1, 3, 1220, 180]],
      jobs: [[0, 0, 770, 450], [0, 3, 830, 500], [1, 1, 960, 410], [2, 2, 1170, 440], [2, 5, 1300, 440], [3, 4, 1500, 470]],
      legend: [1390, 150],
    },
    tall: {
      view: [600, 680], align: 'xMidYMid meet', size: 11,
      globe: { cx: 300, cy: 1050, r: 900, lat: [300, 600], mer: [220, 520, 800] },
      stores: [[110, 260], [250, 215], [390, 225], [520, 290]],
      computes: [[80, 420, 'Tier-2'], [210, 380, 'university'], [340, 450, 'Tier-2'], [40, 560, 'cloud'],
        [560, 520, 'HPC'], [460, 420, 'Tier-2']],
      transfers: [[0, 1, 170, 130], [1, 2, 320, 110], [2, 3, 470, 150], [0, 2, 250, 60], [1, 3, 400, 70]],
      jobs: [[0, 0, 80, 330], [0, 3, 40, 420], [1, 1, 210, 290], [2, 2, 350, 330], [2, 5, 450, 320], [3, 4, 560, 400]],
      legend: [24, 30],
    },
  };

  function grid(mode) {
    const L = layout(GRID, mode);
    const g = L.globe;
    const r = random(53);
    const curve = (a, b, cx, cy) => `M${a[0]} ${a[1]} Q${cx} ${cy} ${b[0]} ${b[1]}`;
    let out = `<g style="font-family:var(--md-code-font-family)">`;

    out += `<circle cx="${g.cx}" cy="${g.cy}" r="${g.r}" fill="${C.globe}" stroke="${C.globeLine}" stroke-width="1.2"></circle>`
      + `<g fill="none" stroke="${C.meridian}">${g.lat.map((ry) => `<ellipse cx="${g.cx}" cy="${g.cy}" rx="${g.r}" ry="${ry}"></ellipse>`).join('')}</g>`
      + `<g fill="none" stroke="${C.meridian}">`
      + g.mer.map((rx) => `<ellipse cx="${g.cx}" cy="${g.cy}" rx="${rx}" ry="${g.r}"></ellipse>`).join('') + '</g>';

    let arcs = '', hops = '';
    L.transfers.forEach(([a, b, cx, cy], i) => {
      const there = curve(L.stores[a], L.stores[b], cx, cy);
      arcs += `<path d="${there}" stroke="${C.cyanDeep}"></path>`;
      /* Transfers go both ways between Tier-1s; alternate the direction of the light along them. */
      const path = i % 2 ? curve(L.stores[b], L.stores[a], cx, cy) : there;
      hops += `<circle class="dxh-hop" r="3.5" fill="${C.transferLight}" style="offset-path:path('${path}');animation-delay:${n(-r() * 3.8)}s"></circle>`;
    });
    L.jobs.forEach(([a, b, cx, cy]) => {
      const path = curve(L.stores[a], L.computes[b], cx, cy);
      arcs += `<path d="${path}" stroke="${C.green}"></path>`;
      hops += `<circle class="dxh-hop" r="3.5" fill="${C.jobLight}" style="offset-path:path('${path}');animation-delay:${n(-r() * 3.8)}s"></circle>`;
    });
    /*
      The globe, its meridians and the arcs hold still and only the lights and pings move. Anything that moves across
      the whole picture (turning the meridians, running the arcs' dashes) has the browser repaint all of it on every
      frame, which on the globe's large shapes is enough to make it stutter.
    */
    out += `<g fill="none" stroke-width="1.4" stroke-linecap="round" stroke-dasharray="4 10" opacity=".8">${arcs}</g>${hops}`;

    const s = L.size;
    const right = L.view.length === 4 ? L.view[0] + L.view[2] : L.view[0];
    L.stores.forEach(([x, y], i) => {
      if (i % 2) out += `<circle class="dxh-ping" cx="${x}" cy="${y}" r="4" fill="none" stroke="${C.cyanLight}" vector-effect="non-scaling-stroke" style="animation-delay:${n(-i * 0.7)}s"></circle>`;
      out += `<circle cx="${x}" cy="${y}" r="5" fill="${C.cyanDeep}"></circle>` + text(x, y - 12, s, C.dim, 'Tier-1', 'middle');
    });
    L.computes.forEach(([x, y, label], i) => {
      if (i % 3 === 1) out += `<circle class="dxh-ping" cx="${x}" cy="${y}" r="4" fill="none" stroke="${C.greenLight}" vector-effect="non-scaling-stroke" style="animation-delay:${n(-i * 0.5)}s"></circle>`;
      const anchor = x > right - 90 ? 'end' : 'start';
      out += `<circle cx="${x}" cy="${y}" r="4" fill="${C.green}"></circle>`
        + text(anchor === 'end' ? x - 10 : x + 10, y + 4, s, C.dim, label, anchor);
    });

    const [lx, ly] = L.legend;
    out += `<g font-size="${s}" fill="${C.dim}">`
      + `<line x1="${lx}" y1="${ly}" x2="${lx + 28}" y2="${ly}" stroke="${C.cyanDeep}" stroke-width="1.6" stroke-dasharray="4 4"></line>`
      + text(lx + 36, ly + 4, s, C.dim, 'transfer')
      + `<line x1="${lx}" y1="${ly + 22}" x2="${lx + 28}" y2="${ly + 22}" stroke="${C.green}" stroke-width="1.6" stroke-dasharray="4 4"></line>`
      + text(lx + 36, ly + 26, s, C.dim, 'job')
      + '</g></g>';

    return svg(L.view, L.align,
      'Sites around a globe with no centre: data transfers arc between Tier-1 storage sites while jobs go out to Tier-2s, a university, a cloud and an HPC centre',
      out);
  }

  const SCENES = { workgraph, grid };

  /* -- The hero --------------------------------------------------------------------------------------------------------- */

  function hero(el) {
    const stage = el.querySelector('[data-dxh-stage]');
    const skyEl = el.querySelector('[data-dxh-sky]');
    const acts = [...el.querySelectorAll('[data-dxh-act]')];
    const tabs = [...el.querySelectorAll('[data-dxh-tab]')];
    const controls = el.querySelector('[data-dxh-controls]');
    const query = (q) => (root.matchMedia ? root.matchMedia(q) : { matches: false });
    /* The breakpoint in home.css below which the copy sits above the picture rather than on it. */
    const stacked = query('screen and (max-width: 59.984375em)');
    /* A reader who prefers reduced motion gets still pictures that only change when they pick one. */
    const still = query('(prefers-reduced-motion: reduce)').matches;

    let index = 0, mode = null, timer = null, started = 0, remaining = DWELL;
    let offscreen = false, pointing = false, focused = false;

    /* Redrawn when the stage's shape crosses into another layout, or the theme's colour scheme changes. */
    let drawn = null;
    function draw() {
      const box = stage.getBoundingClientRect();
      if (!box.width || !box.height) return;
      mode = box.width / box.height < WIDE ? 'tall' : stacked.matches ? 'band' : 'wide';
      const key = `${mode} ${scheme()}`;
      if (key === drawn) return;
      drawn = key;
      C = PALETTES[scheme()];
      skyEl.innerHTML = sky();
      for (const act of acts) {
        const scene = SCENES[act.dataset.dxhScene];
        if (scene) act.innerHTML = scene(mode);
      }
    }

    /*
      On a wide screen the logo lines up with the top of the workgraph's first step. Where that lands depends on how the
      drawing fits the hero, so it is measured and handed to home.css; the line holds while the other acts play.
    */
    function align() {
      const head = el.querySelector('[data-dxh-mark="head"]');
      if (mode !== 'wide' || !head) {
        el.style.removeProperty('--dxh-head');
        return;
      }
      const top = Math.round(head.getBoundingClientRect().top - el.getBoundingClientRect().top);
      if (el.style.getPropertyValue('--dxh-head') !== `${top}px`) el.style.setProperty('--dxh-head', `${top}px`);
    }

    function layoutStage() {
      draw();
      align();
    }

    function restartProgress() {
      const bar = tabs[index].querySelector('.dxh__progress > span');
      if (!bar) return;
      bar.style.animation = 'none';
      void bar.offsetWidth;
      bar.style.animation = '';
    }

    /* Seen: on screen and in the foreground tab. Only then does anything in the hero move. */
    function seen() {
      return !offscreen && !document.hidden;
    }

    /* Held: the reader is pointing at the buttons or has focus in the hero, so the act on screen stays put. */
    function holding() {
      return pointing || focused;
    }

    function advancing() {
      return !still && seen() && !holding();
    }

    function schedule() {
      clearTimeout(timer);
      timer = null;
      if (!advancing()) return;
      started = Date.now();
      timer = setTimeout(() => show((index + 1) % acts.length), remaining);
    }

    /* Stop the clock on the act and keep what it had left, for when the hold ends. */
    function hold() {
      if (timer) remaining = Math.max(0, remaining - (Date.now() - started));
      clearTimeout(timer);
      timer = null;
    }

    function sync() {
      el.classList.toggle('is-still', still);
      el.classList.toggle('is-paused', !seen());
      el.classList.toggle('is-holding', holding());
    }

    function show(i) {
      index = i;
      remaining = DWELL;
      acts.forEach((act, k) => {
        act.classList.toggle('is-active', k === i);
        if (k === i) act.removeAttribute('aria-hidden');
        else act.setAttribute('aria-hidden', 'true');
      });
      tabs.forEach((tab, k) => tab.setAttribute('aria-pressed', String(k === i)));
      restartProgress();
      schedule();
    }

    /* Coming back into view or to the foreground: the act starts its count again, and its bar with it. */
    function onSeen() {
      sync();
      if (seen()) {
        remaining = DWELL;
        restartProgress();
      }
      schedule();
    }

    function onHold() {
      sync();
      if (holding()) hold();
      else schedule();
    }

    const onTab = (event) => show(tabs.indexOf(event.currentTarget));
    const onEnter = () => { pointing = true; onHold(); };
    const onLeave = () => { pointing = false; onHold(); };
    /* Focus holds an act only when it is the keyboard's: a click leaves focus on the tab it pressed, and should not. */
    const onFocusIn = (event) => {
      if (!event.target.matches(':focus-visible')) return;
      focused = true;
      onHold();
    };
    const onFocusOut = (event) => {
      if (el.contains(event.relatedTarget)) return;
      focused = false;
      onHold();
    };
    tabs.forEach((tab) => tab.addEventListener('click', onTab));
    controls.addEventListener('pointerenter', onEnter);
    controls.addEventListener('pointerleave', onLeave);
    el.addEventListener('focusin', onFocusIn);
    el.addEventListener('focusout', onFocusOut);
    document.addEventListener('visibilitychange', onSeen);

    /* The theme's palette toggle writes its scheme on the body; the pictures follow it. */
    const palette = root.MutationObserver ? new MutationObserver(layoutStage) : null;
    if (palette) palette.observe(document.body, { attributes: true, attributeFilter: ['data-md-color-scheme'] });

    const resize = root.ResizeObserver ? new ResizeObserver(layoutStage) : null;
    if (resize) resize.observe(stage);
    else root.addEventListener('resize', layoutStage);

    const view = root.IntersectionObserver
      ? new IntersectionObserver(([entry]) => { offscreen = !entry.isIntersecting; onSeen(); }, { threshold: 0.1 })
      : null;
    if (view) view.observe(el);

    controls.hidden = false;
    layoutStage();
    sync();
    show(0);

    return function dispose() {
      clearTimeout(timer);
      tabs.forEach((tab) => tab.removeEventListener('click', onTab));
      controls.removeEventListener('pointerenter', onEnter);
      controls.removeEventListener('pointerleave', onLeave);
      el.removeEventListener('focusin', onFocusIn);
      el.removeEventListener('focusout', onFocusOut);
      document.removeEventListener('visibilitychange', onSeen);
      if (resize) resize.disconnect();
      else root.removeEventListener('resize', layoutStage);
      if (view) view.disconnect();
      if (palette) palette.disconnect();
    };
  }

  /* Instant loading swaps pages without reloading this script, so the hero is mounted again on every page it is on. */
  let dispose = null;
  function mount() {
    if (dispose) dispose();
    dispose = null;
    const el = document.querySelector('[data-dxh]');
    if (el && !el.dataset.dxhMounted) {
      el.dataset.dxhMounted = '1';
      dispose = hero(el);
    }
  }

  root.DiracxHero = { sky, workgraph, grid };
  if (typeof document !== 'undefined') {
    if (root.document$ && typeof root.document$.subscribe === 'function') root.document$.subscribe(() => mount());
    else if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
    else mount();
  }
})(typeof window !== 'undefined' ? window : globalThis);
