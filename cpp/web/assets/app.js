// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-web: the browser side. Views of the mesh model (/api/mesh), host
// and paglet pages, forms made from interface descriptions, and the shell
// commands that do the same. Elements are built with textContent only.
'use strict';

const state = {
  model: null,
  version: -1,
  route: { view: 'mesh' },
  open: new Set(),      // keys of open <details>, kept across re-renders
  logs: new Map(),      // paglet -> [{time, text, ok}]
  pagletCache: new Map(),
  signedOut: false,
};

// -- helpers --------------------------------------------------------------------

function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v === undefined || v === null || v === false) continue;
      if (k === 'class') el.className = v;
      else if (k === 'text') el.textContent = v;
      else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
      else if (k === 'width') el.style.setProperty('width', v);
      else el.setAttribute(k, v === true ? '' : v);
    }
  }
  for (const c of children.flat()) {
    if (c === undefined || c === null || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

// replaceChildren without the empty children (null would become text).
function fill(el, ...children) {
  el.replaceChildren(...children.flat().filter((c) => c !== null && c !== undefined && c !== false && c !== ''));
  return el;
}

const KB = 1024, MB = KB * 1024, GB = MB * 1024, TB = GB * 1024;
function bytes(n) {
  if (!n || n < 0) return '–';
  if (n >= TB) return (n / TB).toFixed(1) + ' TB';
  if (n >= GB) return (n / GB).toFixed(1) + ' GB';
  if (n >= MB) return (n / MB).toFixed(n >= 100 * MB ? 0 : 1) + ' MB';
  if (n >= KB) return Math.round(n / KB) + ' KB';
  return n + ' bytes';
}
function duration(s) {
  if (s === undefined || s === null) return '–';
  s = Math.max(0, Math.floor(s));
  const d = Math.floor(s / 86400), hh = Math.floor(s % 86400 / 3600), mm = Math.floor(s % 3600 / 60);
  if (d > 0) return d + ' d ' + hh + ' h';
  if (hh > 0) return hh + ' h ' + String(mm).padStart(2, '0') + ' min';
  if (mm > 0) return mm + ' min ' + String(s % 60).padStart(2, '0') + ' s';
  return s + ' s';
}
function ago(ms) {
  if (!ms) return '–';
  return duration((Date.now() - ms) / 1000) + ' ago';
}
function time(ms) {
  if (!ms) return '–';
  return new Date(ms).toLocaleString();
}
function short(id, n = 12) {
  return id ? id.slice(0, n) : '';
}
// As client::shell_quote: unchanged when no quotes are needed.
function shQuote(s) {
  if (s === '') return "''";
  if (/^[A-Za-z0-9_\-.\/:=@,+%]+$/.test(s)) return s;
  return "'" + s.replace(/'/g, "'\\''") + "'";
}

let toastTimer = null;
function toast(text) {
  let t = document.querySelector('.toast');
  if (!t) {
    t = h('div', { class: 'toast', role: 'status' });
    document.body.append(t);
  }
  t.textContent = text;
  t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 1800);
}

async function copy(text) {
  try {
    await navigator.clipboard.writeText(text);
    toast('Copied');
  } catch (e) {
    toast('Copy failed: select the text');
  }
}

// A <details> whose open state survives re-renders.
function keep(details, key) {
  details.dataset.k = key;
  if (state.open.has(key)) details.open = true;
  details.addEventListener('toggle', () => {
    if (details.open) state.open.add(key); else state.open.delete(key);
  });
  return details;
}

// The shell command that does the same as a view or an action.
function shellBox(label, command, key) {
  const pre = h('pre', { text: command });
  const d = h('details', { class: 'shell' },
    h('summary', { text: label || 'Shell' }),
    h('div', { class: 'shell-line' }, pre, h('button', { class: 'small', type: 'button', onclick: () => copy(pre.textContent), text: 'Copy' })));
  if (key) keep(d, 'shell:' + key);
  d.setCommand = (c) => { pre.textContent = c; };
  return d;
}

function info(text) {
  return h('details', { class: 'info' }, h('summary', { 'aria-label': 'About', text: 'ⓘ' }), h('div', { text }));
}

function meter(fraction, hot) {
  const f = Math.max(0, Math.min(1, fraction || 0));
  return h('span', { class: 'meter' + (hot ? ' hot' : '') }, h('span', { width: (f * 100).toFixed(1) + '%' }));
}

function chip(text, kind) {
  return h('span', { class: 'chip' + (kind ? ' ' + kind : ''), text });
}

function stateDot(s) {
  const cls = s === 'online' || s === 'this host' ? 'online' : s === 'one-way' ? 'one-way' : 'offline';
  return h('span', null, h('span', { class: 'dot ' + cls }), s);
}

async function api(path, body) {
  const opts = { credentials: 'same-origin', headers: {} };
  if (body !== undefined) {
    opts.method = 'POST';
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const r = await fetch('/api/' + path, opts);
  let data = null;
  try { data = await r.json(); } catch (e) { data = { error: 'no JSON answer (' + r.status + ')' }; }
  if (r.status === 401) {
    state.signedOut = true;
    render();
  }
  if (!r.ok) {
    const err = new Error(data && data.error ? data.error : r.statusText);
    err.data = data;
    throw err;
  }
  return data;
}

// -- the model ------------------------------------------------------------------

function hostByKey(key) {
  return state.model && state.model.hosts.find((x) => x.key === key);
}
function shellContext() {
  return state.model ? state.model.shell : {};
}
function isAdmin() {
  return state.model && state.model.me && state.model.me.role === 'admin';
}
function pagletRow(hostKey, id) {
  const host = hostByKey(hostKey);
  if (!host || !host.status) return null;
  return host.status.paglets.find((p) => p.id === id) || null;
}

async function poll() {
  try {
    const m = await api('mesh');
    state.signedOut = false;
    const changed = m.version !== state.version;
    state.model = m;
    state.version = m.version;
    document.getElementById('updated').textContent =
      'Updated ' + new Date(m.updated_ms).toLocaleTimeString() + ' · refresh every ' + (m.refresh_ms / 1000) + ' s';
    if (changed && (state.route.view === 'mesh' || state.route.view === 'host' || !state.rendered)) render();
    else if (changed && state.route.view === 'paglet') updatePagletHeader();
    setTimeout(poll, m.refresh_ms || 2000);
  } catch (e) {
    if (!state.signedOut) {
      document.getElementById('updated').textContent = 'paglets-web does not answer: ' + e.message;
    }
    setTimeout(poll, 4000);
  }
}

// -- routing --------------------------------------------------------------------

function parseRoute() {
  const parts = location.hash.replace(/^#\/?/, '').split('/').map(decodeURIComponent);
  if (parts[0] === 'host' && parts[1]) return { view: 'host', key: parts[1] };
  if (parts[0] === 'paglet' && parts[1] && parts[2]) return { view: 'paglet', host: parts[1], id: parts[2] };
  if (parts[0] === 'modules') return { view: 'modules', host: parts[1] || '' };
  if (parts[0] === 'zombies') return { view: 'zombies' };
  return { view: 'mesh' };
}

// A new view when the hash changed (hashchange, history, and after clicks on
// links: some embedded browsers do not report every hash change).
let shownHash = location.hash;
function routeChanged() {
  if (location.hash === shownHash) return;
  shownHash = location.hash;
  state.route = parseRoute();
  render();
  document.getElementById('main').focus({ preventScroll: true });
  window.scrollTo(0, 0);
}
window.addEventListener('hashchange', routeChanged);
window.addEventListener('popstate', routeChanged);
document.addEventListener('click', () => setTimeout(routeChanged, 0));

// Patches `from` to look like `to`, keeping elements (and the focus, open
// details and pending clicks) where they stay the same; elements whose
// data-key differs are replaced, so their handlers belong to their data.
function morph(from, to) {
  if (from.nodeType !== to.nodeType || from.nodeName !== to.nodeName ||
      (from.nodeType === 1 && from.getAttribute('data-key') !== to.getAttribute('data-key'))) {
    from.replaceWith(to);
    return;
  }
  if (from.nodeType === 3) {
    if (from.data !== to.data) from.data = to.data;
    return;
  }
  if (from.nodeType !== 1) return;
  for (const a of [...from.attributes]) if (!to.hasAttribute(a.name)) from.removeAttribute(a.name);
  for (const a of [...to.attributes]) if (from.getAttribute(a.name) !== a.value) from.setAttribute(a.name, a.value);
  if (from.style.cssText !== to.style.cssText) from.style.cssText = to.style.cssText;
  const a = [...from.childNodes], b = [...to.childNodes];
  for (let i = 0; i < b.length; i++) {
    if (i < a.length) morph(a[i], b[i]); else from.append(b[i]);
  }
  for (let i = b.length; i < a.length; i++) a[i].remove();
}

function render() {
  const main = document.getElementById('main');
  for (const a of document.querySelectorAll('nav a')) {
    const section = state.route.view === 'modules' || state.route.view === 'zombies' ? state.route.view : 'mesh';
    const current = a.dataset.nav === section;
    if (current) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
  }
  const me = document.getElementById('me');
  if (state.model && state.model.me) {
    const m = state.model.me;
    me.textContent = m.name + ' (' + m.role + ') · ' + state.model.mesh.name;
  }
  if (state.signedOut) {
    fill(main, h('div', { class: 'banner note' },
      h('strong', { text: 'Not signed in. ' }),
      'Open the link that paglets-web printed in its terminal (it carries a token for this browser).'));
    return;
  }
  if (!state.model) return;
  state.rendered = true;
  const scroll = window.scrollY;
  let view;
  switch (state.route.view) {
    case 'host': view = hostView(state.route.key); break;
    case 'paglet': view = pagletView(state.route.host, state.route.id); break;
    case 'modules': view = modulesView(state.route.host); break;
    case 'zombies': view = zombiesView(); break;
    default: view = meshView();
  }
  // The same view again (a refresh): patched in place.
  const routeKey = JSON.stringify(state.route);
  if (state.shownRoute === routeKey && main.firstChild && main.childNodes.length === 1) {
    morph(main.firstChild, view);
  } else {
    fill(main, view);
  }
  state.shownRoute = routeKey;
  if (state.route.view === 'mesh' || state.route.view === 'host') window.scrollTo(0, scroll);
}

// -- mesh overview --------------------------------------------------------------

function meshView() {
  const m = state.model;
  const out = h('div');
  if (m.error) out.append(h('div', { class: 'banner error' }, h('strong', { text: 'The entry host does not answer: ' }), m.error));
  out.append(h('div', { class: 'title-row' },
    h('div', null,
      h('div', { class: 'crumbs', text: 'Mesh' }),
      h('h1', { text: m.mesh.name || 'mesh' }),
      h('div', { class: 'muted small' }, 'mesh ', h('span', { class: 'mono', text: short(m.mesh.id, 16) }),
        ' · entry host ', m.mesh.entry_name ? m.mesh.entry_name + ' ' : '', h('span', { class: 'mono', text: m.mesh.entry }),
        ' · ', String(m.mesh.records), ' ledger records'))));

  const t = m.totals;
  out.append(h('div', { class: 'tiles' },
    tile('Hosts', t.online + ' / ' + t.hosts, 'online / enrolled'),
    tile('System paglets', t.system, 'services of the hosts'),
    tile('User paglets', t.user, 'started by owners'),
    tile('You', m.me.name, m.me.role + ' · ' + short(m.me.key, 16))));

  const rows = m.hosts.map((x) => {
    const s = x.snapshot || {};
    const st = x.status;
    const load = s.load_per_cpu;
    const memUsed = s.memory_total ? (s.memory_total - s.memory_available) / s.memory_total : 0;
    const tr = h('tr', { class: 'clickable', tabindex: '0', 'data-key': x.key, onclick: (e) => { if (!e.target.closest('a')) location.hash = '#/host/' + x.key; },
      onkeydown: (e) => { if (e.key === 'Enter') location.hash = '#/host/' + x.key; } },
      h('td', null, h('a', { href: '#/host/' + x.key, text: x.name || short(x.key) }),
        x.entry ? h('span', null, ' ', chip('entry', 'accent')) : null,
        h('div', null, x.labels.map((l) => chip(l)))),
      h('td', null, stateDot(x.state), !x.compatible ? h('div', null, chip('incompatible', 'bad')) : null),
      h('td', { class: 'wrap' }, (s.os || (st && st.os) || '–') + ' ' + (s.architecture || (st && st.architecture) || '')),
      h('td', { class: 'num', text: s.cpus || (st && st.cpus) || '–' }),
      h('td', null, load === undefined ? '–' : [meter(load, load > 0.8), load.toFixed(2)]),
      h('td', null, s.memory_total ? [meter(memUsed, memUsed > 0.9), bytes(s.memory_available) + ' free'] : '–'),
      h('td', { class: 'num', text: s.slots !== undefined ? s.slots_free + ' / ' + s.slots : '–' }),
      h('td', { class: 'num', text: x.counts.system }),
      h('td', { class: 'num', text: x.counts.user }),
      h('td', { class: 'wrap small' }, st ? (st.version ? 'v' + st.version + ' · up ' + duration(st.uptime_s) : 'older host') :
        h('span', { class: 'muted', text: x.status_error || '–' })));
    return tr;
  });
  out.append(h('section', { class: 'panel' },
    h('div', { class: 'panel-head' }, h('h2', { text: 'Hosts' }),
      info('Hosts enrolled in the ledger, as the entry host knows them, with mesh-info\'s view of their load. System and user paglets are counted from each host\'s status (system: trust class system, including the native services; user: roaming and resident paglets). Click a host for details.')),
    h('div', { class: 'table-wrap' }, h('table', null,
      h('thead', null, h('tr', null, ['Host', 'State', 'OS', 'CPUs', 'Load/CPU', 'Memory', 'Slots', 'System paglets', 'User paglets', 'Version'].map((c, i) =>
        h('th', { class: i === 3 || i >= 6 && i <= 8 ? 'num' : null, text: c })))),
      h('tbody', null, rows))),
    shellBox('Shell: the hosts', m.shell.hosts, 'mesh-hosts'),
    shellBox('Shell: load, memory and slots', m.shell.landscape, 'mesh-landscape')));
  return out;
}

function tile(label, value, sub) {
  return h('div', { class: 'tile' }, h('div', { class: 'label', text: label }), h('div', { class: 'value', text: String(value) }),
    h('div', { class: 'sub', text: sub }));
}

// -- host page ------------------------------------------------------------------

function hostView(key) {
  const x = hostByKey(key);
  if (!x) return h('div', { class: 'banner error', text: 'No host ' + key + ' in the mesh.' });
  const s = x.snapshot || {};
  const st = x.status;
  const out = h('div');
  out.append(h('div', { class: 'title-row' }, h('div', null,
    h('div', { class: 'crumbs' }, h('a', { href: '#/', text: 'Mesh' }), ' › host'),
    h('h1', null, x.name || short(x.key), ' ', x.entry ? chip('entry host', 'accent') : null),
    h('div', { class: 'small' }, stateDot(x.state), ' ', x.labels.map((l) => chip(l))))));
  if (x.status_error) out.append(h('div', { class: 'banner note' }, h('strong', { text: 'No status from this host: ' }), x.status_error));

  const props = [
    ['Key', h('span', { class: 'mono', text: x.key })],
    ['Address', x.url || 'none known (reached through relays)'],
    ['Version', st && st.version ? st.version + ' · protocol ' + x.protocol + ' · ABI ' + x.abi : 'protocol ' + x.protocol + ' · ABI ' + x.abi],
    ['Up', st && st.extended ? duration(st.uptime_s) : '–'],
    ['System', st && st.extended ? st.os + ' ' + st.os_version + ' · ' + st.architecture : (s.os || '–') + ' ' + (s.architecture || '')],
    ['CPU', st && st.extended ? st.cpus + ' × ' + st.cpu_model : (s.cpus || '–') + ''],
    ['Load', s.load_per_cpu !== undefined ? s.load_per_cpu.toFixed(2) + ' per CPU · ' + (s.cpu_percent || 0).toFixed(0) + ' % CPU' : '–'],
    ['Memory', s.memory_total ? bytes(s.memory_available) + ' free of ' + bytes(s.memory_total) : st && st.memory_total ? bytes(st.memory_total) : '–'],
    ['Paglet storage', s.work_free ? bytes(s.work_free) + ' free' : '–'],
    ['Compute slots', s.slots !== undefined ? s.slots_free + ' of ' + s.slots + ' free · ' + s.queued + ' waiting' : '–'],
    ['Last heard', x.entry ? 'now (entry host)' : ago(x.seen_ms)],
  ];
  if (x.via) props.push(['Via', x.via]);
  if (x.relays.length) props.push(['Relays', x.relays.map((r) => short(r, 8)).join(', ')]);
  if (x.send_error) props.push(['Frames to it', x.send_error]);
  const dl = h('dl', { class: 'kv' });
  for (const [k, v] of props) dl.append(h('dt', { text: k }), h('dd', null, v));

  const links = (s.links || []).map((l) => h('tr', null,
    h('td', null, hostByKey(l.host) ? h('a', { href: '#/host/' + l.host, text: l.host_name || short(l.host) }) : (l.host_name || short(l.host))),
    h('td', null, stateDot(l.up ? 'online' : 'offline')),
    h('td', { text: l.since_ms ? ago(l.since_ms) : '–' }),
    h('td', { class: 'wrap small', text: l.failure || '' })));

  out.append(h('div', { class: 'grid-2' },
    h('section', { class: 'panel' }, h('h2', { text: 'Properties' }), dl, shellBox('Shell: status of this host', x.shell.status, 'host-status-' + key)),
    h('section', { class: 'panel' },
      h('div', { class: 'panel-head' }, h('h2', { text: 'Links' }), info('How this host sees its connections to the other hosts (mesh-info): up while it hears from the other host and its frames to it do not fail.')),
      links.length ? h('div', { class: 'table-wrap' }, h('table', null, h('thead', null, h('tr', null, ['Host', 'Link', 'Since', ''].map((c) => h('th', { text: c })))), h('tbody', null, links)))
        : h('p', { class: 'muted', text: 'No links reported.' }),
      (s.offers && s.offers.length) ? h('div', null, h('h3', { text: 'Offers' }),
        s.offers.map((o) => h('div', { class: 'small' }, h('span', { class: 'mono', text: o.service }), ' ', o.ops.join(', '),
          ' ', o.attributes.map((a) => chip(a.name + '=' + (a.numeric ? a.number : a.text)))))) : null)));

  // User paglets.
  const user = st ? st.paglets.filter((p) => p.trust !== 'system') : [];
  const systemWasm = st ? st.paglets.filter((p) => p.trust === 'system') : [];
  out.append(h('section', { class: 'panel' },
    h('div', { class: 'panel-head' }, h('h2', { text: 'User paglets' }), h('span', { class: 'muted', text: user.length + ' running' }), h('span', { class: 'grow' }),
      h('a', { class: 'button', href: '#/modules/' + key, text: 'Launch here…' })),
    user.length ? pagletTable(key, user) : h('p', { class: 'muted', text: st ? 'No user paglets on this host.' : 'Unknown: the host gave no status.' }),
    shellBox('Shell: paglets of this host', x.shell.status, 'host-paglets-' + key)));

  // System paglets.
  const sys = st ? st.system : [];
  const sysList = h('div', { class: 'stack' });
  for (const id of sys) sysList.append(systemPaglet(key, id));
  for (const p of systemWasm) sysList.append(systemPaglet(key, p.id, p));
  out.append(h('section', { class: 'panel' },
    h('div', { class: 'panel-head' }, h('h2', { text: 'System paglets' }), h('span', { class: 'muted', text: (sys.length + systemWasm.length) + ' services' }),
      info('Paglets of trust class system: the host\'s native services (files, storage, mesh-info, …) and Wasm modules the ledger trusts as system. Expand one to see the operations it offers.')),
    sys.length + systemWasm.length ? sysList : h('p', { class: 'muted', text: 'None reported.' })));

  // Modules and pins: fetched for this page, again every 10 s.
  const extra = h('div', { class: 'grid-2' });
  const cached = state.hostExtras && state.hostExtras.key === key ? state.hostExtras : null;
  if (cached) extra.append(...hostExtras(key, cached.data, x));
  else extra.append(h('section', { class: 'panel' }, h('h2', { text: 'Modules' }), h('p', { class: 'muted', text: 'Loading…' })));
  if (!cached || Date.now() - cached.time > 10000) {
    if (!state.hostExtrasPending) {
      state.hostExtrasPending = true;
      api('host?key=' + encodeURIComponent(key)).then((data) => ({ data }), (e) => ({ data: { error: e.message } })).then(({ data }) => {
        state.hostExtrasPending = false;
        state.hostExtras = { key, data, time: Date.now() };
        if (state.route.view === 'host' && state.route.key === key) render();
      });
    }
  }
  out.append(extra);
  return out;
}

function pagletTable(hostKey, rows) {
  return h('div', { class: 'table-wrap' }, h('table', null,
    h('thead', null, h('tr', null, ['Paglet', 'Module', 'Owner', 'State', 'Trust', ''].map((c) => h('th', { text: c })))),
    h('tbody', null, rows.map((p) => h('tr', { 'data-key': p.id },
      h('td', null, h('a', { class: 'mono', href: '#/paglet/' + hostKey + '/' + encodeURIComponent(p.id), text: short(p.id, 16) })),
      h('td', null, p.module_name ? p.module_name + ' ' : '', h('span', { class: 'mono muted small', text: short(p.module, 12) })),
      h('td', null, p.owner_name || short(p.owner), p.mine ? [' ', chip('you', 'ok')] : null),
      h('td', { text: p.state }),
      h('td', { text: p.trust }),
      h('td', null, h('div', { class: 'row' },
        h('a', { class: 'button small', href: '#/paglet/' + hostKey + '/' + encodeURIComponent(p.id), text: 'Open' }),
        h('button', { class: 'small danger', type: 'button', onclick: () => endPaglet(hostKey, p.id, 'dispose'), text: 'Dispose' }))))))));
}

function systemPaglet(hostKey, id, row) {
  const body = h('div', { class: 'small muted', text: 'Loading…' });
  const d = h('details', { class: 'more', 'data-key': hostKey + ':' + id },
    h('summary', null, h('span', { class: 'mono', text: id }), row ? [' ', chip('Wasm', 'accent')] : null), body);
  keep(d, 'sys:' + hostKey + ':' + id);
  const cached = state.pagletCache.get(hostKey + '/' + id);
  if (cached) {
    fill(body, ...describedBody(hostKey, id, cached));
  } else {
    // Loaded when opened; the page is patched when it arrives.
    const load = () => cachedPaglet(hostKey, id).then(() => render(), (e) => {
      state.pagletCache.set(hostKey + '/' + id, { error: e.message });
      render();
    });
    d.addEventListener('toggle', () => { if (d.open) load(); });
    if (d.open) load();
  }
  return d;
}

function describedBody(hostKey, id, p) {
  if (p.error) return [h('span', { text: 'Not described: ' + p.error })];
  const iface = p.interface;
  return [
    iface && iface.messages && iface.messages.length
      ? h('ul', null, iface.messages.map((m) => h('li', null, h('span', { class: 'mono', text: m.name }), ' ', h('span', { class: 'muted', text: (m.request || 'none') + ' → ' + (m.reply || 'none') }))))
      : h('p', { class: 'muted', text: 'It describes no operations.' }),
    h('a', { class: 'button small', href: '#/paglet/' + hostKey + '/' + encodeURIComponent(id), text: 'Open and send messages…' }),
    shellBox('Shell: describe it', p.shell.describe)];
}

async function cachedPaglet(hostKey, id) {
  const k = hostKey + '/' + id;
  if (state.pagletCache.has(k)) return state.pagletCache.get(k);
  const p = await api('paglet?host=' + encodeURIComponent(hostKey) + '&id=' + encodeURIComponent(id));
  state.pagletCache.set(k, p);
  return p;
}

function hostExtras(key, d, x) {
  const modules = d.modules || [];
  const pins = d.pins || [];
  const modPanel = h('section', { class: 'panel' },
    h('div', { class: 'panel-head' }, h('h2', { text: 'Modules' }), h('span', { class: 'muted', text: modules.length + ' stored' })),
    d.error ? h('p', { class: 'muted', text: d.error }) :
    modules.length ? h('div', { class: 'table-wrap' }, h('table', null,
      h('thead', null, h('tr', null, ['Module', 'Size', 'Paglets', ''].map((c, i) => h('th', { class: i === 1 || i === 2 ? 'num' : null, text: c })))),
      h('tbody', null, modules.map((m) => h('tr', { 'data-key': m.hash },
        h('td', null, (m.names[0] || (m.interface && m.interface.name) || ''), ' ', h('span', { class: 'mono muted small', text: short(m.hash, 12) }),
          m.pinned ? [' ', chip('pinned')] : null, m.interface ? [' ', chip('described', 'ok')] : null),
        h('td', { class: 'num', text: bytes(m.size) }),
        h('td', { class: 'num', text: m.users }),
        h('td', null, h('a', { class: 'button small', href: '#/modules/' + key, onclick: () => { state.launchModule = m.hash; }, text: 'Launch…' })))))))
      : h('p', { class: 'muted', text: 'No modules stored.' }),
    shellBox('Shell: modules of this host', x.shell.modules, 'host-modules-' + key));
  const pinPanel = h('section', { class: 'panel' },
    h('h2', { text: 'Pins' }),
    pins.length ? h('div', { class: 'table-wrap' }, h('table', null,
      h('thead', null, h('tr', null, ['Paglet', 'Until', 'By', 'Reason'].map((c) => h('th', { text: c })))),
      h('tbody', null, pins.map((p) => h('tr', { 'data-key': p.pin }, h('td', { class: 'mono', text: short(p.paglet, 16) }), h('td', { text: time(p.until_ms) }),
        h('td', { class: 'small', text: p.holder.split(':')[0] }), h('td', { class: 'wrap', text: p.reason })))))) : h('p', { class: 'muted', text: 'No pins: paglets may move.' }),
    shellBox('Shell: pins of this host', x.shell.pins, 'host-pins-' + key));
  return [modPanel, pinPanel];
}

async function endPaglet(hostKey, id, how) {
  const what = how === 'terminate' ? 'Terminate (end as failed)' : 'Dispose (end)';
  if (!confirm(what + ' paglet ' + id + '?')) return null;
  try {
    const r = await api(how, { host: hostKey, paglet: id, reason: how === 'terminate' ? 'terminated from paglets-web' : undefined });
    toast(how === 'terminate' ? 'Terminated' : 'Disposed');
    addLog(id, how + ': ok', true, r.shell);
    await poll();
    return r;
  } catch (e) {
    alert(e.message + (e.data && e.data.shell ? '\n\n' + e.data.shell : ''));
    return null;
  }
}

// -- forms from interface descriptions ------------------------------------------

const INTS = new Set(['i8', 'i16', 'i32', 'i64', 'u8', 'u16', 'u32', 'u64']);
const FLOATS = new Set(['f32', 'f64']);

function inner(type, prefix) {
  return type.startsWith(prefix + '<') && type.endsWith('>') ? type.slice(prefix.length + 1, -1) : null;
}

// An editor for a value of `type`: {el, get()} where get() returns JSON text
// (or throws with a message).
function editor(type, types, init, path) {
  const def = types.find((t) => t.name === type);
  if (type === 'bool') {
    const c = h('input', { type: 'checkbox', 'aria-label': path });
    c.checked = init === true;
    return { el: c, get: () => (c.checked ? 'true' : 'false') };
  }
  if (INTS.has(type) || FLOATS.has(type)) {
    const i = h('input', { type: 'text', inputmode: INTS.has(type) ? 'numeric' : 'decimal', 'aria-label': path, size: 14 });
    i.value = init === undefined || init === null ? '0' : String(init);
    return {
      el: i, get: () => {
        const v = i.value.trim();
        const okInt = /^-?\d+$/.test(v) && !(type.startsWith('u') && v.startsWith('-'));
        const okFloat = v !== '' && Number.isFinite(Number(v));
        if (INTS.has(type) ? !okInt : !okFloat) {
          i.classList.add('invalid');
          throw new Error(path + ': ' + (INTS.has(type) ? 'a whole number' : 'a number') + ' (' + type + ')');
        }
        i.classList.remove('invalid');
        return INTS.has(type) ? v : String(Number(v));
      },
    };
  }
  if (type === 'string') {
    const i = h('input', { type: 'text', 'aria-label': path, size: 32 });
    i.value = typeof init === 'string' ? init : '';
    return { el: i, get: () => JSON.stringify(i.value) };
  }
  if (type === 'bytes') {
    const i = h('input', { type: 'text', 'aria-label': path, placeholder: 'hex digits, e.g. 00ff', size: 32 });
    i.value = typeof init === 'string' ? init : '';
    return {
      el: i, get: () => {
        const v = i.value.trim();
        if (!/^([0-9a-fA-F]{2})*$/.test(v)) { i.classList.add('invalid'); throw new Error(path + ': hex digits'); }
        i.classList.remove('invalid');
        return JSON.stringify({ $bytes: v });
      },
    };
  }
  const opt = inner(type, 'optional');
  if (opt) {
    const set = h('input', { type: 'checkbox', 'aria-label': path + ' given' });
    set.checked = init !== undefined && init !== null;
    const e = editor(opt, types, init === null ? undefined : init, path);
    const wrap = h('div', null, h('label', { class: 'small muted' }, set, ' given'), e.el);
    const sync = () => { e.el.hidden = !set.checked; };
    set.addEventListener('change', sync);
    sync();
    return { el: wrap, get: () => (set.checked ? e.get() : 'null') };
  }
  const item = inner(type, 'list');
  if (item) {
    const items = [];
    const box = h('div');
    const add = (v) => {
      const e = editor(item, types, v, path + '[' + items.length + ']');
      const row = h('div', { class: 'list-item' }, e.el, h('button', { type: 'button', class: 'small', 'aria-label': 'Remove', text: '−', onclick: () => {
        items.splice(items.indexOf(entry), 1);
        row.remove();
      } }));
      const entry = { e, row };
      items.push(entry);
      box.append(row);
    };
    for (const v of Array.isArray(init) ? init : []) add(v);
    const el = h('div', null, box, h('button', { type: 'button', class: 'small', text: '+ ' + item, onclick: () => add(undefined) }));
    return { el, get: () => '[' + items.map((x) => x.e.get()).join(',') + ']' };
  }
  if (def && def.kind === 'enum') {
    const s = h('select', { 'aria-label': path }, def.values.map((v) => h('option', { value: v, text: v })));
    if (typeof init === 'string' && def.values.includes(init)) s.value = init;
    return { el: s, get: () => JSON.stringify(s.value) };
  }
  if (def && def.kind === 'record') {
    const fs = h('div', { class: 'form' });
    const getters = [];
    for (const f of def.fields) {
      const v = init && typeof init === 'object' && f.name in init ? init[f.name] : f.default;
      const e = editor(f.type, types, v, path ? path + '.' + f.name : f.name);
      const id = 'f' + Math.random().toString(36).slice(2);
      if (e.el.matches && e.el.matches('input,select')) e.el.id = id;
      fs.append(h('div', { class: 'field' },
        h('label', { for: id }, f.name, h('span', { class: 'type', text: f.type })),
        e.el,
        f.doc ? h('div', { class: 'doc', text: f.doc }) : null));
      getters.push([f.name, e]);
    }
    const el = path ? h('fieldset', null, h('legend', { text: type }), fs) : fs;
    return { el, get: () => '{' + getters.map(([n, e]) => JSON.stringify(n) + ':' + e.get()).join(',') + '}' };
  }
  // any, or a type this page does not know: JSON.
  const ta = h('textarea', { 'aria-label': path, rows: 3 });
  ta.value = init === undefined ? 'null' : JSON.stringify(init, null, 2);
  return {
    el: ta, get: () => {
      try { JSON.parse(ta.value); } catch (e) { ta.classList.add('invalid'); throw new Error(path + ': ' + e.message); }
      ta.classList.remove('invalid');
      return ta.value.trim();
    },
  };
}

// A payload editor for `type`: a form, and the same value as JSON.
function payloadEditor(type, types, init, onChange) {
  if (!type || type === 'none') {
    return { el: h('p', { class: 'muted small', text: 'No payload.' }), get: () => null };
  }
  const e = editor(type, types, init, '');
  const ta = h('textarea', { rows: 4, 'aria-label': 'payload as JSON' });
  ta.hidden = true;
  let mode = 'form';
  const formTab = h('button', { type: 'button', role: 'tab', 'aria-selected': 'true', text: 'Form' });
  const jsonTab = h('button', { type: 'button', role: 'tab', 'aria-selected': 'false', text: 'JSON' });
  const err = h('div', { class: 'small', hidden: true });
  const show = (m) => {
    if (m === 'json' && mode === 'form') {
      try { ta.value = pretty(e.get()); } catch (x) { ta.value = ''; }
    }
    mode = m;
    e.el.hidden = m !== 'form';
    ta.hidden = m !== 'json';
    formTab.setAttribute('aria-selected', String(m === 'form'));
    jsonTab.setAttribute('aria-selected', String(m === 'json'));
    if (onChange) onChange();
  };
  formTab.addEventListener('click', () => show('form'));
  jsonTab.addEventListener('click', () => show('json'));
  const el = h('div', null, h('div', { class: 'tabs', role: 'tablist' }, formTab, jsonTab), e.el, ta, err);
  if (onChange) { el.addEventListener('input', onChange); el.addEventListener('change', onChange); el.addEventListener('click', onChange); }
  return {
    el,
    get: () => {
      if (mode === 'json') {
        try { JSON.parse(ta.value); } catch (x) { ta.classList.add('invalid'); throw new Error('JSON: ' + x.message); }
        ta.classList.remove('invalid');
        return ta.value.trim();
      }
      return e.get();
    },
  };
}

// Pretty JSON text that keeps big integers exact: indentation only.
function pretty(text) {
  try {
    const v = JSON.parse(text);
    if (JSON.stringify(v) === text) return JSON.stringify(v, null, 2);
  } catch (e) { /* fall through */ }
  return text;
}

// -- paglet page ----------------------------------------------------------------

function addLog(id, text, ok, shell) {
  const list = state.logs.get(id) || [];
  list.unshift({ time: Date.now(), text, ok, shell });
  state.logs.set(id, list.slice(0, 50));
  const box = document.getElementById('paglet-log');
  if (box) fill(box, logList(id));
}

function logList(id) {
  const list = state.logs.get(id) || [];
  if (!list.length) return h('p', { class: 'muted small', text: 'Nothing sent from this page yet.' });
  return h('ul', { class: 'small' }, list.map((e) => h('li', null,
    h('span', { class: 'muted', text: new Date(e.time).toLocaleTimeString() + ' ' }),
    h('span', { class: e.ok ? '' : 'bad', text: e.text }))));
}

function updatePagletHeader() {
  const el = document.getElementById('paglet-state');
  if (!el) return;
  const r = pagletRow(state.route.host, state.route.id);
  fill(el, r ? describeRow(r) : h('span', { class: 'muted', text: 'not on this host (anymore): it may have moved or ended' }));
}

function describeRow(r) {
  return h('span', null, chip(r.state, r.state === 'active' ? 'ok' : null), chip(r.trust),
    r.module_name ? r.module_name + ' ' : '', h('span', { class: 'mono muted small', text: short(r.module, 16) }),
    ' · owner ', r.owner_name || short(r.owner), r.mine ? [' ', chip('you', 'ok')] : null);
}

function pagletView(hostKey, id) {
  const host = hostByKey(hostKey);
  const out = h('div');
  out.append(h('div', { class: 'title-row' }, h('div', null,
    h('div', { class: 'crumbs' }, h('a', { href: '#/', text: 'Mesh' }), ' › ', h('a', { href: '#/host/' + hostKey, text: host ? host.name : short(hostKey) }), ' › paglet'),
    h('h1', { class: 'mono', text: id }),
    h('div', { id: 'paglet-state', class: 'small' }))));
  const messages = h('div', { class: 'stack' }, h('p', { class: 'muted', text: 'Loading the interface…' }));
  const side = h('div', { class: 'stack' });
  out.append(h('div', { class: 'grid-2' },
    h('section', { class: 'panel' }, h('div', { class: 'panel-head' }, h('h2', { text: 'Messages' }),
      info('Forms are made from the interface description the paglet\'s module carries (docs: Interface descriptions). Call waits for the reply; Send delivers a one-way message. Every action shows the matching paglets-host command.')), messages),
    side));
  setTimeout(updatePagletHeader);
  loadPaglet(hostKey, id, messages, side);
  return out;
}

async function loadPaglet(hostKey, id, messages, side) {
  let p;
  try {
    p = await api('paglet?host=' + encodeURIComponent(hostKey) + '&id=' + encodeURIComponent(id));
  } catch (e) {
    fill(messages, h('div', { class: 'banner error', text: e.message }));
    return;
  }
  const iface = p.interface;
  const types = (iface && iface.types) || [];
  const callPrefix = p.shell.describe.replace(/ remote describe /, ' remote call ').replace(/ \S+$/, ' ' + shQuote(id));
  const sendPrefix = callPrefix.replace(/ remote call /, ' remote send ');
  const list = [];
  if (iface) {
    list.push(h('div', { class: 'small' }, h('strong', { text: iface.name || '' }), iface.summary ? ' — ' + iface.summary : ''));
    for (const m of iface.messages || []) list.push(messageCard(hostKey, id, m, types, callPrefix, sendPrefix));
  } else {
    list.push(h('p', { class: 'muted', text: 'This paglet\'s module carries no interface description: send messages by name with a JSON payload.' }));
  }
  list.push(messageCard(hostKey, id, null, types, callPrefix, sendPrefix));
  list.push(shellBox('Shell: the interface description', p.shell.describe, 'describe-' + id));
  fill(messages, ...list);

  // Actions.
  const dest = h('input', { type: 'text', list: 'hosts-list', placeholder: 'host name, label:L, any', 'aria-label': 'destination', size: 22 });
  const datalist = h('datalist', { id: 'hosts-list' }, (state.model.hosts || []).filter((x) => x.key !== hostKey).map((x) => h('option', { value: x.name })), h('option', { value: 'any' }));
  const minutes = h('input', { type: 'number', min: 1, max: 1440, value: 10, size: 5, 'aria-label': 'minutes' });
  const result = h('div', { class: 'result' });
  const show = (r, ok, text) => {
    result.className = 'result' + (ok ? '' : ' err');
    fill(result, h('pre', { text }), r && r.shell ? shellBox('Shell: the same', r.shell) : null);
    addLog(id, text, ok, r && r.shell);
  };
  const run = async (name, body, describe) => {
    try {
      const r = await api(name, Object.assign({ host: hostKey, paglet: id }, body));
      show(r, true, describe(r));
      await poll();
    } catch (e) {
      show(e.data, false, name + ' failed: ' + e.message);
    }
  };
  const destShell = shellBox('Shell', p.shell.dispose.replace(/ remote dispose /, ' remote dispatch ') + ' DESTINATION');
  dest.addEventListener('input', () => destShell.setCommand(p.shell.dispose.replace(/ remote dispose /, ' remote dispatch ') + ' ' + shQuote(dest.value || 'DESTINATION')));
  fill(side, 
    h('section', { class: 'panel' }, h('h2', { text: 'Actions' }),
      h('div', { class: 'form' },
        h('div', { class: 'row' }, h('strong', { text: 'Move' }), dest, datalist,
          h('button', { type: 'button', onclick: () => {
            if (!dest.value) return;
            fill(result, h('p', { class: 'muted', text: 'Moving… (waits until the move ended, up to 120 s)' }));
            run('dispatch', { destination: dest.value }, (r) => {
              if (r.moved && r.to) {
                // Follow the paglet to its new host.
                setTimeout(() => { location.hash = '#/paglet/' + r.to + '/' + encodeURIComponent(id); routeChanged(); }, 600);
              }
              return r.moved ? 'moved to ' + r.host_name : r.pending ? 'the move has not ended yet' : 'the move failed, the paglet stays: ' + r.reason;
            });
          }, text: 'Dispatch' }), info('A transfer ticket: a host name or key ID, label:L (a host with that label), or any. The paglet travels with its memory image.')),
        destShell,
        h('div', { class: 'row' }, h('strong', { text: 'Pin' }), minutes, 'minutes',
          h('button', { type: 'button', onclick: () => run('pin', { minutes: Number(minutes.value) }, (r) => 'pinned on ' + r.host_name + ' until ' + time(r.until_ms)), text: 'Pin' }),
          h('button', { type: 'button', onclick: () => run('locate', {}, (r) => 'on ' + r.host_name + ' (' + short(r.host, 16) + '), ' + r.moves + ' moves'), text: 'Locate' })),
        h('div', { class: 'row' }, h('strong', { text: 'End' }),
          h('button', { type: 'button', class: 'danger', onclick: async () => { const r = await endPaglet(hostKey, id, 'dispose'); if (r) show(r, true, 'disposed'); }, text: 'Dispose' }),
          isAdmin() ? h('button', { type: 'button', class: 'danger', onclick: async () => { const r = await endPaglet(hostKey, id, 'terminate'); if (r) show(r, true, 'terminated'); }, text: 'Terminate' }) : null,
          info('Dispose ends the paglet normally (it may clean up). Terminate (admins) ends it as failed at once, also when a handler hangs.'))),
      result,
      shellBox('Shell: dispose', p.shell.dispose), shellBox('Shell: pin', p.shell.pin), shellBox('Shell: locate', p.shell.locate)),
    h('section', { class: 'panel' }, h('h2', { text: 'This session' }), h('div', { id: 'paglet-log' }, logList(id))));
}

function messageCard(hostKey, id, m, types, callPrefix, sendPrefix) {
  const custom = !m;
  const kind = m ? (m.kind || 'call') : 'both';
  const nameInput = custom ? h('input', { type: 'text', placeholder: 'message name', 'aria-label': 'message name', size: 18 }) : null;
  const shell = shellBox('Shell: this message');
  const update = () => {
    let payload = null;
    try { payload = editorRef.get(); } catch (e) { payload = '…'; }
    const name = custom ? (nameInput.value || 'NAME') : m.name;
    const prefix = kind === 'send' ? sendPrefix : callPrefix;
    shell.setCommand(prefix + ' ' + shQuote(name) + (payload === null ? '' : ' ' + shQuote(payload)));
  };
  let editorRef;
  if (custom) {
    const ta = h('textarea', { rows: 3, 'aria-label': 'payload as JSON', placeholder: 'payload as JSON (empty: none)' });
    ta.addEventListener('input', update);
    editorRef = {
      el: ta, get: () => {
        const v = ta.value.trim();
        if (!v) return null;
        try { JSON.parse(v); } catch (e) { ta.classList.add('invalid'); throw new Error('JSON: ' + e.message); }
        ta.classList.remove('invalid');
        return v;
      },
    };
    nameInput.addEventListener('input', update);
  } else {
    editorRef = payloadEditor(m.request, types, m.example, update);
  }
  const result = h('div', { class: 'result' });
  const priority = h('select', { 'aria-label': 'priority' }, [0, 1, 2, 3, 4, 5, 6, 7].map((p) => h('option', { value: p, text: 'priority ' + p })));
  priority.value = '3';
  const go = async (how) => {
    let payload;
    try { payload = editorRef.get(); } catch (e) {
      result.className = 'result err';
      fill(result, h('pre', { text: e.message }));
      return;
    }
    const name = custom ? nameInput.value.trim() : m.name;
    if (!name) { nameInput.focus(); return; }
    if (m && m.danger && !confirm('Send ' + name + ' to ' + id + '?')) return;
    const body = { host: hostKey, paglet: id, name };
    if (payload !== null) body.payload = JSON.parse(payload);
    // Integers beyond 2^53 keep their digits: the payload goes as text.
    const text = '{"host":' + JSON.stringify(hostKey) + ',"paglet":' + JSON.stringify(id) + ',"name":' + JSON.stringify(name) +
      (payload !== null ? ',"payload":' + payload : '') + (how === 'send' ? ',"priority":' + Number(priority.value) : '') + '}';
    result.className = 'result';
    fill(result, h('p', { class: 'muted small', text: how === 'call' ? 'Waiting for the reply…' : 'Sending…' }));
    try {
      const r = await rawPost(how, text);
      const shown = how === 'send' ? 'sent' : r.status === 0 ? (r.reply === null ? 'ok (no payload)' : JSON.stringify(r.reply, null, 2)) : 'error: ' + r.status_name;
      result.className = 'result' + (how === 'call' && r.status !== 0 ? ' err' : '');
      fill(result, h('div', { class: 'small muted', text: how === 'call' ? 'Reply' + (m && m.reply ? ' (' + m.reply + ')' : '') : '' }), h('pre', { text: shown }), shellBox('Shell: the same', r.shell));
      addLog(id, how + ' ' + name + ': ' + (how === 'send' ? 'sent' : r.status === 0 ? 'ok' : r.status_name), how === 'send' || r.status === 0);
    } catch (e) {
      result.className = 'result err';
      fill(result, h('pre', { text: e.message }), e.data && e.data.shell ? shellBox('Shell: the same', e.data.shell) : null);
      addLog(id, how + ' ' + name + ': ' + e.message, false);
    }
  };
  const buttons = h('div', { class: 'row' },
    kind !== 'send' ? h('button', { type: 'button', class: 'primary', onclick: () => go('call'), text: 'Call' }) : null,
    kind !== 'call' ? h('button', { type: 'button', class: kind === 'send' ? 'primary' : '', onclick: () => go('send'), text: 'Send' }) : null,
    kind !== 'call' ? priority : null);
  const card = h('div', { class: 'message' },
    h('div', { class: 'message-head' },
      custom ? h('span', { class: 'name', text: 'Other message' }) : h('span', { class: 'name', text: m.name }),
      chip(kind === 'both' ? 'call or send' : kind, kind === 'send' ? 'warn' : 'accent'),
      m && m.danger ? chip('asks first', 'bad') : null,
      m ? h('span', { class: 'muted small', text: (m.request || 'none') + ' → ' + (kind === 'send' ? '–' : (m.reply || 'none')) }) : null),
    m && m.summary ? h('p', { class: 'small', text: m.summary }) : null,
    custom ? h('div', { class: 'form' }, nameInput, editorRef.el) : editorRef.el,
    h('div', { class: 'row', style: null }, buttons),
    result, shell);
  setTimeout(update);
  return card;
}

async function rawPost(path, text) {
  const r = await fetch('/api/' + path, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: text });
  let data = null;
  try { data = await r.json(); } catch (e) { data = { error: 'no JSON answer (' + r.status + ')' }; }
  if (r.status === 401) { state.signedOut = true; render(); }
  if (!r.ok) { const err = new Error(data.error || r.statusText); err.data = data; throw err; }
  return data;
}

// -- modules page ---------------------------------------------------------------

function modulesView(hostKey) {
  const out = h('div');
  out.append(h('div', { class: 'title-row' }, h('div', null,
    h('div', { class: 'crumbs' }, h('a', { href: '#/', text: 'Mesh' }), ' › modules'),
    h('h1', { text: 'Modules' }),
    h('div', { class: 'muted small', text: 'Paglet modules on the hosts' + ' and in --module-dir; launch one with parameters from its interface description.' }))));
  const body = h('div', null, h('p', { class: 'muted', text: 'Asking the hosts…' }));
  out.append(body);
  loadModules(body, hostKey);
  return out;
}

async function loadModules(body, hostKey) {
  let d;
  try {
    d = await api('modules');
  } catch (e) {
    fill(body, h('div', { class: 'banner error', text: e.message }));
    return;
  }
  state.moduleDir = d.module_dir || '';
  const launch = h('section', { class: 'panel', id: 'launch' });
  const pick = (m) => {
    state.launchModule = m.hash;
    launchPanel(launch, m, hostKey);
    launch.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };
  d.modules.sort((a, b) => (b.interface ? 1 : 0) - (a.interface ? 1 : 0) || a.title.localeCompare(b.title));
  const rows = d.modules.map((m) => h('tr', { 'data-key': m.hash },
    h('td', null, h('strong', { text: m.title || '(unnamed)' }), m.interface && m.interface.summary ? h('div', { class: 'small muted wrap', text: m.interface.summary }) : null),
    h('td', { class: 'mono small', text: short(m.hash, 16) }),
    h('td', { class: 'num', text: bytes(m.size) }),
    h('td', { class: 'wrap' }, m.hosts.map((x) => chip(x.name + (x.users ? ' · ' + x.users : ''))), m.file ? chip('file ' + m.file, 'accent') : null),
    h('td', null, m.interface ? chip((m.interface.messages || []).length + ' messages', 'ok') : chip('not described')),
    h('td', null, h('button', { type: 'button', class: 'small', onclick: () => pick(m), text: 'Launch…' }))));
  const errors = d.errors.length ? h('div', { class: 'banner note' }, 'Some hosts did not answer: ', d.errors.map((e) => (hostByKey(e.host) ? hostByKey(e.host).name : short(e.host)) + ' (' + e.error + ') ')) : null;
  fill(body, 
    errors || '',
    h('section', { class: 'panel' },
      h('div', { class: 'panel-head' }, h('h2', { text: 'Modules' }), h('span', { class: 'muted', text: d.modules.length + ' known' }),
        info('A module is a Wasm file, named by its SHA-256 hash. Hosts keep the modules of their paglets; names come from module signatures in the ledger. Files of --module-dir are sent along when launched or pushed.')),
      d.modules.length ? h('div', { class: 'table-wrap' }, h('table', null,
        h('thead', null, h('tr', null, ['Module', 'Hash', 'Size', 'Where', 'Interface', ''].map((c, i) => h('th', { class: i === 2 ? 'num' : null, text: c })))),
        h('tbody', null, rows))) : h('p', { class: 'muted', text: 'No modules: start paglets-web with --module-dir DIR to launch .wasm files from DIR.' }),
      shellBox('Shell: modules of the entry host', d.shell.modules, 'modules')),
    uploadPanel(launch, hostKey),
    launch);
  const pre = d.modules.find((m) => m.hash === state.launchModule);
  if (pre) launchPanel(launch, pre, hostKey); else launch.hidden = true;
}

// A module from this computer: read here, sent along with the launch or push.
function uploadPanel(launch, hostKey) {
  const input = h('input', { type: 'file', accept: '.wasm,application/wasm', 'aria-label': 'module file' });
  const note = h('span', { class: 'muted small' });
  input.addEventListener('change', async () => {
    const f = input.files && input.files[0];
    if (!f) return;
    const buf = new Uint8Array(await f.arrayBuffer());
    let hash = '';
    try {
      const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', buf));
      hash = [...digest].map((b) => b.toString(16).padStart(2, '0')).join('');
    } catch (e) { /* no WebCrypto: the host computes it */ }
    let iface = null;
    try { iface = interfaceOf(buf); } catch (e) { note.textContent = 'not a Wasm module: ' + e.message; return; }
    let bin = '';
    for (let i = 0; i < buf.length; i += 0x8000) bin += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
    const m = { title: f.name, hash, size: buf.length, interface: iface, upload: { name: f.name, data: btoa(bin) } };
    note.textContent = bytes(buf.length) + (iface ? ' · described' : ' · not described');
    launchPanel(launch, m, hostKey);
    launch.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
  return h('section', { class: 'panel' },
    h('div', { class: 'panel-head' }, h('h2', { text: 'Upload a module' }),
      info('A .wasm file from this computer: launched or pushed to a host directly (the module travels with the request, as with paglets-host remote launch FILE).')),
    h('div', { class: 'row' }, input, note));
}

// The interface description in a module's custom section, or null.
function interfaceOf(buf) {
  const magic = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
  if (buf.length < 8 || magic.some((b, i) => buf[i] !== b)) throw new Error('no Wasm header');
  let pos = 8;
  const leb = () => {
    let v = 0, shift = 0, b;
    do {
      if (pos >= buf.length) throw new Error('truncated');
      b = buf[pos++];
      v += (b & 0x7f) * Math.pow(2, shift);
      shift += 7;
    } while (b & 0x80);
    return v;
  };
  while (pos < buf.length) {
    const id = buf[pos++];
    const size = leb();
    const end = pos + size;
    if (end > buf.length) throw new Error('truncated section');
    if (id === 0) {
      const n = leb();
      const name = new TextDecoder().decode(buf.subarray(pos, pos + n));
      if (name === 'paglets.interface') {
        try { return JSON.parse(new TextDecoder().decode(buf.subarray(pos + n, end))); } catch (e) { return null; }
      }
    }
    pos = end;
  }
  return null;
}

function launchPanel(panel, m, hostKey) {
  panel.hidden = false;
  const hosts = state.model.hosts.filter((x) => x.status);
  const target = h('select', { 'aria-label': 'host' }, hosts.map((x) => h('option', { value: x.key, text: x.name + (x.entry ? ' (entry)' : '') })));
  if (hostKey && hosts.some((x) => x.key === hostKey)) target.value = hostKey;
  const iface = m.interface;
  const types = (iface && iface.types) || [];
  const launchType = iface && iface.launch ? iface.launch.type : (iface ? 'none' : 'any');
  const shell = shellBox('Shell');
  const id = h('input', { type: 'text', placeholder: 'random', size: 34, 'aria-label': 'paglet ID' });
  const hours = h('input', { type: 'number', min: 1, max: 720, value: 24, size: 5, 'aria-label': 'hours' });
  const update = () => {
    const host = hostByKey(target.value);
    const conn = (host ? host.shell.status : state.model.shell.status).replace(/ remote status /, ' remote launch ');
    let args = null;
    try { args = editorRef.get(); } catch (e) { args = '…'; }
    shell.setCommand(conn + ' ' + shQuote(m.upload ? m.upload.name : m.file ? state.moduleDir.replace(/\/$/, '') + '/' + m.file : m.hash) + (args === null ? '' : ' --args ' + shQuote(args)) +
      (id.value ? ' --id ' + shQuote(id.value) : '') + (Number(hours.value) !== 24 ? ' --hours ' + Number(hours.value) : ''));
  };
  const editorRef = launchType === 'none' ? { el: h('p', { class: 'muted small', text: 'It takes no start arguments.' }), get: () => null }
    : payloadEditor(launchType, types, iface && iface.launch ? iface.launch.example : undefined, update);
  for (const el of [target, id, hours]) el.addEventListener('input', update);
  const result = h('div', { class: 'result' });
  const push = m.file || m.upload ? h('button', { type: 'button', onclick: async () => {
    try {
      const r = await api('push-module', m.upload ? { host: target.value, name: m.upload.name, data: m.upload.data } : { host: target.value, file: m.file });
      result.className = 'result';
      fill(result, h('pre', { text: 'stored as ' + r.hash }), shellBox('Shell: the same', r.shell));
    } catch (e) {
      result.className = 'result err';
      fill(result, h('pre', { text: e.message }));
    }
  }, text: 'Push only' }) : null;
  const go = async () => {
    let args;
    try { args = editorRef.get(); } catch (e) {
      result.className = 'result err';
      fill(result, h('pre', { text: e.message }));
      return;
    }
    const what = m.upload ? ',"name":' + JSON.stringify(m.upload.name) + ',"data":' + JSON.stringify(m.upload.data)
      : m.file ? ',"file":' + JSON.stringify(m.file) : ',"module":' + JSON.stringify(m.hash);
    const text = '{"host":' + JSON.stringify(target.value) + what +
      (args !== null ? ',"args":' + args : '') + (id.value ? ',"id":' + JSON.stringify(id.value) : '') + ',"hours":' + Number(hours.value || 24) + '}';
    result.className = 'result';
    fill(result, h('p', { class: 'muted small', text: 'Launching…' }));
    try {
      const r = await rawPost('launch', text);
      fill(result, h('p', null, 'Started ', h('a', { class: 'mono', href: '#/paglet/' + target.value + '/' + encodeURIComponent(r.paglet), text: r.paglet })),
        shellBox('Shell: the same', r.shell));
      poll();
    } catch (e) {
      result.className = 'result err';
      fill(result, h('pre', { text: e.message }), e.data && e.data.shell ? shellBox('Shell: the same', e.data.shell) : null);
    }
  };
  fill(panel, 
    h('div', { class: 'panel-head' }, h('h2', { text: 'Launch ' + (m.title || short(m.hash, 16)) }),
      info('Starts a paglet of yours (an owner key) from this module on the chosen host; its passport is signed here. The arguments arrive in the paglet\'s created event.')),
    !isAdmin() ? null : h('div', { class: 'banner note', text: 'paglets-web runs with an admin key; launching needs an owner key.' }),
    h('div', { class: 'form' },
      h('div', { class: 'field' }, h('label', { text: 'Host' }), target),
      h('div', { class: 'field' }, h('label', null, 'Arguments', h('span', { class: 'type', text: launchType })), editorRef.el),
      h('div', { class: 'field' }, h('label', { text: 'Paglet ID' }), id),
      h('div', { class: 'field' }, h('label', { text: 'Passport valid' }), h('div', null, hours, ' hours')),
      h('div', { class: 'row' }, h('button', { type: 'button', class: 'primary', onclick: go, text: 'Launch' }), push)),
    result, shell);
  update();
}

// -- zombies page ---------------------------------------------------------------

function zombiesView() {
  const out = h('div');
  out.append(h('div', { class: 'title-row' }, h('div', null,
    h('div', { class: 'crumbs' }, h('a', { href: '#/', text: 'Mesh' }), ' › zombie groups'),
    h('h1', { text: 'Zombie groups' }),
    h('div', { class: 'muted small', text: 'Paglets that keep running while one host of the mesh is alive: a replica on every host, one result at the sink.' }))));
  const body = h('div', null, h('p', { class: 'muted', text: 'Asking the hosts…' }));
  out.append(body);
  loadZombies(body);
  return out;
}

async function loadZombies(body) {
  let d;
  try {
    d = await api('zombies');
  } catch (e) {
    fill(body, h('div', { class: 'banner error', text: e.message }));
    return;
  }
  const result = h('div', { class: 'result' });
  const act = async (name, payload, describe) => {
    try {
      const r = await api(name, payload);
      result.className = 'result';
      fill(result, h('pre', { text: describe(r) }), shellBox('Shell: the same', r.shell));
      if (name !== 'zombie-check') setTimeout(() => loadZombies(body), 500);
    } catch (e) {
      result.className = 'result err';
      fill(result, h('pre', { text: e.message }), e.data && e.data.shell ? shellBox('Shell: the same', e.data.shell) : null);
    }
  };
  const entry = state.model.mesh.entry_key;
  const groups = d.groups.map((g) => {
    const s = g.state || {};
    const ended = s.ending && s.ending !== 'none';
    const sink = h('input', { type: 'text', placeholder: 'host:NAME', size: 14, 'aria-label': 'new sink' });
    return h('div', { class: 'message', 'data-key': g.group },
      h('div', { class: 'message-head' }, h('span', { class: 'name', text: g.group }),
        chip(ended ? s.ending : 'running', ended ? (s.ending === 'delivered' ? 'ok' : 'warn') : 'accent'),
        h('span', { class: 'muted small', text: 'sink ' + (s.sink_spec || '–') + ' (#' + (s.sink || 1) + ')' })),
      s.reason ? h('p', { class: 'small', text: s.reason }) : null,
      h('div', { class: 'small' }, 'replicas: ', g.replicas.map((r) => r.paglet
        ? h('a', { href: '#/paglet/' + r.host + '/' + encodeURIComponent(r.paglet) }, chip(r.host_name + ' · ' + short(r.paglet, 8)))
        : chip(r.host_name + ' · none'))),
      h('div', { class: 'small muted', text: 'owner ' + short(g.owner, 16) + ' · module ' + short(g.module, 16) }),
      h('div', { class: 'row' },
        h('button', { type: 'button', onclick: () => act('zombie-check', { host: entry, group: g.group }, (r) =>
          r.records + ' protocol steps from ' + r.hosts + ' hosts, ' + r.replicas + ' replicas; takeovers ' + r.takeovers +
          ', restarts ' + r.restarts + ', results accepted ' + r.accepted + '\n' +
          (r.violations.length ? r.violations.map((v) => 'violation: ' + v).join('\n') : 'conforms to the specification')), text: 'Check' }),
        ended ? null : h('button', { type: 'button', class: 'danger', onclick: () => {
          if (confirm('Cancel group ' + g.group + ' on every host?')) act('zombie-cancel', { host: entry, group: g.group, reason: 'cancelled from paglets-web' }, () => 'cancelled');
        }, text: 'Cancel' }),
        ended ? null : [sink, h('button', { type: 'button', onclick: () => {
          if (sink.value) act('zombie-redirect', { host: entry, group: g.group, sink: sink.value }, (r) => 'results go to ' + sink.value);
        }, text: 'Redirect' })],
        info('Check compares the protocol steps every host recorded with the specification. Cancel ends the group everywhere (also on hosts that are offline now, once they return). Redirect sends the result to another sink.')),
      shellBox('Shell: traces', g.shell.traces), shellBox('Shell: check', g.shell.check), shellBox('Shell: result', g.shell.result));
  });
  fill(body,
    d.errors.length ? h('div', { class: 'banner note' }, 'Some hosts did not answer: ', d.errors.map((e) => (hostByKey(e.host) ? hostByKey(e.host).name : short(e.host)) + ' (' + e.error + ') ')) : null,
    h('section', { class: 'panel' },
      h('div', { class: 'panel-head' }, h('h2', { text: 'Groups' }), h('span', { class: 'muted', text: d.groups.length + ' known' })),
      groups.length ? h('div', { class: 'stack' }, groups) : h('p', { class: 'muted', text: 'No zombie groups' + (isAdmin() ? '.' : ' of yours.') }),
      result,
      shellBox('Shell: the groups', d.shell.list, 'zombies')));
}

// -- start ----------------------------------------------------------------------

state.route = parseRoute();
poll();
