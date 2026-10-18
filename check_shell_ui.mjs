// SpaceOS Shell UI smoke test via CDP (node 18+, no deps).
// Needs a live stack (launch.ps1) + CDP browser; starts nothing.
// Reloads the Shell tab fresh, collects console errors, clicks every
// tab, asserts key panels render, screenshots Workspace to
// C:\spaceos\logs\shell-workspace.png. Exits non-zero on failure.
// Since World became the default entry point at `/`, the Shell lives
// at /index.html — this script targets it explicitly (the 7-tab
// contract is unchanged) and additionally asserts that `/` serves World.
// Usage: node.exe C:\spaceos\check_shell_ui.mjs
import httpLib from 'http';

const CDP = 'http://127.0.0.1:9222';
const SHELL = 'http://127.0.0.1:8765/index.html';

function http(path, method = 'GET') {
  return new Promise((resolve, reject) => {
    const req = httpLib.request(
      CDP + path, { method }, (res) => {
        let b = '';
        res.on('data', (c) => (b += c));
        res.on('end', () => resolve(b));
      });
    req.on('error', reject);
    req.end();
  });
}

async function findShellTab() {
  for (let i = 0; i < 20; i++) {
    const tabs = JSON.parse(await http('/json/list'));
    const hit = tabs.find((t) => (t.url || '').startsWith(SHELL));
    if (hit) return hit;
    await new Promise((r) => setTimeout(r, 500));
  }
  return null;
}
let shell = await findShellTab();
if (!shell) {
  // Chrome takes the new target URL as the raw query string, not url=.
  await http('/json/new?' + SHELL, 'PUT');
  shell = await findShellTab();
}
if (!shell) {
  console.log('ASSERT_FAIL no shell tab appeared on CDP');
  process.exit(1);
}
console.log('TAB ' + shell.url);

const wsUrl = shell.webSocketDebuggerUrl;
const ws = new WebSocket(wsUrl);
let id = 0;
const pending = new Map();
const errors = [];
ws.onmessage = (ev) => {
  const m = JSON.parse(ev.data);
  if (m.id && pending.has(m.id)) {
    pending.get(m.id)(m);
    pending.delete(m.id);
  } else if (m.method === 'Runtime.consoleAPICalled' && m.params.type === 'error') {
    errors.push(m.params.args.map((a) => a.value || a.description).join(' '));
  } else if (m.method === 'Runtime.exceptionThrown') {
    const d = m.params.exceptionDetails || {};
    const frames = ((d.stackTrace || {}).callFrames || []).map((f) => (f.functionName || '?') + '@' + f.lineNumber).join('<');
    errors.push((d.text || '?') + ' [' + frames.slice(0, 200) + ']');
  }
};
await new Promise((r) => ws.addEventListener('open', r, { once: true }));
const send = (method, params = {}, timeoutMs = 20000) =>
  new Promise((res, rej) => {
    const myId = ++id;
    const timer = setTimeout(() => {
      pending.delete(myId);
      rej(new Error('cdp timeout: ' + method));
    }, timeoutMs);
    pending.set(myId, (m) => { clearTimeout(timer); res(m); });
    ws.send(JSON.stringify({ id: myId, method, params }));
  });

await send('Runtime.enable');
await send('Log.enable');
await send('Page.enable');
// Fresh copy of the Shell (the tab may hold yesterday's cached DOM).
await send('Page.reload', { ignoreCache: true });
await new Promise((r) => setTimeout(r, 7000));
await send('Runtime.evaluate', { expression: `window.__rej=[]; window.addEventListener('unhandledrejection', e => { window.__rej.push(String((e.reason && e.reason.message) || e.reason)); });`, returnByValue: true });

const evaluate = async (expr) => {
  const r = await send('Runtime.evaluate', { expression: expr, returnByValue: true });
  return r.result?.result?.value;
};
// Sequenced evaluation: awaits the page promise, so dependent mutations
// apply in order (fire-and-forget racing caused out-of-order rejects).
const evaluateAwait = async (expr) => {
  const r = await send('Runtime.evaluate', { expression: expr, returnByValue: true, awaitPromise: true });
  if (r.result?.exceptionDetails) throw new Error('page error: ' + JSON.stringify(r.result.exceptionDetails.text || r.result.exceptionDetails).slice(0, 200));
  return r.result?.result?.value;
};

const tabNames = await evaluate(
  `[...document.querySelectorAll('.tab')].map(t => t.dataset.tab).join(',')`);
console.log('TABS ' + tabNames);

const checks = [];
for (const name of ['workspace', 'files', 'browser', 'agent', 'tasks', 'activity', 'system']) {
  await evaluate(`gotoTab('${name}')`);
  await new Promise((r) => setTimeout(r, 800));
  const visible = await evaluate(
    `document.getElementById('tab-${name}').classList.contains('active')`);
  const textLen = await evaluate(
    `document.getElementById('tab-${name}').innerText.length`);
  checks.push(`${name}:active=${visible},chars=${textLen}`);
}
console.log(checks.join('\n'));

const wsLine = await evaluate(`document.getElementById('ws-status-line').innerText`);
console.log('WORKSPACE_STATUS ' + wsLine);
const agentBox = await evaluate(`document.getElementById('agent-list').innerText.slice(0,120)`);
console.log('AGENT_BOX ' + agentBox);
const tasksBox = await evaluate(`document.getElementById('tasks-list').innerText.slice(0,120)`);
console.log('TASKS_BOX ' + tasksBox);

console.log('CONSOLE_ERRORS ' + (errors.length ? errors.join(' || ') : '(none)'));
const rejs = await evaluate(`(window.__rej || []).join(' || ')`);
if (rejs) console.log('PAGE_REJECTIONS ' + rejs.slice(0, 600));

// Hard assertions: non-zero exit on regression.
let failed = false;
const need = (cond, label) => {
  if (!cond) {
    console.log('ASSERT_FAIL ' + label);
    failed = true;
  }
};
need((tabNames || '').split(',').length === 7, 'seven shell tabs, got: ' + tabNames);
for (const c of checks) {
  const m = c.match(/^(.*?):active=(.*?),chars=(.*)$/);
  need(m && m[2] === 'true' && Number(m[3]) > 50, 'tab renders: ' + c);
}
need((wsLine || '').includes('operational') || (wsLine || '').includes('Degraded'), 'workspace status line: ' + wsLine);

// Every button must be wired: an onclick handler or a tab switcher.
// (Catches dead/fake buttons.)
const deadBtns = await evaluate(`[...document.querySelectorAll('button')].filter(b =>
  !b.getAttribute('onclick') && !b.dataset.tab).map(b => b.textContent.trim()).join('|')`);
need(!deadBtns, 'dead buttons: ' + deadBtns);

// Interval refresh must not wipe user input (slashes: backslashes
// would be JS-unescaped inside the evaluated snippet).
await evaluate(`document.getElementById('fs-dir').value = 'C:/probe-dir-keepme'`);
await evaluate(`fetchActionHistory(); fetchComponents();`);
await new Promise((r) => setTimeout(r, 1500));
const kept = await evaluate(`document.getElementById('fs-dir').value`);
need(kept === 'C:/probe-dir-keepme', 'input preserved across refresh, got: ' + kept);

// Entry-point contract: `/` must serve the World page (same-origin
// fetch from the Shell tab; asserts without opening another tab).
const rootWorld = await evaluateAwait(
  `fetch('/').then(r => r.text()).then(t => t.includes('SpaceOS World') ? 'yes' : 'no')`);
need(rootWorld === 'yes', 'root / serves the World page, got: ' + rootWorld);

// Mission E2E through the real UI handlers (no prompt()-based flows:
// prompt/confirm would block headless evaluation, so approval clicks
// stay manual; every call below hits a real Core-backed endpoint).
// NOTE: page evaluate() has no awaitPromise, so async UI calls are
// fire-and-forget here and every step polls for its effect.
const sleepMs = (ms) => new Promise((r) => setTimeout(r, ms));
// Patient poll: fire one fetchMissions, WAIT for it to settle (it fans
// out to one detail GET per mission), then read. Never stack overlapping
// fetch waves — that self-DDoSes the bridge behind the storage mutex.
async function uiPollFresh(label, expr, want, timeoutMs = 180000) {
  const t0 = Date.now();
  let got = '';
  while (Date.now() - t0 < timeoutMs) {
    await evaluate(`fetchMissions()`);
    await sleepMs(8000);
    got = String((await evaluate(expr)) || '');
    if (got.includes(want)) return got;
  }
  const diag = await evaluate(`JSON.stringify({views: Object.keys(window.missionViews || {}).length, listed: (window.lastMissions || []).length, inflight: !!window.missionsInflight, gen: window.missionsGen, sample: Object.keys(window.missionViews || {}).slice(0, 3)})`);
  console.log('POLLDIAG ' + label + ' ' + diag);
  need(false, label + ', got: ' + got.slice(0, 160));
  return got;
}
await evaluate(`document.getElementById('mission-goal').value = 'UI smoke mission ' + Date.now()`);
await evaluate(`document.getElementById('mission-workspace').value = 'C:/spaceos/data'`);
await evaluateAwait(`missionCreate()`);
await uiPollFresh('mission created via UI', `document.getElementById('missions-list').innerText`, 'UI smoke mission');
const mid = await evaluate(`(window.lastMissions && window.lastMissions.find(m => (m.goal || '').includes('UI smoke mission')) || {}).mission_id || ''`);
console.log('MISSION_MID ' + mid);
need(!!mid, 'mission id visible to UI');
if (mid) {
  // Sequenced mutations: each confirmed before the next is sent, so
  // dependent transitions cannot be applied out of order.
  await evaluateAwait(`missionApi('POST', '/missions/${mid}/plan', { steps: [{ title: 'probe', detail: '', requires_approval: false }] })`);
  const stepIds = await uiPollFresh('planned step rendered',
    `((window.missionViews['${mid}'] || {}).steps || []).map(s => s.id).join(',')`, 'mission-');
  const sid = (stepIds || '').split(',')[0] || '';
  if (sid && sid.includes('mission-')) {
    await evaluateAwait(`stepTransition('${mid}', '${sid}', 'running')`);
    await evaluateAwait(`stepTransition('${mid}', '${sid}', 'completed', false)`);
    await uiPollFresh('step completed via UI',
      `((((window.missionViews['${mid}'] || {}).steps) || [])[0].properties || {}).state || ''`, 'completed');
  } else {
    need(false, 'no step id to drive');
  }
  await evaluateAwait(`missionTransition('${mid}', 'verifying')`);
  await evaluateAwait(`missionApi('POST', '/missions/${mid}/verify', { passed: true, method: 'ui-smoke', detail: { note: 'ui' } })`);
  await evaluateAwait(`missionTransition('${mid}', 'completed', { result: { summary: 'ui' } })`);
  await uiPollFresh('mission completed via UI',
    `((window.missionViews['${mid}'] || {}).mission.properties || {}).state || ''`, 'completed');
}

// Status filter present with all lifecycle states.
const stOpts = await evaluate(`[...document.getElementById('activity-status').options].map(o => o.text).join(',')`);
need(['Queued', 'Running', 'Completed', 'Failed', 'Denied', 'Unknown'].every((s) => (stOpts || '').includes(s)), 'status filter options: ' + stOpts);

need(errors.length === 0, 'zero console errors');

// Screenshot last and best-effort: CDP capture on a backgrounded tab
// can stall (observed flake); it must never gate the DOM assertions.
await evaluate(`gotoTab('workspace')`);
await new Promise((r) => setTimeout(r, 500));
try {
  const shot = await send('Page.captureScreenshot', { format: 'png' }, 30000);
  const { writeFileSync } = await import('fs');
  writeFileSync('C:\\spaceos\\logs\\shell-workspace.png', Buffer.from(shot.result.data, 'base64'));
  console.log('SHOT ok');
} catch (e) {
  console.log('SHOT skipped: ' + e.message);
}
ws.close();
if (failed) process.exit(1);
console.log('SHELL_UI PASS');
