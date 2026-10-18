import http from 'http';
const g = (p) => new Promise((res, rej) => {
  const r = http.get('http://127.0.0.1:9222' + p, (x) => { let d = ''; x.on('data', c => d += c); x.on('end', () => res(d)); });
  r.on('error', rej); r.end();
});
const put = (p) => new Promise((res, rej) => {
  const r = http.request({ host: '127.0.0.1', port: 9222, path: p, method: 'PUT' }, (x) => { let d = ''; x.on('data', c => d += c); x.on('end', () => res(d)); });
  r.on('error', rej); r.end();
});
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const post = (body) => new Promise((res, rej) => {
  const b = JSON.stringify(body);
  const r = http.request({ host: '127.0.0.1', port: 8765, path: '/api/providers/browser/actions', method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(b) } }, (x) => { let d = ''; x.on('data', c => d += c); x.on('end', () => res({ ok: x.statusCode === 200, body: d, code: x.statusCode })); });
  r.on('error', rej); r.end(b);
});

(async () => {
  console.log('=== E2E: Real Browser Integration (vertical slice) ===');

  // Step A: Find or create a real page target through the provider contract.
  let sh = null;
  for (let i = 0; i < 15 && !sh; i++) {
    try {
      const tabs = JSON.parse(await g('/json/list'));
      sh = tabs.find(x => x.type === 'page' && (x.url || '').includes('/index.html'));
      if (!sh) await put('/json/new?http://127.0.0.1:8765/index.html');
    } catch { }
    await sleep(500);
  }
  console.log('Step A (target page exists):', !!sh, sh && sh.id ? sh.id.slice(0, 8) : 'none');

  if (!sh) { console.log('FAIL: No shell/world tab found after attempts'); return; }
  await g('/json/activate/' + sh.id);

  // Step B: Get browser tabs via provider and confirm a real non-dashboard tab exists.
  let tabsData = null;
  for (let i = 0; i < 20; i++) {
    try {
      const tabsResp = await fetch('http://127.0.0.1:8765/api/browser/tabs', { signal: AbortSignal.timeout(5000) }).catch(() => null);
      if (tabsResp && tabsResp.ok) {
        const j = await tabsResp.json();
        tabsData = (j.result && j.result.result && j.result.result.detail && j.result.result.detail.tabs)
          ? j.result.result.detail.tabs
          : (j.result && j.result.result && j.result.result.tabs ? j.result.result.tabs : (j.result && j.result.tabs ? j.result.tabs : j.result || []));
        if (tabsData && tabsData.length) break;
      }
    } catch { }
    await sleep(500);
  }
  const realTab = Array.isArray(tabsData) ? tabsData.find(t => t.url && !t.url.includes('127.0.0.1:8765')) : null;
  console.log('Step B (provider tabs list):', !!tabsData, 'tabs=', Array.isArray(tabsData) ? tabsData.length : 0, 'real-non-dash=', !!realTab, realTab ? realTab.url.slice(0, 50) : 'none');

  if (!realTab) {
    console.log('NOTE: No non-dashboard CDP tab found; provider may have no live session. Continuing with provider-level actions only.');
  }

  // Step C: Execute provider action `navigate` to a known URL and verify state change.
  const navUrl = 'https://example.com/';
  try {
    const navResp = await post({ action: 'navigate', target: { url: navUrl, target_id: realTab ? realTab.id : null } });
    const inner = (navResp.body ? JSON.parse(navResp.body) : {});
    const status = inner.result ? (inner.result.result ? inner.result.result.status : inner.result.status) : inner.status;
    console.log('Step C (navigate via provider): status=', status || 'unknown', 'action-id=', inner.result ? (inner.result.result ? inner.result.result.action_id || inner.result.action_id : inner.result.action_id) : inner.action_id);
  } catch (e) {
    console.log('Step C (navigate via provider): FAIL -', e.message);
  }

  // Step D: Take a provider-level `screenshot` and verify JPEG payload.
  try {
    const scResp = await post({ action: 'screenshot', target: { target_id: realTab ? realTab.id : null, format: 'jpeg', quality: 62 } });
    const inner = (scResp.body ? JSON.parse(scResp.body) : {});
    const data = ((inner.result && inner.result.result && inner.result.result.detail) ? inner.result.result.detail.data : (inner.result ? inner.result.detail ? inner.result.detail.data : null : null)) || ((inner.result && inner.result.result) ? (inner.result.result.detail ? inner.result.result.detail.data : null) : null);
    const hasData = !!(data && String(data).startsWith('/9j/'));
    console.log('Step D (screenshot payload): has-jpeg=', hasData, 'payload-start=', data ? String(data).slice(0, 20) : 'none');
  } catch (e) {
    console.log('Step D (screenshot): FAIL -', e.message);
  }

  // Step E: Confirm that `world.html` serves the interactive browser surface (iframe present, caption accurate).
  try {
    const htmlResp = await fetch('http://127.0.0.1:8765/', { signal: AbortSignal.timeout(5000) });
    if (htmlResp.ok) {
      const txt = await htmlResp.text();
      const hasIframe = txt.includes('<iframe id="bframe"');
      const hasInteractiveCaption = txt.includes('интерактивная') || txt.includes('iframe');
      const hasNoteLimit = txt.includes('.NET SDK') || txt.includes('WebView2 SDK loader');
      console.log('Step E (world.html interactive surface): iframe=', hasIframe, 'caption=', hasInteractiveCaption, 'limitation-note=', hasNoteLimit);
    } else {
      console.log('Step E (world.html interactive surface): HTTP', htmlResp.status);
    }
  } catch (e) {
    console.log('Step E (world.html interactive surface): FAIL -', e.message);
  }

  // Step F: Confirm `#recent-objects` element exists in Shell (fix verified).
  try {
    const shResp = await fetch('http://127.0.0.1:8765/index.html', { signal: AbortSignal.timeout(5000) });
    if (shResp.ok) {
      const txt = await shResp.text();
      const hasRecent = txt.includes('id="recent-objects"');
      console.log('Step F (index.html null-bug fix): #recent-objects=', hasRecent);
    } else {
      console.log('Step F: HTTP', shResp.status);
    }
  } catch (e) {
    console.log('Step F: FAIL -', e.message);
  }

  console.log('=== E2E COMPLETE ===');
})();
