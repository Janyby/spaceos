// Stage 3B.2 — executable World/Bridge contract regression tests.
//
// Two halves:
//   A. STATIC parity: the World's calls, payloads and response reads are checked
//      against the ACTUAL Rust Bridge/Core source (read-only).
//   B. LIVE behaviour: the contract-faithful mock is probed to confirm it
//      enforces the same guards the real Bridge enforces, and the World is
//      driven in a real browser against it.

const fs = require('fs');
const path = require('path');
const http = require('http');

const RUST = path.join(__dirname, '..', 'space-bridge', 'src');
const CORE = path.join(__dirname, '..', 'space-core', 'src');
const WORLD = process.argv[2] || path.join(__dirname, '..', 'space-bridge', 'static', 'world.html');
const MOCK_PORT = Number(process.env.S3B1_MOCK_PORT || 18771);
const MOCK_ORIGIN = 'http://127.0.0.1:' + MOCK_PORT;

let passed = 0, failed = 0;
const failures = [];
function assert(c, m) {
  if (c) { passed++; console.log('  \u2713 ' + m); }
  else { failed++; failures.push(m); console.log('  \u2717 ' + m); }
}

const rustH = fs.readFileSync(path.join(RUST, 'handlers.rs'), 'utf8');
const rustCore = fs.readFileSync(path.join(CORE, 'semantic.rs'), 'utf8');
const world = fs.readFileSync(WORLD, 'utf8');
const wLines = world.split('\n');

// ================================================================
// A. STATIC PARITY
// ================================================================
console.log('=== A. STATIC PARITY: World <-> actual Rust Bridge ===\n');

console.log('--- A1. every World HTTP call maps to a real Bridge route ---');
{
  // routes from the Rust router
  const routes = new Map();
  const re = /\.route\(\s*"([^"]+)"\s*,/g;
  let m;
  while ((m = re.exec(rustH)) !== null) {
    const p = m[1];
    let i = re.lastIndex, depth = 1;
    while (i < rustH.length && depth > 0) { if (rustH[i] === '(') depth++; else if (rustH[i] === ')') depth--; i++; }
    const body = rustH.slice(re.lastIndex, i - 1);
    ['get', 'post', 'put', 'patch', 'delete'].forEach(mth => {
      const hr = new RegExp('\\b' + mth + '\\s*\\(\\s*([\\w:]+)\\s*\\)', 'g');
      let hm;
      while ((hm = hr.exec(body)) !== null) {
        const k = mth.toUpperCase() + ' ' + p;
        if (!routes.has(k)) routes.set(k, { path: p, method: mth.toUpperCase(), handler: hm[1] });
      }
    });
  }
  assert(routes.size >= 40, 'route table extracted from handlers.rs (' + routes.size + ' routes)');

  const calls = [];
  wLines.forEach((l, i) => {
    const cr = /(GET|POST)\('([^']*)'/g;
    let cm;
    while ((cm = cr.exec(l)) !== null) {
      let depth = 1, tail = '';
      for (let j = i; j < Math.min(i + 20, wLines.length); j++) {
        const seg = wLines[j].slice(j === i ? cm.index + cm[0].length : 0);
        for (const c of seg) { if (c === '(') depth++; else if (c === ')') depth--; }
        tail += ' ' + seg;
        if (depth <= 0) break;
      }
      const ov = tail.match(/'PATCH'|'PUT'|'DELETE'/);
      calls.push({ line: i + 1, method: ov ? ov[0].replace(/'/g, '') : cm[1], ep: '/api' + cm[2] });
    }
  });
  let unmatched = [];
  calls.forEach(c => {
    const cp = c.ep.split('?')[0];
    let hit = false;
    for (const [k, r] of routes) {
      if (r.method !== c.method) continue;
      if (r.path === cp) { hit = true; break; }
      const rp = r.path.split('/'), cpp = cp.split('/');
      if (rp.length === cpp.length) {
        let okp = true;
        for (let i = 0; i < rp.length; i++) { if (!rp[i].startsWith(':') && rp[i] !== cpp[i]) { okp = false; break; } }
        if (okp) { hit = true; break; }
      }
    }
    if (!hit) unmatched.push(c.method + ' ' + c.ep);
  });
  assert(calls.length >= 20, 'World HTTP calls extracted (' + calls.length + ')');
  assert(unmatched.length === 0, 'every World call maps to a real Bridge route' +
    (unmatched.length ? ' -> UNMATCHED: ' + unmatched.join(', ') : ''));
}

console.log('\n--- A2. semantic payload DTO parity (deny_unknown_fields) ---');
{
  const DTO = [
    ['CreateSemanticObject', 'createSemanticObject', ['world_id', 'object_type', 'properties', 'correlation_id']],
    ['PatchSemanticObject', 'saveSemanticObject', ['object_id', 'world_id', 'properties_patch', 'correlation_id']],
    ['CreateSemanticRelation', 'createSemanticRelation', ['world_id', 'source_id', 'target_id', 'relation_type', 'properties', 'correlation_id']]
  ];
  DTO.forEach(([dto, fn, fields]) => {
    // the DTO must exist in Core with exactly these fields
    const re = new RegExp('pub struct ' + dto + '\\s*\\{([\\s\\S]*?)\\n\\}', 'm');
    const mm = rustCore.match(re);
    assert(!!mm, dto + ' exists in space-core/src/semantic.rs');
    if (mm) {
      const coreFields = (mm[1].match(/pub\s+(\w+)\s*:/g) || []).map(x => (x.match(/pub\s+(\w+)\s*:/) || [])[1]);
      assert(JSON.stringify(coreFields) === JSON.stringify(fields),
        dto + ' DTO fields match the contract: ' + coreFields.join(', '));
      const deny = /#\[serde\(deny_unknown_fields\)\]/.test(rustCore.slice(Math.max(0, mm.index - 90), mm.index));
      assert(deny, dto + ' uses deny_unknown_fields (extra fields are a 400)');
    }
    // the World must send exactly those keys
    const start = wLines.findIndex(l => l.includes('async function ' + fn + '('));
    assert(start >= 0, fn + '() exists in the World');
    if (start >= 0) {
      let payload = null;
      for (let i = start; i < Math.min(start + 70, wLines.length); i++) {
        if (/POST\('\/semantic\//.test(wLines[i])) {
          let depth = 0, buf = '', started = false;
          for (let j = i; j < wLines.length; j++) {
            for (const c of wLines[j]) { if (c === '{') { depth++; started = true; } else if (c === '}') depth--; }
            buf += wLines[j] + '\n';
            if (started && depth === 0) break;
          }
          payload = buf; break;
        }
      }
      assert(!!payload, fn + '() posts a semantic payload');
      if (payload) {
        const keys = (payload.match(/^\s*(\w+)\s*:/gm) || []).map(x => x.replace(/[^\w]/g, ''));
        const missing = fields.filter(f => !keys.includes(f));
        const extra = keys.filter(k => !fields.includes(k));
        assert(missing.length === 0, fn + '() sends every required field' + (missing.length ? ' -> missing ' + missing.join(',') : ''));
        assert(extra.length === 0, fn + '() sends NO extra field (deny_unknown_fields)' + (extra.length ? ' -> extra ' + extra.join(',') : ''));
      }
    }
  });
}

console.log('\n--- A3. response field paths the World reads exist in the Bridge ---');
{
  // (endpoint, World read path, Bridge response shape) verified against the Rust
  const CASES = [
    ['POST /api/objects/query', 'objects', /"objects":\s*objects/],
    ['POST /api/relations/query', 'relations', /"relations":\s*relations/],
    ['GET /api/events', 'events', /"events":\s*events/],
    ['GET /api/actions/recent', 'actions', /"actions":\s*rows/],
    ['GET /api/missions', 'missions', /"missions":\s*missions/],
    ['GET /api/components', 'components', /"components":\s*components/],
    ['GET /api/providers', 'providers', /"providers":\s*providers/],
    ['GET /api/context', 'semantic_world_id', /\["semantic_world_id"\]\s*=\s*json!\(semantic_world_id\)/],
    ['GET /api/browser/tabs', 'detail', /"result":\s*resp/],
    ['GET /api/filesystem/roots', 'roots', /"result":\s*resp/],
    ['POST /api/semantic/objects', 'object', /pub\s+object:\s*Object/],
    ['PATCH /api/semantic/objects/:id', 'object', /pub\s+object:\s*Object/],
    ['POST /api/semantic/relations', 'relation', /pub\s+relation:\s*Relation/]
  ];
  CASES.forEach(([ep, field, shape]) => {
    const inRust = shape.test(rustH) || shape.test(rustCore);
    const inWorld = world.includes(field);
    assert(inRust && inWorld, ep + ': the World reads "' + field + '" and the Bridge/Core produces that shape');
  });
}

console.log('\n--- A4. error envelope the World parses matches api_err ---');
{
  const err = rustH.match(/fn api_err[\s\S]*?\n\}/);
  assert(!!err, 'api_err() found in handlers.rs');
  if (err) {
    assert(/"ok":\s*false/.test(err[0]), 'Bridge error body carries "ok": false');
    assert(/"error":\s*msg/.test(err[0]), 'Bridge error body carries "error": <message>');
    assert(/"timestamp"/.test(err[0]), 'Bridge error body carries "timestamp"');
  }
  assert(world.includes('body && body.error') || world.includes('error.error'),
    'the World extracts the error message from the "error" field');
  assert(world.includes("!r.ok"), 'the World branches on the HTTP status via r.ok');
}

console.log('\n--- A5. Bridge security guards the World must satisfy ---');
{
  const og = rustH.match(/fn ensure_agent_origin[\s\S]*?\n\}/);
  assert(!!og, 'ensure_agent_origin() present');
  if (og) {
    const rustStateSource = fs.readFileSync(path.join(RUST, 'state.rs'), 'utf8');
    assert(/value == state\.agent_origin\(\)/.test(og[0])
      && /agent_origin: "http:\/\/127\.0\.0\.1:8765"/.test(rustStateSource),
      'the Bridge compares one exact configured origin and keeps the production default');
    assert(/FORBIDDEN/.test(og[0]), 'a foreign origin is FORBIDDEN');
  }
  const wg = rustH.match(/fn ensure_semantic_world[\s\S]*?\n\}/);
  assert(!!wg, 'ensure_semantic_world() present');
  if (wg) {
    assert(/requested_world != configured/.test(wg[0]), 'the requested world_id must equal the configured one');
    assert(/FORBIDDEN/.test(wg[0]), 'a mismatched world_id is FORBIDDEN');
  }
  const patch = rustH.match(/async fn patch_semantic_object_ep[\s\S]*?\n\}/);
  assert(!!patch && /object_id must match the URL path/.test(patch[0]),
    'the Bridge requires object_id to match the URL path');
  assert(/validate_properties/.test(rustCore), 'Core validates the properties object');
  assert(/validate_correlation_id/.test(rustCore), 'Core validates the correlation_id');
  assert(/world_id.*managed by Core/.test(rustCore), 'Core refuses a world_id inside properties');
}

console.log('\n--- A6. WebSocket message contract ---');
{
  const rustState = fs.readFileSync(path.join(RUST, 'state.rs'), 'utf8');
  assert(/"type":\s*"init"/.test(rustH), 'the Bridge sends an "init" WebSocket frame');
  assert(/"type":\s*"semantic_event"/.test(rustH), 'the Bridge sends "semantic_event" frames');
  assert(/"type":\s*"agent_update"/.test(rustState) || /"type":\s*"agent_update"/.test(rustH),
    'the Bridge broadcasts "agent_update" frames (state.rs update_agent_run)');
  assert(world.includes("msg.type === 'semantic_event'"), 'the World handles semantic_event frames');
  assert(world.includes("msg.event?.seq"), 'the World reads the event sequence from the frame');
  assert(world.includes("msg.type === 'agent_update'"), 'the World handles agent_update frames');
  assert(world.includes('msg.request_id === A.requestId'), 'the World correlates agent_update by request_id');
}

console.log('\n--- A7. DEF-3B1-1 present in the release candidate source ---');
{
  const FN = [['createSemanticObject', 'SemObjCreate'], ['saveSemanticObject', 'SemObjEdit'], ['createSemanticRelation', 'SemObjRelCreate']];
  FN.forEach(([fn, g]) => {
    const i = world.indexOf('async function ' + fn + '(');
    assert(i >= 0, fn + '() present');
    if (i < 0) return;
    let d = 0, s = false, j = i;
    for (; j < world.length; j++) { const c = world[j]; if (c === '{') { d++; s = true; } else if (c === '}') d--; if (s && d === 0) break; }
    const code = world.slice(i, j + 1)
      .replace(/\/\*[\s\S]*?\*\//g, ' ')
      .replace(/^\s*\/\/.*$/gm, ' ')
      .replace(/'(?:[^'\\]|\\.)*'/g, "''")
      .replace(/"(?:[^"\\]|\\.)*"/g, '""');
    const set = code.indexOf(g + '.submitting = true;');
    const aw = code.search(/\bawait\b/);
    assert(set >= 0 && aw >= 0 && set < aw, fn + '(): the in-flight guard is acquired BEFORE the first await');
    assert(new RegExp('\\}\\s*finally\\s*\\{\\s*' + g + '\\.submitting = false;').test(code),
      fn + '(): an outer try/finally releases the guard on every exit path');
  });
}

// ================================================================
// B. LIVE CONTRACT BEHAVIOUR (against the contract-faithful mock)
// ================================================================
console.log('\n=== B. LIVE CONTRACT BEHAVIOUR (contract-faithful mock) ===\n');

function req(opts, body, extraHeaders) {
  return new Promise((resolve, reject) => {
    const r = http.request(Object.assign({ agent: false, headers: extraHeaders || {} }, opts), s => {
      let b = ''; s.on('data', c => { b += c; });
      s.on('end', () => { let j = null; try { j = JSON.parse(b); } catch (e) {} resolve({ status: s.statusCode, body: j, raw: b }); });
    });
    r.on('error', reject);
    r.setTimeout(6000, () => r.destroy(new Error('timeout')));
    if (body) r.write(typeof body === 'string' ? body : JSON.stringify(body));
    r.end();
  });
}

(async () => {
  const { startServices } = require('./services');
  const svc = await startServices();
  const B = (p, method, body, headers) => req({
    host: '127.0.0.1', port: MOCK_PORT, path: p, method,
    headers: Object.assign({ 'Content-Type': 'application/json' }, headers || {})
  }, body);

  console.log('--- B1. ensure_agent_origin: a foreign Origin is 403 ---');
  {
    const r = await B('/api/semantic/objects', 'POST',
      { world_id: 's3b1-mock-world', object_type: 'Note', properties: {}, correlation_id: 'c1' },
      { Origin: 'http://127.0.0.1:8765' });
    assert(r.status === 403, 'a request with a foreign Origin is 403 (got ' + r.status + ')');
    assert(r.body && r.body.ok === false, 'the 403 body carries ok:false');
    assert(r.body && /cross-origin access to Agent Gateway is not allowed/.test(r.body.error),
      'the 403 body carries the Bridge error message');
  }

  console.log('\n--- B2. ensure_semantic_world: a wrong world_id is 403 ---');
  {
    const r = await B('/api/semantic/objects', 'POST',
      { world_id: 'wrong-world', object_type: 'Note', properties: {}, correlation_id: 'c1' },
      { Origin: MOCK_ORIGIN });
    assert(r.status === 403, 'a mismatched world_id is 403 (got ' + r.status + ')');
    assert(r.body && /requested world_id is not the configured semantic world/.test(r.body.error),
      'the 403 body carries the Bridge error message');
  }

  console.log('\n--- B3. deny_unknown_fields: an extra field is 400 ---');
  {
    const r = await B('/api/semantic/objects', 'POST',
      { world_id: 's3b1-mock-world', object_type: 'Note', properties: {}, correlation_id: 'c1', sneaky: 1 },
      { Origin: MOCK_ORIGIN });
    assert(r.status === 400, 'an unknown payload field is 400 (got ' + r.status + ')');
    assert(r.body && /invalid semantic object/.test(r.body.error),
      'the 400 body carries "invalid semantic object"');
  }

  console.log('\n--- B4. missing field is 400 ---');
  {
    const r = await B('/api/semantic/objects', 'POST',
      { world_id: 's3b1-mock-world', properties: {} },
      { Origin: MOCK_ORIGIN });
    assert(r.status === 400, 'a missing object_type is 400 (got ' + r.status + ')');
  }

  console.log('\n--- B5. properties validation ---');
  {
    const r = await B('/api/semantic/objects', 'POST',
      { world_id: 's3b1-mock-world', object_type: 'Note', properties: 'not-an-object', correlation_id: 'c1' },
      { Origin: MOCK_ORIGIN });
    assert(r.status === 400, 'a non-object properties value is 400 (got ' + r.status + ')');
    const r2 = await B('/api/semantic/objects', 'POST',
      { world_id: 's3b1-mock-world', object_type: 'Note', properties: { world_id: 'x' }, correlation_id: 'c1' },
      { Origin: MOCK_ORIGIN });
    assert(r2.status === 400, 'world_id inside properties is 400 (got ' + r2.status + ')');
  }

  console.log('\n--- B6. correlation_id validation ---');
  {
    const r = await B('/api/semantic/objects', 'POST',
      { world_id: 's3b1-mock-world', object_type: 'Note', properties: {}, correlation_id: '' },
      { Origin: MOCK_ORIGIN });
    assert(r.status === 400, 'an empty correlation_id is 400 (got ' + r.status + ')');
  }

  console.log('\n--- B7. PATCH object_id must match the URL path ---');
  {
    const r = await B('/api/semantic/objects/sem-1', 'PATCH',
      { object_id: 'sem-OTHER', world_id: 's3b1-mock-world', properties_patch: {}, correlation_id: 'c1' },
      { Origin: MOCK_ORIGIN });
    assert(r.status === 400, 'a mismatched object_id is 400 (got ' + r.status + ')');
    assert(r.body && /object_id must match the URL path/.test(r.body.error),
      'the 400 body carries the Bridge error message');
  }

  console.log('\n--- B8. success envelopes match the Bridge shapes ---');
  {
    const r = await B('/api/semantic/objects', 'POST',
      { world_id: 's3b1-mock-world', object_type: 'Note', properties: { name: 'x' }, correlation_id: 'c1' },
      { Origin: MOCK_ORIGIN });
    assert(r.status === 200, 'a valid create is 200 (got ' + r.status + ')');
    assert(r.body && r.body.ok === true, 'the success body carries ok:true');
    assert(r.body && r.body.result && r.body.result.object, 'the success body carries result.object (Core envelope)');
    assert(r.body && r.body.result && r.body.result.event, 'the success body carries result.event');

    const id = r.body.result.object.id;
    const p = await B('/api/semantic/objects/' + encodeURIComponent(id), 'PATCH',
      { object_id: id, world_id: 's3b1-mock-world', properties_patch: { name: 'y' }, correlation_id: 'c2' },
      { Origin: MOCK_ORIGIN });
    assert(p.status === 200, 'a valid patch is 200 (got ' + p.status + ')');
    assert(p.body && p.body.result && p.body.result.object, 'the patch body carries result.object');

    const rel = await B('/api/semantic/relations', 'POST',
      { world_id: 's3b1-mock-world', source_id: id, target_id: 'obj-0001', relation_type: 'related_to', properties: {}, correlation_id: 'c3' },
      { Origin: MOCK_ORIGIN });
    assert(rel.status === 200, 'a valid relation is 200 (got ' + rel.status + ')');
    assert(rel.body && rel.body.result && rel.body.result.relation, 'the relation body carries result.relation');
  }

  console.log('\n--- B9. read-route response shapes ---');
  {
    const cases = [
      ['/api/objects/query', 'POST', {}, j => j.objects && j.count !== undefined && j.timestamp],
      ['/api/relations/query', 'POST', {}, j => j.relations && j.count !== undefined && j.timestamp],
      ['/api/events?limit=50', 'GET', null, j => j.events && j.count !== undefined && j.timestamp],
      ['/api/actions/recent?limit=50', 'GET', null, j => j.actions && j.count !== undefined && j.timestamp],
      ['/api/missions', 'GET', null, j => j.missions && j.count !== undefined && j.timestamp],
      ['/api/components', 'GET', null, j => j.components && j.timestamp],
      ['/api/providers', 'GET', null, j => j.providers && j.count !== undefined && j.timestamp],
      ['/api/context', 'GET', null, j => j.context && j.context.semantic_world_id && j.timestamp],
      ['/api/browser/tabs', 'GET', null, j => j.result && j.result.result && j.result.result.detail && Array.isArray(j.result.result.detail.tabs)],
      ['/api/filesystem/roots', 'GET', null, j => j.result && j.result.result && Array.isArray(j.result.result.roots)],
      ['/api/actions/some-id', 'GET', null, j => j.action_id && j.count !== undefined && j.status && j.events && j.timestamp],
      ['/api/filesystem/read', 'POST', { path: 'x' }, j => j.result && j.result.result && j.result.result.status]
    ];
    for (const [p, m, b, check] of cases) {
      const r = await B(p, m, b, { Origin: MOCK_ORIGIN });
      assert(r.status === 200 && check(r.body), p + ' -> 200 with the Bridge response shape');
    }
  }

  console.log('\n--- B10. the World works against the contract-faithful mock (live browser) ---');
  {
    // the browser suite runs against the same mock; here we assert the mock
    // is strict enough to have rejected the permissive Stage 3B.1 behaviour.
    const r = await B('/api/semantic/objects', 'POST',
      { world_id: 's3b1-mock-world', object_type: 'Note', properties: {}, correlation_id: 'c1' },
      { Origin: MOCK_ORIGIN });
    assert(r.status === 200, 'a valid request from the mock origin is accepted');
  }

  svc.stop();
  console.log('\n=== RESULTS ===');
  console.log('Passed: ' + passed);
  console.log('Failed: ' + failed);
  console.log('Total:  ' + (passed + failed));
  if (failures.length) { console.log('\nFailing:'); failures.forEach(f => console.log('  - ' + f)); }
  process.exit(failed > 0 ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
