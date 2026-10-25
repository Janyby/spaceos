// Stage 3B.2 — contract-faithful mock of the SpaceOS Bridge.
//
// Every behaviour here is derived from the ACTUAL Rust implementation:
//   C:\spaceos\space-bridge\src\handlers.rs
//   C:\spaceos\space-bridge\src\state.rs
//   C:\spaceos\space-core\src\semantic.rs
//
// Reproduced semantics (see docs/STAGE_3B2_CONTRACT_MATRIX.md):
//   * ensure_agent_origin  : Origin header, if present, must equal the Bridge's
//                            own origin, else 403.
//   * ensure_semantic_world: the requested world_id must equal the configured
//                            one, else 403.
//   * DTO validation        : deny_unknown_fields -> unknown field => 400.
//   * patch object_id       : must match the URL path, else 400.
//   * validate_properties   : must be a JSON object, must not carry world_id.
//   * validate_correlation_id: 1..=200 characters.
//   * error envelope        : {ok:false, error, timestamp}.
//   * success envelope      : {ok:true, result:{...}} for the semantic routes,
//                            and the per-endpoint shapes for the read routes.
//   * WebSocket             : an "init" frame first, then "semantic_event" and
//                            "agent_update" frames.

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.S3B1_MOCK_PORT || 18771);
const HOST = '127.0.0.1';
// The origin this mock "is served from" — the analogue of the Bridge's own
// origin. The real Bridge allows exactly its own origin; the mock likewise
// allows exactly its own origin, so the guard behaves identically.
const OWN_ORIGIN = 'http://127.0.0.1:' + PORT;
const SEMANTIC_WORLD_ID = process.env.S3B1_SEMANTIC_WORLD || 's3b1-mock-world';

// ---------------------------------------------------------------- fixtures
function makeRng(seed) {
  let s = seed >>> 0;
  return function () { s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0; return s / 4294967296; };
}
const TYPES = ['File', 'Directory', 'BrowserTab', 'Mission', 'MissionStep', 'Agent', 'Project', 'Document', 'Result', 'Setting'];
function buildFixture(nObj, nRel, seed) {
  const rnd = makeRng(seed);
  const objects = [];
  for (let i = 0; i < nObj; i++) {
    const type = TYPES[Math.floor(rnd() * TYPES.length)];
    const ts = new Date(Date.UTC(2026, 0, 1) + i * 60000).toISOString();
    objects.push({
      id: 'obj-' + String(i).padStart(4, '0'), type,
      provider_id: (type === 'Project' || type === 'Document') ? 'space-world-user' : 'filesystem',
      external_id: type === 'BrowserTab' ? 'tab-t' + i : null,
      created_at: ts, updated_at: ts,
      properties: { name: 'Fixture ' + type + ' ' + i, goal: type === 'Mission' ? 'Goal ' + i : undefined }
    });
  }
  const relations = [];
  for (let i = 0; i < nRel; i++) {
    const a = Math.floor(rnd() * nObj), b = Math.floor(rnd() * nObj);
    relations.push({
      id: 'rel-' + String(i).padStart(5, '0'),
      relation_type: ['related_to', 'contains', 'depends_on', 'references'][Math.floor(rnd() * 4)],
      source_object_id: objects[a].id, target_object_id: objects[b].id,
      created_at: new Date(Date.UTC(2026, 0, 1) + i * 1000).toISOString(), properties: {}
    });
  }
  relations.push({ id: 'rel-multi-a', relation_type: 'related_to', source_object_id: 'obj-0000', target_object_id: 'obj-0001', created_at: '2026-01-02T00:00:00.000Z', properties: {} });
  relations.push({ id: 'rel-multi-b', relation_type: 'contains', source_object_id: 'obj-0000', target_object_id: 'obj-0001', created_at: '2026-01-02T00:00:01.000Z', properties: {} });
  relations.push({ id: 'rel-multi-c', relation_type: 'depends_on', source_object_id: 'obj-0001', target_object_id: 'obj-0000', created_at: '2026-01-02T00:00:02.000Z', properties: {} });

  const missions = objects.filter(o => o.type === 'Mission').slice(0, 3)
    .map((m, i) => ({ mission_id: m.id, state: i === 0 ? 'running' : 'completed', goal: 'Mission ' + i }));
  const events = [];
  for (let i = 0; i < 20; i++) {
    events.push({ seq: i + 1, event_type: 'ActionCompleted', timestamp: new Date(Date.UTC(2026, 0, 1) + i * 1000).toISOString(), actor: 'space-core', provider_id: 'space-core', correlation_id: 'corr-' + i, payload: { action: 'op', target: { url: 'x' } } });
  }
  const actions = events.map((e, i) => ({
    action_id: e.correlation_id, correlation_id: e.correlation_id, status: 'Completed',
    actor: 'actor-' + (i % 3), provider_id: 'space-core', operation: 'op', resource: 'x',
    message: null, first_seen: e.timestamp, last_seen: e.timestamp, event_count: 1
  }));
  const tabs = [];
  for (let i = 0; i < 3; i++) tabs.push({ target_id: 'tab-t' + i, title: 'Mock Tab ' + i, url: 'https://example.invalid/' + i });
  return { objects, relations, missions, events, actions, tabs };
}

let FIXTURE = buildFixture(60, 120, 20261020);
let SEMANTIC_SEQ = 100;
let nextObjId = 9000, nextRelId = 9000;
const createdObjects = [], createdRelations = [];

// ---------------------------------------------------------------- recording
const requestLog = [];
let failNext = null, failAll = false;
const DELAY = { queue: [] };
const injections = [];
function log(e) { requestLog.push(Object.assign({ t: Date.now() }, e)); }

// ---------------------------------------------------------------- WS
const wsClients = new Set();
function wsFrame(text) {
  const payload = Buffer.from(text, 'utf8');
  const len = payload.length;
  let header;
  if (len < 126) { header = Buffer.alloc(2); header[1] = len; }
  else if (len < 65536) { header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  header[0] = 0x81;
  return Buffer.concat([header, payload]);
}
function wsSend(sock, obj) { try { sock.write(wsFrame(JSON.stringify(obj))); } catch (e) {} }
function pushWs(obj) { wsClients.forEach(s => wsSend(s, obj)); }

// ---------------------------------------------------------------- helpers
function now() { return new Date().toISOString(); }
function json(res, status, obj) {
  const s = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(s), 'Cache-Control': 'no-store' });
  res.end(s);
}
// api_err equivalent: {ok:false, error, timestamp}
function apiErr(res, status, msg) { json(res, status, { ok: false, error: String(msg), timestamp: now() }); }
// core IPC envelope
function coreOk(result) { return { ok: true, result: result }; }

// ensure_agent_origin equivalent
function ensureAgentOrigin(req) {
  const origin = req.headers['origin'];
  if (origin === undefined) return null;                 // absent -> allowed
  if (origin === OWN_ORIGIN) return null;                // own origin -> allowed
  return apiErr(res => res, 403, 'cross-origin access to Agent Gateway is not allowed');
}

// ensure_semantic_world equivalent
function ensureSemanticWorld(res, requested) {
  if (requested !== SEMANTIC_WORLD_ID) {
    apiErr(res, 403, 'requested world_id is not the configured semantic world');
    return false;
  }
  return true;
}

// DTO validation with deny_unknown_fields + the Core's field rules
function validateDto(res, dtoName, payload, allowed) {
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    apiErr(res, 400, 'invalid ' + dtoName + ': expected a JSON object');
    return false;
  }
  const unknown = Object.keys(payload).filter(k => !allowed.includes(k));
  if (unknown.length) {
    apiErr(res, 400, 'invalid ' + dtoName + ': unknown field `' + unknown[0] + '`');
    return false;
  }
  const missing = allowed.filter(k => !(k in payload));
  if (missing.length) {
    apiErr(res, 400, 'invalid ' + dtoName + ': missing field `' + missing[0] + '`');
    return false;
  }
  return true;
}
function validateProperties(res, value, field) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    apiErr(res, 400, field + ' must be a JSON object');
    return false;
  }
  if (Object.prototype.hasOwnProperty.call(value, 'world_id')) {
    apiErr(res, 400, field + '.world_id is managed by Core and cannot be supplied');
    return false;
  }
  return true;
}
function validateCorrelationId(res, value) {
  if (typeof value !== 'string' || value.trim() === '' || value.length > 200) {
    apiErr(res, 400, 'correlation_id must contain 1..=200 characters');
    return false;
  }
  return true;
}

// ---------------------------------------------------------------- server
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1:' + PORT);
  const p = url.pathname;
  const method = req.method;
  let body = '';
  req.on('data', c => { body += c; });
  req.on('end', () => {
    // ---- delayed / out-of-order responses (async race testing) ----
    if (DELAY.queue.length) {
      const idx = DELAY.queue.findIndex(d => d.path === p && (d.method === method || d.method === '*'));
      if (idx >= 0) {
        const d = DELAY.queue.splice(idx, 1)[0];
        setTimeout(() => handleRequest(req, res, body, p, method, url), d.ms);
        return;
      }
    }
    handleRequest(req, res, body, p, method, url);
  });
});

function handleRequest(req, res, body, p, method, url) {
  {
    log({ method, path: p, query: url.search, body: body.slice(0, 2000), origin: req.headers['origin'] || null });

    // ---- delayed / out-of-order responses (async race testing) ----
    if (DELAY.queue.length) {
      const idx = DELAY.queue.findIndex(d => d.path === p && (d.method === method || d.method === '*'));
      if (idx >= 0) {
        const d = DELAY.queue.splice(idx, 1)[0];
        // re-dispatch this request after the delay (the body is already read)
        setTimeout(() => req.emit('end'), d.ms);
        return;
      }
    }

    // ---- test control (not a Bridge route) ----
    if (p === '/__control') {
      let ctl = {};
      try { ctl = JSON.parse(body || '{}'); } catch (e) {}
      if (ctl.reset) { requestLog.length = 0; failNext = null; failAll = false; }
      if (ctl.fail) { const f = ctl.fail; failNext = { method: f.method || '*', path: f.path || '*', status: f.status || 500, times: f.times || 1 }; }
      if (ctl.failAll !== undefined) failAll = !!ctl.failAll;
      if (ctl.push) pushWs(ctl.push);
      if (ctl.pushSemantic) { SEMANTIC_SEQ++; pushWs({ type: 'semantic_event', event: { seq: SEMANTIC_SEQ, event_type: 'SemanticObjectCreated', timestamp: now(), payload: { semantic_operation: { kind: 'create_object' } } } }); }
      if (ctl.pushSemanticN) {
        for (let i = 0; i < ctl.pushSemanticN; i++) { SEMANTIC_SEQ++; pushWs({ type: 'semantic_event', event: { seq: SEMANTIC_SEQ, event_type: 'SemanticObjectCreated', timestamp: now(), payload: { semantic_operation: { kind: 'create_object' } } } }); }
      }
      if (ctl.pushAgent) pushWs({ type: 'agent_update', request_id: ctl.pushAgent.request_id || 'req-1', update: ctl.pushAgent.update || { status: 'running' } });
      if (ctl.pushAgentN) {
        const reqId = (ctl.pushAgent && ctl.pushAgent.request_id) || 'req-1';
        for (let i = 0; i < ctl.pushAgentN; i++) pushWs({ type: 'agent_update', request_id: reqId, update: { status: 'running', n: i } });
      }
      if (ctl.pushRaw) {
        // a deliberately invalid (non-JSON) frame
        wsClients.forEach(s => { try { s.write(wsFrame(String(ctl.pushRaw))); } catch (e) {} });
      }
      if (ctl.pushUnknown) {
        wsClients.forEach(s => { try { s.write(wsFrame(JSON.stringify({ type: ctl.pushUnknown, payload: { x: 1 } }))); } catch (e) {} });
      }
      if (ctl.pushDuplicate) {
        const m = { type: 'semantic_event', event: { seq: SEMANTIC_SEQ, event_type: 'SemanticObjectCreated', timestamp: now(), payload: { semantic_operation: { kind: 'create_object' } } } };
        for (let i = 0; i < (ctl.pushDuplicate || 2); i++) wsClients.forEach(s => { try { s.write(wsFrame(JSON.stringify(m))); } catch (e) {} });
      }
      if (ctl.dropSockets) {
        // abrupt connection interruption
        const n = wsClients.size;
        wsClients.forEach(s => { try { s.destroy(); } catch (e) {} });
        wsClients.clear();
        log({ method: 'WS', path: '/__control#dropSockets', dropped: n });
      }
      if (ctl.resetSemantic) SEMANTIC_SEQ = 100;
      if (ctl.delay) { const d = ctl.delay; DELAY.queue.push({ path: d.path, method: d.method || '*', ms: d.ms || 0 }); }
      if (ctl.setFixture) {
        const objs = (ctl.setFixture.objects === undefined) ? 60 : ctl.setFixture.objects;
        const rels = (ctl.setFixture.relations === undefined) ? 120 : ctl.setFixture.relations;
        FIXTURE = buildFixture(objs, rels, ctl.setFixture.seed || 20261020);
      }
      if (ctl.loadFixture) {
        // Load a deterministic large-scale fixture from disk.
        const f = path.join(__dirname, '..', 'fixtures', 'fixture-' + String(ctl.loadFixture).replace(/[^A-Za-z0-9_-]/g, '') + '.json');
        if (!fs.existsSync(f)) return apiErr(res, 404, 'unknown fixture: ' + ctl.loadFixture);
        FIXTURE = JSON.parse(fs.readFileSync(f, 'utf8'));
        // Core shape: the read routes expose objects/relations; actions are a
        // projection of the event log.
        FIXTURE.actions = (FIXTURE.events || []).map((e, i) => ({
          action_id: e.correlation_id, correlation_id: e.correlation_id, status: 'Completed',
          actor: e.actor, provider_id: e.provider_id, operation: 'op', resource: 'x',
          message: null, first_seen: e.timestamp, last_seen: e.timestamp, event_count: 1
        }));
      }
      return json(res, 200, { ok: true, requests: requestLog.length, semanticSeq: SEMANTIC_SEQ, semanticWorldId: SEMANTIC_WORLD_ID, ownOrigin: OWN_ORIGIN });
    }
    if (p === '/__log') return json(res, 200, { requests: requestLog, createdObjects, createdRelations });
    if (p === '/world.html') {
      const file = process.env.S3B1_WORLD ? path.resolve(process.env.S3B1_WORLD)
        : path.join(__dirname, '..', 'workspace', 'world.html');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(fs.readFileSync(file));
    }
    if (p === '/favicon.ico') { res.writeHead(204); return res.end(); }

    // ---- failure injection ----
    // NOTE: the request is already recorded by the general log() above, so the
    // injection must NOT log it a second time — a double entry would look
    // like the client submitted one write twice.
    if (failAll) { injections.push({ t: Date.now(), method, path: p, kind: 'failAll', status: 500 }); return apiErr(res, 500, 'injected failure (failAll)'); }
    if (failNext && (failNext.method === method || failNext.method === '*') && (failNext.path === p || failNext.path === '*')) {
      failNext.times--;
      const status = failNext.status;
      if (failNext.times <= 0) failNext = null;
      injections.push({ t: Date.now(), method, path: p, kind: 'fail', status });
      return apiErr(res, status, 'injected failure');
    }

    // ---- ensure_agent_origin (Agent Gateway + semantic routes) ----
    const origin = req.headers['origin'];
    if (origin !== undefined && origin !== OWN_ORIGIN) {
      return apiErr(res, 403, 'cross-origin access to Agent Gateway is not allowed');
    }

    // ============================================================
    // READ ROUTES — exact response shapes from handlers.rs
    // ============================================================
    if (p === '/api/objects/query' && method === 'POST') {
      return json(res, 200, { objects: FIXTURE.objects, count: FIXTURE.objects.length, timestamp: now() });
    }
    if (p === '/api/relations/query' && method === 'POST') {
      return json(res, 200, { relations: FIXTURE.relations, count: FIXTURE.relations.length, timestamp: now() });
    }
    if (p === '/api/events' && method === 'GET') {
      const limit = Math.min(parseInt(url.searchParams.get('limit') || '50', 10) || 50, 200);
      const events = FIXTURE.events.slice(0, limit);
      return json(res, 200, { events, count: events.length, next_seq: events.length ? events[events.length - 1].seq : null, timestamp: now() });
    }
    if (p === '/api/actions/recent' && method === 'GET') {
      const limit = Math.min(parseInt(url.searchParams.get('limit') || '50', 10) || 50, 200);
      const rows = FIXTURE.actions.slice(0, limit);
      return json(res, 200, { actions: rows, count: rows.length, timestamp: now() });
    }
    const act = p.match(/^\/api\/actions\/(.+)$/);
    if (act && method === 'GET') {
      const id = decodeURIComponent(act[1]);
      const evs = FIXTURE.events.filter(e => e.correlation_id === id);
      return json(res, 200, { action_id: id, count: evs.length, status: evs.length ? 'Completed' : 'Unknown', events: evs, timestamp: now() });
    }
    if (p === '/api/missions' && method === 'GET') {
      return json(res, 200, { missions: FIXTURE.missions, count: FIXTURE.missions.length, timestamp: now() });
    }
    if (p === '/api/components' && method === 'GET') {
      return json(res, 200, { components: { 'space-core': { connected: true }, 'space-bridge': { connected: true } }, timestamp: now() });
    }
    if (p === '/api/providers' && method === 'GET') {
      const providers = [{ id: 'filesystem', capabilities: ['fs.read'] }, { id: 'browser', capabilities: ['browser.read'] }];
      return json(res, 200, { providers, count: providers.length, timestamp: now() });
    }
    if (p === '/api/filesystem/roots' && method === 'GET') {
      return json(res, 200, { result: coreOk({ roots: ['C:\\spaceos-3b1-browser-lab\\workspace'] }), timestamp: now() });
    }
    if (p === '/api/browser/tabs' && method === 'GET') {
      return json(res, 200, { result: coreOk({ detail: { tabs: FIXTURE.tabs } }), timestamp: now() });
    }
    if (p === '/api/context' && method === 'GET') {
      return json(res, 200, {
        context: {
          space_id: url.searchParams.get('space_id') || '',
          objects: FIXTURE.objects, object_count: FIXTURE.objects.length,
          relations: FIXTURE.relations, recent_events: FIXTURE.events,
          semantic_world_id: SEMANTIC_WORLD_ID
        },
        timestamp: now()
      });
    }
    if (p === '/api/health' && method === 'GET') return json(res, 200, { ok: true, status: 'ok' });

    // ---- core_action-shaped routes: {result: <core envelope>, timestamp} ----
    const actionResult = (detail) => ({ result: coreOk(detail), timestamp: now() });
    if (p === '/api/filesystem/read' && method === 'POST') {
      const b = parseJson(body);
      if (!b.path) return apiErr(res, 400, 'path is required');
      return json(res, 200, actionResult({ status: 'Completed', detail: { content: 'mock file content for ' + b.path } }));
    }
    if (p === '/api/filesystem/create' && method === 'POST') {
      const b = parseJson(body);
      if (!b.path) return apiErr(res, 400, 'path is required');
      return json(res, 200, actionResult({ status: 'Completed', detail: { object_id: 'obj-file-1', path: b.path } }));
    }
    if (p === '/api/filesystem/scan' && method === 'POST') {
      const b = parseJson(body);
      if (!b.path) return apiErr(res, 400, 'path is required');
      return json(res, 200, actionResult({ status: 'Completed', detail: { files: 0 } }));
    }
    if (p === '/api/browser/activate' && method === 'POST') {
      const b = parseJson(body);
      if (!b.target_id) return apiErr(res, 400, 'target_id is required');
      return json(res, 200, actionResult({ status: 'Completed', detail: { target_id: b.target_id } }));
    }
    if (p === '/api/browser/close' && method === 'POST') {
      const b = parseJson(body);
      if (!b.target_id) return apiErr(res, 400, 'target_id is required');
      return json(res, 200, actionResult({ status: 'Completed', detail: { target_id: b.target_id } }));
    }
    if (p === '/api/browser/new-tab' && method === 'POST') {
      const b = parseJson(body);
      if (!b.url) return apiErr(res, 400, 'url is required');
      return json(res, 200, actionResult({ status: 'Completed', detail: { target_id: 'tab-new' } }));
    }
    const provAct = p.match(/^\/api\/providers\/([^/]+)\/actions$/);
    if (provAct && method === 'POST') {
      const b = parseJson(body);
      const kind = b.action || 'screenshot';
      if (kind === 'screenshot') return json(res, 200, actionResult({ status: 'Completed', detail: { data: 'mock-frame-bytes' } }));
      if (kind === 'navigate') return json(res, 200, actionResult({ status: 'Completed', detail: { navigated: (b.target && b.target.url) || '' } }));
      if (kind === 'reload') return json(res, 200, actionResult({ status: 'Completed', detail: { reloaded: true } }));
      return apiErr(res, 400, 'unknown provider action: ' + kind);
    }

    // ---- Agent Gateway ----
    if (p === '/api/agent/sessions' && method === 'GET') {
      return json(res, 200, { sessions: [{ id: 'sess-1', title: 'Mock Session', status: 'idle', location: { directory: 'C:\\mock' } }], count: 1 });
    }
    if (p === '/api/agent/send' && method === 'POST') {
      const b = parseJson(body);
      if (!b.message) return apiErr(res, 400, 'message is required');
      return json(res, 200, { request_id: 'req-1' });
    }
    const agentStatus = p.match(/^\/api\/agent\/status\/(.+)$/);
    if (agentStatus && method === 'GET') {
      return json(res, 200, { update: { status: 'completed', response: 'mock response' } });
    }

    // ============================================================
    // SEMANTIC ROUTES — exact Bridge semantics
    // ============================================================
    if (p === '/api/semantic/objects' && method === 'POST') {
      const payload = parseJson(body);
      if (!validateDto(res, 'semantic object', payload, ['world_id', 'object_type', 'properties', 'correlation_id'])) return;
      if (!ensureSemanticWorld(res, payload.world_id)) return;
      if (!validateProperties(res, payload.properties, 'properties')) return;
      if (!validateCorrelationId(res, payload.correlation_id)) return;
      const obj = {
        id: 'sem-' + (nextObjId++), type: payload.object_type, provider_id: 'space-world-user',
        external_id: null, created_at: now(), updated_at: now(), properties: Object.assign({}, payload.properties)
      };
      FIXTURE.objects.push(obj);
      createdObjects.push(obj);
      SEMANTIC_SEQ++;
      pushWs({ type: 'semantic_event', event: { seq: SEMANTIC_SEQ, event_type: 'SemanticObjectCreated', timestamp: now(), payload: { semantic_operation: { kind: 'create_object' } } } });
      // semantic_core_response: pass the Core envelope through
      return json(res, 200, coreOk({ object: obj, event: { seq: SEMANTIC_SEQ, event_type: 'SemanticObjectCreated' } }));
    }
    if (p.startsWith('/api/semantic/objects/') && method === 'PATCH') {
      const objectId = decodeURIComponent(p.slice('/api/semantic/objects/'.length));
      const payload = parseJson(body);
      if (!validateDto(res, 'semantic patch', payload, ['object_id', 'world_id', 'properties_patch', 'correlation_id'])) return;
      if (payload.object_id !== objectId) return apiErr(res, 400, 'object_id must match the URL path');
      if (!ensureSemanticWorld(res, payload.world_id)) return;
      if (!validateProperties(res, payload.properties_patch, 'properties_patch')) return;
      if (!validateCorrelationId(res, payload.correlation_id)) return;
      const obj = FIXTURE.objects.find(o => o.id === objectId);
      if (!obj) return apiErr(res, 404, 'semantic object not found: ' + objectId);
      if (obj.provider_id !== 'space-world-user') return apiErr(res, 403, 'denied: object is not semantic-owned');
      Object.assign(obj.properties, payload.properties_patch);
      obj.updated_at = now();
      SEMANTIC_SEQ++;
      pushWs({ type: 'semantic_event', event: { seq: SEMANTIC_SEQ, event_type: 'SemanticObjectPatched', timestamp: now(), payload: { semantic_operation: { kind: 'patch_object' } } } });
      return json(res, 200, coreOk({ object: obj, event: { seq: SEMANTIC_SEQ, event_type: 'SemanticObjectPatched' } }));
    }
    if (p === '/api/semantic/relations' && method === 'POST') {
      const payload = parseJson(body);
      if (!validateDto(res, 'semantic relation', payload, ['world_id', 'source_id', 'target_id', 'relation_type', 'properties', 'correlation_id'])) return;
      if (!ensureSemanticWorld(res, payload.world_id)) return;
      if (!validateProperties(res, payload.properties, 'properties')) return;
      if (!validateCorrelationId(res, payload.correlation_id)) return;
      if (!payload.source_id || !payload.target_id) return apiErr(res, 400, 'invalid semantic relation: source_id and target_id are required');
      if (payload.source_id === payload.target_id) return apiErr(res, 400, 'invalid semantic relation: self relation is not allowed');
      const rel = {
        id: 'semrel-' + (nextRelId++), relation_type: payload.relation_type,
        source_object_id: payload.source_id, target_object_id: payload.target_id,
        created_at: now(), properties: Object.assign({}, payload.properties)
      };
      FIXTURE.relations.push(rel);
      createdRelations.push(rel);
      SEMANTIC_SEQ++;
      pushWs({ type: 'semantic_event', event: { seq: SEMANTIC_SEQ, event_type: 'SemanticRelationCreated', timestamp: now(), payload: { semantic_operation: { kind: 'create_relation' } } } });
      return json(res, 200, coreOk({ relation: rel, event: { seq: SEMANTIC_SEQ, event_type: 'SemanticRelationCreated' } }));
    }

    return apiErr(res, 404, 'unmocked endpoint ' + p);
  }
}

function parseJson(body) {
  try { return JSON.parse(body || '{}'); } catch (e) { return {}; }
}

// ---- WebSocket upgrade ----
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  wsClients.add(socket);
  log({ method: 'WS', path: req.url });
  socket.on('close', () => wsClients.delete(socket));
  socket.on('error', () => wsClients.delete(socket));
  socket.on('data', () => {});
  // The real Bridge sends an "init" frame first (components + timestamp).
  setTimeout(() => wsSend(socket, {
    type: 'init',
    components: { 'space-core': { connected: true }, 'space-bridge': { connected: true } },
    timestamp: now()
  }), 10);
});

server.listen(PORT, HOST, () => {
  console.log('[contract-mock] listening on ' + HOST + ':' + PORT);
  console.log('[contract-mock] own origin (allowlisted) : ' + OWN_ORIGIN);
  console.log('[contract-mock] semantic world id      : ' + SEMANTIC_WORLD_ID);
});
process.on('SIGTERM', () => { server.close(); process.exit(0); });
process.on('SIGINT', () => { server.close(); process.exit(0); });
// A socket-level failure must never take the mock down: the test would then
// mistake a harness crash for a product defect.
process.on('uncaughtException', (e) => { /* swallow: socket teardown races */ });
