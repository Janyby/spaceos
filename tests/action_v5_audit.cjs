const {selectedRun,readEvidence,writeEvidence}=require('./evidence_paths.cjs');
// Post-run read-only verification of the designated synthetic run; no endpoints.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),assert=require('assert/strict'),{DatabaseSync}=require('node:sqlite');
const root='C:/spaceos',dir=selectedRun(),e=readEvidence(dir,'action-v5-runtime.json'),checks=[];
function check(n,v){assert(v,n);checks.push(n);console.log('PASS '+n)}
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const run=fs.realpathSync(e.run),allowed=fs.realpathSync(path.join(root,'test-tmp'));
check('canonical fixture containment',run.startsWith(allowed+path.sep));
check('runtime assertions and owned cleanup complete',!e.failure&&e.checks.length>=80&&Object.values(e.cleanup).every(Boolean));
const db=new DatabaseSync(e.db,{readOnly:true});
check('SQLite v5 and integrity',db.prepare('PRAGMA user_version').get().user_version===5&&db.prepare('PRAGMA integrity_check').get().integrity_check==='ok');
check('SQLite foreign keys consistent',db.prepare('PRAGMA foreign_key_check').all().length===0);
const rows=db.prepare('SELECT * FROM action_authorities').all();
check('private reservations have unique operation and dispatch identity',new Set(rows.map(r=>r.action_id)).size===rows.length&&new Set(rows.map(r=>r.binding_id)).size===rows.length);
check('all credential-free operation digests match',rows.every(r=>hashBytes(r.operation_json)===r.operation_digest&&!r.operation_json.includes('actor_token')));
function hashBytes(s){return crypto.createHash('sha256').update(s).digest('hex')}
for(const r of rows.filter(r=>['completed','failed'].includes(r.state))){
 const event=db.prepare('SELECT * FROM events WHERE id=?').get(r.terminal_event_id);
 assert(event&&event.actor===r.actor&&event.provider_id===r.provider_id&&event.world_id===r.world_id&&event.correlation_id===r.correlation_id);
 assert(hashBytes(r.terminal_json)===r.terminal_digest);
}
check('terminal receipts match durable attributed events and digest',true);
const successful=db.prepare('SELECT * FROM action_authorities WHERE action_id=?').get(e.actionV5.action_id);
check('actual restarted async scan is Completed',successful.state==='completed'&&successful.world_id===e.world&&JSON.parse(successful.terminal_json).status==='Completed');
check('lost callback response exercised',e.actionV5.lostResponses===1&&e.actionV5.callbacks>=3);
check('same dispatch performer persisted through Core restart',successful.performer===e.actionV5.records.find(r=>r.action_id===successful.action_id).performer);
check('missing job remains reserved, never completed',db.prepare('SELECT state FROM action_authorities WHERE action_id=?').get(e.actionV5.unresolved_action).state==='reserved');
const receipt=db.prepare('SELECT * FROM filesystem_file_admissions WHERE object_id=?').get(e.ids.file);
check('File receipt retained unchanged',JSON.stringify(receipt)===JSON.stringify(e.fileReceipt));
check('one BrowserTabCreated snapshot only',db.prepare("SELECT count(*) n FROM events WHERE event_type='BrowserTabCreated' AND object_ids LIKE ?").get('%'+e.ids.browser+'%').n===1);
const table=db.prepare("SELECT sql FROM sqlite_master WHERE name='action_authorities'").get().sql;db.close();
for(const p of e.processes)assert(hash(p.path)===p.sha256,'loaded binary changed: '+p.path);
check('loaded binary hashes match final files',true);
const previous=JSON.parse(fs.readFileSync(path.join(root,'docs/maintenance','scoped-browser-recovery-inventory.json')));
for(const name of ['space-core/src/policy.rs','space-core/src/service_registration.rs','space-bridge/static/world.html','space-bridge/static/index.html','space-bridge/src/state.rs','space-bridge/src/handlers.rs'])assert(hash(path.join(root,name))===previous.sourceHashes[name]);
check('Policy profiles frontend Bridge UI recovery unchanged',true);
const baseline=JSON.parse(fs.readFileSync(path.join(root,'docs/maintenance','full-slice-ii-inventory.json')));
for(const item of baseline.source.filter(v=>/(^|\\)(mission(_storage|_commands)?\.rs|agent_gateway\.rs|reconciliation_read\.rs|watcher\.rs)$|filesystem-provider\\src\\actions\.rs/.test(v.path)))assert(hash(path.join(root,item.path))===item.sha256,item.path);
check('Mission Gateway filesystem watcher action and read contracts unchanged',true);
const c=JSON.parse(fs.readFileSync(path.join(run,'fixture.secret.json')));
const artifacts=fs.readdirSync(dir).filter(n=>/^action-v5-.*\.(json|log)$/.test(n)&&!n.includes('inventory')&&!n.includes('audit'));
for(const name of artifacts){const body=fs.readFileSync(path.join(dir,name),'utf8');for(const secret of Object.values(c.keys))assert(!body.includes(secret),'known synthetic credential in '+name)}
check('known final-fixture credentials absent from public v5 evidence',true);
const files=['space-core/src/action.rs','space-core/src/dispatch.rs','space-core/src/ipc.rs','space-core/src/browser_actions.rs','space-core/src/storage.rs','space-core/src/action_storage.rs','space-core/src/security_config.rs','space-core/src/provider_observation.rs','space-core/src/storage_file_admission_tests.rs','filesystem-provider/src/ipc.rs','browser-provider/src/ipc.rs','browser-provider/src/provider.rs','space-bridge/src/bin/mvp-core-fixture.rs','space-core/tests/durable_file_provenance.rs','filesystem-provider/tests/verified_file_changed.rs','tests/action_v5_runtime.cjs','tests/action_v5_audit.cjs','tests/scoped_browser_actions.cjs','tests/vertical_slice_ii_corrections_runtime.cjs'];
const inventory={at:new Date().toISOString(),run,world:e.world,checks,runtimeAssertions:e.checks.length,sourceHashes:Object.fromEntries(files.map(f=>[f,hash(path.join(root,f))])),loadedBinaries:e.processes,artifactHashes:Object.fromEntries([...artifacts,'ACTION_V5_PROVIDER_SCREENSHOT.jpg'].map(f=>[f,hash(path.join(dir,f))])),actionTableSQL:table,successfulAction:successful};
writeEvidence(dir,'action-v5-inventory.json',JSON.stringify(inventory,null,2));console.log(JSON.stringify({checks:checks.length,runtimeAssertions:e.checks.length,run,world:e.world,action_id:e.actionV5.action_id,outcome:e.actionV5.outcome}));
