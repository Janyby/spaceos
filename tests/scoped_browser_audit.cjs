const {selectedRun,readEvidence,writeEvidence}=require('./evidence_paths.cjs');
// Independent post-cleanup artifact/persistence audit. Synthetic fixture only.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),assert=require('assert/strict');
const root='C:/spaceos',dir=selectedRun(),e=readEvidence(dir,'scoped-browser-runtime.json');
const ui=readEvidence(dir,'scoped-browser-final-ui-evidence.json'),checks=[];
function check(name,value){assert(value,name);checks.push(name);console.log('PASS '+name)}
check('runtime checks complete',e.checks.length===60&&e.scoped.restartComplete&&!e.failure);
check('all owned processes stopped',Object.values(e.cleanup).every(Boolean));
check('UI canonical identity matches final runtime',ui.canonicalActionIds.length===1&&ui.canonicalActionIds[0]===e.ids.browser);
check('actual World frame is decoded',ui.frame.complete&&ui.frame.hasImage&&ui.frame.width>0);
check('unsupported controls disabled',ui.unsupportedButtons.length===4&&ui.unsupportedButtons.every(b=>b.disabled)&&ui.closeControls===0);
check('no final browser console errors',ui.console.length===0);
check('SQLite integrity',Object.values(e.sqlite.integrity)[0]==='ok');
const canonical=e.sqlite.objects.find(o=>o.id===e.ids.browser);
check('canonical snapshot unchanged after real UI and restart',JSON.stringify(JSON.parse(canonical.properties))===JSON.stringify(e.browserCreated.properties)&&canonical.updated_at.replace(/\+00:00$/,'Z')===e.browserCreated.updated_at);
check('no extra BrowserTabCreated on real operational reads',e.sqlite.events.filter(v=>v.event_type==='BrowserTabCreated'&&JSON.parse(v.object_ids).includes(e.ids.browser)).length===1);
check('all scoped action events exact World',e.sqlite.events.filter(v=>v.actor==='space-world-browser').every(v=>v.world_id===e.world));
check('actual UI screenshot completed after Provider restart',e.sqlite.events.some(v=>v.actor==='space-world-browser'&&v.event_type==='ActionCompleted'&&JSON.parse(v.payload).action==='screenshot'&&JSON.parse(v.payload).performer_instance===e.scoped.restartLive.result.detail.performer_instance));
check('no Mission executed',!e.sqlite.objects.some(o=>['Mission','MissionAttempt'].includes(o.type)));
const files=['space-core/src/action.rs','space-core/src/browser_actions.rs','space-core/src/service_registration.rs','space-core/src/policy.rs','space-core/src/provider_observation.rs','space-core/src/ipc.rs','space-core/src/storage.rs','space-bridge/src/state.rs','space-bridge/src/service_credentials.rs','space-bridge/src/handlers.rs','space-bridge/src/bin/mvp-core-fixture.rs','space-bridge/src/bin/mvp-runtime-fixture.rs','space-bridge/static/world.html','space-bridge/static/index.html','browser-provider/src/ipc.rs','browser-provider/src/provider.rs','tests/scoped_browser_actions.cjs','tests/vertical_slice_ii_corrections_runtime.cjs','tests/vertical_slice_ii_ui.cjs','tests/scoped_browser_audit.cjs'];
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const hashes=Object.fromEntries(files.map(f=>[f,hash(path.join(root,f))]));
for(const p of e.processes)check('loaded binary hash preserved '+p.pid,hash(p.path)===p.sha256);
const config=JSON.parse(fs.readFileSync(path.join(e.run,'fixture.secret.json')));
// Exclude this writer's mutable output and its own manifest (no self-hash).
const publicFiles=fs.readdirSync(dir).filter(n=>/^(scoped-browser-|SCOPED_BROWSER_)/.test(n)&&/\.(json|log)$/.test(n)&&!['scoped-browser-inventory.json','scoped-browser-audit.log'].includes(n));
const needles=Object.values(config.keys).filter(v=>typeof v==='string'&&v.length>8);
check('no known fixture credentials in public artifacts',publicFiles.every(f=>{const text=fs.readFileSync(path.join(dir,f),'utf8');return needles.every(v=>!text.includes(v))}));
const artifactHashes=Object.fromEntries(publicFiles.map(f=>[f,hash(path.join(dir,f))]));
for(const f of fs.readdirSync(dir).filter(n=>/^SCOPED_BROWSER_.*\.(png|jpg)$/.test(n)))artifactHashes[f]=hash(path.join(dir,f));
writeEvidence(dir,'scoped-browser-inventory.json',JSON.stringify({at:new Date().toISOString(),run:e.run,checks,sourceHashes:hashes,loadedBinaries:e.processes,artifactHashes},null,2));
console.log(JSON.stringify({passed:checks.length,run:e.run}));
