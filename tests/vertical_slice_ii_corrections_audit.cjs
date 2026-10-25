const {selectedRun,readEvidence,writeEvidence}=require('./evidence_paths.cjs');
// Actual isolated HTTP/WS/SQLite audit. Browser evidence was captured through CUA.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),net=require('net'),assert=require('assert/strict'),{DatabaseSync}=require('node:sqlite');
const dir=selectedRun(),e=readEvidence(dir,'vertical-slice-ii-corrections-runtime.json'),u=readEvidence(dir,'vertical-slice-ii-corrections-browser-ui.json');
assert(fs.realpathSync(e.run).startsWith(fs.realpathSync('C:/spaceos/test-tmp')+path.sep));
const c=JSON.parse(fs.readFileSync(path.join(e.run,'fixture.secret.json'))),origin='http://127.0.0.1:21973',out={at:new Date().toISOString(),run:e.run,world:e.world,checks:[],wsEvents:[]};
function check(name,v){assert(v,name);out.checks.push(name)}
function handshake(cookie,otherOrigin){return new Promise((resolve,reject)=>{const s=net.connect({host:'127.0.0.1',port:21973});let b='';s.setTimeout(4000);s.on('error',reject);s.on('timeout',()=>{s.destroy();reject(Error('isolated handshake timeout'))});s.on('connect',()=>s.write('GET /ws HTTP/1.1\r\nHost: 127.0.0.1:21973\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: '+crypto.randomBytes(16).toString('base64')+'\r\nOrigin: '+otherOrigin+'\r\n'+(cookie?'Cookie: '+cookie+'\r\n':'')+'\r\n'));s.on('data',d=>{b+=d;if(b.includes('\r\n')){s.destroy();resolve(Number(b.split(' ')[1]))}})});}
(async()=>{
 const db=new DatabaseSync(c.db,{readOnly:true}),events=db.prepare('SELECT * FROM events ORDER BY seq').all();
 check('synthetic SQLite integrity',db.prepare('PRAGMA integrity_check').get().integrity_check==='ok');
 for(const type of ['FileChanged','BrowserTabCreated']){const rows=e.wsEvents.filter(v=>v.type==='semantic_event'&&v.event.event_type===type);check(type+' committed event over authenticated Bridge WS',rows.length>0&&rows.every(v=>events.some(x=>x.id===v.event.id)));out.wsEvents.push(...rows.map(v=>v.event));}
 check('no duplicate provider success notification on replay',new Set(out.wsEvents.map(v=>v.id)).size===out.wsEvents.length);
 check('FileChanged and Browser replays explicitly no-op',e.fileReplay.ok&&e.fileReplay.result.applied===false&&e.browserReplay.ok&&e.browserReplay.result.applied===false);
 check('forged Browser and cross-World File rejected',e.browserDenied.ok===false&&e.fileDenied.ok===false&&!db.prepare('SELECT id FROM objects WHERE id=?').get('forged-browser'));
 const foreign=e.foreignAdmission.result.event_id;check('foreign event really persisted but never delivered to World A',events.some(x=>x.id===foreign)&&!e.wsEvents.some(v=>v.event?.id===foreign)&&e.foreignQuery.ok===false);
 check('no Mission or model attempt created',db.prepare("SELECT count(*) AS n FROM objects WHERE type IN ('Mission','MissionAttempt')").get().n===0&&e.gateway_results.length===0);
 db.close();
 const login=await fetch(origin+'/api/lab-auth/login',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({proof:c.keys.login,world_id:c.world})});assert(login.ok);const v=await login.json(),cookie=login.headers.get('set-cookie').split(';')[0];
 const headers={Origin:origin,Cookie:cookie,'Content-Type':'application/json','x-spaceos-csrf':v.csrf};
 const objectsReply=await fetch(origin+'/api/objects/query',{method:'POST',headers,body:'{}'}),objects=await objectsReply.json();check('actual scoped HTTP has exactly one canonical BrowserTab',objectsReply.ok&&objects.objects.filter(o=>o.type==='BrowserTab').length===1&&objects.objects.some(o=>o.id===e.ids.browser)&&!objects.objects.some(o=>o.id===e.foreignAdmission.result.object.id));
 const eventReply=await fetch(origin+'/api/events?limit=500',{headers}),eventBody=await eventReply.json();check('HTTP event query cannot expose the committed foreign event',eventReply.ok&&!eventBody.events.some(ev=>ev.id===foreign));
 const overlayReply=await fetch(origin+'/api/browser/tabs',{headers});out.browserOverlayHTTP={status:overlayReply.status,body:await overlayReply.json()};
 out.anonymousWs=await handshake(null,origin);out.foreignOriginWs=await handshake(cookie,'http://foreign.invalid');check('unauthenticated WS denied',out.anonymousWs===401);check('foreign-Origin WS denied with valid session',out.foreignOriginWs===403);
 const html=await(await fetch(origin+'/?corrections=20261024-1')).text();out.servedWorldSha256=crypto.createHash('sha256').update(html).digest('hex');check('served World asset matches updated disk source',out.servedWorldSha256===crypto.createHash('sha256').update(fs.readFileSync('C:/spaceos/space-bridge/static/world.html')).digest('hex'));
 check('browser verified the corrected served handlers',u.asset.correctedHandler&&u.asset.canonicalHelpers);check('real browser search opens Focus and closes dropdown',u.focus.dropdownDisplay==='none'&&u.focus.text.includes('SpaceOS synthetic acceptance page'));check('real browser search has one canonical result',u.search.length===1&&u.search[0].id===e.ids.browser);
 for(const view of ['tree','space'])check(view+' has one canonical Browser node',u[view].filter(x=>x.id===e.ids.browser).length===1);
 check('real browser selection produces no console error',u.consoleErrors.length===0);check('intentional Graph omission preserved',!u.graph.some(x=>x.id===e.ids.browser));
 check('unavailable live overlay does not enable canonical CDP actions',u.focus.sidebar.includes('matching live browser authority unavailable')&&!u.focus.sidebar.includes('Show in Chromium'));
 out.providerActions=e.providerActions;out.browserCanonical=e.browserCreated;out.foreignEventID=foreign;out.status='PASS_WITH_DECLARED_OPERATIONAL_LIMITATIONS';let text=JSON.stringify(out,null,2);for(const secret of [...Object.values(c.keys),cookie,v.csrf])text=text.split(secret).join('[REDACTED]');writeEvidence(dir,'vertical-slice-ii-corrections-audit.json',text);console.log(JSON.stringify({checks:out.checks.length,status:out.status,anonymousWs:out.anonymousWs,foreignOriginWs:out.foreignOriginWs,providerEventIDs:out.wsEvents.map(v=>v.id)}));
})().catch(err=>{console.error(err.message);process.exitCode=1});
