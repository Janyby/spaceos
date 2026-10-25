const {snapshotWriter,writeEvidence}=require('./evidence_paths.cjs');
// Actual Core/Bridge/provider/Chrome regression. No model or Mission execution.
// No scripted model/tool substitution. Fresh synthetic data under this repository.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),cp=require('child_process'),net=require('net'),assert=require('assert/strict'),{DatabaseSync}=require('node:sqlite');
const LAB='C:\\spaceos',cli='C:\\Users\\Gamer\\AppData\\Roaming\\ai.opencode.desktop\\cli\\2.1.3-x64\\opencode-cli.exe';
const run=fs.mkdtempSync(path.join(LAB,'test-tmp/vertical-corrections-')),uid=crypto.randomUUID(),key=()=>crypto.randomBytes(32).toString('base64url');
const keys=Object.fromEntries(['transport','system','semantic','reader','mcp','login','dashboard','browser_world','filesystem','browser','instance','browser_instance','browser_instance_b','dispatchfs','dispatchbrowser'].map(k=>[k,key()]));
keys.login='synthetic-corrections-login';
const c={run,keys,port:21973,world:crypto.randomUUID(),other_world:crypto.randomUUID(),pipe:'\\\\.\\pipe\\spaceos-4-3a4-process-mvp2b1-'+uid,fs_pipe:'\\\\.\\pipe\\spaceos-4-3a4-unused-fs-'+uid,browser_pipe:'\\\\.\\pipe\\spaceos-4-3a4-unused-browser-'+uid,db:path.join(run,'core.db')};
const recovery=process.env.SPACEOS_SCOPED_BROWSER_RECOVERY_TEST==='1';
const scoped=process.env.SPACEOS_SCOPED_BROWSER_TEST==='1'||recovery;c.scoped_browser=scoped;
const v5=process.env.SPACEOS_ACTION_V5_TEST==='1';
const config=path.join(run,'fixture.secret.json'),output=path.join(run,v5?'action-v5-runtime.json':recovery?'scoped-browser-recovery-runtime.json':scoped?'scoped-browser-runtime.json':'vertical-slice-ii-corrections-runtime.json'),secrets=Object.values(keys),actor='canonical-mvp-agent';
const e={started_at:new Date().toISOString(),run,world:c.world,db:c.db,checks:[],setup:[],gateway_results:[],snapshots:[],model:'unsloth/qwen-q8'},sha=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');let core,bridge,agentToken,home,fsProvider,browserProvider,chrome,pageServer,wsSocket;
const checkpoint=snapshotWriter(run,path.basename(output));
const save=()=>checkpoint(e,text=>{for(const s of secrets)if(s)text=text.split(s).join('[REDACTED]');return text});
const check=(n,v)=>{assert(v,n);e.checks.push(n);save();console.log('PASS '+n)},sleep=ms=>new Promise(r=>setTimeout(r,ms));
function api(method,url,body,cwd=LAB){const args=['api',method,url];if(body)args.push('--data',JSON.stringify(body));const r=cp.spawnSync(cli,args,{cwd,windowsHide:true,encoding:'utf8',timeout:20000,maxBuffer:8*2**20});assert(r.status===0,'supported OpenCode API failed; raw output suppressed');return r.stdout.trim()?JSON.parse(r.stdout):null}
function ipc(method,params,record=true){return new Promise((resolve,reject)=>{const s=net.connect(c.pipe);let b='';s.setTimeout(6000);s.on('connect',()=>s.write(JSON.stringify({method,params})+'\n'));s.on('error',reject);s.on('timeout',()=>{s.destroy();reject(Error('isolated Core timeout'))});s.on('data',d=>{b+=d;if(!b.includes('\n'))return;s.destroy();try{const v=JSON.parse(b.split('\n')[0]);if(v.result?.token)secrets.push(v.result.token);if(record){e.setup.push({method,params,response:v});save()}resolve(v)}catch(err){reject(err)}})})}
function start(name,args,label){const exe=path.join(LAB,'target/debug',name+'.exe');const env={...process.env,SPACEOS_TEST_ISOLATION_ROOT:LAB,SPACEOS_CORE_TOKEN:keys.transport,SPACEOS_CORE_PIPE:c.pipe,SPACEOS_BRIDGE_CORE_PIPE:c.pipe,SPACEOS_FILESYSTEM_PROVIDER_PIPE:c.fs_pipe,SPACEOS_BROWSER_PROVIDER_PIPE:c.browser_pipe,SPACEOS_DISPATCH_KEY_FILESYSTEM:keys.dispatchfs,SPACEOS_DISPATCH_KEY_BROWSER:keys.dispatchbrowser,MVP2B1_CONFIG:config,MVP2_CONFIG:config,SPACEOS_EXISTING_OPENCODE:'1',SPACEOS_EXISTING_OPENCODE_PID:'not-used-no-model'};if(label.startsWith('filesystem'))Object.assign(env,{SPACEOS_PROVIDER_PRINCIPAL_TOKEN:keys.filesystem,SPACEOS_PROVIDER_INSTANCE_ID:'vertical-filesystem',SPACEOS_PROVIDER_INSTANCE_TOKEN:keys.instance,SPACEOS_ALLOWED_ROOTS:c.providers.root,SPACEOS_ROOT:c.providers.root,SPACEOS_FIXTURE_DISABLE_PROCESS_OBSERVER:'1'});if(label.startsWith('browser'))Object.assign(env,{SPACEOS_PROVIDER_PRINCIPAL_TOKEN:keys.browser,SPACEOS_BROWSER_CDP_BASE:'http://127.0.0.1:21975',SPACEOS_POLL_SECS:'1',SPACEOS_PROVIDER_INSTANCE_ID:'vertical-browser',SPACEOS_PROVIDER_INSTANCE_TOKEN:keys.browser_instance,SPACEOS_BROWSER_OBSERVATION_SOURCE:JSON.stringify(c.providers.browser_source)});if(v5){if(label.startsWith('filesystem'))Object.assign(env,{SPACEOS_CORE_PIPE:c.callback_pipe,SPACEOS_FILESYSTEM_PROVIDER_PIPE:c.fs_actual_pipe});if(label.startsWith('browser'))env.SPACEOS_BROWSER_PROVIDER_PIPE=c.browser_actual_pipe;}const p=cp.spawn(exe,args,{cwd:LAB,env,windowsHide:true,stdio:['ignore',fs.openSync(path.join(run,label+'.out'),'wx'),fs.openSync(path.join(run,label+'.err'),'wx')]});e.processes??=[];e.processes.push({pid:p.pid,path:exe,sha256:sha(exe)});return p}
async function stop(p){if(p&&p.exitCode===null&&p.signalCode===null){p.kill();await Promise.race([new Promise(r=>p.once('exit',r)),sleep(3000)])}}
async function snapshot(label){const p={actor,actor_token:agentToken,world_id:c.world};const objects=await ipc('query_objects',p,false),relations=await ipc('query_relations',p,false),events=await ipc('query_events',{...p,limit:500},false);assert(objects.ok&&relations.ok&&events.ok,'authorized snapshot denied');const r={label,at:new Date().toISOString(),objects:objects.result,relations:relations.result,events:events.result};e.current=r;save();return r}
async function readyCore(){let ok=false;for(let i=0;i<80;i++){try{if((await ipc('ping',{},false)).ok){ok=true;break}}catch{}await sleep(100)}check('canonical isolated Core ready',ok)}
async function registerAgent(){const r=await ipc('register_agent',{principal:'system',principal_token:keys.system,name:actor,capabilities:['semantic.read','semantic.object.patch'],worlds:[c.world]});check('agent exact World and minimal read/patch grants',r.ok);agentToken=r.result.token}
async function readyBridge(){let ok=false;for(let i=0;i<80;i++){try{if((await fetch('http://127.0.0.1:21973/api/health')).ok){ok=true;break}}catch{}await sleep(100)}check('canonical Bridge ready at dedicated origin',ok)}

async function waitFor(n,fn){for(let i=0;i<100;i++){const v=await fn();if(v)return v;await sleep(200)}throw Error(n+' unavailable')}
async function actionInspection(){
 const reg=await ipc('register_agent',{principal:'system',principal_token:keys.system,name:'vertical-provider-inspector',capabilities:['semantic.read','action.stat','action.get_tabs'],worlds:[c.world]},false);assert(reg.ok);secrets.push(reg.result.token);
 e.providerActions=[];
 for(const [provider_id,action,target]of [['filesystem','stat',{path:c.document}],['browser','get_tabs',{}]]){
 const r=await ipc('request_action',{id:crypto.randomUUID(),actor:'vertical-provider-inspector',actor_token:reg.result.token,provider_id,action,target},false);
 e.providerActions.push({provider_id,action,response:r});assert(r.ok&&r.result.status==='Completed',provider_id+' signed action failed');
 }save();
}
async function connectEvidenceWs(){
 const login=await fetch('http://127.0.0.1:21973/api/lab-auth/login',{method:'POST',headers:{Origin:'http://127.0.0.1:21973','Content-Type':'application/json'},body:JSON.stringify({proof:keys.login,world_id:c.world})});
 assert(login.ok,'synthetic collector login failed');const cookie=login.headers.get('set-cookie').split(';')[0];secrets.push(cookie);
 const socket=net.connect({host:'127.0.0.1',port:21973});wsSocket=socket;e.wsEvents=[];let buffer=Buffer.alloc(0),up=false;
 socket.on('error',err=>{e.wsError=err.code;save()});socket.on('connect',()=>socket.write('GET /ws HTTP/1.1\r\nHost: 127.0.0.1:21973\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: '+crypto.randomBytes(16).toString('base64')+'\r\nOrigin: http://127.0.0.1:21973\r\nCookie: '+cookie+'\r\n\r\n'));
 socket.on('data',d=>{buffer=Buffer.concat([buffer,d]);if(!up){const i=buffer.indexOf('\r\n\r\n');if(i<0)return;assert(buffer.subarray(0,i).toString().includes('101 Switching Protocols'),'WS upgrade rejected');up=true;buffer=buffer.subarray(i+4);e.wsConnected=true;save()}
 while(buffer.length>=2){const op=buffer[0]&15;let len=buffer[1]&127,offset=2;if(len===126){if(buffer.length<4)return;len=buffer.readUInt16BE(2);offset=4}else if(len===127){if(buffer.length<10)return;len=Number(buffer.readBigUInt64BE(2));offset=10}if(buffer.length<offset+len)return;const data=buffer.subarray(offset,offset+len);buffer=buffer.subarray(offset+len);
 if(op===1){try{const v=JSON.parse(data.toString());e.wsEvents.push(v);save()}catch{}}if(op===9){const mask=crypto.randomBytes(4),body=Buffer.from(data);for(let i=0;i<body.length;i++)body[i]^=mask[i%4];socket.write(Buffer.concat([Buffer.from([0x8a,0x80|data.length]),mask,body]))}
 }});
 await waitFor('authorized event collector',async()=>e.wsConnected);
}
async function prepareBrowserRuntime(){
 for(const port of [21974,21975])await new Promise((resolve,reject)=>{const probe=net.createServer();probe.once('error',reject);probe.listen(port,'127.0.0.1',()=>probe.close(resolve))});
 const http=require('http');await new Promise((resolve,reject)=>{pageServer=http.createServer((req,res)=>{res.setHeader('Content-Type','text/html');res.end('<!doctype html><title>SpaceOS synthetic acceptance page</title><h1>SpaceOS vertical slice synthetic document</h1><p>No personal data. Verification page.</p>')});pageServer.once('error',reject);pageServer.listen(21974,'127.0.0.1',resolve)});
 const exe='C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',args=['--headless=new','--remote-debugging-address=127.0.0.1','--remote-debugging-port=21975','--user-data-dir='+path.join(run,'chrome-profile'),'--no-first-run','--no-default-browser-check','--disable-extensions','http://127.0.0.1:21974/evidence.html'];
 chrome=cp.spawn(exe,args,{windowsHide:true,stdio:['ignore',fs.openSync(path.join(run,'chrome.out'),'wx'),fs.openSync(path.join(run,'chrome.err'),'wx')]});
 e.browser={pid:chrome.pid,path:exe,args,real:true,headless:true};save();
 await waitFor('dedicated real Chrome',async()=>{try{const r=await fetch('http://127.0.0.1:21975/json/version');if(r.ok){e.browser.version=await r.json();return true}}catch{}});
 c.providers.browser_source={source_id:crypto.randomUUID(),world_id:c.world,cdp_origin:'http://127.0.0.1:21975',runtime_epoch:crypto.randomUUID(),expected_browser_debugger_id:e.browser.version.webSocketDebuggerUrl.split('/devtools/browser/')[1]};
 assert(c.providers.browser_source.expected_browser_debugger_id,'missing actual browser guard');
 c.providers.foreign_browser_source={...c.providers.browser_source,source_id:crypto.randomUUID(),world_id:c.other_world,cdp_origin:'http://127.0.0.1:21976'};fs.writeFileSync(config,JSON.stringify(c));
}
async function startProviders(){
 fsProvider=start('filesystem-provider',[],'filesystem');browserProvider=start('browser-provider',[],'browser');
 await waitFor('Filesystem instance',async()=>{try{return (await new Promise((resolve,reject)=>{const s=net.connect(c.fs_pipe);let b='';s.on('error',reject);s.on('connect',()=>s.write('{"method":"ping","params":{}}\n'));s.on('data',d=>{b+=d;if(b.includes('\n')){s.destroy();resolve(JSON.parse(b.split('\n')[0]))}})})).ok}catch{}});
 await sleep(1000);fs.writeFileSync(c.document,'Synthetic SpaceOS acceptance document.\nReview the linked evidence before completing the synthetic task.\n');
 const file=await waitFor('actual FileCreated',async()=>{const snap=await snapshot('await_file');return snap.objects.find(o=>o.type==='File'&&o.external_id===c.document)});
 e.ids.file=file.id;e.fileCreated=file;check('actual filesystem File object has exact World/provider',file.provider_id==='filesystem'&&file.properties.world_id===c.world);
 await sleep(1000);await actionInspection();
 e.beforeFileChange=await snapshot('before_disk_change');fs.appendFileSync(c.document,'A harmless synthetic revision.\\n');
 await waitFor('verified FileChanged',async()=>{const snap=await snapshot('await_changed');const current=snap.objects.find(o=>o.id===file.id),base=e.beforeFileChange.objects.find(o=>o.id===file.id);return current&&current.updated_at>base.updated_at&&current.properties.meta.size===fs.statSync(c.document).size&&snap.events.some(v=>v.event_type==='FileChanged'&&v.object_ids.includes(file.id)&&v.seq>Math.max(...e.beforeFileChange.events.map(x=>x.seq)))});
 e.afterFileChange=await snapshot('after_disk_change');
 const changed=e.afterFileChange.objects.find(o=>o.id===file.id);
 check('FileChanged stable identity and Core revision',changed.external_id===file.external_id&&changed.created_at===file.created_at&&changed.updated_at>file.updated_at);
 const db=new DatabaseSync(c.db,{readOnly:true});e.fileReceipt=db.prepare('SELECT * FROM filesystem_file_admissions WHERE object_id=?').get(file.id);db.close();check('durable File admission receipt retained',!!e.fileReceipt);
 const tab=await waitFor('actual verified BrowserTabCreated',async()=>{const snap=await snapshot('await_browser');return snap.objects.find(o=>o.type==='BrowserTab'&&o.properties.source_id===c.providers.browser_source.source_id&&o.properties.target_id!=='forged-history-target')});
 e.ids.browser=tab.id;e.browserCreated=tab;check('BrowserTab canonical ID and exact World',tab.id.startsWith('browser-tab-v1-')&&tab.provider_id==='browser'&&tab.properties.world_id===c.world);
 e.browser.targets=await(await fetch('http://127.0.0.1:21975/json/list')).json();save();
}

function browserObservation(instance,r,target){
 const parts=['spaceos/browser-tab/v1','browser',instance,r.source_id,r.runtime_epoch,target],h=crypto.createHash('sha256');
 for(const part of parts){const raw=Buffer.from(part),len=Buffer.alloc(4);len.writeUInt32BE(raw.length);h.update(len).update(raw)}const d=h.digest('hex'),id='browser-tab-v1-'+d,eventId='browser-tab-created-event-v1:'+d,now=new Date().toISOString();
 const props={instance_id:instance,source_id:r.source_id,runtime_epoch:r.runtime_epoch,target_id:target,target_type:'page',url:'about:blank',title:'Synthetic replay',world_id:r.world_id};
 return {observation_id:'browser-tab-created-v1:'+d,world_id:r.world_id,objects:[{id,type:'BrowserTab',provider_id:'browser',external_id:'browser-tab-v1:'+d,properties:props,created_at:now,updated_at:now}],relations:[],facts:[],delete_ids:[],
 event:{id:eventId,event_type:'BrowserTabCreated',timestamp:now,actor:'browser',provider_id:'browser',world_id:r.world_id,object_ids:[id],correlation_id:eventId,payload:{...props,tab_id:id},seq:0}};
}
async function observationChecks(){
 await waitFor('both new provider event types over actual authenticated Bridge WS',async()=>['FileChanged','BrowserTabCreated'].every(k=>e.wsEvents.some(v=>v.type==='semantic_event'&&v.event.event_type===k)));check('actual Bridge WS carries both provider event types',true);
 const report=(actor,instance,token,obs)=>ipc('report_observation',{...obs,actor,principal_token:keys[actor],instance_id:instance,instance_token:token},false);
 const count=()=>{const db=new DatabaseSync(c.db,{readOnly:true});const counts=['objects','events','observation_log'].map(t=>db.prepare('SELECT count(*) AS n FROM '+t).get().n);db.close();return counts;};
 await waitFor('prior relation delivery drained',async()=>e.wsEvents.some(v=>v.event?.id===e.baseline.events.at(-1).id));
 const original=browserObservation('vertical-browser',c.providers.browser_source,e.browserCreated.properties.target_id);
 e.browserReplay=await report('browser','vertical-browser',keys.browser_instance,original);assert(e.browserReplay.ok&&e.browserReplay.result.applied===false);
 const snap=await snapshot('before_rejection_replay'),file=snap.objects.find(o=>o.id===e.ids.file),evt=[...snap.events].reverse().find(v=>v.event_type==='FileChanged'&&v.object_ids.includes(file.id));
 const db=new DatabaseSync(c.db,{readOnly:true}),log=db.prepare('SELECT observation_id FROM observation_log WHERE event_id=?').get(evt.id);db.close();
 const changed={observation_id:log.observation_id,world_id:c.world,objects:[file],event:evt,relations:[],facts:evt.payload.facts||[],delete_ids:[]};
 const before=count(),beforeWs=e.wsEvents.length;
 e.fileReplay=await report('filesystem','vertical-filesystem',keys.instance,changed);assert(e.fileReplay.ok&&e.fileReplay.result.applied===false,'FileChanged replay must be recognized');
 const forged=structuredClone(original);forged.objects[0].id='forged-browser';e.browserDenied=await report('browser','vertical-browser',keys.browser_instance,forged);assert(e.browserDenied.ok===false);
 const badFile=structuredClone(changed);badFile.world_id=c.other_world;e.fileDenied=await report('filesystem','vertical-filesystem',keys.instance,badFile);assert(e.fileDenied.ok===false);
 await sleep(1700);assert.deepEqual(count(),before);assert.equal(e.wsEvents.length,beforeWs);check('rejected and replayed observations produce no writes or success notifications',true);
 const r=c.providers.foreign_browser_source;
 const registration=await ipc('register_provider_instance',{actor:'browser',principal_token:keys.browser,instance_id:'corrections-browser-B',instance_token:keys.browser_instance_b,source_id:r.source_id,runtime_epoch:r.runtime_epoch,browser_debugger_id:r.expected_browser_debugger_id},false);assert(registration.ok,'foreign instance registration');
 const foreign=browserObservation('corrections-browser-B',r,'synthetic-other-world');e.foreignAdmission=await report('browser','corrections-browser-B',keys.browser_instance_b,foreign);assert(e.foreignAdmission.ok&&e.foreignAdmission.result.applied);
 await sleep(1700);assert(!e.wsEvents.some(v=>v.event?.id===foreign.event.id));const scoped=await snapshot('after_foreign_event');assert(!scoped.objects.some(v=>v.id===foreign.objects[0].id)&&!scoped.events.some(v=>v.id===foreign.event.id));
 e.foreignQuery=await ipc('query_events',{actor,actor_token:agentToken,world_id:c.other_world},false);assert(e.foreignQuery.ok===false);check('committed foreign-World event absent from authorized World WS and queries',true);e.wsChecksComplete=true;save();
}

(async()=>{try{
 assert(fs.realpathSync(run).startsWith(fs.realpathSync(path.join(LAB,'test-tmp'))+path.sep));
 await new Promise((resolve,reject)=>{const server=net.createServer();server.once('error',reject);server.listen(21973,'127.0.0.1',()=>server.close(resolve))});
 c.providers={root:path.join(run,'project-files')};fs.mkdirSync(c.providers.root);c.document=path.join(c.providers.root,'acceptance.txt');if(v5)await require('./action_v5_runtime.cjs').setup({c,e,check,save});fs.writeFileSync(config,JSON.stringify(c));await prepareBrowserRuntime();core=start('mvp-core-fixture',[],'core');await readyCore();await registerAgent();
 const owner=await ipc('register_service_actor',{profile:'semantic_world',name:'space-world-user',registrar_token:keys.semantic,capabilities:['mission.command','semantic.object.create','semantic.object.patch','semantic.relation.create'],worlds:[c.world]});assert(owner.ok);
 const command=(method,body)=>ipc(method,{actor:'space-world-user',actor_token:owner.result.token,command:{world_id:c.world,correlation_id:crypto.randomUUID(),...body}});
 const seed=JSON.parse(fs.readFileSync(path.join(run,'seeds.json')));e.ids={task:seed.task_a.id,summary:seed.summary_a.id,foreign:seed.foreign_task.id};const task=await command('create_semantic_object',{object_type:'Task',properties:{name:'Review observed project acceptance',status:'unfinished'}});assert(task.ok);e.ids.task=task.result.object.id;const project=await command('create_semantic_object',{object_type:'Project',properties:{name:'Synthetic full-product MVP project'}});assert(project.ok);e.ids.project=project.result.object.id;
 assert((await command('patch_semantic_object',{object_id:e.ids.task,properties_patch:{kind:'Task',title:'Review synthetic integration evidence',status:'unfinished',description:'Follow the related_to edge to the Evidence before deciding completion.'}})).ok);
 assert((await command('patch_semantic_object',{object_id:e.ids.summary,properties_patch:{kind:'ProgressSummary',title:'Canonical MVP synthetic progress',summary:'Not reviewed yet'}})).ok);
 const evidence=await command('create_semantic_object',{object_type:'Note',properties:{kind:'Evidence',title:'Synthetic project acceptance document',result:'PASS',target_unchanged:true,description:'Synthetic business input for this isolated Mission, not a claim of production readiness.'}});assert(evidence.ok);e.ids.evidence=evidence.result.object.id;
 const relation=await command('create_semantic_relation',{source_id:e.ids.task,target_id:e.ids.evidence,relation_type:'related_to',properties:{purpose:'Synthetic acceptance dependency'}});assert(relation.ok);e.ids.relation=relation.result.relation.id;
 bridge=start('mvp-runtime-fixture',['bridge'],'bridge');await readyBridge();await connectEvidenceWs();await startProviders();const projectRelation=await command('create_semantic_relation',{source_id:e.ids.project,target_id:e.ids.task,relation_type:'related_to',properties:{purpose:'Project work'}});assert(projectRelation.ok);e.ids.project_relation=projectRelation.result.relation.id;
 const relationActor=await ipc('register_agent',{principal:'system',principal_token:keys.system,name:'vertical-file-linker',capabilities:['semantic.read','semantic.relation.create'],worlds:[c.world]});assert(relationActor.ok);
 const fileRelation=await ipc('create_semantic_relation',{actor:'vertical-file-linker',actor_token:relationActor.result.token,command:{world_id:c.world,correlation_id:crypto.randomUUID(),source_id:e.ids.task,target_id:e.ids.file,relation_type:'related_to',properties:{purpose:'Verified observed source document'}}});assert(fileRelation.ok,'authorized receipt-backed Task -> File relation failed');e.ids.file_relation=fileRelation.result.relation.id;check('synthetic scoped actor establishes Task to verified File',true);e.baseline=await snapshot('before_browser');e.hashes={core:sha(path.join(LAB,'target/debug/mvp-core-fixture.exe')),bridge:sha(path.join(LAB,'target/debug/mvp-runtime-fixture.exe')),mcp:sha(path.join(LAB,'target/debug/spaceos-mcp.exe')),world:sha(path.join(LAB,'space-bridge/static/world.html')),shell:sha(path.join(LAB,'space-bridge/static/index.html'))};e.status='READY_FOR_ONE_WORLD_MISSION';save();console.log(JSON.stringify({status:e.status,url:'http://127.0.0.1:21973/',run,world:c.world,ids:e.ids}));
 await observationChecks();
 if(scoped){
  await require('./scoped_browser_actions.cjs')({c,e,ipc,check,secrets,save,browserObservation,LAB,recovery});
  if(v5) await require('./action_v5_runtime.cjs')({c,e,ipc,check,secrets,save,run,config,start,stop,sleep,
    restartCore:async()=>{await stop(core);core=start('mvp-core-fixture',[],'core-v5-restart');await readyCore();await registerAgent();}});
  if(recovery){
   e.status='READY_FOR_RECOVERY_UI';save();console.log(JSON.stringify({status:e.status,run,world:c.world,browser:e.ids.browser}));
   const deadline=Date.now()+10*60000;while(Date.now()<deadline&&!fs.existsSync(path.join(run,'RESTART'))&&!fs.existsSync(path.join(run,'STOP')))await sleep(500);
   assert(fs.existsSync(path.join(run,'RESTART')),'recovery UI restart signal missing');
  }
  // Controlled restart of owned fixture processes; Chrome/DB/source remain.
  await stop(browserProvider);await stop(fsProvider);await stop(core);
  core=start('mvp-core-fixture',[],'core-restart');await readyCore();await registerAgent();
  const req={profile:'browser_world',name:'space-world-browser',worlds:[c.world],capabilities:['semantic.read','action.get_tabs','action.screenshot'],registrar_token:keys.browser_world};
  e.scoped.restartMissingBinding=await ipc('register_service_actor',req,false);
  check('Core restart without Provider registration fails closed',e.scoped.restartMissingBinding.ok===false);
  browserProvider=start('browser-provider',[],'browser-restart');fsProvider=start('filesystem-provider',[],'filesystem-restart');
  const registered=await waitFor('scoped Browser re-registration after Core restart',async()=>{const r=await ipc('register_service_actor',req,false);return r.ok?r:null});secrets.push(registered.result.token);
  const fresh=await ipc('request_action',{id:crypto.randomUUID(),actor:'space-world-browser',actor_token:registered.result.token,provider_id:'browser',action:'get_tabs',target:{scope_version:1,world_id:c.world}},false);
  e.scoped.restartLive=fresh;
  check('Core Provider restart reconstructs exact canonical live identity',fresh.ok&&fresh.result.status==='Completed'&&fresh.result.detail.tabs.length===1&&fresh.result.detail.tabs[0].tab_id===e.ids.browser);
  check('Provider restart changes signed performer identity',fresh.result.detail.performer_instance!==e.scoped.performer_instance);
  const after=await snapshot('after_scoped_restart');const current=after.objects.find(o=>o.id===e.ids.browser);
  check('Browser creation snapshot persists exactly after restart',JSON.stringify(current)===JSON.stringify(e.browserCreated));
  const db=new DatabaseSync(c.db,{readOnly:true});const receipt=db.prepare('SELECT * FROM filesystem_file_admissions WHERE object_id=?').get(e.ids.file);db.close();
  check('File admission receipt survives scoped runtime restart',JSON.stringify(receipt)===JSON.stringify(e.fileReceipt));
  e.scoped.restartComplete=true;save();
  if(recovery){
   await require('./scoped_browser_recovery_checks.cjs')({c,e,check,secrets,save});
   await stop(bridge);bridge=start('mvp-runtime-fixture',['bridge'],'bridge-restart');await readyBridge();
   e.recovery.bridgeRestarted=true;save();
  }
 }
 if(v5) {await require('./action_v5_runtime.cjs').afterRestart({c,ipc,check});fs.writeFileSync(path.join(run,'STOP'),'v5 transport-only test complete');require('./action_v5_runtime.cjs').close();}
 e.status='READY_FOR_REAL_UI';save();console.log(JSON.stringify({status:e.status,run,world:c.world,browser:e.ids.browser}));
 const until=Date.now()+20*60000;while(Date.now()<until&&!fs.existsSync(path.join(run,'STOP'))){await snapshot('ui_poll');await sleep(1200)}
 }catch(err){e.failure=err.message;e.status=e.status==='LIVE_MODEL_NOT_RUN'?e.status:'BLOCKED';save();console.error(err.message);process.exitCode=1}
 finally{if(v5)require('./action_v5_runtime.cjs').close();wsSocket?.destroy();await stop(browserProvider);await stop(fsProvider);await stop(chrome);pageServer?.close();await stop(bridge);await stop(core);if(home)for(const id of fs.readdirSync(home)){const file=path.join(home,id,'gateway-result.json');if(fs.existsSync(file)){const v=JSON.parse(fs.readFileSync(file)),b=v.attempt?.binding;if(b?.opencode_session_id){try{api('POST','/api/session/'+b.opencode_session_id+'/interrupt')}catch{}try{const directory=path.join(home,id,'project');api('DELETE','/api/experimental/mcp/spaceos?'+new URLSearchParams({'location[directory]':directory}),null,directory)}catch{}}}}
 if(fs.existsSync(c.db)){const db=new DatabaseSync(c.db,{readOnly:true});e.sqlite={receipts:db.prepare('SELECT * FROM filesystem_file_admissions').all(),objects:db.prepare('SELECT * FROM objects ORDER BY id').all(),relations:db.prepare('SELECT * FROM relations ORDER BY id').all(),events:db.prepare('SELECT * FROM events ORDER BY seq').all(),integrity:db.prepare('PRAGMA integrity_check').get()};db.close()}
 e.cleanup={coreStopped:!core||core.exitCode!==null||core.signalCode!==null,bridgeStopped:!bridge||bridge.exitCode!==null||bridge.signalCode!==null,filesystemStopped:!fsProvider||fsProvider.exitCode!==null||fsProvider.signalCode!==null,browserStopped:!browserProvider||browserProvider.exitCode!==null||browserProvider.signalCode!==null,chromeStopped:!chrome||chrome.exitCode!==null||chrome.signalCode!==null};e.stopped_at=new Date().toISOString();save();console.log(JSON.stringify({cleanup:e.cleanup,status:e.status,failure:e.failure}))}})();
