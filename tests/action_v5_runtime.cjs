// Real private IPC proxy: observes credential-free dispatch and controls only
// synthetic completion transport. No model, personal profile or production pipe.
const net=require('net'),assert=require('assert/strict'),crypto=require('crypto'),{DatabaseSync}=require('node:sqlite');
const servers=[],sockets=new Set(),wire=[];let mode='pass',callbacks=0,lost=0,last,capturedProof,oldCompletion,unconfirmedId;
const delay=ms=>new Promise(r=>setTimeout(r,ms));
async function until(fn){for(let n=0;n<180;n++){const v=await fn();if(v)return v;await delay(200);}throw Error('v5 synthetic state deadline');}
async function proxy(pipe,target,kind){
 const server=net.createServer(client=>{sockets.add(client);client.on('close',()=>sockets.delete(client));client.on('error',()=>{});let raw='';
  client.on('data',d=>{raw+=d;if(!raw.includes('\n'))return;client.removeAllListeners('data');const req=JSON.parse(raw.split('\n')[0]);
   if(req.method==='action'){const p=req.params;capturedProof=p.dispatch_proof;wire.push({kind,action:p.action,id:p.id,outerClean:!Object.hasOwn(p,'actor_token'),proofClean:!Object.hasOwn(p.dispatch_proof.payload.request,'actor_token'),version:p.dispatch_proof.payload.version});}
   const callback=req.method==='complete_provider_action';if(callback){callbacks++;last=req.params;if(mode==='hold'){client.destroy();return;}}
   const lose=callback&&mode==='loseOnce';if(lose)mode='pass';
   const upstream=net.connect(target);sockets.add(upstream);upstream.on('close',()=>sockets.delete(upstream));upstream.on('error',()=>client.end('{"ok":false,"error":"synthetic proxy upstream unavailable"}\n'));client.on('close',()=>upstream.destroy());
   upstream.setTimeout(5000,()=>{client.destroy();upstream.destroy()});
   upstream.on('connect',()=>upstream.write(JSON.stringify(req)+'\n'));let response='';upstream.on('data',d=>{response+=d;if(!response.includes('\n'))return;
    if(lose){lost++;client.destroy();}else client.end(response);upstream.destroy();});
  });
 });servers.push(server);await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(pipe,resolve)});
}
module.exports=async({c,e,ipc,check,secrets,save,restartCore})=>{
 const db=()=>new DatabaseSync(c.db);const row=id=>{const d=db();const r=d.prepare('SELECT * FROM action_authorities WHERE action_id=?').get(id);d.close();return r};
 const terminalCount=id=>{const d=db();const r=d.prepare("SELECT count(*) n FROM events WHERE correlation_id=? AND event_type IN ('ActionCompleted','ActionFailed')").get(id).n;d.close();return r};
 const sql=s=>{const d=db();d.exec(s);d.close()};
 check('actual synthetic SQLite schema is v5',(()=>{const d=db();const v=d.prepare('PRAGMA user_version').get().user_version;d.close();return v===5})());
 const reg=await ipc('register_agent',{principal:'system',principal_token:c.keys.system,name:'v5-scan-agent',capabilities:['action.scan'],worlds:[c.world]},false);assert(reg.ok);secrets.push(reg.result.token);
 const scan=(id,path=c.providers.root)=>ipc('request_action',{id,actor:'v5-scan-agent',actor_token:reg.result.token,provider_id:'filesystem',action:'scan',target:{path}},false);
 const failure=crypto.randomUUID();sql(`CREATE TRIGGER v5_reserve_failure BEFORE INSERT ON action_authorities WHEN NEW.action_id='${failure}' BEGIN SELECT RAISE(ABORT,'synthetic reservation failure'); END`);
 const before=wire.length;const rejected=await scan(failure);check('reservation INSERT failure prevents external dispatch',!rejected.ok&&wire.length===before&&!row(failure));sql('DROP TRIGGER v5_reserve_failure');
 mode='hold';const id=crypto.randomUUID(),started=await scan(id,require('path').join(c.providers.root,'scan-success'));check('real filesystem scan returns Started',started.ok&&started.result.status==='Started');
 await until(()=>last?.completion.action_id===id);check('actual job callback withheld; durable action is Started not Completed',row(id).state==='started'&&terminalCount(id)===0);
 const original=row(id);await restartCore();check('Core restart preserves exact pending authority',row(id).state==='started'&&row(id).performer===original.performer&&row(id).binding_id===original.binding_id);
 mode='loseOnce';await until(()=>['completed','failed'].includes(row(id)?.state));await until(()=>lost===1);await until(()=>callbacks>=3&&mode==='pass');await delay(1600);
 check('lost completion response retry commits exactly one attributed Completed outcome',terminalCount(id)===1&&row(id).state==='completed');
 const completion=last;const replay=await ipc('complete_provider_action',completion,false);check('real duplicate completion recognized without another terminal event',replay.ok&&replay.result.replay===true&&terminalCount(id)===1);
 oldCompletion=JSON.parse(JSON.stringify(completion));
 const conflict=JSON.parse(JSON.stringify(completion));if(conflict.completion.status==='Completed')conflict.completion.detail.stats.created++;else conflict.completion.detail.error='Different synthetic failure';check('valid-shaped conflicting outcome denied',(await ipc('complete_provider_action',conflict,false)).ok===false);
 const wrong=JSON.parse(JSON.stringify(completion));wrong.completion.performer=crypto.randomUUID();check('substituted performer denied',(await ipc('complete_provider_action',wrong,false)).ok===false);
 const bad=JSON.parse(JSON.stringify(completion));bad.instance_token=c.keys.browser_instance;check('wrong instance credential denied',(await ipc('complete_provider_action',bad,false)).ok===false);
 const unknown=JSON.parse(JSON.stringify(completion));unknown.completion.action_id=crypto.randomUUID();check('unknown action denied',(await ipc('complete_provider_action',unknown,false)).ok===false);
 check('foreign World completion envelope denied',(await ipc('complete_provider_action',{...completion,world_id:c.other_world},false)).ok===false);
 check('substituted source instance denied',(await ipc('complete_provider_action',{...completion,instance_id:'foreign-instance'},false)).ok===false);
 const captured=JSON.stringify(capturedProof);check('actual captured operation proof is not service actor authentication',(await ipc('query_objects',{actor:'space-world-browser',actor_token:captured,world_id:c.world},false)).ok===false);
 // Re-register only this explicitly scoped synthetic scan actor after restart.
 const again=await ipc('register_agent',{principal:'system',principal_token:c.keys.system,name:'v5-scan-agent',capabilities:['action.scan'],worlds:[c.world]},false);assert(again.ok);reg.result.token=again.result.token;secrets.push(reg.result.token);
 mode='hold';const failedStart=crypto.randomUUID();sql(`CREATE TRIGGER v5_started_failure BEFORE INSERT ON events WHEN NEW.event_type='ActionStarted' AND NEW.correlation_id='${failedStart}' BEGIN SELECT RAISE(ABORT,'synthetic Started failure'); END`);
 const unresolved=await scan(failedStart);unconfirmedId=failedStart;await until(()=>last?.completion.action_id===failedStart);check('Started write failure returns no success and leaves reserved authority',!unresolved.ok&&row(failedStart).state==='reserved'&&terminalCount(failedStart)===0);sql('DROP TRIGGER v5_started_failure');
 const early=await ipc('complete_provider_action',last,false);check('completion before durable Started is retryable without terminal event',!early.ok&&early.retryable===true&&terminalCount(failedStart)===0);
 const terminalFault=crypto.randomUUID();const sr=await scan(terminalFault);assert(sr.result?.status==='Started');await until(()=>last?.completion.action_id===terminalFault);const terminalBody=JSON.parse(JSON.stringify(last));
 sql(`CREATE TRIGGER v5_terminal_failure BEFORE INSERT ON events WHEN NEW.correlation_id='${terminalFault}' AND NEW.event_type IN ('ActionCompleted','ActionFailed') BEGIN SELECT RAISE(ABORT,'synthetic terminal failure'); END`);
 check('terminal event failure rolls back action and event together',(await ipc('complete_provider_action',terminalBody,false)).ok===false&&row(terminalFault).state==='started'&&terminalCount(terminalFault)===0);sql('DROP TRIGGER v5_terminal_failure');
 check('same completion succeeds after storage fault removed',(await ipc('complete_provider_action',terminalBody,false)).ok===true&&terminalCount(terminalFault)===1);
 mode='pass';check('actual Browser and Filesystem wire use v2 and omit both bearer fields',wire.some(w=>w.kind==='filesystem')&&wire.some(w=>w.kind==='browser')&&wire.every(w=>w.outerClean&&w.proofClean&&w.version===2));
 const d=db();const records=d.prepare('SELECT * FROM action_authorities ORDER BY rowid').all();const events=d.prepare('SELECT * FROM events WHERE correlation_id=?').all(id);d.close();
 check('private operation records contain no caller bearer value',records.every(r=>!r.operation_json.includes('actor_token')&&!r.operation_json.includes(reg.result.token)));
 check('sync authority bookkeeping never stores screenshot or read response bodies',records.filter(r=>r.terminal_json&&!JSON.parse(r.operation_json).completion).every(r=>Object.keys(JSON.parse(r.terminal_json).detail).join(',')==='action'));
 check('terminal actor World correlation restored from protected authority',events.some(v=>['ActionCompleted','ActionFailed'].includes(v.event_type)&&v.actor==='v5-scan-agent'&&v.world_id===c.world&&v.correlation_id===id));
 e.actionV5={wire,callbacks,lostResponses:lost,action_id:id,outcome:row(id).state,unresolved_action:failedStart,terminalFaultAction:terminalFault,records,events};save();
};
module.exports.setup=async({c})=>{require('fs').mkdirSync(require('path').join(c.providers.root,'scan-success'));c.callback_pipe=c.pipe+'-completion-proxy';c.fs_actual_pipe=c.fs_pipe+'-actual';c.browser_actual_pipe=c.browser_pipe+'-actual';
 await proxy(c.callback_pipe,c.pipe,'core');await proxy(c.fs_pipe,c.fs_actual_pipe,'filesystem');await proxy(c.browser_pipe,c.browser_actual_pipe,'browser');};
module.exports.close=()=>{for(const socket of sockets)socket.destroy();for(const server of servers)server.close();};
module.exports.afterRestart=async({c,ipc,check})=>{
 check('old callback cannot be reassigned to restarted Provider performer',(await ipc('complete_provider_action',oldCompletion,false)).ok===false);
 const d=new DatabaseSync(c.db,{readOnly:true});const r=d.prepare('SELECT state FROM action_authorities WHERE action_id=?').get(unconfirmedId);d.close();
 check('missing background job remains explicitly unresolved after Provider restart',r.state==='reserved');
};
