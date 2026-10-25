const {selectedRun,readEvidence,writeEvidence}=require('./evidence_paths.cjs');
// Read-only audit of the identified synthetic runtime and its own database.
const fs=require('fs'),path=require('path'),net=require('net'),assert=require('assert/strict'),crypto=require('crypto'),{DatabaseSync}=require('node:sqlite');
const dir=selectedRun(),e=readEvidence(dir,'full-slice-smoke-evidence.json'),c=JSON.parse(fs.readFileSync(path.join(e.run,'fixture.secret.json')));
assert(fs.realpathSync(e.run).startsWith(fs.realpathSync('C:/spaceos/test-tmp')+path.sep));assert(c.db===e.db&&c.world===e.world&&c.pipe.startsWith('\\\\.\\pipe\\spaceos-4-3a4-process-mvp2b1-'));
const db=new DatabaseSync(e.db,{readOnly:true}),objects=db.prepare('SELECT * FROM objects ORDER BY id').all(),relations=db.prepare('SELECT * FROM relations ORDER BY id').all(),events=db.prepare('SELECT * FROM events ORDER BY seq').all(),integrity=db.prepare('PRAGMA integrity_check').get();db.close();
const props=id=>JSON.parse(objects.find(o=>o.id===id).properties),missions=objects.filter(o=>o.type==='Mission'),attempts=objects.filter(o=>o.type==='MissionAttempt');
const out={at:new Date().toISOString(),run:e.run,world:e.world,ids:e.ids,checks:[],integrity,mission:missions.map(v=>({id:v.id,...JSON.parse(v.properties)})),attempts:attempts.map(v=>({id:v.id,...JSON.parse(v.properties)})),events:events.map(v=>({id:v.id,seq:v.seq,event_type:v.event_type,actor:v.actor,world_id:v.world_id,object_ids:JSON.parse(v.object_ids),correlation_id:v.correlation_id})),relationIds:relations.map(v=>v.id),objectIds:objects.map(v=>v.id),wsTypes:e.wsEvents?.map(v=>({type:v.type,event_type:v.event?.event_type,id:v.event?.id}))};
function check(n,v){assert(v,n);out.checks.push(n)}
function ipc(method,params){return new Promise((resolve,reject)=>{const s=net.connect(c.pipe);let b='';s.setTimeout(5000);s.on('error',reject);s.on('timeout',()=>{s.destroy();reject(Error('synthetic pipe timeout'))});s.on('connect',()=>s.write(JSON.stringify({method,params})+'\n'));s.on('data',d=>{b+=d;if(b.includes('\n')){s.destroy();resolve(JSON.parse(b.split('\n')[0]))}})})}
(async()=>{
 check('SQLite integrity',integrity.integrity_check==='ok');
 const file=objects.find(o=>o.id===e.ids.file);check('one actual observed File and exact provider/World',file?.type==='File'&&file.provider_id==='filesystem'&&JSON.parse(file.properties).world_id===e.world&&objects.filter(o=>o.external_id===file.external_id).length===1);
 check('actual committed FileCreated with same File identity',events.some(v=>v.event_type==='FileCreated'&&v.provider_id==='filesystem'&&v.actor==='filesystem'&&v.world_id===e.world&&JSON.parse(v.object_ids).includes(file.id)));
 check('actual FileCreated delivered over authenticated WS',e.wsEvents.some(v=>v.type==='semantic_event'&&v.event.event_type==='FileCreated'&&v.event.object_ids.includes(file.id)));
 check('provider-owned relation denial preserved',e.providerRelationDenied?.ok===false);
 check('exactly one UI-created Mission',missions.length===1);
 if(e.gateway_results.length){const a=e.gateway_results[0].attempt;out.calls=a.calls.map(v=>({name:v.name,ok:v.core_response?.ok}));out.binding=a.binding;
 check('one real attempt and immutable World/Mission binding',e.gateway_results.length===1&&attempts.length===1&&a.binding.world_id===e.world&&a.binding.mission_id===missions[0].id);
 check('local execution succeeded within 30 calls',a.outcome==='succeeded'&&a.admitted>0&&a.admitted<=30&&a.calls.every(v=>v.core_response?.ok));
 const ri=a.calls.findIndex(v=>v.name==='spaceos_query_relations'),pi=a.calls.findIndex(v=>v.name==='spaceos_patch_object');check('model read actual relation before mutation',ri>=0&&pi>ri&&a.calls[ri].core_response.result.some(v=>v.id===e.ids.relation));
 check('unfinished Task now completed with correct evidence',props(e.ids.task).status==='completed'&&props(e.ids.task).reviewed_evidence_id===e.ids.evidence);
 check('meaningful summary records observed File identity',typeof props(e.ids.summary).summary==='string'&&props(e.ids.summary).summary.includes(e.ids.file));
 check('foreign guard unchanged',props(e.ids.foreign).status==='unfinished');
 check('same canonical Project/Task/Evidence relations',relations.length===2&&relations.some(v=>v.id===e.ids.relation));
 check('agent updates only authorized Task and Summary',events.filter(v=>v.actor==='canonical-mvp-agent'&&v.event_type==='ObjectUpdated').every(v=>JSON.parse(v.object_ids).every(id=>[e.ids.task,e.ids.summary].includes(id))));
 check('model mutation delivered via WS',e.wsEvents.some(v=>v.type==='semantic_event'&&v.event.event_type==='ObjectUpdated'&&v.event.actor==='canonical-mvp-agent'));
 if(process.argv.includes('--completed'))check('authoritative completion is nonempty and attempt-bound',props(missions[0].id).state==='completed'&&props(missions[0].id).verification?.passed===true&&props(missions[0].id).verification.detail.note.trim().length>0&&events.some(v=>v.event_type==='MissionVerified'&&JSON.parse(v.payload).operation.expected_attempt_id===a.binding.attempt_id));
 }
 try{out.traceStats=await ipc('trace_stats',{actor:'canonical-mvp-agent',actor_token:c.execution.actor_token});out.traceWorld=await ipc('trace_query',{actor:'canonical-mvp-agent',actor_token:c.execution.actor_token,world_id:c.world});}catch(err){out.traceUnavailable=err.message}
 if(e.afterRestart){const sorted=a=>[...a].sort((x,y)=>String(x.id??x.seq).localeCompare(String(y.id??y.seq)));for(const k of ['objects','relations'])check(k+' stable across actual restart',JSON.stringify(sorted(e.beforeRestart[k]))===JSON.stringify(sorted(e.afterRestart[k])));const old=e.beforeRestart.events;check('historical events unchanged after restart',old.every(v=>e.afterRestart.events.some(n=>n.id===v.id&&JSON.stringify(n)===JSON.stringify(v))));}
 out.state=missions.length?props(missions[0].id).state:null;out.summary=props(e.ids.summary);out.status='AUDIT_PASS_WITH_DECLARED_COMPONENT_GAPS';
 let text=JSON.stringify(out,null,2);for(const s of [...Object.values(c.keys),c.execution.actor_token])text=text.split(s).join('[REDACTED]');writeEvidence(dir,'full-slice-audit.json',text);console.log(JSON.stringify({state:out.state,checks:out.checks.length,calls:out.calls,traceCount:out.traceWorld?.result?.length}));
})().catch(err=>{console.error(err.message);process.exitCode=1});
