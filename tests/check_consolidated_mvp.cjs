const {selectedRun,readEvidence,writeEvidence}=require('./evidence_paths.cjs');
// Independent read-only verifier; no model calls or authoritative DB writes.
const fs=require('fs'),path=require('path'),assert=require('assert/strict'),{DatabaseSync}=require('node:sqlite');
const root=selectedRun(),e=readEvidence(root,'consolidated-mvp-smoke-evidence.json');
const r={run:e.run,at:new Date().toISOString(),checks:[]},check=(n,v)=>{assert(v,n);r.checks.push(n)};
check('exactly one genuine executor attempt',e.gateway_results.length===1);const g=e.gateway_results[0],a=g.attempt,calls=a.calls;
check('selected local model succeeded within budget',a.outcome==='succeeded'&&a.admitted===4&&calls.length===4);
check('all actual MCP calls succeeded in Core',calls.every(c=>c.core_response.ok===true));
check('relations were actually read before mutation',calls[0].name==='spaceos_query_objects'&&calls[1].name==='spaceos_query_relations'&&calls[2].name==='spaceos_patch_object'&&calls[3].name==='spaceos_patch_object');
const edge=calls[1].core_response.result.find(v=>v.id===e.ids.relation);check('returned existing relationship has correct direction and World',edge?.source_object_id===e.ids.task&&edge.target_object_id===e.ids.evidence&&edge.world_id===e.world);
const document=calls[0].core_response.result.find(v=>v.id===edge.target_object_id);check('connected synthetic evidence satisfies goal',document.properties.result==='PASS'&&document.properties.target_unchanged===true);
const db=new DatabaseSync(e.db,{readOnly:true}),objects=db.prepare('SELECT * FROM objects ORDER BY id').all(),events=db.prepare('SELECT * FROM events ORDER BY seq').all(),relations=db.prepare('SELECT * FROM relations ORDER BY id').all(),integrity=db.prepare('PRAGMA integrity_check').get();db.close();
const object=id=>objects.find(o=>o.id===id),props=id=>JSON.parse(object(id).properties),m=props(a.binding.mission_id),p=objects.filter(o=>o.type==='MissionAttempt').map(v=>JSON.parse(v.properties));
check('real task progressed unfinished to completed',e.baseline.objects.find(v=>v.id===e.ids.task).properties.status==='unfinished'&&props(e.ids.task).status==='completed');
check('task records evidence obtained through relation',props(e.ids.task).reviewed_evidence_id===e.ids.evidence);
check('meaningful summary persisted',props(e.ids.summary).summary.includes(e.ids.evidence));
check('two committed actual-agent updates',events.filter(v=>v.actor==='canonical-mvp-agent'&&v.event_type==='ObjectUpdated'&&v.world_id===e.world).length===2);
check('foreign guard unchanged',props(e.ids.foreign).status==='unfinished'&&!events.some(v=>v.actor==='canonical-mvp-agent'&&JSON.parse(v.object_ids).includes(e.ids.foreign)));
check('same existing relationship preserved',relations.length===1&&relations[0].id===edge.id);
check('immutable session/attempt/World/model correlation persisted',p.length===1&&p[0].attempt_id===a.binding.attempt_id&&p[0].mission_id===a.binding.mission_id&&p[0].opencode_session_id===a.binding.opencode_session_id&&p[0].world_id===e.world&&p[0].model.provider_id==='unsloth'&&p[0].model.model_id==='qwen-q8'&&m.attempt_id===p[0].attempt_id);
check('integrity and execution status',integrity.integrity_check==='ok'&&m.execution_status==='succeeded');
if(process.argv.includes('--completed'))check('authoritative completed with nonempty bound manual evidence',m.state==='completed'&&m.verification.passed===true&&m.verification.detail.note.trim().length>0&&events.some(v=>v.event_type==='MissionVerified'&&JSON.parse(v.payload).operation.expected_attempt_id===a.binding.attempt_id));
else check('successful work awaits independent verification',m.state==='verifying'&&m.verification?.passed!==true);
if(e.afterRestart){const sort=v=>[...v].sort((a,b)=>String(a.id??a.seq).localeCompare(String(b.id??b.seq)));for(const k of ['objects','relations','events'])check(k+' unchanged across actual restart',JSON.stringify(sort(e.beforeRestart[k]))===JSON.stringify(sort(e.afterRestart[k])))}
r.binding=a.binding;r.state=m.state;r.calls=calls.map(c=>c.name);r.summary=props(e.ids.summary).summary;r.integrity=integrity;r.verdict='PASS';writeEvidence(root,process.argv.includes('--completed')?'consolidated-mvp-final-check.json':'consolidated-mvp-before-verification.json',JSON.stringify(r,null,2));console.log(JSON.stringify(r,null,2));
