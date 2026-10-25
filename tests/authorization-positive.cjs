const {snapshotWriter,writeEvidence}=require('./evidence_paths.cjs');
// Real IPC diagnosis against the unchanged Core. A reproduced denial is NOT
// a successful agent mutation and cannot satisfy the live-execution gate.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),net=require('net'),{spawn}=require('child_process'),assert=require('assert/strict'),{DatabaseSync}=require('node:sqlite');
const LAB='C:\\spaceos',run=path.join(LAB,'test-tmp','authorization-'+crypto.randomUUID());fs.mkdirSync(run,{recursive:true});
if(!fs.realpathSync(run).startsWith(fs.realpathSync(LAB)+path.sep))throw Error('containment');
const key=()=>crypto.randomBytes(32).toString('base64url'),keys={transport:key(),system:key(),semantic:key(),reader:key()},uid=crypto.randomUUID();
const c={run,keys,world:'mvp2b1-world-A-'+uid,other_world:'mvp2b1-world-B-'+uid,pipe:'\\\\.\\pipe\\spaceos-4-3a4-process-mvp2b1-'+uid,fs_pipe:'\\\\.\\pipe\\spaceos-4-3a4-mvp2b1-unused-fs-'+uid,browser_pipe:'\\\\.\\pipe\\spaceos-4-3a4-mvp2b1-unused-browser-'+uid,db:path.join(run,'core.db')};
const config=path.join(run,'fixture.secret.json');fs.writeFileSync(config,JSON.stringify(c));writeEvidence(run,'consolidation-authorization-pointer.json',JSON.stringify({run},null,2));
const secrets=Object.values(keys),calls=[],checks=[],timeline=[];let proc,seeds;
const scrub=s=>secrets.reduce((s,k)=>s.split(k).join('[REDACTED]'),s);
const ipcCheckpoint=snapshotWriter(run,'ipc.json'),timelineCheckpoint=snapshotWriter(run,'timeline.json');
function save(){ipcCheckpoint({calls},scrub);timelineCheckpoint({timeline});}
function ipc(method,params){return new Promise((resolve,reject)=>{const s=net.connect(c.pipe);let b='';s.setTimeout(5000);s.on('connect',()=>s.write(JSON.stringify({method,params})+'\n'));s.on('data',d=>{b+=d;const i=b.indexOf('\n');if(i>=0){s.destroy();try{const response=JSON.parse(b.slice(0,i));if(response.result?.token)secrets.push(response.result.token);calls.push({method,request:params,response});save();resolve(response);}catch(e){reject(e)}}});s.on('error',reject);s.on('timeout',()=>{s.destroy();reject(Error('isolated IPC timeout'))});});}
const denied=r=>r.ok===false&&/denied|unsupported/i.test(r.error||'');
function check(name,condition,acceptance='PASS'){checks.push({name,diagnosticMatched:!!condition,acceptance});assert(condition,name);}
const actor='mvp2b1-mission-agent',caps=['semantic.read','semantic.object.patch','semantic.relation.create'];
async function register(){return ipc('register_agent',{principal:'system',principal_token:keys.system,name:actor,capabilities:caps,worlds:[c.world]});}
function patch(id,world=c.world){return {object_id:id,world_id:world,properties_patch:{status:'completed-by-agent'},correlation_id:crypto.randomUUID()};}
function relation(type='related_to'){return {world_id:c.world,source_id:seeds.task_a.id,target_id:seeds.summary_a.id,relation_type:type,properties:{reason:'synthetic diagnostic only'},correlation_id:crypto.randomUUID()};}
function snapshot(name){const db=new DatabaseSync(c.db,{readOnly:true});const value={objects:db.prepare('SELECT * FROM objects ORDER BY id').all(),relations:db.prepare('SELECT * FROM relations ORDER BY id').all(),events:db.prepare('SELECT * FROM events ORDER BY seq').all(),integrity:db.prepare('PRAGMA integrity_check').all(),fk:db.prepare('PRAGMA foreign_key_check').all()};db.close();writeEvidence(run,'snapshot-'+name+'.json',JSON.stringify(value,null,2));return value;}
(async()=>{try{
 const env={SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR,PATH:process.env.PATH,TEMP:path.join(LAB,'test-tmp'),TMP:path.join(LAB,'test-tmp'),SPACEOS_TEST_ISOLATION_ROOT:LAB,SPACEOS_CORE_TOKEN:keys.transport,MVP2B1_CONFIG:config};
 proc=spawn(path.join(LAB,'target/debug/mvp-core-fixture.exe'),[],{cwd:LAB,env,windowsHide:true,stdio:['ignore',fs.openSync(path.join(run,'core.stdout.log'),'a'),fs.openSync(path.join(run,'core.stderr.log'),'a')]});timeline.push({kind:'start',pid:proc.pid,executable:path.join(LAB,'target/debug/mvp-core-fixture.exe'),pipe:c.pipe});
 let ready=false;for(let n=0;n<80;n++){try{if((await ipc('ping',{})).ok){ready=true;break}}catch{}if(proc.exitCode!==null)throw Error('fixture exited');await new Promise(r=>setTimeout(r,100));}assert(ready,'isolated Core readiness');
 seeds=JSON.parse(fs.readFileSync(path.join(run,'seeds.json')));const registered=await register();check('agent registered by authenticated synthetic fixture principal',registered.ok===true);let token=registered.result.token;
 const grants=await ipc('policy_caps',{actor,actor_token:token});check('exact narrow agent grants',grants.ok&&JSON.stringify([...grants.result.capabilities].sort())===JSON.stringify([...caps].sort()));
 const read=await ipc('query_objects',{actor,actor_token:token,world_id:c.world});check('authorized read succeeds via actor_token without general transport token',read.ok&&read.result.length===2&&read.result.every(o=>o.properties.world_id===c.world));
 check('cross-World read denied',denied(await ipc('query_objects',{actor,actor_token:token,world_id:c.other_world})));
 const before=snapshot('before-agent-mutations');
 const mutation=await ipc('patch_semantic_object',{actor,actor_token:token,command:patch(seeds.task_a.id)});check('authorized distinct agent PATCH succeeds',mutation.ok===true);
 const edge=await ipc('create_semantic_relation',{actor,actor_token:token,command:relation()});check('authorized distinct agent related_to succeeds',edge.ok===true);
 const after=snapshot('after-agent-mutations');check('agent success preserves ownership and identity',mutation.result.object.provider_id==='space-world-user'&&mutation.result.object.external_id===null);
 check('committed semantic events attribute actual agent',after.events.filter(e=>['ObjectUpdated','RelationCreated'].includes(e.event_type)).every(e=>e.actor===actor));
 const narrow=await ipc('register_agent',{principal:'system',principal_token:keys.system,name:'read-only-agent',capabilities:['semantic.read'],worlds:[c.world]});
 check('registered read-only agent PATCH denied',denied(await ipc('patch_semantic_object',{actor:'read-only-agent',actor_token:narrow.result.token,command:patch(seeds.task_a.id)})));
 check('registered agent without relation capability denied',denied(await ipc('create_semantic_relation',{actor:'read-only-agent',actor_token:narrow.result.token,command:relation()})));
 check('cross-World target with authorized command World denied',(await ipc('patch_semantic_object',{actor,actor_token:token,command:patch(seeds.foreign_task.id)})).ok===false);
 check('cross-World relation endpoint denied',(await ipc('create_semantic_relation',{actor,actor_token:token,command:{...relation(),target_id:seeds.foreign_task.id}})).ok===false);
 check('unsupported agent relation denied',(await ipc('create_semantic_relation',{actor,actor_token:token,command:relation('contains')})).ok===false);
 const badBefore=snapshot('before-negative');
 check('anonymous arbitrary identity denied',denied(await ipc('patch_semantic_object',{actor,actor_token:'invalid',command:patch(seeds.task_a.id)})));
 check('identity fields cannot be patched',(await ipc('patch_semantic_object',{actor,actor_token:token,command:{...patch(seeds.task_a.id),provider_id:actor}})).ok===false);
 const badAfter=snapshot('after-negative');
 check('negative mutations are atomic',JSON.stringify(badBefore.objects)===JSON.stringify(badAfter.objects)&&JSON.stringify(badBefore.relations)===JSON.stringify(badAfter.relations));
 check('anonymous mutation denied',denied(await ipc('patch_semantic_object',{actor:'space-world-user',actor_token:'',command:patch(seeds.task_a.id)})));
 const reader=await ipc('register_service_actor',{profile:'world_read',name:'space-world-reader',registrar_token:keys.reader,capabilities:['semantic.read'],worlds:[c.world]});check('separate World reader registration',reader.ok===true);
 check('read-only identity mutation denied',denied(await ipc('patch_semantic_object',{actor:'space-world-reader',actor_token:reader.result.token,command:patch(seeds.task_a.id)})));
 check('cross-World agent mutation denied',denied(await ipc('patch_semantic_object',{actor,actor_token:token,command:patch(seeds.foreign_task.id,c.other_world)})));
 check('forged World-user identity denied',denied(await ipc('patch_semantic_object',{actor:'space-world-user',actor_token:token,command:patch(seeds.task_a.id)})));
 check('agent cannot register broader authority',denied(await ipc('register_agent',{principal:actor,actor_token:token,name:actor,capabilities:[...caps,'agent.register'],worlds:[c.world,c.other_world]})));
 check('agent credential cannot claim system principal',denied(await ipc('register_agent',{principal:'system',principal_token:token,name:actor,capabilities:caps,worlds:[c.world]})));
 const old=token;const renewed=await register();check('exact agent re-registration rotates its token',renewed.ok===true&&renewed.result.token!==old);token=renewed.result.token;
 check('revoked agent read denied',denied(await ipc('query_objects',{actor,actor_token:old,world_id:c.world})));
 check('revoked agent mutation denied',denied(await ipc('patch_semantic_object',{actor,actor_token:old,command:patch(seeds.task_a.id)})));
 check('fresh narrow agent token restores only read', (await ipc('query_objects',{actor,actor_token:token,world_id:c.world})).ok===true);
 const owner=await ipc('register_service_actor',{profile:'semantic_world',name:'space-world-user',registrar_token:keys.semantic,capabilities:['mission.command','semantic.object.create','semantic.object.patch','semantic.relation.create'],worlds:[c.world]});check('existing owner service control registration',owner.ok===true);
 const ownerPatch=await ipc('patch_semantic_object',{actor:'space-world-user',actor_token:owner.result.token,command:patch(seeds.task_a.id)});check('same mutation payload succeeds only as actual owner service control',ownerPatch.ok===true);
 const ownerEdge=await ipc('create_semantic_relation',{actor:'space-world-user',actor_token:owner.result.token,command:relation()});check('same relation payload succeeds only as actual owner service control',ownerEdge.ok===true);
 check('unsupported relation denied even for owner control',(await ipc('create_semantic_relation',{actor:'space-world-user',actor_token:owner.result.token,command:relation('contains')})).ok===false);
 const final=snapshot('final');check('isolated SQLite integrity',final.integrity[0].integrity_check==='ok'&&final.fk.length===0);
 check('foreign synthetic object unchanged',JSON.parse(final.objects.find(o=>o.id===seeds.foreign_task.id).properties).status==='unfinished');
 writeEvidence(run,'result.json',JSON.stringify({verdict:'PASS',checks,world:c.world,other_world:c.other_world,actor,capabilities:caps,requiredAgentMutationAccepted:mutation.ok===true,requiredAgentRelationAccepted:edge.ok===true,modelInvoked:false},null,2));
 console.log(JSON.stringify({verdict:'PASS',checks:checks.length,requiredAgentMutationAccepted:true,requiredAgentRelationAccepted:true,run}));
 }catch(e){writeEvidence(run,'failure.json',scrub(JSON.stringify({error:e.stack,checks},null,2)));console.error(e.message);process.exitCode=1;}
 finally{if(proc&&proc.exitCode===null){proc.kill();await Promise.race([new Promise(r=>proc.once('exit',r)),new Promise(r=>setTimeout(r,3000))]);}timeline.push({kind:'stopped',pid:proc?.pid,exitCode:proc?.exitCode});save();}
})().then(()=>process.exit(process.exitCode||0));
