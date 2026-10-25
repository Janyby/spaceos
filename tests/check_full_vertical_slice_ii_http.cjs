const {selectedRun,readEvidence,writeEvidence}=require('./evidence_paths.cjs');
// Actual isolated Bridge read/auth checks, no model calls or resource mutations.
const fs=require('fs'),path=require('path'),assert=require('assert/strict');
const dir=selectedRun(),e=readEvidence(dir,'full-slice-ii-smoke-evidence.json');
assert(fs.realpathSync(e.run).startsWith(fs.realpathSync('C:/spaceos/test-tmp')+path.sep));
const c=JSON.parse(fs.readFileSync(path.join(e.run,'fixture.secret.json'))),origin='http://127.0.0.1:21973';assert(c.port===21973&&c.world===e.world);
(async()=>{
 const login=await fetch(origin+'/api/lab-auth/login',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({proof:c.keys.login,world_id:c.world})});assert(login.ok);const auth=await login.json(),cookie=login.headers.get('set-cookie').split(';')[0];assert(auth.ok&&auth.csrf);
 const records=[];
 async function read(route,body){const r=await fetch(origin+route,{method:body?'POST':'GET',headers:{Origin:origin,Cookie:cookie,'x-spaceos-csrf':auth.csrf,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});const result=await r.json();records.push({route,status:r.status,body:result});assert(r.ok&&result.ok!==false,route);return result;}
 const objects=await read('/api/objects/query',{}),relations=await read('/api/relations/query',{}),events=await read('/api/events?limit=500'),context=await read('/api/context');
 const rows=objects.objects||objects.result;assert(Array.isArray(rows),'object response contract');for(const id of [e.ids.project,e.ids.task,e.ids.summary,e.ids.file,e.ids.browser])assert(rows.some(o=>o.id===id),'missing authorized object '+id);assert(!rows.some(o=>o.id===e.ids.foreign));
 const eventRows=events.events||events.result;assert(Array.isArray(eventRows));for(const type of ['FileCreated','FileChanged','BrowserTabCreated','MissionVerified'])assert(eventRows.some(v=>v.event_type===type),'missing HTTP event '+type);
 const anonymous=await fetch(origin+'/api/objects/query',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:'{}'});assert.equal(anonymous.status,401);
 const foreign=await fetch(origin+'/api/objects/query',{method:'POST',headers:{Origin:'http://foreign.invalid',Cookie:cookie,'x-spaceos-csrf':auth.csrf,'Content-Type':'application/json'},body:'{}'});assert.equal(foreign.status,403);
 const out={at:new Date().toISOString(),run:e.run,world:e.world,records,anonymous_status:anonymous.status,foreign_origin_status:foreign.status,checks:8};let text=JSON.stringify(out,null,2);for(const token of [...Object.values(c.keys),cookie,auth.csrf,c.execution.actor_token])text=text.split(token).join('[REDACTED]');writeEvidence(dir,'full-slice-ii-http.json',text);console.log(JSON.stringify({checks:out.checks,status:'PASS',routes:records.map(v=>({route:v.route,status:v.status})),objects:rows.length,anonymous:anonymous.status,foreignOrigin:foreign.status}));
})().catch(err=>{console.error(err.message);process.exitCode=1});
