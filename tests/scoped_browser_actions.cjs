const {writeEvidence}=require('./evidence_paths.cjs');
// Real HTTP/IPC/SQLite extension of the existing guarded four-process fixture.
const assert=require('assert/strict'),crypto=require('crypto'),net=require('net'),{DatabaseSync}=require('node:sqlite');
module.exports=async({c,e,ipc,check,secrets,save,browserObservation,recovery=false})=>{
 const actor='space-world-browser',caps=['semantic.read','action.get_tabs','action.screenshot'];
 const reg=(over={})=>ipc('register_service_actor',{profile:'browser_world',name:actor,worlds:[c.world],capabilities:caps,registrar_token:c.keys.browser_world,...over},false);
 for(const [label,over]of [['reader credential',{registrar_token:c.keys.reader}],['provider credential',{registrar_token:c.keys.browser_instance}],['transport credential',{registrar_token:c.keys.transport}],['foreign World',{worlds:[c.other_world]}],['missing read',{capabilities:caps.slice(1)}],['missing screenshot',{capabilities:caps.slice(0,2)}],['expanded grant',{capabilities:[...caps,'action.navigate']}],['actor substitution',{name:'space-bridge'}]])check('registration denies '+label,(await reg(over)).ok===false);
 check('reserved actor cannot be delegated',(await ipc('register_agent',{principal:'system',principal_token:c.keys.system,name:actor,capabilities:caps,worlds:[c.world]},false)).ok===false);
 const registered=await reg();assert(registered.ok);let token=registered.result.token;secrets.push(token);
 const input=(object=e.browserCreated.id,world=c.world)=>({scope_version:1,world_id:world,object_id:object,options:{format:'jpeg',quality:62}});
 const request=(action,target,over={})=>ipc('request_action',{id:crypto.randomUUID(),actor,actor_token:token,provider_id:'browser',action,target,...over},false);
 const denied=r=>r.ok===false||r.result?.status!=='Completed';
 const before=JSON.stringify(e.browserCreated);
 e.scoped={negative:[],positive:[],providerNegative:[]};
 for(const [label,action,target,over]of [
  ['bad token','get_tabs',{scope_version:1,world_id:c.world},{actor_token:'invalid'}],
  ['foreign World','screenshot',input(e.browserCreated.id,c.other_world)],
  ['foreign object','screenshot',input(e.foreignAdmission.result.object.id)],
  ['raw-only target','screenshot',{target_id:e.browserCreated.properties.target_id}],
  ['unsupported action','navigate',input()],
  ['filesystem substitution','get_tabs',{scope_version:1,world_id:c.world},{provider_id:'filesystem'}],
  ['missing canonical','screenshot',input('absent')],
  ['supplied authority','screenshot',{...input(),runtime:c.providers.browser_source}],
  ['legacy actor cannot use scoped input','screenshot',input(),{actor:'canonical-mvp-agent',actor_token:'invalid'}],
  ['forged historical canonical','screenshot',input(browserObservation('vertical-browser',c.providers.browser_source,'forged-history-target').objects[0].id)],
 ]){const r=await request(action,target,over);e.scoped.negative.push({label,response:r});check('Core denies '+label,denied(r));}
 const listed=await request('get_tabs',{scope_version:1,world_id:c.world});e.scoped.positive.push({action:'get_tabs',response:listed});
 check('Core scoped get_tabs returns only admitted live World',listed.ok&&listed.result.status==='Completed'&&listed.result.detail.tabs.length===1&&listed.result.detail.tabs[0].tab_id===e.browserCreated.id);
 const provider=(method,params)=>new Promise((resolve,reject)=>{const socket=net.connect(c.browser_pipe);let raw='';socket.setTimeout(10000);socket.on('connect',()=>socket.write(JSON.stringify({method,params})+'\n'));socket.on('error',reject);socket.on('timeout',()=>{socket.destroy();reject(Error('fixture provider timeout'))});socket.on('data',d=>{raw+=d;if(raw.includes('\n')){socket.destroy();resolve(JSON.parse(raw.split('\n')[0]))}})});
 const hello=await provider('ping',{});check('actual Provider advertises scoped protocol',hello.result.browser_world_protocol===1);
 e.scoped.performer_instance=hello.result.instance_id;
 const stable=v=>Array.isArray(v)?v.map(stable):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])])):v;
 const internal={scope_version:1,instance_id:'vertical-browser',runtime:c.providers.browser_source,object:e.browserCreated,options:{format:'jpeg',quality:62}};
 async function signedProvider(target,action='screenshot',actorName=actor){
 const id=crypto.randomUUID(),req={id,correlation_id:id,action,target:stable(target),provider_id:'browser',actor:actorName,requested_at:new Date().toISOString()};
  const now=Math.floor(Date.now()/1000),payload={version:2,provider_instance:hello.result.instance_id,request:req,issued_at:now,expires_at:now+15,nonce:crypto.randomUUID()};
  const mac=crypto.createHmac('sha256',Buffer.from(c.keys.dispatchbrowser,'base64url')).update(JSON.stringify(payload)).digest('base64url');
  return provider('action',{...req,auth:c.keys.transport,dispatch_proof:{payload,mac}});
 }
 // Fixture owns the dispatch key. These are real verifier/Provider negatives,
 // not claims that an HTTP client can create Core proofs.
 for(const [label,mutate]of [
  ['stale epoch',t=>t.runtime.runtime_epoch=crypto.randomUUID()],
  ['foreign source',t=>t.runtime.source_id=crypto.randomUUID()],
  ['foreign World',t=>t.runtime.world_id=c.other_world],
  ['substituted observation instance',t=>t.instance_id='other-instance'],
  ['substituted raw target',t=>t.object.properties.target_id='not-the-selected-target'],
  ['forged cached snapshot',t=>t.object.properties.title='forged title'],
 ]){const t=structuredClone(internal);mutate(t);const r=await signedProvider(t);e.scoped.providerNegative.push({label,response:r});check('signed Provider denies '+label,r.ok&&r.result.status==='Failed'&&!/mac|dispatch request/.test(r.result.detail.error));}
 const login=await fetch('http://127.0.0.1:21973/api/lab-auth/login',{method:'POST',headers:{Origin:'http://127.0.0.1:21973','Content-Type':'application/json'},body:JSON.stringify({proof:c.keys.login,world_id:c.world})});assert(login.ok);
 const session=await login.json(),cookie=login.headers.get('set-cookie').split(';')[0];secrets.push(cookie,session.csrf);
 const http=async(route,body,origin='http://127.0.0.1:21973',auth=true)=>{
  const headers={Origin:origin,...(auth?{Cookie:cookie,'x-spaceos-csrf':session.csrf}:{})};if(body)headers['Content-Type']='application/json';
  const r=await fetch('http://127.0.0.1:21973'+route,{method:body?'POST':'GET',headers,body:body?JSON.stringify(body):undefined});return{status:r.status,body:await r.json()};
 };
 check('HTTP dashboard authentication enforced',(await http('/api/browser/tabs',null,undefined,false)).status===401);
 check('HTTP foreign Origin denied',(await http('/api/browser/tabs',null,'http://foreign.invalid')).status===403);
 const tabHttp=await http('/api/browser/tabs');e.scoped.positive.push({route:'/api/browser/tabs',response:tabHttp});
 check('real Bridge scoped get_tabs succeeds',tabHttp.status===200&&tabHttp.body.browser_mode==='scoped'&&tabHttp.body.result.result.detail.tabs[0].tab_id===e.browserCreated.id);
 const shot=await http('/api/providers/browser/actions',{action:'screenshot',target:{object_id:e.browserCreated.id,options:{format:'jpeg',quality:62}}});
 const result=shot.body.result?.result;
 check('real HTTP Core signed screenshot succeeds',shot.status===200&&result?.status==='Completed'&&result.detail.data.length>100&&result.detail.performer_instance===hello.result.instance_id);
 e.scoped.positive.push({route:'screenshot',status:shot.status,result:{...result,detail:{...result.detail,data:'[IMAGE OMITTED]',image_bytes:Buffer.from(result.detail.data,'base64').length}}});
 writeEvidence(c.run,(process.env.SPACEOS_ACTION_V5_TEST==='1'?'ACTION_V5_PROVIDER_SCREENSHOT.jpg':recovery?'SCOPED_BROWSER_RECOVERY_PROVIDER_SCREENSHOT.jpg':'SCOPED_BROWSER_PROVIDER_SCREENSHOT.jpg'),Buffer.from(result.detail.data,'base64'));
 for(const route of ['/api/browser/activate','/api/browser/close','/api/browser/new-tab','/api/browser/navigate','/api/browser/reload','/api/browser/content'])check('scoped HTTP cannot fall back '+route,(await http(route,{target_id:e.browserCreated.properties.target_id,url:'about:blank'})).status>=400);
 check('generic raw screenshot rejected',(await http('/api/providers/browser/actions',{action:'screenshot',target:{target_id:e.browserCreated.properties.target_id}})).status>=400);
 const db=new DatabaseSync(c.db,{readOnly:true});
 const stored=db.prepare('SELECT * FROM objects WHERE id=?').get(e.browserCreated.id);const rows=db.prepare("SELECT * FROM events WHERE actor='space-world-browser' ORDER BY seq").all();
 const created=db.prepare("SELECT COUNT(*) n FROM events WHERE event_type='BrowserTabCreated' AND object_ids LIKE ?").get('%'+e.browserCreated.id+'%').n;
 db.close();e.scoped.events=rows;e.scoped.canonical_sqlite=stored;
 check('operational reads do not create BrowserTabCreated',created===1);
 check('canonical Browser snapshot unchanged',JSON.stringify(JSON.parse(stored.properties))===JSON.stringify(e.browserCreated.properties)&&stored.updated_at.replace(/\+00:00$/,'Z')===e.browserCreated.updated_at&&before===JSON.stringify(e.browserCreated));
 check('committed screenshot has exact actor World and performer',rows.some(r=>r.event_type==='ActionCompleted'&&r.world_id===c.world&&JSON.parse(r.object_ids).includes(e.browserCreated.id)&&JSON.parse(r.payload).performer_instance===hello.result.instance_id));
 check('no fabricated success for rejected Core actions',rows.filter(r=>r.event_type==='ActionCompleted').length===3);
 save();
};
