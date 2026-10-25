// Real post-Core-restart Bridge HTTP; no action replay or direct agent substitute.
const assert=require('assert/strict'),{DatabaseSync}=require('node:sqlite');
module.exports=async({c,e,check,secrets,save})=>{
 const base='http://127.0.0.1:21973';
 const login=await fetch(base+'/api/lab-auth/login',{method:'POST',headers:{Origin:base,'Content-Type':'application/json'},body:JSON.stringify({proof:c.keys.login,world_id:c.world})});assert(login.ok);
 const session=await login.json(),cookie=login.headers.get('set-cookie').split(';')[0];secrets.push(cookie,session.csrf);
 const call=async(route,body)=>{const response=await fetch(base+route,{method:body?'POST':'GET',headers:{Origin:base,Cookie:cookie,'x-spaceos-csrf':session.csrf,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});return{status:response.status,body:await response.json()}};
 const counts=()=>{const db=new DatabaseSync(c.db,{readOnly:true});const r=db.prepare("SELECT event_type,COUNT(*) AS n FROM events WHERE actor='space-world-browser' GROUP BY event_type").all();db.close();return r};
 e.recovery={http:[],beforeStale:counts()};
 const shot={action:'screenshot',target:{object_id:e.ids.browser,options:{format:'jpeg',quality:62}}};
 const stale=await call('/api/providers/browser/actions',shot);e.recovery.http.push({label:'stale actor first request',...stale});
 check('stale actor request returns 403 and explicit no-retry',stale.status===403&&stale.body.error.includes('identity refreshed')&&stale.body.error.includes('not retried'));
 assert.deepEqual(counts(),e.recovery.beforeStale);check('stale screenshot produced no request or success event',true);
 const next=await call('/api/providers/browser/actions',shot);const result=next.body.result?.result;
 check('subsequent deliberate screenshot succeeds after registration refresh',next.status===200&&result?.status==='Completed'&&result.detail.object_id===e.ids.browser&&result.detail.data.length>100);
 e.recovery.http.push({label:'subsequent screenshot',status:next.status,body:{status:result.status,action_id:result.action_id,object_id:result.detail.object_id,performer_instance:result.detail.performer_instance,image_bytes:Buffer.from(result.detail.data,'base64').length}});
 const tabs=await call('/api/browser/tabs');e.recovery.http.push({label:'capability descriptor',...tabs});
 check('authoritative tab mode and capabilities delivered',tabs.status===200&&tabs.body.browser_mode==='scoped'&&JSON.stringify(tabs.body.browser_capabilities)===JSON.stringify(['action.get_tabs','action.screenshot']));
 const context=await call('/api/context');check('context has same authoritative mode and capabilities',context.body.context.browser_mode==='scoped'&&JSON.stringify(context.body.context.browser_capabilities)===JSON.stringify(tabs.body.browser_capabilities));
 for(const [label,body,status]of [
  ['raw target',{action:'screenshot',target:{target_id:e.browserCreated.properties.target_id}},400],
  ['bad screenshot options',{action:'screenshot',target:{object_id:e.ids.browser,options:{format:'png',quality:62}}},400],
  ['unsupported action',{action:'navigate',target:{}},403],
  ['absent canonical',{action:'screenshot',target:{object_id:'absent'}},404],
 ]){const response=await call('/api/providers/browser/actions',body);e.recovery.http.push({label,...response});check('HTTP category '+label+' '+status,response.status===status);}
 const db=new DatabaseSync(c.db,{readOnly:true});const forged=db.prepare("SELECT id FROM objects WHERE type='BrowserTab' AND json_extract(properties,'$.target_id')='forged-history-target'").get();db.close();
 const conflict=await call('/api/providers/browser/actions',{action:'screenshot',target:{object_id:forged.id}});e.recovery.http.push({label:'unavailable historical live handle',...conflict});check('unavailable canonical live handle returns conflict 409',conflict.status===409);
 check('canonical snapshot still unchanged after HTTP failures',(()=>{const db=new DatabaseSync(c.db,{readOnly:true});const o=db.prepare('SELECT properties,updated_at FROM objects WHERE id=?').get(e.ids.browser);db.close();return JSON.stringify(JSON.parse(o.properties))===JSON.stringify(e.browserCreated.properties)&&o.updated_at.replace(/\+00:00$/,'Z')===e.browserCreated.updated_at})());
 save();
};
