import evidencePaths from '../tests/evidence_paths.cjs';
const {writeEvidence}=evidencePaths;
// Dedicated headless Chrome only. Never reads Chrome user configuration.
import {spawn} from 'node:child_process';
import {mkdir,writeFile} from 'node:fs/promises';
import {createServer} from 'node:net';
import {randomUUID,createHash} from 'node:crypto';
import path from 'node:path';
const root='C:\\spaceos';
const chrome='C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const run=path.join(root,'test-tmp',`browser-runtime-probe-${randomUUID()}`);
await mkdir(run,{recursive:true});
const listener=createServer();await new Promise(r=>listener.listen(0,'127.0.0.1',r));
const port=listener.address().port;await new Promise(r=>listener.close(r));
if([8765,9222].includes(port))throw Error('unsafe test port');
let child,ws;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function version(){
 for(let i=0;i<100;i++){try{const r=await fetch(`http://127.0.0.1:${port}/json/version`,{signal:AbortSignal.timeout(1000)});if(r.ok)return await r.json()}catch{}await sleep(50)}throw Error('dedicated Chrome not ready');
}
async function connect(url){const w=new WebSocket(url);await new Promise((r,j)=>{w.addEventListener('open',r,{once:true});w.addEventListener('error',j,{once:true})});return w}
let serial=0;
async function command(w,method,params={}){
 const id=++serial;return await new Promise((resolve,reject)=>{
  const timer=setTimeout(()=>{w.removeEventListener('message',receive);reject(Error('CDP command deadline'))},5000);
  function receive(e){const v=JSON.parse(e.data);if(v.id!==id)return;clearTimeout(timer);w.removeEventListener('message',receive);v.error?reject(Error('CDP rejected command')):resolve(v.result)}
  w.addEventListener('message',receive);w.send(JSON.stringify({id,method,params}));
 });
}
async function start(phase){
 const args=[`--user-data-dir=${path.join(run,'synthetic-profile')}`,`--remote-debugging-port=${port}`,'--remote-debugging-address=127.0.0.1','--headless=new','--no-first-run','--no-default-browser-check','--disable-background-networking','--disable-component-update','--disable-sync','--disable-extensions','about:blank'];
 child=spawn(chrome,args,{windowsHide:true,stdio:'ignore'});
 const v=await version();const u=new URL(v.webSocketDebuggerUrl);
 if(u.hostname!=='127.0.0.1'||+u.port!==port||!u.pathname.startsWith('/devtools/browser/'))throw Error('wrong browser endpoint');
 const id=u.pathname.slice('/devtools/browser/'.length);let target;
 for(let i=0;i<3;i++){
  const again=await version();if(again.webSocketDebuggerUrl!==v.webSocketDebuggerUrl)throw Error('runtime identity changed on reconnect');
  ws=await connect(v.webSocketDebuggerUrl);const result=await command(ws,'Target.getTargets');
  target=result.targetInfos.find(t=>t.type==='page'&&t.url==='about:blank');if(!target)throw Error('missing synthetic page');ws.close();await sleep(30);
 }
 return {phase,pid:child.pid,executable:chrome,args,browser:v.Browser,debugger_id_sha256:createHash('sha256').update(id).digest('hex'),target_id:target.targetId,reconnects:3,version:v};
}
async function stop(){
 if(!child)return;
 try{const v=await version();ws=await connect(v.webSocketDebuggerUrl);await command(ws,'Browser.close').catch(()=>{});ws.close()}catch{}
 await Promise.race([new Promise(r=>child.once('exit',r)),sleep(2000)]);
 if(child.exitCode===null){child.kill();await sleep(200)}child=undefined;
}
try{
 const a=await start('initial');await stop();const b=await start('same-port-new-Chrome');
 if(a.debugger_id_sha256===b.debugger_id_sha256)throw Error('runtime debugger identity reused after Chrome restart');
 delete a.version;delete b.version;
 await stop();const evidence={run,port,initial:a,restarted:b,stable_reconnect:true,new_runtime_changed_guard:true,owned_chrome_stopped:true};
 const artifact=writeEvidence(run,'browser-runtime-identity-probe.json',JSON.stringify(evidence,null,2));
 console.log(JSON.stringify({run,artifact}));
 console.log(JSON.stringify({result:'PASS',run,port,browser:a.browser,reconnects:a.reconnects,changed_guard_on_restart:true}));
}finally{await stop()}
