import evidencePaths from '../tests/evidence_paths.cjs';
const {writeEvidence}=evidencePaths;
import {spawn} from 'node:child_process';
import {mkdir,writeFile,open} from 'node:fs/promises';
import {createServer} from 'node:http';
import {createServer as tcpServer} from 'node:net';
import {randomUUID,createHash} from 'node:crypto';
import path from 'node:path';
const root='C:\\spaceos',chrome='C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const run=path.join(root,'test-tmp',`browser-verified-real-${randomUUID()}`);await mkdir(run,{recursive:true});
const portPicker=tcpServer();await new Promise(r=>portPicker.listen(0,'127.0.0.1',r));const cdpPort=portPicker.address().port;await new Promise(r=>portPicker.close(r));
if([8765,9222].includes(cdpPort))throw Error('unsafe test CDP port');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));let child,current,sequence=0,pageUrl;
async function version(){for(let i=0;i<100;i++){try{const r=await fetch(`http://127.0.0.1:${cdpPort}/json/version`,{signal:AbortSignal.timeout(500)});if(r.ok)return await r.json()}catch{}await sleep(50)}throw Error('Chrome not ready')}
async function closeChrome(){if(!child)return;try{const v=await version();const w=new WebSocket(v.webSocketDebuggerUrl);await new Promise((r,j)=>{w.addEventListener('open',r,{once:true});w.addEventListener('error',j,{once:true})});w.send(JSON.stringify({id:++sequence,method:'Browser.close'}));await sleep(300);w.close()}catch{}if(child.exitCode===null){await Promise.race([new Promise(r=>child.once('exit',r)),sleep(2000)])}if(child.exitCode===null)child.kill();child=undefined;}
async function startChrome(){
 const args=[`--user-data-dir=${path.join(run,'synthetic-profile')}`,`--remote-debugging-port=${cdpPort}`,'--remote-debugging-address=127.0.0.1','--headless=new','--no-first-run','--no-default-browser-check','--disable-background-networking','--disable-component-update','--disable-sync','--disable-extensions',pageUrl];
 child=spawn(chrome,args,{windowsHide:true,stdio:'ignore'});const v=await version();const u=new URL(v.webSocketDebuggerUrl);if(u.hostname!=='127.0.0.1'||+u.port!==cdpPort||!u.pathname.startsWith('/devtools/browser/'))throw Error('wrong runtime');
 return {id:u.pathname.slice('/devtools/browser/'.length),browser:v.Browser,pid:child.pid,args};
}
const server=createServer(async(req,res)=>{
 try{if(req.url==='/fixture-restart'&&req.method==='POST'){await closeChrome();current=await startChrome();res.setHeader('Content-Type','application/json');res.end(JSON.stringify({changed_debugger_sha256:createHash('sha256').update(current.id).digest('hex')}));return}
 res.setHeader('Content-Type','text/html; charset=utf-8');res.end('<!doctype html><title>SpaceOS synthetic BrowserTab</title><p>Metadata only. No scripts.</p>');}catch{res.statusCode=500;res.end('fixture error')}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const httpPort=server.address().port;
pageUrl=`http://127.0.0.1:${httpPort}/synthetic?remove=synthetic#remove`;
try{
 current=await startChrome();const config={run,pipe:`\\\\.\\pipe\\spaceos-4-3a4-browser-${randomUUID()}`,provider_pipe:`\\\\.\\pipe\\spaceos-4-3a4-browser-provider-${randomUUID()}`,control_port:httpPort,
  runtime:{source_id:randomUUID(),world_id:'browser-A',cdp_origin:`http://127.0.0.1:${cdpPort}`,runtime_epoch:randomUUID(),expected_browser_debugger_id:current.id},browser:current.browser,chrome_pid:current.pid,chrome_args:current.args};
 const file=path.join(run,'fixture.json');await writeFile(file,JSON.stringify(config,null,2),{flag:'wx'});
 const log=await open(path.join(run,'browser-observation-real-runtime.log'),'wx');
 const p=spawn('cargo',['test','--offline','--locked','-p','browser-provider','--test','verified_observation','real_chrome_browser_observation','-j2','--','--nocapture','--test-threads=1'],{cwd:root,windowsHide:true,env:{...process.env,CARGO_TARGET_DIR:path.join(root,'target'),CARGO_INCREMENTAL:'0',SPACEOS_TEST_ISOLATION_ROOT:root,SPACEOS_CORE_TOKEN:'synthetic-browser-observation-tests-only',SPACEOS_BROWSER_REAL_TEST_CONFIG:file},stdio:['ignore','pipe','pipe']});
 p.stdout.on('data',b=>{process.stdout.write(b);log.write(b)});p.stderr.on('data',b=>{process.stdout.write(b);log.write(b)});const code=await new Promise((r,j)=>{p.once('error',j);p.once('exit',r)});await log.close();if(code!==0)process.exitCode=code;
 console.log(JSON.stringify({runtime_test_exit:code,run,artifacts:[path.join(run,'browser-observation-real-runtime.log'),path.join(run,'browser-observation-runtime-evidence.json')],chrome:current.browser}));
}finally{await closeChrome();await new Promise(r=>server.close(r))}
