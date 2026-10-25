// Isolated writer checks only: no Core, Chrome, OpenCode, network or model execution.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),cp=require('node:child_process'),assert=require('node:assert/strict'),{createRequire}=require('node:module'),crypto=require('node:crypto');
const E=require('./evidence_paths.cjs'),root=path.resolve(__dirname,'..');
const a=fs.mkdtempSync(path.join(root,'test-tmp/evidence-writer-A-')),b=fs.mkdtempSync(path.join(root,'test-tmp/evidence-writer-B-'));
(async()=>{
let checks=0;const check=(name,fn)=>{fn();checks++;console.log('PASS '+name)};
check('two runs have distinct exclusive JSON and image paths',()=>{
 for(const run of [a,b]){E.writeEvidence(run,'summary.json',JSON.stringify({ok:true}));E.writeEvidence(run,'screen.jpg',Buffer.from([1,2,3]));}
 assert.notEqual(E.artifactPath(a,'summary.json'),E.artifactPath(b,'summary.json'));
 assert.equal(E.readEvidence(a,'summary.json').run,a);
});
check('collision preserves previous JSON and binary bytes',()=>{
 for(const name of ['summary.json','screen.jpg']){const before=fs.readFileSync(path.join(a,name));assert.throws(()=>E.writeEvidence(a,name,'{"replacement":true}'),{code:'EEXIST'});assert.deepEqual(fs.readFileSync(path.join(a,name)),before);}
});
check('path containment and traversal fail closed',()=>{
 for(const run of [undefined,root,path.join(root,'test-tmp'),path.join(root,'docs/maintenance')])assert.throws(()=>E.runDirectory(run));
 for(const name of ['../escape.json','sub/file.json','..',''])assert.throws(()=>E.writeEvidence(a,name,'{}'));
});
check('immutable checkpoints and latest selected read',()=>{
 const save=E.snapshotWriter(a,'runtime.json');save({n:1});save({n:2});
 assert.equal(E.readEvidence(a,'runtime.json').n,2);
 assert.equal(JSON.parse(fs.readFileSync(path.join(a,'runtime.000001.json'))).n,1);
 assert.throws(()=>E.snapshotWriter(a,'runtime.json')({n:3}),{code:'EEXIST'});
});
check('foreign run summary cannot be substituted',()=>{
 E.writeEvidence(a,'foreign.json','{}');
 const foreign=path.join(a,'foreign.json'); // only this fresh synthetic fixture is deliberately malformed
 const other=path.join(a,'mismatched.json');fs.writeFileSync(other,JSON.stringify({run:b}),{flag:'wx'});
 assert.throws(()=>E.readEvidence(a,'mismatched.json'),/different run/);
});
const runtimeFiles=['full_vertical_slice.cjs','full_vertical_slice_ii.cjs','consolidated_mvp_smoke.cjs','vertical_slice_ii_corrections_runtime.cjs'];
for(const file of runtimeFiles){
 const source=fs.readFileSync(path.join(__dirname,file),'utf8');
 const end=source.indexOf('const check=');assert(end>0);
 const flags=file==='vertical_slice_ii_corrections_runtime.cjs'?[{}, {SPACEOS_SCOPED_BROWSER_TEST:'1'},{SPACEOS_SCOPED_BROWSER_RECOVERY_TEST:'1'},{SPACEOS_ACTION_V5_TEST:'1'}]:[{}];
 for(const env of flags)check('actual save prelude '+file+' '+JSON.stringify(env),()=>{
  const context={require:createRequire(path.join(__dirname,file)),process:{env},console,Buffer};
  vm.createContext(context);vm.runInContext(source.slice(0,end)+';globalThis.writer={save,e,run,output};',context);
  const {save,e,run,output}=context.writer;e.writer_test='first';save();const first=e.evidence_artifact;const bytes=fs.readFileSync(first);
  e.writer_test='second';save();assert.notEqual(first,e.evidence_artifact);assert.deepEqual(fs.readFileSync(first),bytes);
  assert.equal(E.readEvidence(run,path.basename(output)).writer_test,'second');
  assert(first.startsWith(run+path.sep));assert(!first.includes('maintenance'));
 });
}
check('actual screenshot writer is exclusive and per-run in all modes',()=>{
 const source=fs.readFileSync(path.join(__dirname,'scoped_browser_actions.cjs'),'utf8');
 const line=source.split(/\r?\n/).find(s=>s.includes('writeEvidence(c.run,'));
 assert(line);
 for(const env of [{},{SPACEOS_ACTION_V5_TEST:'1'}]){
  const run=fs.mkdtempSync(path.join(root,'test-tmp/evidence-screen-'));
  const context={writeEvidence:E.writeEvidence,c:{run},process:{env},recovery:false,result:{detail:{data:Buffer.from('synthetic-image').toString('base64')}},Buffer};
  vm.runInNewContext(line,context);assert.throws(()=>vm.runInNewContext(line,context),{code:'EEXIST'});
 }
});
const audits=['check_consolidated_mvp.cjs','check_full_vertical_slice.cjs','check_full_vertical_slice_ii.cjs','check_full_vertical_slice_ii_http.cjs','vertical_slice_ii_corrections_audit.cjs','scoped_browser_audit.cjs','scoped_browser_recovery_audit.cjs','action_v5_audit.cjs'];
check('actual browser probe summary writer stays in its run',()=>{
 const source=fs.readFileSync(path.join(root,'scripts/browser-observation-probe.mjs'),'utf8');
 const line=source.split(/\r?\n/).find(s=>s.includes("const artifact=writeEvidence(run,"));assert(line);
 const context={writeEvidence:E.writeEvidence,run:b,evidence:{ok:true}};
 vm.runInNewContext(line,context);assert.throws(()=>vm.runInNewContext(line,{...context}),{code:'EEXIST'});
});
check('browser Rust writer has an exclusive create and no historical destination',()=>{
 const source=fs.readFileSync(path.join(root,'browser-provider/tests/verified_observation.rs'),'utf8');
 assert(source.includes('create_new(true)'));assert(source.includes('write_evidence(&artifact,'));assert(!source.includes('docs/maintenance/'));
});
for(const file of audits)check('no implicit historical fallback '+file,()=>{
 const r=cp.spawnSync(process.execPath,[path.join(__dirname,file)],{cwd:root,env:{...process.env,SPACEOS_EVIDENCE_RUN:''},encoding:'utf8'});
 assert.notEqual(r.status,0);assert.match(r.stderr,/Explicit --run/);
});
const logSource=fs.readFileSync(path.join(root,'scripts/browser-observation-real-test.mjs'),'utf8');
const logLine=logSource.split(/\r?\n/).find(s=>s.includes('const log=await open('));assert(logLine);
const openLog=new (Object.getPrototypeOf(async function(){}).constructor)('run','path','open',logLine+';return log;');
const log=await openLog(b,path,fs.promises.open);await log.write('synthetic log chunk\n');await log.close();
await assert.rejects(()=>openLog(b,path,fs.promises.open),{code:'EEXIST'});
check('actual browser harness log create is exclusive',()=>assert.equal(fs.readFileSync(path.join(b,'browser-observation-real-runtime.log'),'utf8'),'synthetic log chunk\n'));
check('new private fixture remains ignored',()=>{
 const file=E.writeEvidence(a,'fixture.secret.json',JSON.stringify({synthetic:'writer-test-only-not-a-runtime-credential'}));
 const r=cp.spawnSync('git',['check-ignore','--',file],{cwd:root,encoding:'utf8'});assert.equal(r.status,0);
});
const report={checks,run:a,second_run:b,result:'PASS',scope:'filesystem writers and extracted runner preludes only; no runtime integration'};
E.writeEvidence(a,'writer-checks.json',JSON.stringify(report,null,2));console.log(JSON.stringify(report));
})().catch(err=>{console.error(err);process.exitCode=1});
