// Test-only filesystem helpers. No runtime, credential or evidence publication service.
const fs=require('node:fs'),path=require('node:path');
const root=path.resolve(__dirname,'..');
function runDirectory(value){
 if(!value)throw Error('Explicit --run <existing test-tmp run> or SPACEOS_EVIDENCE_RUN required');
 const allowed=fs.realpathSync(path.join(root,'test-tmp')),run=fs.realpathSync(value);
 const rel=path.relative(allowed,run);
 if(!rel||rel==='..'||rel.startsWith('..'+path.sep)||path.isAbsolute(rel)||!fs.statSync(run).isDirectory())throw Error('Evidence run must be contained in repository test-tmp');
 return run;
}
function selectedRun(){
 const i=process.argv.indexOf('--run');
 return runDirectory(i<0?process.env.SPACEOS_EVIDENCE_RUN:process.argv[i+1]);
}
function artifactPath(run,name){
 if(typeof name!=='string'||!name||name!==path.basename(name)||name==='.'||name==='..')throw Error('Evidence filename must be a basename');
 return path.join(runDirectory(run),name);
}
function writeEvidence(run,name,data){
 const file=artifactPath(run,name);
 if(name.endsWith('.json')){
  const value=JSON.parse(data);
  if(value&&typeof value==='object'&&!Array.isArray(value))data=JSON.stringify({...value,run:runDirectory(run),evidence_artifact:file},null,2);
 }
 fs.writeFileSync(file,data,{flag:'wx'});
 return file;
}
// Immutable checkpoints replace the former mutable shared report. Never skip a collision.
function snapshotWriter(run,name){
 run=runDirectory(run);let sequence=0;
 return (value,scrub=s=>s)=>{
  if(sequence>=999999)throw Error('Evidence checkpoint sequence exhausted');
  const ext=path.extname(name),stem=name.slice(0,name.length-ext.length);
  const file=artifactPath(run,stem+'.'+String(++sequence).padStart(6,'0')+ext);
  value.run=run;value.evidence_artifact=file;
  value.artifact_paths=[...fs.readdirSync(run).map(f=>path.join(run,f)),file];
  writeEvidence(run,path.basename(file),scrub(JSON.stringify(value,null,2)));
  return file;
 };
}
function readEvidence(run,name){
 run=runDirectory(run);
 const ext=path.extname(name),stem=name.slice(0,name.length-ext.length);
 const files=fs.readdirSync(run).filter(f=>f.startsWith(stem+'.')&&f.endsWith(ext)&&/^\d{6}$/.test(f.slice(stem.length+1,f.length-ext.length))).sort();
 const file=artifactPath(run,files.length?files.at(-1):name);
 if(path.dirname(fs.realpathSync(file))!==run)throw Error('Evidence file escaped selected run');
 const value=JSON.parse(fs.readFileSync(file,'utf8'));
 if(value.run&&runDirectory(value.run)!==run)throw Error('Evidence belongs to a different run');
 return value;
}
module.exports={runDirectory,selectedRun,artifactPath,writeEvidence,snapshotWriter,readEvidence};
