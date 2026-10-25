// Executable extracted production functions. These are unit contracts, not browser evidence.
const fs=require('fs'),vm=require('vm'),assert=require('assert/strict');
const source=fs.readFileSync('C:/spaceos/space-bridge/static/world.html','utf8');
const section=(start,end)=>{const a=source.indexOf(start),b=source.indexOf(end,a);assert(a>=0&&b>a);return source.slice(a,b);};
let passed=0;function check(name,value){assert(value,name);passed++;console.log('PASS '+name);}
const tab={id:'browser-tab-v1-canonical',type:'BrowserTab',provider_id:'browser',external_id:'browser-tab-v1:hash-not-a-target',properties:{title:'Canonical <tab>',url:'https://synthetic.test/snapshot',target_id:'RAW-TARGET',source_id:'source-A',runtime_epoch:'epoch-A',instance_id:'instance-A',world_id:'world-A'}};
const W={objects:[tab],tabs:[],selCluster:null,events:[]},calls=[],drop={style:{display:'block'}};
const context={W,console,CL:{Web:{label:'Web',icon:'web',col:'blue'}},memberObjs:()=>W.objects,byId:id=>W.objects.find(o=>o.id===id),
 canBrowser:action=>!W.browserScoped||['get_tabs','screenshot'].includes(action),
 selectObj:(id,focus)=>calls.push(['select',id,focus]),hideDrop:()=>{drop.style.display='none';calls.push(['hide']);},
 openBrowser:(...a)=>calls.push(['open',...a]),activateTab:t=>calls.push(['activate',t]),toast:()=>calls.push(['denied']),
 esc:v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),trunc:s=>String(s??'')};
vm.createContext(context);vm.runInContext(section('        const objName =','        const objPath =')
 +section('        function tabObjs()','        function focusPickDefault()')
 +section('        function tline(','        // ---- graph mode')
 +section('        function handleDelegatedClick(','        // ============ sidebar'),context);
const click=(action,target,hide)=>context.handleDelegatedClick({target:{closest:()=>({getAttribute:n=>({'data-action':action,'data-target':target,'data-open':'true','data-hide-drop':hide}[n]??null)})}});
click('selectObj',tab.id,'true');check('actual delegated search handler selects exact ID and Focus then closes dropdown',calls[0][0]==='select'&&calls[0][1]===tab.id&&calls[0][2]===true&&drop.style.display==='none'&&calls[1][0]==='hide');
check('canonical title is used instead of hashed external identity',vm.runInContext('objName(W.objects[0])',context)===tab.properties.title);
const live={tab_id:tab.id,target_id:'RAW-TARGET',title:'Live changed title'};W.tabs=[live];
check('live cache does not mint another semantic object',context.tabObjs().length===1&&context.tabObjs()[0]===tab&&context.clusterCounts('Web').total===1);
check('exact canonical ID and raw target join works',context.browserLiveTarget(tab)==='RAW-TARGET');
for(const row of [{...live,tab_id:'other'},{...live,target_id:'other'},{...live,source_id:'other'},{...live,runtime_epoch:'other'},{...live,instance_id:'other'},{...live,world_id:'other'}])check('mismatched live authority denied '+JSON.stringify(row),context.browserLiveTarget(tab,[row])===null);
check('ambiguous duplicate overlay denied',context.browserLiveTarget(tab,[live,live])===null);
W.browserScoped=true;
check('scoped overlay rejects missing authority columns',context.browserLiveTarget(tab,[live])===null);
const fullLive={...live,...Object.fromEntries(['source_id','runtime_epoch','instance_id','world_id'].map(k=>[k,tab.properties[k]]))};
check('scoped overlay accepts exact full authority',context.browserLiveTarget(tab,[fullLive])==='RAW-TARGET');
context.openBrowserObject(tab.id,true);check('scoped UI activation does not call legacy action',calls.at(-1)[0]==='denied');
W.browserScoped=false;
check('hashed external identity alone never becomes target',context.browserLiveTarget({...tab,properties:{title:'legacy'}})===null);
check('live metadata never overrides canonical label',vm.runInContext('objName(W.objects[0])',context)==='Canonical <tab>');
const el={};context.renderTreeMode(el);check('Tree renders canonical ID exactly once with escaped snapshot title',el.innerHTML.includes('Canonical &lt;tab&gt;')&&el.innerHTML.split('data-target="'+tab.id+'"').length===2&&!el.innerHTML.includes('tab:RAW-TARGET'));
click('openBrowserObject',tab.id);check('canonical action resolves to explicit raw target plus object binding',calls.at(-1)[0]==='open'&&calls.at(-1)[1]==='RAW-TARGET'&&calls.at(-1)[2]===tab.id);
click('activateBrowserObject',tab.id);check('activation mapping uses raw target not external hash',calls.at(-1)[0]==='activate'&&calls.at(-1)[1]==='RAW-TARGET');
W.tabs=[];click('openBrowserObject',tab.id);check('missing live overlay cannot issue an action',calls.at(-1)[0]==='denied');
check('Graph bounded fallback intentionally unchanged',source.includes("for (const cl of ['Mission', 'Files', 'Projects'])"));
check('canonical panel refresh never falls back to another live tab',source.includes('B.sel = browserLiveTarget(byId(B.objectId), list)'));
(async()=>{
 const nodes={};context.$=id=>nodes[id]??=( {classList:{add(){},remove(){}},innerHTML:'',textContent:''} );
 context.setInterval=()=>1;context.clearInterval=()=>{};context.bfRenderTabs=()=>{};context.bfShowCurrent=()=>{};context.bfShot=()=>{};
 context.liveRows=[live];context.GET=async()=>({result:{result:{detail:{tabs:context.liveRows}}}});
 context.applyBrowserMode=()=>{};
 vm.runInContext(section('        const B =','        function bfRenderTabs()'),context);
 W.tabs=[live];context.openBrowserObject(tab.id);await context.bfRefresh();
 check('real panel functions preserve canonical binding and matching raw target',vm.runInContext('B.objectId',context)===tab.id&&vm.runInContext('B.sel',context)==='RAW-TARGET');
 context.liveRows=[{tab_id:'other-canonical',target_id:'another-live-tab'}];await context.bfRefresh();
 check('lost canonical overlay never selects the unrelated available tab',vm.runInContext('B.sel',context)===null&&vm.runInContext('B.objectId',context)===tab.id);
 context.GET=async()=>{throw Error('controlled unavailable')};await context.bfRefresh();
 check('failed refresh clears the bound live handle',vm.runInContext('B.sel',context)===null&&vm.runInContext('B.tabs.length',context)===0);
 check('operational refresh never changes the Core creation snapshot',tab.properties.title==='Canonical <tab>'&&tab.external_id==='browser-tab-v1:hash-not-a-target');
 console.log(JSON.stringify({passed,kind:'extracted JS functions with controlled DOM/action doubles'}));
})().catch(e=>{console.error(e);process.exitCode=1});
