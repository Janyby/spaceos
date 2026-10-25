// Execute actual production authority helpers; DOM doubles, not browser evidence.
const fs=require('fs'),vm=require('vm'),assert=require('assert/strict');
const root='C:/spaceos/space-bridge/static/',world=fs.readFileSync(root+'world.html','utf8'),shell=fs.readFileSync(root+'index.html','utf8');
let passed=0;const check=(n,v)=>{assert(v,n);passed++;console.log('PASS '+n)};
function slice(s,a,b){const i=s.indexOf(a),j=s.indexOf(b,i);assert(i>=0&&j>i);return s.slice(i,j)}
for(const [name,source,names] of [['World',world,['bfNewTab','bfGo','bfReload','bfShow']],['Shell',shell,['navigateBrowser','openNewTab','reloadBrowser','getPageContent']]]) {
 for(const n of names)check(name+' initially disables '+n,new RegExp('<button[^>]*onclick="'+n+'\\(\\)"[^>]*\\bdisabled\\b').test(source));
}
const caps=['action.get_tabs','action.screenshot'];
const buttons=['bfNewTab','bfGo','bfReload','bfShow'].map(name=>({disabled:false,getAttribute:()=>name+'()'}));
const document={querySelector:()=>({textContent:''}),querySelectorAll:s=>s==='#browser button[onclick]'?buttons:[]};
const wctx={W:{},document};vm.createContext(wctx);vm.runInContext(slice(world,'        function canBrowser(','        async function loadBrowserAuthority()'),wctx);
check('World unknown authority cannot screenshot',!wctx.canBrowser('screenshot'));
wctx.applyBrowserMode('legacy',undefined);check('mode alone cannot enable World',!wctx.canBrowser('navigate')&&buttons.every(b=>b.disabled));
wctx.applyBrowserMode('scoped',caps);check('valid scoped metadata enables only bounded actions',wctx.canBrowser('screenshot')&&!wctx.canBrowser('navigate')&&buttons.every(b=>b.disabled));
wctx.applyBrowserMode('scoped',[...caps,'action.navigate']);check('unexpected scoped grant fails closed',!wctx.canBrowser('screenshot'));
wctx.applyBrowserMode('legacy',[...caps,'action.navigate']);check('authoritative legacy capabilities preserved',wctx.canBrowser('navigate')&&!wctx.canBrowser('close_tab'));
wctx.applyBrowserMode(null,null);check('failed authority fetch clears previous permission',!wctx.canBrowser('navigate')&&buttons.every(b=>b.disabled));
const sbuttons=['openNewTab','navigateBrowser','reloadBrowser','getPageContent'].map(name=>({disabled:false,getAttribute:()=>name+'()'}));
const sctx={document:{querySelectorAll:()=>sbuttons}};vm.createContext(sctx);vm.runInContext(slice(shell,'        let scopedBrowserMode =','        async function loadBrowserAuthority()'),sctx);
check('Shell unknown authority denies operations',!sctx.shellCanBrowser('navigate'));
sctx.applyShellBrowserAuthority('legacy',undefined);check('Shell mode without capabilities remains disabled',sbuttons.every(b=>b.disabled));
sctx.applyShellBrowserAuthority('scoped',caps);check('Shell scoped controls remain disabled',sbuttons.every(b=>b.disabled)&&sctx.shellCanBrowser('get_tabs'));
sctx.applyShellBrowserAuthority('legacy',[...caps,'action.navigate']);check('Shell explicit legacy capability enables Navigate only',!sbuttons[1].disabled&&sbuttons[0].disabled);
sctx.applyShellBrowserAuthority(null,null);check('Shell loses authority closed',sbuttons.every(b=>b.disabled));
for(const s of [world,shell])for(const match of s.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))new vm.Script(match[1]);
check('both final HTML scripts compile',true);
console.log(JSON.stringify({passed,kind:'executed production helpers with DOM doubles'}));
