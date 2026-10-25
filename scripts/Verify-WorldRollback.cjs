const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const html = fs.readFileSync('C:/spaceos/target/stage2b3-preflight/rollback-world.html', 'utf8');
for (const [, script] of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)) {
  new vm.Script(script);
}
(async () => {
  let notices = 0;
  const context = vm.createContext({toast: () => { notices++; }, GET: () => {throw Error('Unexpected HTTP');}, POST: () => {throw Error('Unexpected HTTP');}});
  for (const name of ['openSemObjCreate','createSemanticObject','openSemObjEdit','saveSemanticObject','openSemRelCreate','createSemanticRelation']) {
    const body = html.match(new RegExp('(?:async )?function ' + name + '\\([^)]*\\)\\s*\\{[\\s\\S]*?\\n        \\}'));
    assert(body, name);
    vm.runInContext(body[0], context);
    await vm.runInContext(name + '()', context);
  }
  assert.equal(notices, 6);
  for (const [tag] of html.matchAll(/<button[^>]*onclick="(?:openSemObjCreate|createSemanticObject|openSemObjEdit|saveSemanticObject|openSemRelCreate|createSemanticRelation)\([^>]*>/g)) {
    assert.match(tag, /<button disabled /);
  }
  console.log('PASS: full inline JS parses; six semantic handlers return without DOM/HTTP; semantic buttons disabled.');
})().catch(error => { console.error(error); process.exitCode = 1; });
