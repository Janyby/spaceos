const {DatabaseSync} = require('node:sqlite');
const db = new DatabaseSync(process.argv[2], {readOnly:true});
const integrity = db.prepare('PRAGMA integrity_check').all();
if (integrity.length !== 1 || integrity[0].integrity_check !== 'ok') throw Error('SQLite integrity check failed');
console.log(JSON.stringify({integrity:'ok',schema:db.prepare('PRAGMA user_version').get().user_version,objects:db.prepare('SELECT count(*) AS n FROM objects').get().n}));
db.close();
