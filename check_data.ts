import Database from 'better-sqlite3';
const db = new Database('opt/adserver/adserver.db');
const settings = db.prepare("SELECT * FROM settings").all();
console.log(JSON.stringify(settings, null, 2));
const ads = db.prepare("SELECT * FROM ads").all();
console.log(JSON.stringify(ads, null, 2));
