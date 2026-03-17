import Database from 'better-sqlite3';
import path from 'path';

const dbPath = path.resolve('./opt/adserver/adserver.db');
const db = new Database(dbPath);

console.log('Inserting test ad...');
db.prepare("INSERT OR IGNORE INTO ads (folder_name, active, placement_pre, placement_mid, placement_post, priority) VALUES (?, 1, 1, 1, 1, 5)").run('advert1');

console.log('Checking ads table again:');
const adsAfter = db.prepare('SELECT * FROM ads').all();
console.log(JSON.stringify(adsAfter, null, 2));

console.log('Checking settings table:');
const settings = db.prepare('SELECT * FROM settings').all();
console.log(JSON.stringify(settings, null, 2));
