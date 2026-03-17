const Database = require('better-sqlite3');
const db = new Database('opt/adserver/adserver.db');
const schema = db.prepare("SELECT sql FROM sqlite_master WHERE type='table'").all();
schema.forEach(row => console.log(row.sql));
