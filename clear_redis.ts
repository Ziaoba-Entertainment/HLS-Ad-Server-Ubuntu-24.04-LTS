import Redis from 'ioredis';
import fs from 'fs';

let password = undefined;
const redisEnvPath = '/etc/ziaoba/redis.env';
if (fs.existsSync(redisEnvPath)) {
    const redisEnv = fs.readFileSync(redisEnvPath, 'utf-8');
    const match = redisEnv.match(/REDIS_PASSWORD=(.*)/);
    if (match) {
        password = match[1].trim();
    }
}

const redis = new Redis({ 
    host: '127.0.0.1', 
    port: 6379, 
    db: 1,
    password: password
});
async function clear() {
    await redis.flushdb();
    console.log('Cleared DB');
    process.exit(0);
}
clear();
