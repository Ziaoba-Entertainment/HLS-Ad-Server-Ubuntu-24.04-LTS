import Redis from 'ioredis';
const redis = new Redis({ host: '127.0.0.1', port: 6379, db: 1 });
async function clear() {
    await redis.flushdb();
    console.log('Cleared DB');
    process.exit(0);
}
clear();
