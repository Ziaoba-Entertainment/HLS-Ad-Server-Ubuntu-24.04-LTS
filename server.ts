import express from "express";
import { createServer as createViteServer } from "vite";
import { spawn, execSync } from "child_process";
import path from "path";
import fs from "fs";
import RedisServer from "redis-server";
import { createProxyMiddleware } from "http-proxy-middleware";

const logFile = "server_debug.log";
function log(msg: string) {
  fs.appendFileSync(logFile, `[${new Date().toISOString()}] ${msg}\n`);
}

async function startServer() {
  log("Server starting...");
  const app = express();
  const PORT = 3000;

  // Check python3
  try {
    const pythonVersion = execSync("python3 --version").toString().trim();
    log(`Python version: ${pythonVersion}`);
  } catch (err) {
    log(`Python check failed: ${err}`);
  }

  // Try to install pip
  try {
    log("Downloading get-pip.py...");
    execSync("curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py");
    log("Installing pip...");
    execSync("python3 /tmp/get-pip.py --user");
    log("pip installed successfully");
  } catch (err) {
    log(`pip installation failed: ${err}`);
  }

  // Diagnostics
  try {
    log(`pwd: ${execSync("pwd").toString().trim()}`);
    log(`ls -la .: ${execSync("ls -la .").toString().trim()}`);
    log(`ls -la opt: ${execSync("ls -la opt").toString().trim()}`);
    log(`ls -la opt/adserver: ${execSync("ls -la opt/adserver").toString().trim()}`);
    log(`ls -ld /opt/adserver: ${execSync("ls -ld /opt/adserver").toString().trim()}`);
    log(`ls -la /opt/adserver: ${execSync("ls -la /opt/adserver").toString().trim()}`);
    log(`id: ${execSync("id").toString().trim()}`);
  } catch (err) {
    log(`Diagnostics failed: ${err}`);
  }

  // Try to install requirements directly
  try {
    log("Installing requirements directly...");
    const adserverDir = path.resolve("opt/adserver");
    const requirementsPath = path.join(adserverDir, "requirements.txt");
    execSync(`python3 -m pip install --user -r ${requirementsPath}`);
    log("Requirements installed successfully");
  } catch (err) {
    log(`Requirements installation failed: ${err}`);
  }

  let redisBin = "redis-server";
  try {
    // Try different ways to find redislite binary
    const findRedisScript = `
import redislite
import os
try:
    print(redislite.Redis().executable)
except:
    try:
        print(redislite.redis_server_path)
    except:
        # Fallback: look for it in the package directory
        base = os.path.dirname(redislite.__file__)
        for root, dirs, files in os.walk(base):
            for f in files:
                if f == "redis-server":
                    print(os.path.join(root, f))
                    exit(0)
`;
    const redisPath = execSync(`python3 -c '${findRedisScript}'`).toString().trim().split('\n')[0];
    if (redisPath && fs.existsSync(redisPath)) {
      redisBin = redisPath;
      log(`Found redis-server binary at: ${redisBin}`);
    } else {
      // Check if it's in PATH
      try {
        const whichRedis = execSync("which redis-server").toString().trim();
        if (whichRedis) {
          redisBin = whichRedis;
          log(`Found redis-server in PATH at: ${redisBin}`);
        }
      } catch (e) {
        log("redis-server not found in PATH");
      }
    }
  } catch (err) {
    log(`Redis binary discovery failed: ${err}`);
  }

  // Try to start redis
  try {
    log(`Starting redis-server using bin: ${redisBin}`);
    const redisServer = new RedisServer({
      port: 6379,
      bin: redisBin,
    });

    redisServer.on('error', (err: any) => {
      log(`redis-server error event: ${err.message}`);
    });

    redisServer.open((err) => {
      if (err) {
        log(`redis-server open error: ${err.message}`);
      } else {
        log("redis-server started successfully on port 6379");
      }
    });

    redisServer.on('stdout', (data) => {
      log(`redis stdout: ${data}`);
    });
  } catch (err) {
    log(`redis-server initialization failed: ${err}`);
  }

  const adserverDir = path.resolve("opt/adserver");
  const srvDir = path.resolve("srv/vod");
  
  // Create directories
  [
    path.join(srvDir, "hls/movies"),
    path.join(srvDir, "hls/tv"),
    path.join(srvDir, "ads"),
    path.join(srvDir, "output")
  ].forEach(dir => {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
      log(`Created directory: ${dir}`);
    }
  });

  const dbPath = path.join(adserverDir, "adserver.db");

  // Initialize DB
  log("Initializing database...");
  try {
    const initDb = spawn("python3", ["init_db.py"], { cwd: adserverDir });
    initDb.on("error", (err) => log(`init_db spawn error: ${err.message}`));
    initDb.stdout.on("data", (data) => log(`init_db: ${data}`));
    initDb.stderr.on("data", (data) => log(`init_db error: ${data}`));
    await new Promise((resolve) => initDb.on("close", resolve));

    log("Running database migrations (v2)...");
    const migrateV2 = spawn("python3", ["db_migrate_v2.py"], { cwd: adserverDir });
    migrateV2.on("error", (err) => log(`migrate_v2 spawn error: ${err.message}`));
    migrateV2.stdout.on("data", (data) => log(`migrate_v2: ${data}`));
    migrateV2.stderr.on("data", (data) => log(`migrate_v2 error: ${data}`));
    await new Promise((resolve) => migrateV2.on("close", resolve));
  } catch (err) {
    log(`Failed to run init_db.py: ${err}`);
  }

  // Start Ad Server (Port 8083)
  log("Starting Ad Server...");
  const adServer = spawn("python3", [
    "-m", "uvicorn", 
    "main:app", 
    "--host", "127.0.0.1", 
    "--port", "8083"
  ], { 
    cwd: adserverDir,
    env: { ...process.env, PYTHONPATH: adserverDir }
  });
  
  adServer.on("error", (err) => log(`adserver spawn error: ${err.message}`));
  adServer.stdout.on("data", (data) => log(`adserver: ${data}`));
  adServer.stderr.on("data", (data) => log(`adserver error: ${data}`));

  // Start Admin UI (Port 8089)
  log("Starting Admin UI...");
  const adminUI = spawn("python3", [
    "-m", "uvicorn", 
    "admin_app:app", 
    "--host", "127.0.0.1", 
    "--port", "8089"
  ], { 
    cwd: adserverDir,
    env: { ...process.env, PYTHONPATH: adserverDir }
  });

  adminUI.on("error", (err) => log(`admin spawn error: ${err.message}`));
  adminUI.stdout.on("data", (data) => log(`admin: ${data}`));
  adminUI.stderr.on("data", (data) => log(`admin error: ${data}`));

  // Proxy API requests to Ad Server
  app.use("/api", createProxyMiddleware({
    target: "http://127.0.0.1:8083",
    changeOrigin: true,
  }));

  // Proxy Playlist requests to Ad Server
  app.use("/playlist", createProxyMiddleware({
    target: "http://127.0.0.1:8083",
    changeOrigin: true,
  }));

  // Serve HLS segments
  app.use("/segments/hls", express.static("/srv/vod/hls"));
  app.use("/segments/ads", express.static("/srv/vod/ads"));

  // Proxy Admin requests
  app.use("/admin", createProxyMiddleware({
    target: "http://127.0.0.1:8089",
    changeOrigin: true,
    pathRewrite: {
      "^/admin": "", // remove /admin prefix when sending to 8089
    },
  }));

  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static("dist"));
    app.get("*", (req, res) => {
      res.sendFile(path.resolve("dist/index.html"));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    log(`Server running on http://localhost:${PORT}`);
  });
}

startServer().catch(err => {
  log(`FATAL ERROR: ${err}`);
});
