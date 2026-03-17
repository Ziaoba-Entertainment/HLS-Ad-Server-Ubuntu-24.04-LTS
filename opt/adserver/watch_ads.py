import os
import sys
import time
import shutil
import json
import redis
import signal
import subprocess
from config import settings
from ad_selector import AdSelector
from verify_ad_segments import AdVerifier

# Configuration
INCOMING_DIR = "/srv/vod/ads/incoming"
ADS_DIR = "/srv/vod/ads"
REJECTED_DIR = "/srv/vod/ads/rejected"
LOG_FILE = "/var/log/adserver/watch_ads.log"
REDIS_CHANNEL = "ad:ad_registered_events"
REDIS_PASS = settings.REDIS_PASSWORD

# Redis initialization for DB 1 (Ad Metadata)
redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB, password=REDIS_PASS, decode_responses=True)
selector = AdSelector(redis_client=redis_client)
verifier = AdVerifier(verbose=False)

def log(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}\n"
    with open(LOG_FILE, "a") as f:
        f.write(line)
    print(line.strip())

def handle_signal(signum, frame):
    log("Watcher daemon stopping...")
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

def get_dir_size(path):
    total = 0
    try:
        for entry in os.scandir(path):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    except Exception:
        pass
    return total

def wait_for_stability(path, timeout=60, interval=5):
    log(f"Waiting for stability in {path}...")
    start_time = time.time()
    last_size = -1
    
    while time.time() - start_time < timeout:
        current_size = get_dir_size(path)
        if current_size > 0 and current_size == last_size:
            log(f"Directory {path} is stable at {current_size} bytes.")
            return True
        last_size = current_size
        time.sleep(interval)
    
    log(f"Timeout waiting for stability in {path}.")
    return False

def process_ad(folder_name):
    incoming_path = os.path.join(INCOMING_DIR, folder_name)
    log(f"Processing new ad: {folder_name}")
    
    # Wait for stability with multiple checks
    max_stability_retries = 3
    is_stable = False
    for i in range(max_stability_retries):
        if wait_for_stability(incoming_path, timeout=60, interval=10):
            is_stable = True
            break
        log(f"Stability check {i+1}/{max_stability_retries} failed for {folder_name}. Retrying...")
    
    if not is_stable:
        log(f"Warning: Processing {folder_name} despite lack of confirmed stability.")
    
    # Validation with retries (handles slow uploads)
    max_val_retries = 3
    passed = False
    duration = 0
    renditions = 0
    
    for i in range(max_val_retries):
        passed, duration, renditions = verifier.verify_folder(incoming_path)
        if passed:
            break
        
        log(f"Validation attempt {i+1}/{max_val_retries} FAILED for {folder_name}. Waiting for potential upload completion...")
        time.sleep(15) # Wait longer between validation retries
    
    if passed:
        target_path = os.path.join(ADS_DIR, folder_name)
        if os.path.exists(target_path):
            log(f"Warning: Target folder {folder_name} already exists. Overwriting.")
            shutil.rmtree(target_path)
        
        try:
            shutil.move(incoming_path, target_path)
            os.system(f"chown -R media:media {target_path}")
            
            selector.upsert_ad(folder_name, priority=3, placement_pre=1, placement_mid=1, placement_post=1, active=1)
            
            event = {
                "folder": folder_name,
                "timestamp": time.time(),
                "duration": duration,
                "renditions": renditions
            }
            redis_client.lpush(REDIS_CHANNEL, json.dumps(event))
            log(f"Successfully registered ad: {folder_name} ({duration:.2f}s)")
        except Exception as e:
            log(f"Error moving/registering ad {folder_name}: {e}")
    else:
        log(f"Validation FAILED for {folder_name}. Moving to rejected.")
        rejected_path = os.path.join(REJECTED_DIR, folder_name)
        if os.path.exists(rejected_path):
            shutil.rmtree(rejected_path)
        shutil.move(incoming_path, rejected_path)
        
        # Log reasons
        for err in verifier.errors:
            log(f"  - {err['message']}")

def run_inotify():
    log("Starting inotify watcher...")
    # Watch for directory moves or creations in incoming
    cmd = ["inotifywait", "-m", "-e", "create", "-e", "moved_to", "--format", "%f", INCOMING_DIR]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    while True:
        line = process.stdout.readline()
        if not line:
            break
        folder_name = line.strip()
        if folder_name.startswith("advert"):
            # Check if it has master.m3u8
            if os.path.exists(os.path.join(INCOMING_DIR, folder_name, "master.m3u8")):
                try:
                    process_ad(folder_name)
                except Exception as e:
                    log(f"Unexpected error processing {folder_name}: {e}")

def run_polling():
    log("inotifywait not found. Falling back to polling every 30s...")
    while True:
        try:
            with os.scandir(INCOMING_DIR) as it:
                for entry in it:
                    if entry.is_dir() and entry.name.startswith("advert"):
                        if os.path.exists(os.path.join(entry.path, "master.m3u8")):
                            process_ad(entry.name)
        except Exception as e:
            log(f"Polling error: {e}")
        time.sleep(30)

if __name__ == "__main__":
    log("Watcher daemon started.")
    
    # Check if inotifywait exists
    if shutil.which("inotifywait"):
        run_inotify()
    else:
        run_polling()
