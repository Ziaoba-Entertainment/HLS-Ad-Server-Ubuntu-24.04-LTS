import os
import time
import psutil
import logging
import shutil
from datetime import datetime, timedelta

# CONFIGURATION
# IMPORTANT: Ensure this script runs with permissions to delete files in the OUTPUT_DIR.
# On Windows, this may require running as Administrator or a service account with Write/Delete access.
OUTPUT_DIR = r"C:\transcoder\output"
MAX_SEGMENT_AGE_MINUTES = 10
CHECK_INTERVAL_SECONDS = 60
LOG_FILE = r"C:\transcoder\reaper.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def cleanup_segments():
    """Delete .ts segments older than MAX_SEGMENT_AGE_MINUTES"""
    now = datetime.now()
    cutoff = now - timedelta(minutes=MAX_SEGMENT_AGE_MINUTES)
    
    count = 0
    try:
        if not os.path.exists(OUTPUT_DIR):
            return

        for root, dirs, files in os.walk(OUTPUT_DIR):
            for file in files:
                if file.endswith(".ts"):
                    file_path = os.path.join(root, file)
                    try:
                        mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if mtime < cutoff:
                            os.remove(file_path)
                            count += 1
                    except Exception as e:
                        logging.error(f"Failed to delete {file_path}: {e}")
    except Exception as e:
        logging.error(f"Error during segment cleanup: {e}")
    
    if count > 0:
        logging.info(f"Cleaned up {count} stale segments.")

def kill_zombie_ffmpeg():
    """Kill ffmpeg processes that have been running for too long or are orphaned"""
    # In a real scenario, we might check if the process is still active in Redis
    # For now, we'll kill any ffmpeg process older than 2 hours as a safety measure
    now = time.time()
    max_age_seconds = 7200 # 2 hours
    
    count = 0
    for proc in psutil.process_iter(['pid', 'name', 'create_time']):
        try:
            if proc.info['name'] == 'ffmpeg.exe':
                age = now - proc.info['create_time']
                if age > max_age_seconds:
                    logging.warning(f"Killing zombie ffmpeg process {proc.info['pid']} (Age: {age/60:.1f} min)")
                    proc.kill()
                    count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
            
    if count > 0:
        logging.info(f"Killed {count} zombie ffmpeg processes.")

def main():
    logging.info("Windows Reaper Service Started")
    while True:
        cleanup_segments()
        kill_zombie_ffmpeg()
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
