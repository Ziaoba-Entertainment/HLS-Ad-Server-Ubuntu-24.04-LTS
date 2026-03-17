import redis
import json
import logging
import os
import sqlite3
from config import settings

# LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("adserver.events")

# CONFIG
REDIS_PASS = settings.REDIS_PASSWORD
REDIS_DB = 0  # Transcoder events are on DB 0 as per spec
REDIS_HOST = settings.REDIS_HOST
REDIS_PORT = settings.REDIS_PORT
CHANNEL = "transcoder:events"
DB_PATH = settings.DB_PATH

def update_ad_status(job_id, status, metadata=None):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Try to find by job_id first
        cursor.execute("SELECT id, folder_name FROM ads WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        
        if not row and metadata and metadata.get("folder_name"):
            # If not found by job_id, try folder_name
            cursor.execute("SELECT id FROM ads WHERE folder_name = ?", (metadata["folder_name"],))
            row = cursor.fetchone()

        if row:
            ad_id = row["id"]
            folder_name = row["folder_name"]
            
            update_fields = ["status = ?", "updated_at = datetime('now')"]
            params = [status]
            
            if job_id:
                update_fields.append("job_id = ?")
                params.append(job_id)

            if metadata:
                if "description" in metadata:
                    update_fields.append("ad_description = ?")
                    params.append(metadata["description"])
                if "advertiser" in metadata:
                    update_fields.append("advertiser_name = ?")
                    params.append(metadata["advertiser"])
                if "campaign" in metadata:
                    update_fields.append("campaign_name = ?")
                    params.append(metadata["campaign"])
                if "max_plays" in metadata:
                    update_fields.append("max_plays = ?")
                    params.append(metadata["max_plays"])
                if "active" in metadata:
                    update_fields.append("active = ?")
                    params.append(1 if metadata["active"] else 0)

            params.append(ad_id)
            cursor.execute(f"UPDATE ads SET {', '.join(update_fields)} WHERE id = ?", params)
            conn.commit()
            logger.info(f"Updated ad {folder_name} status to {status}")
        else:
            # If it's a new ad and we have metadata, insert it
            if metadata and metadata.get("folder_name"):
                folder_name = metadata["folder_name"]
                cursor.execute("""
                    INSERT INTO ads (folder_name, job_id, status, ad_description, advertiser_name, campaign_name, max_plays, active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    folder_name, job_id, status, 
                    metadata.get("description", ""), 
                    metadata.get("advertiser", ""), 
                    metadata.get("campaign", ""),
                    metadata.get("max_plays", 0),
                    1 if metadata.get("active", True) else 0
                ))
                conn.commit()
                logger.info(f"Created new ad entry for {folder_name} with status {status}")
        
        conn.close()
    except Exception as e:
        logger.error(f"Database update failed: {e}")

def handle_event(event_data):
    try:
        event_type = event_data.get("event")
        job_id = event_data.get("job_id")
        logger.info(f"Received event: {event_type} for job {job_id}")
        
        if event_type == "job_queued":
            update_ad_status(job_id, "In Queue", event_data)
            
        elif event_type == "transcoding_started":
            update_ad_status(job_id, "Encoding", event_data)

        elif event_type == "transcoding_completed":
            update_ad_status(job_id, "Ready", event_data)
            
        elif event_type == "transcoding_failed":
            update_ad_status(job_id, "Failed", event_data)

        elif event_type == "encoding_profile_updated":
            # Logic to revalidate ad profiles
            logger.info("Encoding profiles updated, clearing manifest cache")
            r_ad = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, password=REDIS_PASS)
            keys = r_ad.keys("ad:manifest_cache:*")
            if keys:
                r_ad.delete(*keys)
                
        elif event_type == "ad_upload_complete":
            # Logic to register new ad campaign
            ad_folder = event_data.get("folder_name")
            logger.info(f"New ad uploaded: {ad_folder}")
            # Could trigger scan_ads.py logic here
            
    except Exception as e:
        logger.error(f"Error handling event: {e}")

def main():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, 
                        password=REDIS_PASS, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe(CHANNEL)
        
        logger.info(f"Subscribed to {CHANNEL} on Redis DB {REDIS_DB}")
        
        for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    handle_event(data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON received: {message['data']}")
                    
    except Exception as e:
        logger.error(f"Redis listener failed: {e}")

if __name__ == "__main__":
    main()
