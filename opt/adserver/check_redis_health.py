import os
import redis
import sys
import json
from config import settings

def check_health():
    print(f"[*] Reading Redis credentials from {settings.Config.env_file}")
    print(f"[*] Host: {settings.REDIS_HOST}, Port: {settings.REDIS_PORT}, DB: {settings.REDIS_DB}")
    
    try:
        r = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=5
        )
        
        # 1. Validate Connection
        if r.ping():
            print("[PASS] Redis connection validated.")
        else:
            print("[FAIL] Redis ping failed.")
            return False
            
        # 2. Check Queue Health
        # Check 'transcoder:events' (DB 0)
        r0 = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=0,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=5
        )
        
        queues = ["transcoder:events", "ad:ad_registered_events"]
        health_data = {}
        
        for q in queues:
            # Note: transcoder:events might be a channel (pubsub) or a list
            # In redis_listener.py it's a channel.
            # ad:ad_registered_events in watch_ads.py is a list (lpush)
            
            if q == "ad:ad_registered_events":
                q_len = r.llen(q)
                print(f"[*] Queue '{q}' length: {q_len}")
                health_data[q] = {"type": "list", "length": q_len}
            else:
                # For pubsub channels, we can check number of subscribers
                channels = r0.pubsub_channels(pattern=q)
                num_subs = r0.pubsub_numsub(q)
                print(f"[*] Channel '{q}' active: {q in channels}, Subscribers: {num_subs}")
                health_data[q] = {"type": "channel", "active": q in channels, "subscribers": num_subs}
        
        # 3. Check Ad Registry
        registry_size = r.zcard("ad:ad_registry")
        print(f"[*] Ad Registry size: {registry_size}")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] Redis health check failed: {e}")
        return False

if __name__ == "__main__":
    if check_health():
        print("\n[SUCCESS] Redis health check passed.")
        sys.exit(0)
    else:
        print("\n[FAILURE] Redis health check failed.")
        sys.exit(1)
