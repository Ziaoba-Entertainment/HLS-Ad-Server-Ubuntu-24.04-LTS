import sqlite3
import os
import sys
import time
import redis
import requests
import m3u8
from ad_selector import AdSelector
from playlist_builder import PlaylistBuilder

def test_sqlite():
    print("[*] Testing SQLite connection and schema...")
    try:
        selector = AdSelector()
        ads = selector.get_all_ads()
        print(f"[PASS] SQLite connected. Found {len(ads)} ads.")
        return True
    except Exception as e:
        print(f"[FAIL] SQLite test failed: {e}")
        return False

def test_ad_selector_weights():
    print("[*] Testing Ad Selector weighted distribution (1000 iterations)...")
    selector = AdSelector()
    # Ensure we have at least 2 ads with different priorities
    selector.upsert_ad("test_high", priority=1) # Weight 5
    selector.upsert_ad("test_low", priority=5)  # Weight 1
    
    counts = {"test_high": 0, "test_low": 0}
    for _ in range(1000):
        ad = selector.select_ad("pre")
        if ad['folder_name'] in counts:
            counts[ad['folder_name']] += 1
            
    total = counts["test_high"] + counts["test_low"]
    if total == 0:
        print("[FAIL] No test ads selected.")
        return False
        
    ratio = counts["test_high"] / counts["test_low"]
    # Ideal ratio is 5:1 = 5.0
    print(f"    Distribution: High={counts['test_high']}, Low={counts['test_low']}, Ratio={ratio:.2f}")
    
    if 3.5 <= ratio <= 6.5:
        print("[PASS] Weighted distribution is within acceptable range.")
        return True
    else:
        print("[FAIL] Weighted distribution is outside acceptable range (expected ~5.0).")
        return False

def test_playlist_builder():
    print("[*] Testing PlaylistBuilder logic...")
    builder = PlaylistBuilder(hls_base_url="http://test.local")
    
    # Create synthetic content playlist
    content_path = "/tmp/test_content.m3u8"
    with open(content_path, "w") as f:
        f.write("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n")
        for i in range(180): # 180 * 10s = 30 minutes
            f.write(f"#EXTINF:10.0,\nsegment_{i}.ts\n")
        f.write("#EXT-X-ENDLIST\n")
        
    # Create synthetic ad folder
    ad_dir = "/tmp/test_ad"
    os.makedirs(ad_dir, exist_ok=True)
    with open(os.path.join(ad_dir, "master.m3u8"), "w") as f:
        f.write("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:5\n")
        f.write("#EXTINF:5.0,\nad_seg_0.ts\n")
        f.write("#EXTINF:5.0,\nad_seg_1.ts\n")
        f.write("#EXT-X-ENDLIST\n")
        
    try:
        output = builder.build_stitched_playlist(content_path, ad_dir, ["pre", "mid", "post"])
        
        # Verification
        discontinuity_count = output.count("#EXT-X-DISCONTINUITY")
        # Pre-roll (1), Mid-roll 1 (1), Mid-roll 2 (1), Post-roll (1) = 4 blocks
        # Each block has a discontinuity tag at the start.
        # Total should be at least 4.
        
        ad_seg_count = output.count("ad_seg_")
        # 2 segments per block * 4 blocks = 8 segments
        
        print(f"    Found {discontinuity_count} discontinuity tags and {ad_seg_count} ad segments.")
        
        if discontinuity_count >= 4 and ad_seg_count == 8:
            print("[PASS] PlaylistBuilder correctly stitched ads and tags.")
            return True
        else:
            print("[FAIL] PlaylistBuilder output verification failed.")
            return False
    finally:
        os.remove(content_path)
        import shutil
        shutil.rmtree(ad_dir)

def test_api_health():
    print("[*] Testing FastAPI health endpoints...")
    try:
        r1 = requests.get("http://127.0.0.1:8083/health", timeout=2)
        r2 = requests.get("http://127.0.0.1:8089/health", timeout=2)
        if r1.status_code == 200 and r2.status_code == 200:
            print("[PASS] Both APIs are healthy.")
            return True
        else:
            print(f"[FAIL] API health status: {r1.status_code}, {r2.status_code}")
            return False
    except Exception as e:
        print(f"[FAIL] API health test failed: {e}")
        return False

def test_redis():
    print("[*] Testing Redis connection...")
    from config import settings
    try:
        r = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, 
                        db=settings.REDIS_DB, password=settings.REDIS_PASSWORD)
        if r.ping():
            print("[PASS] Redis is connected.")
            return True
        return False
    except Exception as e:
        print(f"[FAIL] Redis test failed: {e}")
        return False

if __name__ == "__main__":
    success = True
    success &= test_sqlite()
    success &= test_redis()
    success &= test_api_health()
    success &= test_ad_selector_weights()
    success &= test_playlist_builder()
    
    if success:
        print("\n[SUCCESS] All integration tests passed.")
        sys.exit(0)
    else:
        print("\n[FAILURE] Some integration tests failed.")
        sys.exit(1)
