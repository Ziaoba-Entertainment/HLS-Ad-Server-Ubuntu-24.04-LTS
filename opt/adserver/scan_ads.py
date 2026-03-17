import os
import sys
import argparse
from ad_selector import AdSelector
from verify_ad_segments import AdVerifier

def main():
    parser = argparse.ArgumentParser(description="Scan and register HLS ads")
    parser.add_argument("--dry-run", action="store_true", help="Validate but do not update DB")
    parser.add_argument("--rescan", action="store_true", help="Re-validate existing ads in DB")
    args = parser.parse_args()

    ads_dir = "/srv/vod/ads"
    from config import settings
    import redis
    try:
        redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB, password=settings.REDIS_PASSWORD, decode_responses=True)
        redis_client.ping()
    except:
        redis_client = None
        
    selector = AdSelector(redis_client=redis_client)
    verifier = AdVerifier(verbose=False)
    
    existing_ads = {ad['folder_name']: ad for ad in selector.get_all_ads()}
    
    results = []
    all_valid = True

    print(f"{'Folder':<15} | {'Status':<10} | {'Duration':<10} | {'Rends':<6} | {'DB Action':<15}")
    print("-" * 65)

    with os.scandir(ads_dir) as it:
        for entry in it:
            if entry.is_dir() and entry.name.startswith("advert"):
                folder_name = entry.name
                
                # Skip if already in DB and not rescanning
                if folder_name in existing_ads and not args.rescan:
                    continue

                passed, duration, renditions = verifier.verify_folder(entry.path)
                
                db_action = "None"
                status = "PASS" if passed else "FAIL"
                
                if not passed:
                    all_valid = False
                    if folder_name in existing_ads:
                        db_action = "Deactivate"
                        if not args.dry_run:
                            selector.update_ad(existing_ads[folder_name]['id'], {"active": 0})
                else:
                    if folder_name not in existing_ads:
                        db_action = "Insert"
                        if not args.dry_run:
                            selector.upsert_ad(folder_name, priority=3, placement_pre=1, placement_mid=1, placement_post=1, active=1)
                    elif args.rescan:
                        db_action = "Activate"
                        if not args.dry_run:
                            selector.update_ad(existing_ads[folder_name]['id'], {"active": 1})

                print(f"{folder_name:<15} | {status:<10} | {duration:>8.2f}s | {renditions:<6} | {db_action:<15}")
                results.append(passed)

    if not all_valid:
        print("\n[!] Some ads failed validation.")
        sys.exit(1)
    else:
        print("\n[+] All scanned ads are valid.")
        sys.exit(0)

if __name__ == "__main__":
    main()
