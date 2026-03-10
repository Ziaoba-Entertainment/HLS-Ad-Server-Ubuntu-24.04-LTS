import os
import sys
import json
import subprocess
import m3u8
import argparse

class AdVerifier:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.errors = []

    def log(self, message):
        if self.verbose:
            print(message)

    def check(self, condition, message, rendition=None, segment=None, actual=None, expected=None):
        if condition:
            self.log(f"[PASS] {message}")
            return True
        else:
            err = {
                "rendition": rendition,
                "segment": segment,
                "message": message,
                "actual": actual,
                "expected": expected
            }
            self.errors.append(err)
            print(f"[FAIL] {message}" + (f" (Actual: {actual}, Expected: {expected})" if actual is not None else ""))
            return False

    def get_ffprobe_info(self, file_path):
        cmd = [
            "ffprobe", "-v", "quiet", 
            "-print_format", "json", 
            "-show_streams", "-show_format", 
            file_path
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return json.loads(result.stdout)
        except Exception as e:
            self.check(False, f"ffprobe failed for {file_path}: {e}")
            return None

    def verify_folder(self, folder_path):
        self.errors = []
        master_path = os.path.join(folder_path, "master.m3u8")
        
        # Step 1: Playlist Structure
        if not self.check(os.path.exists(master_path), "master.m3u8 exists"):
            return False

        try:
            master = m3u8.load(master_path)
        except Exception as e:
            return self.check(False, f"Failed to parse master.m3u8: {e}")

        if not self.check(len(master.playlists) > 0, "Master playlist references at least one rendition", actual=len(master.playlists), expected=">0"):
            return False

        total_duration = 0
        rendition_count = 0

        for playlist in master.playlists:
            rendition_count += 1
            rendition_path = os.path.join(folder_path, playlist.uri)
            rendition_name = playlist.uri

            if not self.check(os.path.exists(rendition_path), f"Rendition {rendition_name} exists on disk"):
                continue

            try:
                r_m3u8 = m3u8.load(rendition_path)
            except Exception as e:
                self.check(False, f"Failed to parse rendition {rendition_name}: {e}")
                continue

            self.check(r_m3u8.target_duration is not None, f"Rendition {rendition_name} contains #EXT-X-TARGETDURATION")
            self.check(r_m3u8.is_endlist, f"Rendition {rendition_name} contains #EXT-X-ENDLIST (VOD)")
            self.check(len(r_m3u8.segments) > 0, f"Rendition {rendition_name} has at least one segment", actual=len(r_m3u8.segments))

            # Duration check (sum of segments vs target duration)
            r_duration = sum(s.duration for s in r_m3u8.segments)
            if rendition_count == 1:
                total_duration = r_duration

            # Verify segments exist and check codecs on first/last
            segments_to_probe = []
            if len(r_m3u8.segments) > 0:
                segments_to_probe.append(r_m3u8.segments[0])
                if len(r_m3u8.segments) > 1:
                    segments_to_probe.append(r_m3u8.segments[-1])

            for seg in r_m3u8.segments:
                seg_path = os.path.join(os.path.dirname(rendition_path), seg.uri)
                if not self.check(os.path.exists(seg_path), f"Segment {seg.uri} exists", rendition=rendition_name):
                    continue
                
                if seg in segments_to_probe:
                    info = self.get_ffprobe_info(seg_path)
                    if info:
                        v_stream = next((s for s in info['streams'] if s['codec_type'] == 'video'), None)
                        a_stream = next((s for s in info['streams'] if s['codec_type'] == 'audio'), None)
                        
                        self.check(v_stream and v_stream.get('codec_name') == 'h264', 
                                   f"Video codec is h264 for {seg.uri}", rendition=rendition_name, segment=seg.uri, 
                                   actual=v_stream.get('codec_name') if v_stream else "None", expected="h264")
                        
                        self.check(a_stream and a_stream.get('codec_name') == 'aac', 
                                   f"Audio codec is aac for {seg.uri}", rendition=rendition_name, segment=seg.uri,
                                   actual=a_stream.get('codec_name') if a_stream else "None", expected="aac")
                        
                        if a_stream:
                            self.check(int(a_stream.get('sample_rate', 0)) == 48000, 
                                       f"Audio sample rate is 48000 for {seg.uri}", rendition=rendition_name, segment=seg.uri,
                                       actual=a_stream.get('sample_rate'), expected="48000")
                            self.check(int(a_stream.get('channels', 0)) >= 2, 
                                       f"Audio channels >= 2 for {seg.uri}", rendition=rendition_name, segment=seg.uri,
                                       actual=a_stream.get('channels'), expected=">=2")

                        # Duration check from ffprobe
                        probe_dur = float(info.get('format', {}).get('duration', 0))
                        self.check(2.0 <= probe_dur <= 12.1, 
                                   f"Segment duration {probe_dur}s is within 2-12s range for {seg.uri}", 
                                   rendition=rendition_name, segment=seg.uri, actual=probe_dur, expected="2.0-12.0")

        success = len(self.errors) == 0
        return success, total_duration, rendition_count

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify HLS ad segments")
    parser.add_argument("folder", help="Ad folder name (e.g. advert0001)")
    parser.add_argument("--verbose", action="store_true", help="Print detailed check results")
    args = parser.parse_args()

    base_dir = "/srv/vod/ads"
    target_path = os.path.join(base_dir, args.folder)
    
    verifier = AdVerifier(verbose=args.verbose)
    passed, duration, renditions = verifier.verify_folder(target_path)

    print("\n--- Summary Report ---")
    if passed:
        print(f"STATUS: PASS")
        print(f"Duration: {duration:.2f}s")
        print(f"Renditions: {renditions}")
        sys.exit(0)
    else:
        print(f"STATUS: FAIL")
        print(f"Errors found: {len(verifier.errors)}")
        for err in verifier.errors:
            loc = f"[{err['rendition'] or 'Master'}]"
            if err['segment']: loc += f" {err['segment']}:"
            print(f"  {loc} {err['message']}")
        sys.exit(1)
