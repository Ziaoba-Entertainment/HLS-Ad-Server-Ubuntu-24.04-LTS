import m3u8
import os

class PlaylistBuilder:
    def __init__(self, hls_base_url="http://127.0.0.1:8081"):
        self.hls_base_url = hls_base_url

    def _get_absolute_url(self, base_path, relative_path):
        # If it's already an absolute URL, return it
        if relative_path.startswith('http'):
            return relative_path
        
        # Construct absolute URL for nginx delivery
        # base_path is something like /srv/vod/hls/movies/Title
        # relative_path is stream_0.ts
        # We want http://127.0.0.1:8081/hls/movies/Title/stream_0.ts
        
        # Remove /srv/vod prefix for URL mapping
        url_path = base_path.replace('/srv/vod', '')
        return f"{self.hls_base_url}{url_path}/{relative_path}"

    def build_stitched_playlist(self, content_m3u8_path, ad_folder_path, placements: list) -> str:
        """
        content_m3u8_path: Absolute path to the content .m3u8 file
        ad_folder_path: Absolute path to the ad folder (e.g. /srv/vod/ads/advert0001/)
        placements: List of strings ['pre', 'mid', 'post']
        """
        content_playlist = m3u8.load(content_m3u8_path)
        ad_master_path = os.path.join(ad_folder_path, 'master.m3u8')
        
        # For simplicity, we assume the ad has a single media playlist or we use the first one
        ad_master = m3u8.load(ad_master_path)
        if ad_master.is_variant:
            # Load the first variant
            ad_media_url = ad_master.playlists[0].uri
            ad_media_path = os.path.join(ad_folder_path, ad_media_url)
            ad_playlist = m3u8.load(ad_media_path)
            ad_base_path = os.path.dirname(ad_media_path)
        else:
            ad_playlist = ad_master
            ad_base_path = ad_folder_path

        new_playlist = m3u8.M3U8()
        new_playlist.target_duration = max(content_playlist.target_duration, ad_playlist.target_duration)
        new_playlist.version = content_playlist.version
        new_playlist.media_sequence = content_playlist.media_sequence

        content_base_path = os.path.dirname(content_m3u8_path)

        # 1. Pre-roll
        if 'pre' in placements:
            self._append_ad_segments(new_playlist, ad_playlist, ad_base_path)

        # 2. Content with Mid-rolls
        cumulative_duration = 0.0
        last_midroll_time = 0.0
        midroll_interval = 600.0 # 10 minutes

        first_content_seg = True
        for segment in content_playlist.segments:
            # Check for mid-roll
            if 'mid' in placements and cumulative_duration - last_midroll_time >= midroll_interval:
                self._append_ad_segments(new_playlist, ad_playlist, ad_base_path)
                last_midroll_time = cumulative_duration
                first_content_seg = True # Force discontinuity after mid-roll

            # Add content segment
            new_seg = m3u8.Segment(
                uri=self._get_absolute_url(content_base_path, segment.uri),
                duration=segment.duration,
                title=segment.title,
                base_uri=None
            )
            if first_content_seg:
                new_seg.discontinuity = True
                first_content_seg = False
            
            new_playlist.add_segment(new_seg)
            cumulative_duration += segment.duration

        # 3. Post-roll
        if 'post' in placements:
            self._append_ad_segments(new_playlist, ad_playlist, ad_base_path)

        # Finalize
        if content_playlist.is_endlist:
            new_playlist.is_endlist = True

        return new_playlist.dumps()

    def _append_ad_segments(self, target_playlist, ad_playlist, ad_base_path):
        first_ad_seg = True
        for seg in ad_playlist.segments:
            new_seg = m3u8.Segment(
                uri=self._get_absolute_url(ad_base_path, seg.uri),
                duration=seg.duration,
                title=seg.title,
                base_uri=None
            )
            if first_ad_seg:
                new_seg.discontinuity = True
                first_ad_seg = False
            target_playlist.add_segment(new_seg)

    def rewrite_master_playlist(self, master_path, content_type, original_path):
        """
        Rewrites master.m3u8 variants to point to our middleware
        """
        master = m3u8.load(master_path)
        base_path = os.path.dirname(original_path)
        
        for playlist in master.playlists:
            # Original: stream_0.m3u8
            # New: /playlist/movies/Title/stream_0.m3u8
            playlist.uri = f"/playlist/{content_type}/{base_path}/{playlist.uri}"
            
        return master.dumps()
