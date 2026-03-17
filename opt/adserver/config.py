import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Public Facing
    NGINX_PUBLIC_PORT: int = 80
    PUBLIC_BASE_URL: str = "https://stream.ziaoba.com"

    # Ad Server Components
    FASTAPI_PORT: int = 8083
    FASTAPI_HOST: str = "127.0.0.1"
    ADSERVER_UI_PORT: int = 8082
    INTERNAL_FASTAPI_URL: str = "http://127.0.0.1:8083"

    # Transcoder Components
    TRANSCODER_UI_PORT: int = 8081
    TRANSCODER_API_URL: str = "http://127.0.0.1:6666/api"

    # Security
    TRUSTED_PROXY: str = "127.0.0.1"
    LOCAL_SUBNET: str = "192.168.0.0/24"

    # Paths
    BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
    DB_PATH: str = os.path.join(BASE_DIR, "adserver.db")
    
    # These will be overridden by env vars if present
    ADS_PATH: str = "/srv/vod/ads"
    HLS_PATH: str = "/srv/vod/hls"

    def get_ads_path(self) -> str:
        if os.path.exists(self.ADS_PATH):
            return self.ADS_PATH
        # Fallback to relative path from BASE_DIR
        return os.path.abspath(os.path.join(self.BASE_DIR, "../../srv/vod/ads"))

    def get_hls_path(self) -> str:
        if os.path.exists(self.HLS_PATH):
            return self.HLS_PATH
        # Fallback to relative path from BASE_DIR
        return os.path.abspath(os.path.join(self.BASE_DIR, "../../srv/vod/hls"))
    
    # Redis
    REDIS_DB: int = 1
    REDIS_PREFIX: str = "ad:"
    REDIS_PASS: str = ""

    # Logic
    MID_ROLL_INTERVAL: int = 600
    HLS_VERSION: int = 3
    SEGMENT_DURATION: int = 6

    class Config:
        env_file = ".env"
        extra = "allow"

settings = Settings()
