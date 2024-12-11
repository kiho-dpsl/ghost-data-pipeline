import os

import pytz
from dotenv import load_dotenv
from pathlib import Path


BASE_DIR = Path(__name__).absolute().parent
DATA_DIR = BASE_DIR / "data"
load_dotenv(BASE_DIR / ".env")
# timezone
KST = pytz.timezone('Asia/Seoul')
# ghost
GHOST_ADMIN_API_KEY = os.getenv('GHOST_ADMIN_API_KEY')
GHOST_API_URL = os.getenv('GHOST_API_URL')
GHOST_ADMIN_URL = f"{GHOST_API_URL}/ghost/api/admin/"
# google
GOOGLE_API_CREDENTIALS = BASE_DIR / "auth" / "google" / "google_api_credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_API_CREDENTIALS)
PROJECT_ID = os.getenv('PROJECT_ID')
RAW_DATASET = "ghost_raw"
STATS_DATASET = "ghost_statistics"
MEMBER_EN_STATS_TABLE = "member_en_stats"
MEMBER_KO_STATS_TABLE = "member_ko_stats"
SUBSCRIBER_EN_STATS_TABLE = "subscriber_en_stats"
SUBSCRIBER_KO_STATS_TABLE = "subscriber_ko_stats"
NEWSLETTER_EN_STATS_TABLE = "newsletter_en_stats"
NEWSLETTER_KO_STATS_TABLE = "newsletter_ko_stats"
LOCATION="asia-northeast1"