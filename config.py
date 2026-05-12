import os
from dotenv import load_dotenv

load_dotenv()

WATCH_FOLDER          = os.getenv("WATCH_FOLDER")
SHARED_DRIVE          = os.getenv("SHARED_DRIVE")
MAX_WORKERS           = int(os.getenv("MAX_WORKERS"))
MAX_RETRIES           = int(os.getenv("MAX_RETRIES"))
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL"))
QUEUE_MAXSIZE         = int(os.getenv("QUEUE_MAXSIZE"))
METRICS_WINDOW_SECONDS = int(os.getenv("METRICS_WINDOW_SECONDS"))

# CHANGE #3: Both .jpg and .jpeg supported
IMAGE_EXTENSIONS = {".jpg", ".jpeg"}
