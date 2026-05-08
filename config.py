import os
from dotenv import load_dotenv

load_dotenv()

WATCH_FOLDER          = os.getenv("WATCH_FOLDER", r"C:\cropped_faces")
SHARED_DRIVE          = os.getenv("SHARED_DRIVE", r"\\100.11.98.122\received_faces")
MAX_WORKERS           = int(os.getenv("MAX_WORKERS", 4))
MAX_RETRIES           = int(os.getenv("MAX_RETRIES", 5))
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", 10))
QUEUE_MAXSIZE         = int(os.getenv("QUEUE_MAXSIZE", 5000))

# CHANGE #3: Both .jpg and .jpeg supported
IMAGE_EXTENSIONS = {".jpg", ".jpeg"}
