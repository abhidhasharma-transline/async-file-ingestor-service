# service.py
# ------------------------------------------------------------
# Production-Grade Async File Transfer Service
#
# Features:
#   ✅ Async worker pool
#   ✅ Duplicate protection
#   ✅ Startup folder scan
#   ✅ File stability verification
#   ✅ SMB/network-share safe transfer
#   ✅ Infinite retry with cooldown
#   ✅ Queue backpressure
#   ✅ Graceful shutdown
#   ✅ Rotating logs
#   ✅ Cross-platform safe
# ------------------------------------------------------------

import asyncio
import shutil
import os
import sys
import uuid
import logging
import logging.handlers

from pathlib import Path
from contextlib import asynccontextmanager

import aiofiles
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ============================================================
# CONFIG
# ============================================================

WATCH_FOLDER = r"C:\cropped_faces"
SHARED_DRIVE = r"\\100.11.98.122\received_faces"

MAX_WORKERS = 4
MAX_RETRIES = 5

HEALTH_CHECK_INTERVAL = 10

QUEUE_MAXSIZE = 10000

LOG_FILE = "service.log"

# ============================================================
# LOGGING
# ============================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("file_transfer")

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger


logger = setup_logging()

# ============================================================
# GLOBALS
# ============================================================

file_queue: asyncio.Queue = None

# Prevent duplicate processing
processing_files: set[str] = set()

# Lock protects processing_files set
processing_lock = asyncio.Lock()

# ============================================================
# HEALTH CHECK
# ============================================================

async def is_drive_accessible() -> bool:
    """
    Lightweight shared-drive availability check.
    """
    try:
        await asyncio.to_thread(os.scandir, SHARED_DRIVE)
        return True
    except Exception as e:
        logger.warning(f"HEALTH CHECK FAILED | {e}")
        return False


async def health_monitor():
    logger.info(
        f"HEALTH MONITOR | Started | Interval={HEALTH_CHECK_INTERVAL}s"
    )

    while True:
        ok = await is_drive_accessible()

        if ok:
            logger.debug("HEALTH MONITOR | Shared drive reachable")
        else:
            logger.warning(
                "HEALTH MONITOR | Shared drive unreachable"
            )

        await asyncio.sleep(HEALTH_CHECK_INTERVAL)

# ============================================================
# FILE STABILITY CHECK
# ============================================================

async def wait_for_file_complete(
    filepath: str,
    checks: int = 3,
    delay: float = 1.0,
) -> bool:
    """
    Wait until file size stabilizes.
    Prevents partial-copy issues.
    """

    previous_size = -1

    for _ in range(checks):
        try:
            current_size = await asyncio.to_thread(os.path.getsize, filepath)

            if current_size == previous_size:
                return True

            previous_size = current_size

        except FileNotFoundError:
            return False

        await asyncio.sleep(delay)

    return False

# ============================================================
# QUEUE HELPERS
# ============================================================

async def enqueue_file(filepath: str):
    """
    Adds file to queue only if not already processing.
    """

    async with processing_lock:

        if filepath in processing_files:
            logger.debug(f"DUPLICATE SKIPPED | {filepath}")
            return

        processing_files.add(filepath)

    try:
        await file_queue.put(filepath)

        logger.info(
            f"QUEUED | {Path(filepath).name} | "
            f"Queue={file_queue.qsize()}"
        )

    except Exception:
        async with processing_lock:
            processing_files.discard(filepath)
        raise


async def release_processing(filepath: str):
    async with processing_lock:
        processing_files.discard(filepath)

# ============================================================
# TRANSFER LOGIC
# ============================================================

async def move_to_shared_drive(filepath: str) -> bool:

    filename = Path(filepath).name

    unique_id = uuid.uuid4().hex[:8]

    temp_dest = os.path.join(
        SHARED_DRIVE,
        f"_tmp_{unique_id}_{filename}"
    )

    final_dest = os.path.join(
        SHARED_DRIVE,
        filename
    )

    attempt = 0

    while True:

        attempt += 1

        # ----------------------------------------------------
        # Source file existence
        # ----------------------------------------------------

        if not os.path.exists(filepath):
            logger.warning(
                f"SOURCE MISSING | {filename}"
            )
            return False

        # ----------------------------------------------------
        # Wait until file fully written
        # ----------------------------------------------------

        stable = await wait_for_file_complete(filepath)

        if not stable:
            logger.warning(
                f"FILE NOT STABLE | {filename}"
            )
            await asyncio.sleep(2)
            continue

        # ----------------------------------------------------
        # Drive health
        # ----------------------------------------------------

        if not await is_drive_accessible():

            wait = min(2 ** attempt, 120)

            logger.warning(
                f"DRIVE DOWN | {filename} | "
                f"Retry in {wait}s"
            )

            await asyncio.sleep(wait)
            continue

        try:

            # ------------------------------------------------
            # Copy to temp
            # ------------------------------------------------

            await asyncio.to_thread(
                shutil.copy2,
                filepath,
                temp_dest,
            )

            logger.debug(
                f"COPY OK | {filename}"
            )

            # ------------------------------------------------
            # Atomic replace
            # Safer for SMB/network shares
            # ------------------------------------------------

            await asyncio.to_thread(
                os.replace,
                temp_dest,
                final_dest,
            )

            logger.debug(
                f"RENAME OK | {filename}"
            )

            # ------------------------------------------------
            # Delete source
            # ------------------------------------------------

            await asyncio.to_thread(
                os.remove,
                filepath,
            )

            logger.info(
                f"TRANSFER SUCCESS | "
                f"{filename} | "
                f"Attempts={attempt}"
            )

            return True

        except FileNotFoundError as e:

            logger.warning(
                f"FILE NOT FOUND | {filename} | {e}"
            )

            return False

        except PermissionError as e:

            wait = min(2 ** attempt, 120)

            logger.error(
                f"PERMISSION ERROR | {filename} | "
                f"{e} | Retry in {wait}s"
            )

            await asyncio.sleep(wait)

        except OSError as e:

            wait = min(2 ** attempt, 120)

            logger.error(
                f"OS ERROR | {filename} | "
                f"{e} | Retry in {wait}s"
            )

            await asyncio.sleep(wait)

        except Exception as e:

            wait = min(2 ** attempt, 120)

            logger.exception(
                f"UNEXPECTED ERROR | {filename} | "
                f"Retry in {wait}s | {e}"
            )

            await asyncio.sleep(wait)

        # ----------------------------------------------------
        # Retry cooldown
        # ----------------------------------------------------

        if attempt % MAX_RETRIES == 0:

            logger.warning(
                f"RETRY CYCLE EXHAUSTED | "
                f"{filename} | Cooling down 120s"
            )

            await asyncio.sleep(120)

            attempt = 0

# ============================================================
# WORKERS
# ============================================================

async def worker(worker_id: int):

    logger.info(f"WORKER {worker_id} | Ready")

    while True:

        filepath = await file_queue.get()

        try:

            if filepath is None:

                logger.info(
                    f"WORKER {worker_id} | Shutdown"
                )

                return

            logger.info(
                f"WORKER {worker_id} | "
                f"Processing={Path(filepath).name} | "
                f"Queue={file_queue.qsize()}"
            )

            await move_to_shared_drive(filepath)

        finally:

            if filepath is not None:
                await release_processing(filepath)

            file_queue.task_done()

# ============================================================
# WATCHDOG
# ============================================================

class JPEGFileHandler(FileSystemEventHandler):

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
    ):
        self.loop = loop

    def on_created(self, event):

        if event.is_directory:
            return

        if not event.src_path.lower().endswith(".jpg"):
            return

        filepath = event.src_path

        asyncio.run_coroutine_threadsafe(
            enqueue_file(filepath),
            self.loop
        )

# ============================================================
# STARTUP SCAN
# ============================================================

async def scan_existing_files():

    logger.info(
        "STARTUP SCAN | Scanning existing JPG files"
    )

    count = 0

    for file in Path(WATCH_FOLDER).glob("*.jpg"):

        if file.is_file():

            await enqueue_file(str(file))
            count += 1

    logger.info(
        f"STARTUP SCAN | {count} existing file(s) queued"
    )

# ============================================================
# LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan():

    global file_queue

    logger.info("=" * 60)
    logger.info("FILE TRANSFER SERVICE | STARTING")
    logger.info("=" * 60)

    logger.info(f"Watch Folder : {WATCH_FOLDER}")
    logger.info(f"Shared Drive : {SHARED_DRIVE}")
    logger.info(f"Workers      : {MAX_WORKERS}")
    logger.info(f"Queue Max    : {QUEUE_MAXSIZE}")

    os.makedirs(WATCH_FOLDER, exist_ok=True)

    file_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    # --------------------------------------------------------
    # Initial drive check
    # --------------------------------------------------------

    if await is_drive_accessible():
        logger.info("STARTUP | Shared drive reachable")
    else:
        logger.warning(
            "STARTUP | Shared drive unavailable"
        )

    # --------------------------------------------------------
    # Workers
    # --------------------------------------------------------

    worker_tasks = [
        asyncio.create_task(worker(i + 1))
        for i in range(MAX_WORKERS)
    ]

    logger.info(
        f"STARTUP | {MAX_WORKERS} worker(s) started"
    )

    # --------------------------------------------------------
    # Health monitor
    # --------------------------------------------------------

    health_task = asyncio.create_task(
        health_monitor()
    )

    # --------------------------------------------------------
    # Startup scan
    # --------------------------------------------------------

    await scan_existing_files()

    # --------------------------------------------------------
    # Watchdog
    # --------------------------------------------------------

    loop = asyncio.get_event_loop()

    observer = Observer()

    observer.schedule(
        JPEGFileHandler(loop),
        WATCH_FOLDER,
        recursive=False,
    )

    observer.start()
    await scan_existing_files()   

    logger.info(
        f"WATCHDOG | Monitoring={WATCH_FOLDER}"
    )

    logger.info("SERVICE READY")
    logger.info("=" * 60)

    yield

    # ========================================================
    # SHUTDOWN
    # ========================================================

    logger.info("SHUTDOWN | Stopping service")

    observer.stop()
    observer.join(timeout=5)

    logger.info("SHUTDOWN | Watchdog stopped")

    # --------------------------------------------------------
    # Stop workers
    # --------------------------------------------------------

    for _ in range(MAX_WORKERS):
        await file_queue.put(None)

    await asyncio.gather(*worker_tasks)

    logger.info("SHUTDOWN | Workers stopped")

    # --------------------------------------------------------
    # Stop health monitor
    # --------------------------------------------------------

    health_task.cancel()

    try:
        await health_task
    except asyncio.CancelledError:
        pass

    logger.info("SHUTDOWN | Health monitor stopped")

    logger.info("SERVICE STOPPED")
    logger.info("=" * 60)

# ============================================================
# MAIN
# ============================================================

async def main():

    async with lifespan():

        try:
            while True:
                await asyncio.sleep(1)

        except (
            asyncio.CancelledError,
            KeyboardInterrupt,
        ):
            pass

# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("SERVICE INTERRUPTED")
