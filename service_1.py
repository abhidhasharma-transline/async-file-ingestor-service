# service.py
# ------------------------------------------------------------
# Production-Grade Async File Transfer Service
#
# Changes in this version:
#   CHANGE #1 — Removed temp/rename pattern. Files are now
#               copied directly to final destination then
#               source is deleted. Repeated/duplicate face
#               crops are fine to overwrite or miss.
#   CHANGE #2 — Replaced setup_logging() with CustomLogger
#               from logger.py (date-wise files, colored
#               console, daily rotation at midnight).
#   CHANGE #3 — Added .jpeg support alongside .jpg in watchdog
#               and startup scan.
# ------------------------------------------------------------

import asyncio
import shutil
import os
import sys
from pathlib import Path
from contextlib import asynccontextmanager
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from logger import get_logger
from config import (
    WATCH_FOLDER,
    SHARED_DRIVE,
    MAX_WORKERS,
    MAX_RETRIES,
    HEALTH_CHECK_INTERVAL,
    QUEUE_MAXSIZE,
    IMAGE_EXTENSIONS,
)

logger = get_logger("transfer_service")



def _validate_config():
    """Fail fast on startup if required config is missing."""
    errors = []
    if not WATCH_FOLDER:
        errors.append("WATCH_FOLDER is not set in .env")
    if not SHARED_DRIVE:
        errors.append("SHARED_DRIVE is not set in .env")
    if errors:
        raise ValueError("Config errors:\n" + "\n".join(errors))


# ============================================================
# GLOBALS
# Initialised inside lifespan() after event loop is running.
# asyncio primitives must not be created at module level.
# ============================================================

file_queue:       asyncio.Queue = None
processing_files: set[str]      = set()
processing_lock:  asyncio.Lock  = None


# ============================================================
# HEALTH CHECK
# Lightweight — os.path.isdir only, no file write.
# ============================================================

async def is_drive_accessible() -> bool:
    try:
        result = await asyncio.to_thread(os.path.isdir, SHARED_DRIVE)
        if not result:
            logger.warning("HEALTH CHECK FAILED | Path is not a directory")
        return result
    except Exception as e:
        logger.warning(f"HEALTH CHECK FAILED | {e}")
        return False


async def health_monitor():
    logger.info(f"HEALTH MONITOR | Started | Interval={HEALTH_CHECK_INTERVAL}s")

    while True:
        ok = await is_drive_accessible()
        if ok:
            logger.debug("HEALTH MONITOR | Shared drive reachable")
        else:
            logger.warning("HEALTH MONITOR | Shared drive unreachable")
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


# ============================================================
# FILE STABILITY CHECK
# Polls size via asyncio.to_thread — never blocks event loop.
# ============================================================

async def wait_for_file_complete(
    filepath: str,
    checks: int = 3,
    delay: float = 0.2,
) -> bool:
    """
    Waits until file size is stable across `checks` reads.
    All stat() calls run in thread pool — event loop stays free.
    """
    previous_size = -1

    for _ in range(checks):
        try:
            current_size = await asyncio.to_thread(os.path.getsize, filepath)

            if current_size == previous_size and current_size > 0:
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
    Adds a file to the transfer queue only if not already
    in-flight (dedup guard via processing_files set).
    """
    async with processing_lock:
        if filepath in processing_files:
            logger.debug(f"DUPLICATE SKIPPED | {Path(filepath).name}")
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
    """Remove file from in-flight set after transfer completes."""
    async with processing_lock:
        processing_files.discard(filepath)


# ============================================================
# WATCHDOG
# Uses on_created + wait_for_file_complete for cross-OS safety.
# on_closed is unreliable on Windows/macOS watchdog backends.
#
# run_coroutine_threadsafe used (not call_soon_threadsafe +
# create_task) — correct way to schedule coroutines from a
# non-async OS thread.
#
# CHANGE #3: Checks IMAGE_EXTENSIONS set {.jpg, .jpeg}
# ============================================================

class ImageFileHandler(FileSystemEventHandler):

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def on_created(self, event):
        if event.is_directory:
            return

        # CHANGE #3: was only .jpg — now handles .jpeg too
        if Path(event.src_path).suffix.lower() not in IMAGE_EXTENSIONS:
            return

        asyncio.run_coroutine_threadsafe(
            enqueue_file(event.src_path),
            self.loop,
        )


# ============================================================
# STARTUP SCAN
# Observer starts before this runs — no files missed in gap.
# CHANGE #3: glob covers both *.jpg and *.jpeg
# ============================================================

async def scan_existing_files():
    """Enqueue any image files already present on startup."""
    logger.info("STARTUP SCAN | Scanning for existing image files")

    count = 0
    for ext in IMAGE_EXTENSIONS:
        # CHANGE #3: iterate both extensions
        for file in Path(WATCH_FOLDER).glob(f"*{ext}"):
            if file.is_file():
                await enqueue_file(str(file))
                count += 1

    logger.info(f"STARTUP SCAN | {count} existing file(s) queued")


# ============================================================
# TRANSFER LOGIC
#
# CHANGE #1: Removed temp/rename pattern entirely.
#   Before: copy → _tmp_<uuid>_<name> → os.replace → delete src
#   After:  copy → final destination directly → delete src
#
# Reason: cropped face images are repeated/similar — missing
# one or overwriting is acceptable. The extra rename step added
# latency and complexity that is not needed here.
#
# All blocking I/O still runs in asyncio.to_thread so the
# event loop is never blocked.
# ============================================================

async def copy_to_shared_drive(filepath: str) -> bool:
    """
    Copies a single file directly to the shared drive destination,
    then deletes the source.

    Retry strategy:
      - Exponential backoff: 2^n seconds per attempt (max 120s)
      - After MAX_RETRIES attempts: 2-minute cooldown, reset
      - Retries indefinitely until delivered
    """
    filename   = Path(filepath).name
    # CHANGE #1: direct final path — no temp file
    final_dest = os.path.join(SHARED_DRIVE, filename)

    attempt = 0

    while True:
        attempt += 1

        # Source existence check
        if not await asyncio.to_thread(os.path.exists, filepath):
            logger.warning(f"SOURCE MISSING | {filename} | skipping")
            return False

        # File stability — wait until fully written to disk
        stable = await wait_for_file_complete(filepath)
        if not stable:
            logger.warning(f"FILE NOT STABLE | {filename} | retrying")
            await asyncio.sleep(2)
            continue

        # Drive availability
        if not await is_drive_accessible():
            wait = min(2 ** attempt, 120)
            logger.warning(
                f"DRIVE DOWN | {filename} | "
                f"attempt={attempt} | retry in {wait}s"
            )
            await asyncio.sleep(wait)
            continue

        try:
            # CHANGE #1: direct copy to final_dest (no temp)
            await asyncio.to_thread(shutil.copyfile, filepath, final_dest)
            logger.debug(f"COPY OK | {filename} → {SHARED_DRIVE}")

            # Delete source only after copy confirmed
            await asyncio.to_thread(os.remove, filepath)
            logger.debug(f"SOURCE DELETED | {filepath}")

            logger.info(
                f"TRANSFER SUCCESS | {filename} | attempts={attempt}"
            )
            return True

        except FileNotFoundError as e:
            logger.warning(f"FILE NOT FOUND | {filename} | {e} | skipping")
            return False

        except PermissionError as e:
            wait = min(2 ** attempt, 120)
            logger.error(
                f"PERMISSION ERROR | {filename} | {e} | retry in {wait}s"
            )
            await asyncio.sleep(wait)

        except OSError as e:
            wait = min(2 ** attempt, 120)
            logger.error(
                f"OS ERROR | {filename} | {e} | retry in {wait}s"
            )
            await asyncio.sleep(wait)

        except Exception as e:
            wait = min(2 ** attempt, 120)
            logger.exception(
                f"UNEXPECTED ERROR | {filename} | retry in {wait}s | {e}"
            )
            await asyncio.sleep(wait)

        # Retry cycle exhausted — 2-minute cooldown then reset
        if attempt % MAX_RETRIES == 0:
            logger.warning(
                f"RETRY CYCLE EXHAUSTED | {filename} | "
                f"{MAX_RETRIES} attempts done | 2-min cooldown"
            )
            await asyncio.sleep(120)
            attempt = 0


# ============================================================
# WORKERS
# ============================================================

async def worker(worker_id: int):
    """
    Dequeues filepaths and calls copy_to_shared_drive.
    Exits cleanly on None sentinel (shutdown signal).
    Always releases processing_files entry in finally.
    """
    logger.info(f"WORKER {worker_id} | Ready")

    while True:
        filepath = await file_queue.get()

        try:
            if filepath is None:
                logger.info(f"WORKER {worker_id} | Shutdown signal — exiting")
                return

            logger.info(
                f"WORKER {worker_id} | "
                f"Processing={Path(filepath).name} | "
                f"Queue={file_queue.qsize()}"
            )

            await copy_to_shared_drive(filepath)

        finally:
            if filepath is not None:
                await release_processing(filepath)
            file_queue.task_done()


# ============================================================
# LIFESPAN
# Startup order matters:
#   1. Validate config
#   2. Create event-loop-bound primitives (queue, lock)
#   3. Initial drive check
#   4. Start workers
#   5. Start health monitor
#   6. Start watchdog observer  ← before scan (no gap)
#   7. Scan existing files      ← after observer is live
# ============================================================

@asynccontextmanager
async def lifespan():
    global file_queue, processing_lock, processing_files

    logger.info("=" * 60)
    logger.info("FILE TRANSFER SERVICE | STARTING")
    logger.info("=" * 60)
    logger.info(f"Watch Folder      : {WATCH_FOLDER}")
    logger.info(f"Shared Drive      : {SHARED_DRIVE}")
    logger.info(f"Workers           : {MAX_WORKERS}")
    logger.info(f"Retries/cycle     : {MAX_RETRIES} then 2-min cooldown")
    logger.info(f"Health interval   : {HEALTH_CHECK_INTERVAL}s")
    logger.info(f"Queue max         : {QUEUE_MAXSIZE}")
    logger.info(f"Image extensions  : {IMAGE_EXTENSIONS}")
    logger.info("=" * 60)

    _validate_config()

    os.makedirs(WATCH_FOLDER, exist_ok=True)

    # Initialise async primitives inside running event loop
    file_queue       = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    processing_lock  = asyncio.Lock()
    processing_files = set()

    # Initial drive check
    if await is_drive_accessible():
        logger.info("STARTUP | Shared drive reachable")
    else:
        logger.warning("STARTUP | Shared drive unreachable — will retry")

    # Start workers
    worker_tasks = [
        asyncio.create_task(worker(i + 1))
        for i in range(MAX_WORKERS)
    ]
    logger.info(f"STARTUP | {MAX_WORKERS} worker(s) started")

    # Start health monitor
    health_task = asyncio.create_task(health_monitor())

    # Start watchdog BEFORE scan — no files missed in the gap
    loop     = asyncio.get_event_loop()
    observer = Observer()
    observer.schedule(ImageFileHandler(loop), WATCH_FOLDER, recursive=True)
    observer.start()
    logger.info(f"WATCHDOG | Monitoring: {WATCH_FOLDER}")

    # Scan existing files AFTER observer is live
    await scan_existing_files()

    logger.info("SERVICE READY")
    logger.info("=" * 60)

    yield  # service runs here

    # ── GRACEFUL SHUTDOWN ────────────────────────────────────
    logger.info("SHUTDOWN | Stopping service")

    observer.stop()
    observer.join(timeout=5)
    if observer.is_alive():
        logger.warning("SHUTDOWN | Watchdog did not stop within timeout")
    logger.info("SHUTDOWN | Watchdog stopped")

    for _ in range(MAX_WORKERS):
        await file_queue.put(None)
    await asyncio.gather(*worker_tasks)
    logger.info("SHUTDOWN | Workers stopped")

    health_task.cancel()
    try:
        await health_task
    except asyncio.CancelledError:
        pass
    logger.info("SHUTDOWN | Health monitor stopped")

    logger.info("SERVICE STOPPED")
    logger.info("=" * 60)


# ============================================================
# MAIN + ENTRY
# ============================================================

async def main():
    async with lifespan():
        try:
            while True:
                await asyncio.sleep(1)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("SERVICE INTERRUPTED")
