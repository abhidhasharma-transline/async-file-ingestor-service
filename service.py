# service.py
# ------------------------------------------------------------
# Production-Grade Async File Transfer Service
#
# Changes in this version:
#   CHANGE #1 Removed temp/rename pattern. Files are now
#               copied directly to final destination then
#               source is deleted. Repeated/duplicate face
#               crops are fine to overwrite or miss.
#   CHANGE #2 Replaced setup_logging() with CustomLogger
#               from logger.py (date-wise files, colored
#               console, daily rotation at midnight).
#   CHANGE #3 Added .jpeg support alongside .jpg in watchdog
#               and startup scan.
#   CHANGE #4 Recursive folder support. WATCH_FOLDER can now
#               contain nested date/hour subfolders. Folder
#               structure is preserved on destination.
#               e.g. test067/2026-03-04/10/face.jpg �
#                    SHARED_DRIVE/2026-03-04/10/face.jpg
#   CHANGE #5 shutil.copy2 replaced with shutil.copyfile
#               for GVFS mount compatibility (Errno 95 fix).
#               copyfile copies only data, not metadata 
#               works on GVFS/SMB mounts that reject chmod.
# ------------------------------------------------------------

import asyncio
import shutil
import os
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
# asyncio primitives must not be created at module level 
# they bind to the wrong loop on Python 3.10+ if created here.
# ============================================================

file_queue:       asyncio.Queue = None
processing_files: set[str]      = set()
processing_lock:  asyncio.Lock  = None


# ============================================================
# HEALTH CHECK
# Lightweight  os.path.isdir only, no file write needed.
# os.scandir was removed (leaves open directory handle).
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
# Polls file size until stable  ensures file is fully written
# before transfer begins. All stat() calls in thread pool so
# event loop is never blocked.
# ============================================================

async def wait_for_file_complete(
    filepath: str,
    checks: int = 3,
    delay: float = 0.2,
) -> bool:
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
# Dedup guard  same filepath never queued twice simultaneously.
# ============================================================

async def enqueue_file(filepath: str):
    """
    Adds file to queue only if not already in-flight.
    Rolls back dedup registration if queue.put() fails.
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
# on_created used (not on_closed)  cross-OS portable.
# on_closed is unreliable on Windows/macOS watchdog backends.
#
# run_coroutine_threadsafe used to bridge watchdog OS thread �
# asyncio event loop. call_soon_threadsafe + create_task was
# unsafe (create_task must run from within the loop thread).
#
# CHANGE #4: recursive=True set in lifespan observer.schedule
#            so nested subfolders are watched automatically.
# CHANGE #3: IMAGE_EXTENSIONS covers both .jpg and .jpeg
# ============================================================

class ImageFileHandler(FileSystemEventHandler):

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def on_created(self, event):
        if event.is_directory:
            return

        # CHANGE #3: handles both .jpg and .jpeg
        if Path(event.src_path).suffix.lower() not in IMAGE_EXTENSIONS:
            return

        # Safe cross-thread coroutine scheduling
        asyncio.run_coroutine_threadsafe(
            enqueue_file(event.src_path),
            self.loop,
        )


# ============================================================
# STARTUP SCAN
# Runs AFTER observer.start()  no files missed in the gap
# between scan-end and observer-start.
#
# CHANGE #4: rglob("*{ext}") instead of glob  scans all
#            nested subfolders recursively.
# CHANGE #3: iterates both .jpg and .jpeg extensions.
# ============================================================

async def scan_existing_files():
    """Enqueue all image files already present on startup."""
    logger.info("STARTUP SCAN | Scanning recursively for existing image files")

    count = 0
    for ext in IMAGE_EXTENSIONS:
        # CHANGE #4: rglob walks all subdirectories
        # Before: Path(WATCH_FOLDER).glob(f"*{ext}")  flat only
        # After:  Path(WATCH_FOLDER).rglob(f"*{ext}")  recursive
        for file in Path(WATCH_FOLDER).rglob(f"*{ext}"):
            if file.is_file():
                await enqueue_file(str(file))
                count += 1

    logger.info(f"STARTUP SCAN | {count} existing file(s) queued")


# ============================================================
# TRANSFER LOGIC
#
# CHANGE #1: Removed temp/rename pattern.
#   Direct copy � delete source (no _tmp_ intermediate file).
#
# CHANGE #4: Folder structure preserved on destination.
#   Before: all files flat in SHARED_DRIVE/filename
#   After:  relative path from WATCH_FOLDER preserved:
#     source:  WATCH_FOLDER/2026-03-04/10/face.jpg
#     dest:    SHARED_DRIVE/2026-03-04/10/face.jpg
#   Destination subfolders created automatically if missing.
#
# CHANGE #5: shutil.copyfile instead of shutil.copy2.
#   copy2 copies metadata (timestamps, permissions) which
#   GVFS SMB mounts reject with Errno 95 (Operation not
#   supported). copyfile copies only raw file data  works
#   on GVFS, CIFS, and standard mounts equally.
#
# All blocking I/O in asyncio.to_thread  event loop free.
# ============================================================

async def copy_to_shared_drive(filepath: str) -> bool:
    """
    Copies a single file to the shared drive preserving the
    subfolder structure relative to WATCH_FOLDER, then deletes
    the source file.

    Retry strategy:
      - Exponential backoff: 2^n seconds (max 120s)
      - After MAX_RETRIES attempts: 2-minute cooldown, reset
      - Retries indefinitely until delivered
    """
    filename = Path(filepath).name

    # CHANGE #4: preserve relative path from WATCH_FOLDER
    # e.g. /home/anpr/Downloads/test067/2026-03-04/10/face.jpg
    #   relative_path = 2026-03-04/10/face.jpg
    #   final_dest    = SHARED_DRIVE/2026-03-04/10/face.jpg
    try:
        relative_path = Path(filepath).relative_to(WATCH_FOLDER)
    except ValueError:
        # filepath is not under WATCH_FOLDER  use filename only
        logger.warning(
            f"PATH NOT RELATIVE | {filename} | "
            f"falling back to flat destination"
        )
        relative_path = Path(filename)

    final_dest     = os.path.join(SHARED_DRIVE, relative_path)
    dest_dir       = os.path.dirname(final_dest)

    attempt = 0

    while True:
        attempt += 1

        # Source existence check
        if not await asyncio.to_thread(os.path.exists, filepath):
            logger.warning(f"SOURCE MISSING | {filename} | skipping")
            return False

        # File stability  wait until fully written to disk
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
            # CHANGE #4: create destination subfolder if missing
            # e.g. SHARED_DRIVE/2026-03-04/10/ may not exist yet
            await asyncio.to_thread(os.makedirs, dest_dir, exist_ok=True)
            logger.debug(f"DEST DIR OK | {dest_dir}")

            # CHANGE #5: copyfile (data only)  GVFS compatible
            # copy2 was rejected by GVFS with Errno 95 because
            # it tries to set timestamps/permissions on the dest.
            await asyncio.to_thread(shutil.copyfile, filepath, final_dest)
            logger.debug(
                f"COPY OK | {filename} � "
                f"{relative_path}"
            )

            # Delete source only after copy confirmed
            await asyncio.to_thread(os.remove, filepath)
            logger.debug(f"SOURCE DELETED | {filepath}")

            logger.info(
                f"TRANSFER SUCCESS | {relative_path} | "
                f"attempts={attempt}"
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

        # Retry cycle exhausted  2-minute cooldown then reset
        if attempt % MAX_RETRIES == 0:
            logger.warning(
                f"RETRY CYCLE EXHAUSTED | {filename} | "
                f"{MAX_RETRIES} attempts done | 2-min cooldown"
            )
            await asyncio.sleep(120)
            attempt = 0


# ============================================================
# WORKERS
# Pull filepaths from queue and call copy_to_shared_drive.
# None sentinel = shutdown signal from lifespan.
# finally block always releases processing_files entry.
# ============================================================

async def worker(worker_id: int):
    logger.info(f"WORKER {worker_id} | Ready")

    while True:
        filepath = await file_queue.get()

        try:
            if filepath is None:
                logger.info(f"WORKER {worker_id} | Shutdown signal  exiting")
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
# LIFESPAN  startup and graceful shutdown
#
# Startup order (order matters):
#   1. Validate config
#   2. Create event-loop-bound primitives (queue, lock)
#   3. Initial drive check
#   4. Start workers
#   5. Start health monitor
#   6. Start watchdog observer  � BEFORE scan (closes race gap)
#   7. Scan existing files      � AFTER observer is live
#
# CHANGE #4: observer.schedule recursive=True  watches all
#            nested subfolders under WATCH_FOLDER.
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
    logger.info(f"Recursive watch   : Yes")
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
        logger.warning("STARTUP | Shared drive unreachable  will retry")

    # Start workers
    worker_tasks = [
        asyncio.create_task(worker(i + 1))
        for i in range(MAX_WORKERS)
    ]
    logger.info(f"STARTUP | {MAX_WORKERS} worker(s) started")

    # Start health monitor
    health_task = asyncio.create_task(health_monitor())

    # CHANGE #4: recursive=True  watches all nested subfolders
    # Before: recursive=False  only top-level WATCH_FOLDER
    # After:  recursive=True   all date/hour subfolders watched
    loop     = asyncio.get_event_loop()
    observer = Observer()
    observer.schedule(
        ImageFileHandler(loop),
        WATCH_FOLDER,
        recursive=True,   # CHANGE #4
    )
    observer.start()
    logger.info(f"WATCHDOG | Monitoring recursively: {WATCH_FOLDER}")

    # Scan existing files AFTER observer is live
    await scan_existing_files()

    logger.info("SERVICE READY")
    logger.info("=" * 60)

    yield  # service runs here

    #    GRACEFUL SHUTDOWN                                     
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