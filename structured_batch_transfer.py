# service.py
# ------------------------------------------------------------
# Production-Grade Batch Async File Transfer Service
#
# Architecture:
#   - Watchdog detects new images → raw_queue
#   - Batcher: har 5 seconds mein raw_queue se max 1000 files
#     uthao → batch_queue mein daalo as a list
#   - 10 async workers batch_queue se batch uthao →
#     drive check (sirf ek baar per batch) →
#     asyncio.gather se parallel copy →
#     confirmed files alag delete_task mein delete
#   - Metrics: har 1 second mein files/sec + MB/sec log
#
# Key decisions:
#   - Drive check: sirf batch start pe, har file pe nahi
#   - wait_for_file_complete: removed — cropper writes
#     complete files, polling overhead at 71fps too expensive
#   - Delete: alag asyncio.Task — copy workers block nahi hote
#   - Source folder untouched — sirf images delete hoti hain
#   - Folder structure preserved: WATCH/batch/date/hour/img
#     → SHARED/batch/date/hour/img
# ------------------------------------------------------------

import asyncio
import shutil
import os
import time
from collections import deque
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
    METRICS_WINDOW_SECONDS,
    BATCH_SIZE,
    BATCH_INTERVAL_SECONDS,
    CONCURRENT_COPIES,       # max parallel copies per batch
)

logger         = get_logger("transfer")
metrics_logger = get_logger("metrics")


# ============================================================
# CONFIG VALIDATION
# ============================================================

def _validate_config():
    errors = []
    if not WATCH_FOLDER:
        errors.append("WATCH_FOLDER is not set in .env")
    if not SHARED_DRIVE:
        errors.append("SHARED_DRIVE is not set in .env")
    if errors:
        raise ValueError("Config errors:\n" + "\n".join(errors))


# ============================================================
# GLOBALS
# All asyncio primitives created inside lifespan() —
# never at module level (wrong event loop on Python 3.10+)
# ============================================================

# raw_queue: individual filepaths from watchdog
raw_queue: asyncio.Queue = None

# batch_queue: List[str] — groups of up to BATCH_SIZE files
batch_queue: asyncio.Queue = None

# dedup — prevents same file queued twice simultaneously
processing_files: set[str]  = set()
processing_lock:  asyncio.Lock = None

# Semaphore — limits concurrent disk/network I/O per batch.
# Without this, 1000 parallel asyncio.to_thread calls would
# spawn 1000 OS threads causing disk contention and thrashing.
# CONCURRENT_COPIES (default 20) keeps parallelism controlled.
copy_semaphore: asyncio.Semaphore = None

# delete_queue: filepaths confirmed copied → delete worker picks up.
# Single queue + single worker replaces per-batch fire-and-forget
# tasks which caused delete I/O storms under high batch throughput.
delete_queue: asyncio.Queue = None

# metrics
metrics_lock:            asyncio.Lock = None
transfer_events:         deque        = deque()
total_files_transferred: int          = 0
total_bytes_transferred: int          = 0
current_window_bytes:    int          = 0


def _now() -> float:
    return time.monotonic()


# ============================================================
# HEALTH CHECK
# Called once per batch — not per file.
# Lightweight: os.path.isdir only, no file write.
# ============================================================

async def is_drive_accessible() -> bool:
    try:
        result = await asyncio.to_thread(os.path.isdir, SHARED_DRIVE)
        if not result:
            logger.warning("HEALTH CHECK FAILED | Not a directory")
        return result
    except Exception as e:
        logger.warning(f"HEALTH CHECK FAILED | {e}")
        return False


async def health_monitor():
    logger.info(f"HEALTH MONITOR | Started | Interval={HEALTH_CHECK_INTERVAL}s")
    while True:
        ok = await is_drive_accessible()
        if ok:
            # ✅ This will show on terminal
            logger.info("✅ HEALTH CHECK | Drive reachable")
        else:
            logger.warning("❌ HEALTH CHECK | Drive unreachable")
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


# ============================================================
# METRICS
# Tracks files/sec and MB/sec over a rolling window.
# Logged every 1 second to metrics.log
# ============================================================

async def record_transfer(size_bytes: int):
    global total_files_transferred, total_bytes_transferred, current_window_bytes
    async with metrics_lock:
        transfer_events.append((_now(), size_bytes))
        total_files_transferred += 1
        total_bytes_transferred += size_bytes
        current_window_bytes    += size_bytes


async def metrics_monitor():
    global current_window_bytes
    logger.info(f"METRICS MONITOR | Started | Window={METRICS_WINDOW_SECONDS}s")

    while True:
        await asyncio.sleep(1)

        now    = _now()
        cutoff = now - METRICS_WINDOW_SECONDS

        async with metrics_lock:
            # Evict events outside the rolling window
            while transfer_events and transfer_events[0][0] < cutoff:
                _, old_size = transfer_events.popleft()
                current_window_bytes -= old_size
            current_window_bytes = max(0, current_window_bytes)
            window_files = len(transfer_events)

        window_secs  = float(METRICS_WINDOW_SECONDS)
        mb_per_sec   = current_window_bytes / 1024 / 1024 / window_secs
        files_per_sec = window_files / window_secs

        metrics_logger.info(
            f"METRICS | "
            f"files/sec={files_per_sec:.1f} | "
            f"MB/sec={mb_per_sec:.2f} | "
            f"raw_queue={raw_queue.qsize()} | "
            f"batch_queue={batch_queue.qsize()} | "
            f"in_flight={len(processing_files)} | "
            f"total_files={total_files_transferred} | "
            f"total_MB={total_bytes_transferred/1024/1024:.2f}"
        )


# ============================================================
# DEDUP HELPERS
# ============================================================

async def enqueue_file(filepath: str):
    """
    Add filepath to raw_queue only if not already in-flight.
    Rolls back on queue failure.
    """
    async with processing_lock:
        if filepath in processing_files:
            logger.debug(f"DUPLICATE SKIPPED | {Path(filepath).name}")
            return
        processing_files.add(filepath)

    try:
        await raw_queue.put(filepath)
    except Exception:
        async with processing_lock:
            processing_files.discard(filepath)
        raise


async def release_files(filepaths: list[str]):
    """Release multiple files from in-flight set."""
    async with processing_lock:
        for fp in filepaths:
            processing_files.discard(fp)


# ============================================================
# BATCHER
# Runs continuously — every BATCH_INTERVAL_SECONDS drains
# raw_queue into a batch (max BATCH_SIZE) and pushes the
# batch as a list into batch_queue for workers to pick up.
# ============================================================

async def batcher():
    """
    Aggregates individual filepaths into batches.

    Every BATCH_INTERVAL_SECONDS:
      - Drain up to BATCH_SIZE items from raw_queue
      - Push as List[str] into batch_queue
      - Workers pick up whole batches — not individual files

    This reduces per-file overhead and allows asyncio.gather
    to copy many files in parallel per batch.
    """
    logger.info(
        f"BATCHER | Started | "
        f"interval={BATCH_INTERVAL_SECONDS}s | "
        f"max_batch={BATCH_SIZE}"
    )

    while True:
        await asyncio.sleep(BATCH_INTERVAL_SECONDS)

        batch = []
        try:
            # Drain up to BATCH_SIZE without blocking
            while len(batch) < BATCH_SIZE:
                filepath = raw_queue.get_nowait()
                batch.append(filepath)
                raw_queue.task_done()
        except asyncio.QueueEmpty:
            pass

        if not batch:
            continue

        logger.info(f"BATCHER | Batch of {len(batch)} files ready")
        await batch_queue.put(batch)


# ============================================================
# SINGLE FILE COPY
# No drive check here — done once per batch in the worker.
# No wait_for_file_complete — removed (expensive at 71fps,
# cropper writes complete files before closing handle).
# os.makedirs called per unique dest_dir within the batch.
# ============================================================

async def copy_one(filepath: str) -> tuple[str, int]:
    """
    Copy one file to shared drive preserving folder structure.

    Returns:
        (filepath, 1) on success
        (filepath, -1) on failure
    """

    filename = Path(filepath).name

    try:
        relative_path = Path(filepath).relative_to(WATCH_FOLDER)
    except ValueError:
        relative_path = Path(filename)
        logger.warning(
            f"PATH NOT RELATIVE | {filename} | using flat dest"
        )

    final_dest = os.path.join(SHARED_DRIVE, relative_path)
    dest_dir   = os.path.dirname(final_dest)

    try:
        # Always safe.
        # exist_ok=True prevents errors if folder already exists.
        await asyncio.to_thread(
            os.makedirs,
            dest_dir,
            exist_ok=True
        )

        # Copy file
        t0 = _now()

        await asyncio.to_thread(
            shutil.copyfile,
            filepath,
            final_dest
        )

        elapsed = _now() - t0

        # Minimal validation:
        # just ensure destination is not zero-byte
        dest_size = await asyncio.to_thread(
            os.path.getsize,
            final_dest
        )

        if dest_size <= 0:
            logger.error(
                f"ZERO BYTE FILE | {final_dest}"
            )
            return filepath, -1

        logger.info(
            f"COPY OK | {relative_path} | "
            f"{dest_size/1024:.1f} KB | "
            f"{elapsed*1000:.1f}ms"
        )

        return filepath, dest_size

    except FileNotFoundError as e:
        logger.warning(
            f"FILE NOT FOUND | {filename} | {e}"
        )
        return filepath, -1

    except OSError as e:
        logger.error(
            f"OS ERROR | {filename} | {e}"
        )
        return filepath, -1

    except Exception as e:
        logger.exception(
            f"COPY ERROR | {filename} | {e}"
        )
        return filepath, -1


# ============================================================
# DELETE WORKER
# Single background worker — consumes delete_queue sequentially.
#
# WHY single worker instead of per-batch fire-and-forget tasks:
#   Before: asyncio.create_task(delete_files(batch)) per batch
#           → dozens of parallel delete tasks under high load
#           → disk I/O storm, unpredictable latency
#   After:  one delete_queue + one delete_worker
#           → sequential controlled deletes
#           → copy workers never blocked (just enqueue and move on)
#           → no storm regardless of batch throughput
# ============================================================

async def delete_worker():
    """
    Single long-running coroutine that drains delete_queue.

    Each item in delete_queue is a List[str] — a full batch of
    confirmed-copied filepaths. Worker deletes the entire batch
    before picking up the next one.

    Why batch-at-a-time (not file-at-a-time):
      - Copy workers confirm the whole batch then enqueue as list
      - Deleting the batch together keeps source in sync with what
        was actually transferred — no partial state
      - Still sequential between batches — no I/O storm
    """
    logger.info("DELETE WORKER | Started")
    deleted_total = 0

    while True:
        batch = await delete_queue.get()

        try:
            if batch is None:
                logger.info(
                    f"DELETE WORKER | Shutdown | "
                    f"total deleted={deleted_total}"
                )
                return

            deleted_in_batch = 0
            for filepath in batch:
                try:
                    await asyncio.to_thread(os.remove, filepath)
                    deleted_in_batch += 1
                    logger.debug(f"DELETED | {Path(filepath).name}")
                except FileNotFoundError:
                    pass  # Already gone — fine
                except Exception as e:
                    logger.error(f"DELETE ERROR | {filepath} | {e}")

            deleted_total += deleted_in_batch
            logger.info(
                f"DELETE WORKER | Batch deleted={deleted_in_batch} | "
                f"total={deleted_total}"
            )

        except Exception as e:
            logger.exception(f"DELETE WORKER | Unexpected error | {e}")
        finally:
            delete_queue.task_done()


# ============================================================
# BATCH WORKER
# Picks up a batch (List[str]) from batch_queue.
# Drive check: ONCE per batch.
# Copy: asyncio.gather — all files in batch run concurrently.
# Delete: fire-and-forget asyncio.Task after copy confirm.
# Retry: exponential backoff on drive down only.
#        Per-file copy errors are logged and skipped (not
#        retried) — at 71fps missing one crop is acceptable.
# ============================================================

async def batch_worker(worker_id: int):
    logger.info(f"WORKER {worker_id} | Ready")

    while True:
        batch = await batch_queue.get()

        try:
            if batch is None:
                logger.info(f"WORKER {worker_id} | Shutdown — exiting")
                return

            logger.info(
                f"WORKER {worker_id} | "
                f"Batch={len(batch)} files | "
                f"batch_queue={batch_queue.qsize()}"
            )

            # ── Drive check — once per batch ─────────────────
            attempt = 0
            while not await is_drive_accessible():
                attempt += 1
                wait = min(2 ** attempt, 120)
                logger.warning(
                    f"WORKER {worker_id} | DRIVE DOWN | "
                    f"attempt={attempt} | retry in {wait}s"
                )
                await asyncio.sleep(wait)

            # Semaphore limits concurrent copies to CONCURRENT_COPIES.
            # Each copy_one acquires the semaphore before doing I/O —
            # so at most CONCURRENT_COPIES files copy simultaneously
            # regardless of batch size. Prevents disk/network thrashing.
            async def copy_with_sem(fp):
                async with copy_semaphore:
                    return await copy_one(fp)

            results = await asyncio.gather(
                *[copy_with_sem(fp) for fp in batch],
                return_exceptions=False,
            )

            # ── Separate confirmed copies from failures ───────
            succeeded = [fp for fp, sz in results if sz >= 0]
            failed    = [fp for fp, sz in results if sz < 0]
            sizes     = [sz for _, sz  in results if sz >= 0]

            logger.info(
                f"WORKER {worker_id} | "
                f"Batch done: {len(succeeded)} ok, {len(failed)} failed"
            )

            # ── Record metrics ────────────────────────────────
            for sz in sizes:
                await record_transfer(sz)

            # ── Enqueue entire confirmed batch for deletion ────
            # Push the whole succeeded list as one item.
            # delete_worker deletes the full batch before next.
            # Copy worker does not wait — free for next batch immediately.
            if succeeded:
                await delete_queue.put(succeeded)

            # ── Release failed files from dedup set ──────────
            # So they can be re-queued if watchdog sees them again
            if failed:
                await release_files(failed)

        except Exception as e:
            logger.exception(f"WORKER {worker_id} | Batch error | {e}")

        finally:
            if batch and batch[0] is not None:
                try:
                    # results might not exist if exception occurred before gather
                    succeeded_fps = [fp for fp, sz in results if sz >= 0]
                except (NameError, UnboundLocalError):
                    # If results doesn't exist, release whole batch
                    succeeded_fps = batch
                await release_files(succeeded_fps)
            batch_queue.task_done()

# ============================================================
# WATCHDOG
# on_created: new file written → enqueue_file → raw_queue
# on_moved:   file renamed into folder → enqueue_file
# recursive=True: watches all nested date/hour subfolders
# run_coroutine_threadsafe: safe bridge watchdog thread → loop
# ============================================================

class ImageFileHandler(FileSystemEventHandler):

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def _enqueue(self, path: str):
        asyncio.run_coroutine_threadsafe(
            enqueue_file(path), self.loop
        )

    def on_created(self, event):
        if event.is_directory:
            return
        if Path(event.src_path).suffix.lower() in IMAGE_EXTENSIONS:
            self._enqueue(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        if Path(event.dest_path).suffix.lower() in IMAGE_EXTENSIONS:
            self._enqueue(event.dest_path)


# ============================================================
# STARTUP SCAN
# Runs after observer.start() — no gap where files are missed.
# rglob: recursive — scans all nested date/hour subfolders.
# ============================================================

async def scan_existing_files():
    logger.info("STARTUP SCAN | Scanning recursively")
    count = 0
    for ext in IMAGE_EXTENSIONS:
        for file in Path(WATCH_FOLDER).rglob(f"*{ext}"):
            if file.is_file():
                await enqueue_file(str(file))
                count += 1
    logger.info(f"STARTUP SCAN | {count} file(s) queued")


# ============================================================
# LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan():
    global raw_queue, batch_queue, processing_lock, \
           processing_files, metrics_lock, copy_semaphore, delete_queue

    logger.info("FILE TRANSFER SERVICE | STARTING")
    logger.info(f"Watch Folder     : {WATCH_FOLDER}")
    logger.info(f"Shared Drive     : {SHARED_DRIVE}")
    logger.info(f"Workers          : {MAX_WORKERS}")
    logger.info(f"Batch size       : {BATCH_SIZE}")
    logger.info(f"Batch interval   : {BATCH_INTERVAL_SECONDS}s")
    logger.info(f"Health interval  : {HEALTH_CHECK_INTERVAL}s")
    logger.info(f"Metrics window   : {METRICS_WINDOW_SECONDS}s")
    logger.info(f"Queue max        : {QUEUE_MAXSIZE}")
    logger.info(f"Extensions       : {IMAGE_EXTENSIONS}")

    _validate_config()
    os.makedirs(WATCH_FOLDER, exist_ok=True)

    # Async primitives — created inside running event loop
    raw_queue        = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    batch_queue      = asyncio.Queue()
    delete_queue     = asyncio.Queue()
    processing_lock  = asyncio.Lock()
    metrics_lock     = asyncio.Lock()
    processing_files = set()

    # Semaphore init — must be inside running event loop
    # CONCURRENT_COPIES controls max simultaneous file copies
    # across ALL workers combined (shared semaphore).
    # Tune in .env: higher = more parallelism but more disk load
    # Recommended: 10-30 for SMB/GVFS mounts
    copy_semaphore   = asyncio.Semaphore(CONCURRENT_COPIES)

    # Initial drive check
    if await is_drive_accessible():
        logger.info("STARTUP | Shared drive reachable")
    else:
        logger.warning("STARTUP | Shared drive unreachable — workers will retry")

    # Start batch workers
    worker_tasks = [
        asyncio.create_task(batch_worker(i + 1))
        for i in range(MAX_WORKERS)
    ]
    logger.info(f"STARTUP | {MAX_WORKERS} batch worker(s) started")

    # Start batcher
    batcher_task = asyncio.create_task(batcher())

    # Start single delete worker
    # One worker — sequential deletes, no I/O storm
    delete_task = asyncio.create_task(delete_worker())

    # Start monitors
    health_task  = asyncio.create_task(health_monitor())
    metrics_task = asyncio.create_task(metrics_monitor())

    # Start watchdog BEFORE scan
    loop     = asyncio.get_event_loop()
    observer = Observer()
    observer.schedule(ImageFileHandler(loop), WATCH_FOLDER, recursive=True)
    observer.start()
    logger.info(f"WATCHDOG | Monitoring: {WATCH_FOLDER}")

    # Scan existing files AFTER observer is live
    await scan_existing_files()

    logger.info("SERVICE READY")
    yield

    # ── GRACEFUL SHUTDOWN ────────────────────────────────────
    logger.info("SHUTDOWN | Stopping")

    observer.stop()
    observer.join(timeout=5)
    if observer.is_alive():
        logger.warning("SHUTDOWN | Watchdog timeout")
    logger.info("SHUTDOWN | Watchdog stopped")

    batcher_task.cancel()
    try:
        await batcher_task
    except asyncio.CancelledError:
        pass

    # Send None sentinel to each worker
    for _ in range(MAX_WORKERS):
        await batch_queue.put(None)
    await asyncio.gather(*worker_tasks)
    logger.info("SHUTDOWN | Workers stopped")

    # Drain remaining deletes then shut down delete worker
    # Wait for all pending deletes to complete before exit
    await delete_queue.join()
    await delete_queue.put(None)
    await delete_task
    logger.info("SHUTDOWN | Delete worker stopped")

    for task in [health_task, metrics_task]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    logger.info("SERVICE STOPPED")


# ============================================================
# MAIN
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
