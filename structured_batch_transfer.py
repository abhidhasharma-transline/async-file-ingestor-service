# service.py
# ------------------------------------------------------------
# Production-Grade Batch Async File Transfer Service
#
# Architecture:
#   - Watchdog detects new images → raw_queue
#   - Batcher: every BATCH_INTERVAL_SECONDS drains raw_queue
#     up to BATCH_SIZE files → batch_queue as List[str]
#   - Batch workers: drive check once per batch →
#     asyncio.gather + semaphore (CONCURRENT_COPIES) →
#     confirmed batch → delete_queue
#   - Delete worker: single worker, parallel deletes within
#     batch (semaphore 10), sequential between batches
#   - Metrics: every 1s → files/sec + MB/sec → metrics.log
#
# Key decisions:
#   - Drive check: once per batch only
#   - wait_for_file_complete: removed — cropper writes complete
#     files, polling at 71fps too expensive
#   - os.path.exists: removed — EAFP, FileNotFoundError caught
#   - os.stat: once on dest after copy — zero byte check only
#   - dest_dirs_created set: removed — makedirs exist_ok safe
#   - Delete: single queue + single worker with internal
#     parallelism — no task storm
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
    HEALTH_CHECK_INTERVAL,
    QUEUE_MAXSIZE,
    IMAGE_EXTENSIONS,
    METRICS_WINDOW_SECONDS,
    BATCH_SIZE,
    BATCH_INTERVAL_SECONDS,
    CONCURRENT_COPIES,
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
# All asyncio primitives initialised inside lifespan() —
# never at module level (binds to wrong loop on Python 3.10+)
# ============================================================

raw_queue:        asyncio.Queue     = None  # individual filepaths from watchdog
batch_queue:      asyncio.Queue     = None  # List[str] batches for workers
delete_queue:     asyncio.Queue     = None  # List[str] confirmed batches to delete
processing_files: set               = set() # dedup: in-flight filepath set
processing_lock:  asyncio.Lock      = None
copy_semaphore:   asyncio.Semaphore = None  # limits concurrent copies
metrics_lock:     asyncio.Lock      = None

# rolling metrics state
transfer_events:         deque = deque()
total_files_transferred: int   = 0
total_bytes_transferred: int   = 0
current_window_bytes:    int   = 0


def _now() -> float:
    return time.monotonic()


# ============================================================
# HEALTH CHECK
# Lightweight — os.path.isdir, no file write.
# Called once per batch in batch_worker, not per file.
# Also polled by health_monitor for observability.
# ============================================================

async def is_drive_accessible() -> bool:
    try:
        result = await asyncio.to_thread(os.path.isdir, SHARED_DRIVE)
        if not result:
            logger.warning("HEALTH CHECK | Path is not a directory")
        return result
    except Exception as e:
        logger.warning(f"HEALTH CHECK | Failed: {e}")
        return False


async def health_monitor():
    logger.info(f"HEALTH MONITOR | Started | interval={HEALTH_CHECK_INTERVAL}s")
    while True:
        ok = await is_drive_accessible()
        if ok:
            logger.debug("HEALTH MONITOR | Drive reachable")
        else:
            logger.warning("HEALTH MONITOR | Drive unreachable")
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


# ============================================================
# METRICS
# Rolling window — files/sec and MB/sec logged every 1 second.
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
    logger.info(f"METRICS MONITOR | Started | window={METRICS_WINDOW_SECONDS}s")

    while True:
        await asyncio.sleep(1)

        now    = _now()
        cutoff = now - METRICS_WINDOW_SECONDS

        async with metrics_lock:
            while transfer_events and transfer_events[0][0] < cutoff:
                _, old_size = transfer_events.popleft()
                current_window_bytes -= old_size
            current_window_bytes = max(0, current_window_bytes)
            window_files = len(transfer_events)

        window_secs   = float(METRICS_WINDOW_SECONDS)
        mb_per_sec    = current_window_bytes / 1024 / 1024 / window_secs
        files_per_sec = window_files / window_secs

        metrics_logger.info(
            f"METRICS | "
            f"files/sec={files_per_sec:.1f} | "
            f"MB/sec={mb_per_sec:.2f} | "
            f"raw_q={raw_queue.qsize()} | "
            f"batch_q={batch_queue.qsize()} | "
            f"delete_q={delete_queue.qsize()} | "
            f"in_flight={len(processing_files)} | "
            f"total_files={total_files_transferred} | "
            f"total_MB={total_bytes_transferred/1024/1024:.2f}"
        )


# ============================================================
# DEDUP HELPERS
# Prevents the same filepath being queued twice simultaneously.
# ============================================================

async def enqueue_file(filepath: str):
    """
    Add filepath to raw_queue only if not already in-flight.
    Rolls back dedup registration on queue failure.
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


async def release_files(filepaths: list):
    """Remove filepaths from in-flight dedup set."""
    async with processing_lock:
        for fp in filepaths:
            processing_files.discard(fp)


# ============================================================
# BATCHER
# Every BATCH_INTERVAL_SECONDS: drain raw_queue up to
# BATCH_SIZE → push List[str] into batch_queue.
# ============================================================

async def batcher():
    logger.info(
        f"BATCHER | Started | "
        f"interval={BATCH_INTERVAL_SECONDS}s | "
        f"max={BATCH_SIZE}"
    )

    while True:
        await asyncio.sleep(BATCH_INTERVAL_SECONDS)

        batch = []
        try:
            while len(batch) < BATCH_SIZE:
                filepath = raw_queue.get_nowait()
                batch.append(filepath)
                raw_queue.task_done()
        except asyncio.QueueEmpty:
            pass

        if not batch:
            continue

        logger.info(f"BATCHER | Batch ready | size={len(batch)}")
        await batch_queue.put(batch)


# ============================================================
# SINGLE FILE COPY
#
# No os.path.exists check — EAFP: watchdog confirmed file
# exists; FileNotFoundError is caught below. Saves one syscall
# per file.
#
# os.makedirs with exist_ok=True on every file — filesystem
# is already idempotent; removed dest_dirs_created set which
# had a race condition between concurrent coroutines.
#
# Verification: single os.stat on destination AFTER copy.
# Zero-byte check catches silent truncations on SMB/GVFS.
# Source stat removed — trust copyfile completed fully.
# ============================================================

async def copy_one(filepath: str) -> tuple:
    """
    Copy one file to shared drive, preserving folder structure.

    Returns:
        (filepath, dest_size_bytes)  on success
        (filepath, -1)               on failure
    """
    filename = Path(filepath).name

    try:
        relative_path = Path(filepath).relative_to(WATCH_FOLDER)
    except ValueError:
        relative_path = Path(filename)
        logger.warning(f"PATH NOT RELATIVE | {filename} | flat dest used")

    final_dest = os.path.join(SHARED_DRIVE, relative_path)
    dest_dir   = os.path.dirname(final_dest)

    try:
        # exist_ok=True — safe to call every time, no race condition.
        # Removed dest_dirs_created set optimisation which introduced
        # a race between concurrent coroutines sharing the same set.
        await asyncio.to_thread(os.makedirs, dest_dir, exist_ok=True)

        t0 = _now()

        # copyfile: data only — GVFS/SMB compatible.
        # copy2 fails on GVFS with Errno 95 (metadata not supported).
        await asyncio.to_thread(shutil.copyfile, filepath, final_dest)

        elapsed = _now() - t0

        # Single os.stat on destination after copy.
        # Zero-byte check catches truncated transfers.
        # Source stat removed — no race, fewer syscalls.
        stat      = await asyncio.to_thread(os.stat, final_dest)
        dest_size = stat.st_size

        if dest_size <= 0:
            logger.error(f"ZERO BYTE DEST | {filename} | removing")
            try:
                await asyncio.to_thread(os.remove, final_dest)
            except Exception:
                pass
            return filepath, -1

        logger.info(
            f"COPY OK | {relative_path} | "
            f"{dest_size/1024:.1f} KB | {elapsed*1000:.1f}ms"
        )
        return filepath, dest_size

    except FileNotFoundError as e:
        logger.warning(f"FILE NOT FOUND | {filename} | {e}")
        return filepath, -1

    except OSError as e:
        logger.error(f"OS ERROR | {filename} | {e}")
        return filepath, -1

    except Exception as e:
        logger.exception(f"COPY ERROR | {filename} | {e}")
        return filepath, -1


# ============================================================
# DELETE WORKER
# Single worker — sequential between batches (no task storm).
# Within each batch: controlled parallelism via semaphore(10).
# Faster than pure sequential, safer than unbounded parallel.
# ============================================================

async def delete_worker():
    logger.info("DELETE WORKER | Started")
    deleted_total    = 0
    # 10 concurrent deletes within a batch — controlled I/O
    delete_semaphore = asyncio.Semaphore(10)

    async def _delete_one(fp: str) -> bool:
        async with delete_semaphore:
            try:
                await asyncio.to_thread(os.remove, fp)
                logger.debug(f"DELETED | {Path(fp).name}")
                return True
            except FileNotFoundError:
                return True   # already gone — acceptable
            except Exception as e:
                logger.error(f"DELETE ERROR | {fp} | {e}")
                return False

    while True:
        batch = await delete_queue.get()

        try:
            if batch is None:
                logger.info(f"DELETE WORKER | Shutdown | total={deleted_total}")
                return

            results          = await asyncio.gather(*[_delete_one(fp) for fp in batch])
            deleted_in_batch = sum(results)
            deleted_total   += deleted_in_batch

            logger.info(
                f"DELETE WORKER | "
                f"deleted={deleted_in_batch}/{len(batch)} | "
                f"total={deleted_total}"
            )

        except Exception as e:
            logger.exception(f"DELETE WORKER | Unexpected error | {e}")
        finally:
            delete_queue.task_done()


# ============================================================
# BATCH WORKER
# Picks up List[str] from batch_queue.
# Drive check: once per batch.
# Copy: asyncio.gather with copy_semaphore (CONCURRENT_COPIES).
# On success: push confirmed batch to delete_queue.
# On failure: release files from dedup so watchdog can re-detect.
#
# finally block: always releases files from dedup set.
# results may not exist if exception occurs before gather —
# handled explicitly to avoid NameError/UnboundLocalError.
# ============================================================

async def batch_worker(worker_id: int):
    logger.info(f"WORKER {worker_id} | Ready")

    while True:
        batch   = await batch_queue.get()
        results = []  # initialise here — safe for finally block

        try:
            if batch is None:
                logger.info(f"WORKER {worker_id} | Shutdown")
                return

            logger.info(
                f"WORKER {worker_id} | "
                f"batch={len(batch)} | "
                f"batch_q={batch_queue.qsize()}"
            )

            # Drive check — once per batch, not per file
            attempt = 0
            while not await is_drive_accessible():
                attempt += 1
                wait = min(2 ** attempt, 120)
                logger.warning(
                    f"WORKER {worker_id} | DRIVE DOWN | "
                    f"attempt={attempt} | retry in {wait}s"
                )
                await asyncio.sleep(wait)

            # Parallel copy with semaphore — max CONCURRENT_COPIES
            # simultaneous across all workers combined.
            async def copy_with_sem(fp):
                async with copy_semaphore:
                    return await copy_one(fp)

            results = await asyncio.gather(
                *[copy_with_sem(fp) for fp in batch]
            )

            succeeded = [fp for fp, sz in results if sz >= 0]
            failed    = [fp for fp, sz in results if sz  < 0]
            sizes     = [sz for _,  sz in results if sz >= 0]

            logger.info(
                f"WORKER {worker_id} | "
                f"ok={len(succeeded)} failed={len(failed)}"
            )

            # Record metrics for all successful copies
            for sz in sizes:
                await record_transfer(sz)

            # Push confirmed batch to delete_queue as a single item.
            # delete_worker handles deletion — copy worker moves on.
            if succeeded:
                await delete_queue.put(succeeded)

            # Release failed files — they can be re-detected by watchdog
            if failed:
                await release_files(failed)

        except Exception as e:
            logger.exception(f"WORKER {worker_id} | Batch error | {e}")

        finally:
            # Always release files from dedup set.
            # results initialised to [] above — no NameError risk.
            # If gather ran: release only succeeded (failed already released).
            # If gather never ran (exception before it): release whole batch.
            if batch is not None:
                if results:
                    to_release = [fp for fp, sz in results if sz >= 0]
                else:
                    to_release = batch  # exception before gather — release all
                await release_files(to_release)
            batch_queue.task_done()


# ============================================================
# WATCHDOG
# on_created: new file written to WATCH_FOLDER → enqueue
# on_moved:   file moved/renamed into folder → enqueue
# recursive=True: all nested date/hour subfolders watched
# run_coroutine_threadsafe: safe bridge OS thread → event loop
# ============================================================

class ImageFileHandler(FileSystemEventHandler):

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def _enqueue(self, path: str):
        asyncio.run_coroutine_threadsafe(enqueue_file(path), self.loop)

    def on_created(self, event):
        if not event.is_directory and \
           Path(event.src_path).suffix.lower() in IMAGE_EXTENSIONS:
            self._enqueue(event.src_path)

    def on_moved(self, event):
        if not event.is_directory and \
           Path(event.dest_path).suffix.lower() in IMAGE_EXTENSIONS:
            self._enqueue(event.dest_path)


# ============================================================
# STARTUP SCAN
# Runs AFTER observer.start() — closes the gap where files
# arriving between scan-end and observer-start would be missed.
# rglob is acceptable here — face crops are deleted after
# transfer so WATCH_FOLDER stays lean in normal operation.
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
# LIFESPAN — startup and graceful shutdown
#
# Startup order:
#   1. Validate config
#   2. Create event-loop-bound primitives
#   3. Initial drive check
#   4. Start batch workers
#   5. Start batcher
#   6. Start delete worker
#   7. Start health + metrics monitors
#   8. Start watchdog observer  ← before scan
#   9. Startup scan             ← after observer live
#
# Shutdown order:
#   1. Stop watchdog  (no new enqueues)
#   2. Cancel batcher
#   3. Stop batch workers (None sentinel)
#   4. Drain + stop delete worker
#   5. Cancel monitors
# ============================================================

@asynccontextmanager
async def lifespan():
    global raw_queue, batch_queue, delete_queue, \
           processing_lock, processing_files, \
           metrics_lock, copy_semaphore

    logger.info("=" * 60)
    logger.info("FILE TRANSFER SERVICE | STARTING")
    logger.info("=" * 60)
    logger.info(f"Watch folder     : {WATCH_FOLDER}")
    logger.info(f"Shared drive     : {SHARED_DRIVE}")
    logger.info(f"Workers          : {MAX_WORKERS}")
    logger.info(f"Batch size       : {BATCH_SIZE}")
    logger.info(f"Batch interval   : {BATCH_INTERVAL_SECONDS}s")
    logger.info(f"Concurrent copies: {CONCURRENT_COPIES}")
    logger.info(f"Health interval  : {HEALTH_CHECK_INTERVAL}s")
    logger.info(f"Metrics window   : {METRICS_WINDOW_SECONDS}s")
    logger.info(f"Queue max        : {QUEUE_MAXSIZE}")
    logger.info(f"Extensions       : {IMAGE_EXTENSIONS}")
    logger.info("=" * 60)

    _validate_config()
    os.makedirs(WATCH_FOLDER, exist_ok=True)

    # Initialise all asyncio primitives inside the running loop
    raw_queue        = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    batch_queue      = asyncio.Queue()
    delete_queue     = asyncio.Queue()  # unbounded — deletes must not be dropped
    processing_lock  = asyncio.Lock()
    metrics_lock     = asyncio.Lock()
    processing_files = set()
    copy_semaphore   = asyncio.Semaphore(CONCURRENT_COPIES)

    if await is_drive_accessible():
        logger.info("STARTUP | Shared drive reachable")
    else:
        logger.warning("STARTUP | Shared drive unreachable — workers will retry")

    # Batch workers
    worker_tasks = [
        asyncio.create_task(batch_worker(i + 1))
        for i in range(MAX_WORKERS)
    ]
    logger.info(f"STARTUP | {MAX_WORKERS} worker(s) started")

    # Batcher
    batcher_task = asyncio.create_task(batcher())

    # Delete worker — single, sequential between batches
    delete_task = asyncio.create_task(delete_worker())

    # Monitors
    health_task  = asyncio.create_task(health_monitor())
    metrics_task = asyncio.create_task(metrics_monitor())

    # Watchdog — start BEFORE scan to close detection gap
    loop     = asyncio.get_event_loop()
    observer = Observer()
    observer.schedule(ImageFileHandler(loop), WATCH_FOLDER, recursive=True)
    observer.start()
    logger.info(f"WATCHDOG | Monitoring: {WATCH_FOLDER}")

    # Startup scan — AFTER observer is live
    await scan_existing_files()

    logger.info("SERVICE READY")
    logger.info("=" * 60)

    yield  # service runs here

    # ── GRACEFUL SHUTDOWN ────────────────────────────────────
    logger.info("SHUTDOWN | Starting graceful shutdown")

    # 1. Stop watchdog — no new files enqueued after this
    observer.stop()
    observer.join(timeout=5)
    if observer.is_alive():
        logger.warning("SHUTDOWN | Watchdog did not stop within timeout")
    logger.info("SHUTDOWN | Watchdog stopped")

    # 2. Cancel batcher
    batcher_task.cancel()
    try:
        await batcher_task
    except asyncio.CancelledError:
        pass
    logger.info("SHUTDOWN | Batcher stopped")

    # 3. Drain batch workers
    for _ in range(MAX_WORKERS):
        await batch_queue.put(None)
    await asyncio.gather(*worker_tasks)
    logger.info("SHUTDOWN | Workers stopped")

    # 4. Drain delete queue fully before stopping delete worker
    await delete_queue.join()
    await delete_queue.put(None)
    await delete_task
    logger.info("SHUTDOWN | Delete worker stopped")

    # 5. Cancel monitors
    for task in [health_task, metrics_task]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    logger.info("SHUTDOWN | Monitors stopped")

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