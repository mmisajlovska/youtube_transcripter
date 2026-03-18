"""
yt_chunked_db_minio.py — Transcript + audio chunker v20 (MinIO backend)

Thin wrapper around chunker_core.py.
All shared logic lives in chunker_core — this file only implements
MinioBackend (the Strategy) and the CLI entry point.
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from minio import Minio
from minio.error import S3Error

from config import get_config, DEFAULT_OUTPUT_DIR
from db import init_pool
from chunker_core import (
    RATE_GUARD_MIN, RATE_GUARD_MAX,
    StorageBackend,
    clean_entries, write_chunk_txt, download_audio_chunk, safe_name,
    get_channel_name, get_videos_for_channel, get_videos_by_ids,
    process_video, reset_ban_state, is_ip_banned,
    log,
    _ip_ban_stop, _consecutive_ban_hits,
)
import chunker_core

_cfg              = get_config()
CHUNK_SIZE        = _cfg["CHUNK_SIZE"]
LANGS             = _cfg["LANGS"]
MAX_WORKERS       = _cfg["MAX_WORKERS"]
MAX_VIDEO_WORKERS = _cfg["MAX_VIDEO_WORKERS"]


# ── MinIO client factory ───────────────────────────────────────────────────────

def make_minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)


def ensure_bucket(client: Minio, bucket: str):
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log(f"[MinIO] Created bucket: {bucket}")
        else:
            log(f"[MinIO] Bucket already exists: {bucket}")
    except S3Error as exc:
        raise RuntimeError(f"[MinIO] Failed to ensure bucket '{bucket}': {exc}") from exc


def _upload_folder(client: Minio, bucket: str, prefix: str, folder: Path):
    uploaded, failed = 0, 0
    for f in sorted(folder.rglob("*")):
        if not f.is_file():
            continue
        if f.suffix not in (".wav", ".json", ".txt"):   # Fix 3: never upload mp3 or temp files
            continue
        obj_name = prefix + f.relative_to(folder).as_posix()
        try:
            client.fput_object(bucket, obj_name, str(f))
            uploaded += 1
        except S3Error as exc:
            log(f"  [MinIO ERROR] Failed to upload {f.name}: {exc}")
            failed += 1
    return uploaded, failed


# ── MinIO storage backend ─────────────────────────────────────────────────────

class MinioBackend(StorageBackend):
    """Saves chunks to a MinIO bucket via a local temp dir."""

    def __init__(self, client: Minio, bucket: str):
        self.client = client
        self.bucket = bucket
        self._tmp   = Path(tempfile.gettempdir()) / "yt_minio_batch"

    def _video_tmp(self, channel_context: str, video_context: str) -> Path:
        return self._tmp / channel_context / video_context

    def save_chunk(self, video_id, label, file_label, c_start, c_end, entries,
                   channel_context, video_context) -> tuple:
        # Fix 1: each chunk gets its own isolated subdir so concurrent threads
        # never see each other's in-progress files during upload.
        chunk_dir = self._video_tmp(channel_context, video_context) / file_label
        chunk_dir.mkdir(parents=True, exist_ok=True)

        audio_path = chunk_dir / f"{file_label}_audio.wav"
        cleaned = clean_entries(entries)
        with open(chunk_dir / f"{file_label}.json", "w", encoding="utf-8") as f:
            json.dump(cleaned, f, ensure_ascii=False, indent=2)
        write_chunk_txt(chunk_dir / f"{file_label}.txt", cleaned)
        log(f"  ↓ {label} …")
        ok = download_audio_chunk(video_id, c_start, c_end, audio_path)

        # Fix 2: purge any stray .mp3 files yt-dlp may have left with an
        # unexpected name (e.g. when download "fails" but still wrote bytes).
        for stale in chunk_dir.glob("*.mp3"):
            stale.unlink(missing_ok=True)

        # Upload only this chunk's isolated directory
        prefix = f"{channel_context}/{video_context}/"
        uploaded, upload_failed = _upload_folder(self.client, self.bucket, prefix, chunk_dir)

        # Clean up temp files that were uploaded successfully
        if upload_failed == 0:
            shutil.rmtree(chunk_dir, ignore_errors=True)

        return label, len(entries), ok and upload_failed == 0

    def cleanup_video(self, channel_context, video_context):
        tmp_dir = self._video_tmp(channel_context, video_context)
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Chunk YouTube transcripts & audio v20 (MinIO).")
    p.add_argument("--channel-id",       default="")
    p.add_argument("--video-ids",        default="")
    p.add_argument("--rate-guard-min",   type=float, default=RATE_GUARD_MIN)
    p.add_argument("--rate-guard-max",   type=float, default=RATE_GUARD_MAX)
    p.add_argument("--minio-endpoint",   default=os.getenv("MINIO_ENDPOINT",   "localhost:9000"))
    p.add_argument("--minio-access-key", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    p.add_argument("--minio-secret-key", default=os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    p.add_argument("--minio-bucket",     default=os.getenv("MINIO_BUCKET",     "youtube-chunks"))
    return p.parse_args()


def main():
    args = parse_args()

    chunker_core.RATE_GUARD_MIN = args.rate_guard_min
    chunker_core.RATE_GUARD_MAX = args.rate_guard_max

    if not shutil.which("yt-dlp"):
        raise SystemExit("[ERROR] yt-dlp not found. Install: pip install yt-dlp")
    if not shutil.which("ffmpeg"):
        raise SystemExit("[ERROR] ffmpeg not found. See: https://ffmpeg.org/")

    log(f"[MinIO] Connecting to {args.minio_endpoint} …")
    minio_client = make_minio_client(args.minio_endpoint, args.minio_access_key, args.minio_secret_key)
    try:
        ensure_bucket(minio_client, args.minio_bucket)
    except RuntimeError as exc:
        raise SystemExit(str(exc))

    import psycopg2
    try:
        init_pool()
    except psycopg2.Error as exc:
        raise SystemExit(f"[ERROR] PostgreSQL: {exc}")

    reset_ban_state()

    channel_name = get_channel_name(args.channel_id)
    channel_safe = safe_name(channel_name)

    if args.video_ids.strip():
        video_id_list = [v.strip() for v in args.video_ids.split(",") if v.strip()]
        videos = get_videos_by_ids(video_id_list)
        mode   = f"individual ({len(videos)} selected)"
    else:
        videos = get_videos_for_channel(args.channel_id)
        mode   = "full channel"

    if not videos:
        raise SystemExit("[ERROR] No videos found.")

    total       = len(videos)
    num_workers = MAX_VIDEO_WORKERS or 1

    backend = MinioBackend(minio_client, args.minio_bucket)

    print(f"[INFO] Channel : {channel_name}")
    print(f"[INFO] Mode    : {mode}")
    print(f"[INFO] Videos  : {total}")
    print(f"\n{'='*60}")
    print(f"YouTube Chunker v20 (MinIO)  |  Channel : {channel_name}")
    print(f"Videos : {total}  |  Video workers : {num_workers}  |  Chunk workers : {MAX_WORKERS}")
    print(f"Rate guard : {chunker_core.RATE_GUARD_MIN}-{chunker_core.RATE_GUARD_MAX}s (sequential)")
    print(f"MinIO  : {args.minio_endpoint}/{args.minio_bucket}/{channel_safe}/")
    print(f"{'='*60}\n")

    results   = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {
            pool.submit(
                process_video,
                v["video_id"], v.get("video_title") or "",
                backend, CHUNK_SIZE, LANGS, MAX_WORKERS,
                channel_safe,
            ): v["video_id"]
            for v in videos
        }
        for future in as_completed(futures):
            video_id = futures[future]
            completed += 1
            try:
                results[video_id] = future.result()
            except Exception as exc:
                log(f"\n  Exception for {video_id}: {exc}\n")
                results[video_id] = False
            print(f"[PROGRESS] {completed}/{total}", flush=True)

            if is_ip_banned():
                for pending in futures:
                    pending.cancel()
                break

    successful = sum(1 for v in results.values() if v)

    print(f"\n{'='*60}")
    if is_ip_banned():
        print(f"Stopped early — IP ban after {chunker_core._consecutive_ban_hits} consecutive blocked requests.")
    print(f"All Done! {successful}/{total} videos processed successfully")
    print(f"MinIO  : {args.minio_endpoint}/{args.minio_bucket}/{channel_safe}/")
    print(f"{'='*60}\n")

    print(f"[RESULT] Videos chunked successfully : {successful}")
    print(f"[RESULT] Videos failed/skipped       : {total - successful}")
    if is_ip_banned():
        print(f"[RESULT] Stop reason                 : IP ban ({chunker_core._consecutive_ban_hits} consecutive blocked requests)")
    print(f"[RESULT] MinIO bucket                : {args.minio_bucket}")


if __name__ == "__main__":
    main()