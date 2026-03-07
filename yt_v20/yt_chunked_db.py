"""
yt_chunked_db.py — Transcript + audio chunker v20 (local filesystem backend)

Thin wrapper around chunker_core.py.
All shared logic lives in chunker_core — this file only implements
LocalBackend (the Strategy) and the CLI entry point.
"""

import argparse
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from config import get_config, DEFAULT_OUTPUT_DIR
from db import init_pool
from chunker_core import (
    RATE_GUARD_MIN, RATE_GUARD_MAX,
    StorageBackend,
    clean_entries, download_audio_chunk, safe_name,
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


# ── Local storage backend ─────────────────────────────────────────────────────

class LocalBackend(StorageBackend):
    """Saves chunks to a local directory tree."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir

    def save_chunk(self, video_id, label, file_label, c_start, c_end, entries,
                   channel_context, video_context) -> tuple:
        out_dir = self.base_dir / video_context
        out_dir.mkdir(exist_ok=True, parents=True)

        import json
        audio_path = out_dir / f"{file_label}_audio.mp3"
        with open(out_dir / f"{file_label}.json", "w", encoding="utf-8") as f:
            json.dump(clean_entries(entries), f, ensure_ascii=False, indent=2)
        log(f"  ↓ {label} …")
        ok = download_audio_chunk(video_id, c_start, c_end, audio_path)
        return label, len(entries), ok

    def cleanup_video(self, channel_context, video_context):
        target = self.base_dir / video_context
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Chunk YouTube transcripts & audio v20 (local).")
    p.add_argument("--channel-id",       default="")
    p.add_argument("--video-ids",        default="",
                   help="Comma-separated video IDs (overrides full-channel mode)")
    p.add_argument("--output-dir",       default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--rate-guard-min",   type=float, default=RATE_GUARD_MIN)
    p.add_argument("--rate-guard-max",   type=float, default=RATE_GUARD_MAX)
    return p.parse_args()


def main():
    args = parse_args()

    chunker_core.RATE_GUARD_MIN = args.rate_guard_min
    chunker_core.RATE_GUARD_MAX = args.rate_guard_max

    if not shutil.which("yt-dlp"):
        raise SystemExit("[ERROR] yt-dlp not found. Install: pip install yt-dlp")
    if not shutil.which("ffmpeg"):
        raise SystemExit("[ERROR] ffmpeg not found. See: https://ffmpeg.org/")

    import psycopg2
    try:
        init_pool()
    except psycopg2.Error as exc:
        raise SystemExit(f"[ERROR] PostgreSQL: {exc}")

    reset_ban_state()

    channel_name = get_channel_name(args.channel_id)

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
    out_base    = Path(args.output_dir)
    channel_dir = out_base / safe_name(channel_name)
    channel_dir.mkdir(exist_ok=True, parents=True)
    num_workers = MAX_VIDEO_WORKERS or 1

    backend = LocalBackend(channel_dir)

    print(f"[INFO] Channel : {channel_name}")
    print(f"[INFO] Mode    : {mode}")
    print(f"[INFO] Videos  : {total}")
    print(f"\n{'='*60}")
    print(f"YouTube Chunker v20  |  Channel : {channel_name}")
    print(f"Videos : {total}  |  Video workers : {num_workers}  |  Chunk workers : {MAX_WORKERS}")
    print(f"Rate guard : {chunker_core.RATE_GUARD_MIN}-{chunker_core.RATE_GUARD_MAX}s (sequential)")
    print(f"Output : {channel_dir.resolve()}")
    print(f"{'='*60}\n")

    results       = {}
    completed     = 0
    deleted_count = 0

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {
            pool.submit(
                process_video,
                v["video_id"], v.get("video_title") or "",
                backend, CHUNK_SIZE, LANGS, MAX_WORKERS,
                safe_name(channel_name),
            ): v["video_id"]
            for v in videos
        }
        for future in as_completed(futures):
            video_id = futures[future]
            completed += 1
            try:
                ok = future.result()
                results[video_id] = ok
                if not ok:
                    deleted_count += 1
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
    print(f"Output : {channel_dir.resolve()}")
    print(f"{'='*60}\n")

    print(f"[RESULT] Videos chunked successfully : {successful}")
    print(f"[RESULT] Videos failed               : {total - successful - deleted_count}")
    print(f"[RESULT] Deleted (no transcript)     : {deleted_count}")
    if is_ip_banned():
        print(f"[RESULT] Stop reason                 : IP ban ({chunker_core._consecutive_ban_hits} consecutive blocked requests)")


if __name__ == "__main__":
    main()
