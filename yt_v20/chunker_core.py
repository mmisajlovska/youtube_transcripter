"""
chunker_core.py — Shared chunker engine for YouTube Pipeline Orchestrator v20

Extracted from yt_chunked_db.py and yt_chunked_db_minio.py to eliminate
~300 lines of copy-pasted code.

Contains everything that is identical between the local and MinIO backends:
  - Logging
  - IP-ban circuit breaker
  - Rate-guard sleep
  - Single-pass transcript fetch
  - Transcript chunking helpers
  - safe_name() filename sanitizer
  - DB helpers (mark_chunked, delete_video, get_videos_*)
  - Audio download via yt-dlp

The storage backends (local vs MinIO) implement the StorageBackend interface
and are passed into process_video() — Strategy pattern.
"""

import json
import random
import re
import subprocess
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path

from requests import Session
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
)

from db import PooledConn

# ── Rate-guard defaults (seconds) — overridable via CLI ───────────────────────
RATE_GUARD_MIN: float = 4.0
RATE_GUARD_MAX: float = 8.0

# ── Locks ─────────────────────────────────────────────────────────────────────
_print_lock  = threading.Lock()
_yt_api_lock = threading.Lock()  # Serialises ALL YouTube transcript API calls

# ── IP-ban circuit breaker ─────────────────────────────────────────────────────
IP_BAN_THRESHOLD     = 3
_ip_ban_lock          = threading.Lock()
_consecutive_ban_hits = 0
_ip_ban_stop          = threading.Event()

_IP_BAN_SIGNATURES = (
    "blocking requests from your ip",
    "requestblocked",
    "ipblocked",
    "too many requests",
)

# ── Shared HTTP session ────────────────────────────────────────────────────────
_http_session = Session()
_http_session.headers.update({"Accept-Encoding": "gzip, deflate"})


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str):
    with _print_lock:
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] {msg}", flush=True)


# ── IP-ban helpers ────────────────────────────────────────────────────────────

def is_ip_banned() -> bool:
    return _ip_ban_stop.is_set()


def reset_ban_state():
    """Call at the start of a fresh run to clear any leftover ban state."""
    global _consecutive_ban_hits
    _ip_ban_stop.clear()
    with _ip_ban_lock:
        _consecutive_ban_hits = 0


def _is_ip_ban_exc(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(sig in msg for sig in _IP_BAN_SIGNATURES)


def _record_ban_hit(video_id: str) -> bool:
    global _consecutive_ban_hits
    with _ip_ban_lock:
        _consecutive_ban_hits += 1
        hits = _consecutive_ban_hits
    if hits >= IP_BAN_THRESHOLD:
        _ip_ban_stop.set()
        log(
            f"\n[FATAL] IP BAN DETECTED — {hits} consecutive requests blocked by YouTube.\n"
            f"  Stopping immediately. All videos processed so far are saved.\n"
            f"  Triggering video: {video_id}\n"
        )
        print(f"[RESULT] IP ban detected after {hits} consecutive blocked requests", flush=True)
        return True
    log(f"  [IP-BAN WARNING] Consecutive blocked requests: {hits}/{IP_BAN_THRESHOLD}")
    return False


def _reset_ban_counter():
    global _consecutive_ban_hits
    with _ip_ban_lock:
        _consecutive_ban_hits = 0


# ── Naming helper ─────────────────────────────────────────────────────────────

def safe_name(name: str, max_len: int = 80) -> str:
    name = re.sub(r'[^\w\s\-\u2013\u2014]', '', name, flags=re.UNICODE)
    name = re.sub(r'[\\/: *?"<>|]', '', name)
    name = re.sub(r'\s+', ' ', name).strip().strip('.')
    return (name[:max_len].rstrip() if len(name) > max_len else name) or "unnamed"


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_channel_name(channel_id: str) -> str:
    with PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT channel_name FROM channels WHERE channel_id = %s", (channel_id,))
            row = cur.fetchone()
    return row["channel_name"] if row and row["channel_name"] else channel_id


def get_videos_for_channel(channel_id: str) -> list:
    with PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT video_id, video_title FROM videos WHERE channel_id = %s", (channel_id,))
            return cur.fetchall()


def get_videos_by_ids(video_ids: list) -> list:
    if not video_ids:
        return []
    placeholders = ",".join(["%s"] * len(video_ids))
    with PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT video_id, video_title FROM videos WHERE video_id IN ({placeholders})",
                video_ids,
            )
            return cur.fetchall()


def db_mark_chunked(video_id: str):
    with PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE videos SET is_chunked = TRUE WHERE video_id = %s", (video_id,))


def db_delete_video(video_id: str):
    with PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM videos WHERE video_id = %s", (video_id,))


# ── Rate guard ────────────────────────────────────────────────────────────────

def _rate_guard_sleep():
    delay = random.uniform(RATE_GUARD_MIN, RATE_GUARD_MAX)
    log(f"[RATE GUARD] sleeping {delay:.1f}s before transcript request …")
    time.sleep(delay)


# ── Single-pass transcript fetch ──────────────────────────────────────────────

def fetch_transcript_single_pass(video_id: str, langs: list) -> list:
    """
    Acquire the global YT API lock, sleep rate guard, then list()+find()+fetch()
    in one atomic block. One call per video, zero parallel hits to YouTube.
    Raises NoTranscriptFound / TranscriptsDisabled / VideoUnavailable on skip.
    """
    with _yt_api_lock:
        _rate_guard_sleep()
        tlist = YouTubeTranscriptApi(http_client=_http_session).list(video_id)
        raw   = tlist.find_transcript(langs).fetch().to_raw_data()

    _reset_ban_counter()

    for item in raw:
        item["end"] = float(item["start"]) + float(item["duration"])
    return raw


# ── Chunking helpers ──────────────────────────────────────────────────────────

def chunk_transcript(raw: list, chunk_size: int) -> list:
    if not raw:
        return []
    entries = sorted(raw, key=lambda x: x["start"])
    for i in range(len(entries) - 1):
        entries[i]["true_end"] = entries[i + 1]["start"]
    entries[-1]["true_end"] = entries[-1]["end"]

    chunks, idx, n, chunk_num = [], 0, len(entries), 1
    while idx < n:
        boundary      = chunk_num * chunk_size
        chunk_entries = []
        j             = idx
        while j < n and entries[j]["true_end"] <= boundary:
            chunk_entries.append(entries[j])
            j += 1
        if not chunk_entries:
            chunk_entries.append(entries[idx])
            j = idx + 1
        c_start = chunk_entries[0]["start"]
        c_end   = chunk_entries[-1]["true_end"]
        chunks.append((c_start, c_end, chunk_entries))
        idx = j
        while idx < n and entries[idx]["start"] < c_end:
            idx += 1
        chunk_num += 1
    return chunks


def clean_entries(entries: list) -> list:
    return [
        {
            "text": re.sub(r"^>>\s*", "", e["text"]).strip(),
            "start": e["start"],
            "duration": e["duration"],
            "end": e["true_end"],
        }
        for e in entries
    ]


def write_chunk_txt(path: Path, cleaned: list):
    """Write transcript text only, one entry per line."""
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(e["text"] for e in cleaned))


# ── Audio download ────────────────────────────────────────────────────────────

# Whisper requires 16 kHz mono WAV
_WHISPER_SR = 16000


def download_full_audio(video_id: str, out_path: Path) -> bool:
    """
    Download the FULL audio track for a video ONCE via yt-dlp (no
    --download-sections), convert to 16kHz mono WAV. Chunks are sliced
    locally from this file afterward — zero extra YouTube requests per chunk.
    """
    from pydub import AudioSegment

    stem     = out_path.with_suffix("")
    mp3_path = out_path.with_suffix(".mp3")

    cmd = [
        "yt-dlp", "--no-playlist", "-x",
        "--audio-format", "mp3", "--audio-quality", "192K",
        "-o", str(stem),
        f"https://www.youtube.com/watch?v={video_id}",
    ]
    result     = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    candidates = list(out_path.parent.glob(f"{stem.name}*.mp3"))

    if not candidates:
        stderr_tail = result.stderr[-500:] if result.stderr else ""
        log(f"  [warn] full-audio download failed for {video_id}: {stderr_tail[-300:] if stderr_tail else '(none)'}")
        if _is_ip_ban_exc(Exception(stderr_tail)):
            _record_ban_hit(video_id)
        return False

    actual_mp3 = candidates[0]
    if actual_mp3 != mp3_path:
        actual_mp3.rename(mp3_path)

    try:
        audio = AudioSegment.from_mp3(str(mp3_path))
        audio = audio.set_frame_rate(_WHISPER_SR).set_channels(1)
        audio.export(str(out_path), format="wav")
        mp3_path.unlink(missing_ok=True)
        return True
    except Exception as exc:
        log(f"  [warn] WAV conversion failed for {mp3_path.name}: {exc}")
        mp3_path.unlink(missing_ok=True)
        return False


def slice_audio_chunk(full_audio_path: Path, start_sec, end_sec, out_path: Path) -> bool:
    """Cut one chunk out of the already-downloaded full WAV. Pure local ffmpeg — no network."""
    cmd = [
        "ffmpeg", "-y", "-nostdin",
        "-i", str(full_audio_path),
        "-ss", str(start_sec), "-to", str(end_sec),
        "-ar", str(_WHISPER_SR), "-ac", "1",
        str(out_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if not out_path.exists():
        log(f"  [warn] ffmpeg slice failed {start_sec}-{end_sec}s: {result.stderr[-300:] if result.stderr else '(none)'}")
        return False
    return True

# ── Storage backend interface (Strategy pattern) ──────────────────────────────

class StorageBackend(ABC):
    """
    Abstract storage backend.
    Concrete implementations: LocalBackend, MinioBackend.
    """

    @abstractmethod
    def save_chunk(
        self,
        video_id: str,
        label: str,
        file_label: str,
        c_start,
        c_end,
        entries: list,
        channel_context: str,
        video_context: str,
        full_audio_path: str
    ) -> tuple[str, int, bool]:
        """
        Persist one chunk (JSON + audio).
        Returns (label, entry_count, success).
        """

    @abstractmethod
    def cleanup_video(self, channel_context: str, video_context: str):
        """Called when a video has no transcript — clean up any partial data."""

    def finalize(self):
        """Called after all videos are processed. Override if needed."""


# ── Per-video pipeline ────────────────────────────────────────────────────────

def process_video(
    video_id: str,
    video_title: str,
    backend: StorageBackend,
    chunk_size: int,
    langs: list,
    max_workers: int,
    channel_context: str,
) -> bool:
    """
    Full pipeline for one video.
    Works with any StorageBackend — local or MinIO.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if is_ip_banned():
        return False

    display    = video_title or video_id
    video_safe = safe_name(video_title) if video_title else video_id
    log(f"\n{'='*60}\nProcessing: {display}\n{'='*60}\n")

    # Print the title so the Flask SSE bridge can emit current_video events
    print(f"Processing: {display}", flush=True)

    full_audio_path = Path(tempfile.gettempdir()) / "yt_full_audio" / f"{video_id}.wav"
    full_audio_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        log(f"  fetching video subtitles (1 request) …")
        raw    = fetch_transcript_single_pass(video_id, langs)
        chunks = chunk_transcript(raw, chunk_size)
        log(f"  {len(raw)} entries → {len(chunks)} chunks\n")

        log(f"  downloading full audio (1 request) …")
        if not download_full_audio(video_id, full_audio_path):
            log(f"\n  Audio download failed for {video_id} — skipping\n")
            return False

        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    backend.save_chunk,
                    video_id,
                    f"{round(s,2)}-{round(e,2)}s",
                    f"{round(s,2)}-{round(e,2)}s".replace(".", "_"),
                    s, e, ents,
                    channel_context,
                    video_safe,
                    full_audio_path,
                ): (s, e)
                for s, e, ents in chunks
            }
            for future in as_completed(futures):
                label, n_ents, ok = future.result()
                log(f"  [{'OK' if ok else 'FAIL'}] {label} ({n_ents} entries)")
                results[label] = ok

        failed = [lbl for lbl, ok in results.items() if not ok]
        log(f"\n  Done: {len(results) - len(failed)}/{len(results)} chunks OK")
        if failed:
            log(f"  Failed chunks: {', '.join(sorted(failed))}")

        db_mark_chunked(video_id)
        return True

    except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable) as exc:
        log(f"\n  No transcript for {video_id} ({exc}) - deleting from DB\n")
        backend.cleanup_video(channel_context, video_safe)
        db_delete_video(video_id)
        return False

    except Exception as exc:
        if _is_ip_ban_exc(exc):
            _record_ban_hit(video_id)
        else:
            log(f"\n  Error processing {video_id}: {exc}\n")
        return False

    finally:
        full_audio_path.unlink(missing_ok=True)