"""
youtube_api_db.py — YouTube video ID collector v20

Fetches video IDs, titles, published_at, and duration from the
YouTube Data API v3 and stores them in PostgreSQL.

v20 changes:
  - Uses db.PooledConn (centralized pool) instead of get_connection()
  - insert_videos() uses executemany() — one round-trip instead of N
  - All MySQL remnant comments removed
"""

import argparse
import json
import re
import sys
from datetime import datetime

import psycopg2
import requests

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from config import get_config, BASE_URL, STOP_THRESHOLD
from db import init_pool, PooledConn

DEFAULT_API_KEY    = ""
DEFAULT_CHANNEL_ID = ""


# ── DB ────────────────────────────────────────────────────────────────────────

def init_db():
    with PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id   VARCHAR(64)  PRIMARY KEY,
                    channel_name VARCHAR(255) DEFAULT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    video_id     VARCHAR(32)  PRIMARY KEY,
                    channel_id   VARCHAR(64)  NOT NULL,
                    video_title  VARCHAR(500) DEFAULT NULL,
                    published_at TIMESTAMP,
                    is_chunked   BOOLEAN      DEFAULT FALSE,
                    duration     VARCHAR(16)  DEFAULT NULL,
                    CONSTRAINT fk_channel
                        FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
                );
            """)


def migrate_db():
    """Safely add new columns to existing tables without losing data."""
    for col, definition in [
        ("is_chunked", "BOOLEAN DEFAULT FALSE"),
        ("duration",   "VARCHAR(16) DEFAULT NULL"),
    ]:
        try:
            with PooledConn() as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(f"SAVEPOINT migrate_{col};")
                    cur.execute(f"ALTER TABLE videos ADD COLUMN IF NOT EXISTS {col} {definition};")
                    cur.execute(f"RELEASE SAVEPOINT migrate_{col};")
                conn.commit()
            print(f"[MIGRATE] Added column: {col}")
        except psycopg2.Error:
            pass


def upsert_channel(channel_id: str, channel_name: str):
    with PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO channels (channel_id, channel_name) VALUES (%s, %s)
                ON CONFLICT (channel_id) DO UPDATE SET channel_name = EXCLUDED.channel_name
            """, (channel_id, channel_name))


def get_existing_video_ids_for_channel(channel_id: str) -> set:
    with PooledConn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT video_id FROM videos WHERE channel_id = %s", (channel_id,))
            return {row["video_id"] for row in cur.fetchall()}


def insert_videos(channel_id: str, videos: list) -> int:
    """Bulk insert using executemany — one round-trip for all rows."""
    if not videos:
        return 0
    rows = [
        (v["video_id"], channel_id, v.get("video_title"), v["published_at"], v.get("duration"))
        for v in videos
    ]
    inserted = 0
    with PooledConn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # executemany with ON CONFLICT DO NOTHING — single round-trip
            cur.executemany("""
                INSERT INTO videos (video_id, channel_id, video_title, published_at, duration)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING
            """, rows)
            inserted = cur.rowcount
        conn.commit()
    return inserted


# ── YouTube API helpers ───────────────────────────────────────────────────────

def parse_iso8601_duration(iso: str) -> str:
    if not iso:
        return "00:00:00"
    m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso)
    if not m:
        return "00:00:00"
    h  = int(m.group(1) or 0)
    mn = int(m.group(2) or 0)
    s  = int(m.group(3) or 0)
    return f"{h:02d}:{mn:02d}:{s:02d}"


def fetch_durations(api_key: str, video_ids: list) -> dict:
    durations = {}
    for i in range(0, len(video_ids), 50):
        batch  = video_ids[i:i + 50]
        params = {"part": "contentDetails", "id": ",".join(batch), "key": api_key}
        try:
            resp = requests.get(f"{BASE_URL}/videos", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"[WARN] Duration fetch error: {e}", file=sys.stderr)
            continue
        if "error" in data:
            print(f"[WARN] Duration API error: {data['error']}", file=sys.stderr)
            continue
        for item in data.get("items", []):
            iso = item.get("contentDetails", {}).get("duration", "")
            durations[item["id"]] = parse_iso8601_duration(iso)
    return durations


def fetch_channel_info(api_key: str, channel_id: str):
    params = {"part": "contentDetails,snippet", "id": channel_id, "key": api_key}
    try:
        resp = requests.get(f"{BASE_URL}/channels", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"[ERROR] Network error: {e}", file=sys.stderr)
        return None, None
    if "error" in data:
        err = data["error"]
        print(f"[ERROR] YouTube API {err.get('code')}: {err.get('message')}", file=sys.stderr)
        return None, None
    items = data.get("items")
    if not items:
        print(f"[ERROR] No channel found for ID: {channel_id}", file=sys.stderr)
        return None, None
    return (
        items[0]["contentDetails"]["relatedPlaylists"]["uploads"],
        items[0]["snippet"].get("title", "Unknown Channel"),
    )


def _parse_dt(iso_str):
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def fetch_new_videos_incremental(api_key: str, playlist_id: str,
                                  existing_ids: set, stop_threshold: int = STOP_THRESHOLD):
    new_videos, total_fetched, next_page_token, consecutive_known = [], 0, None, 0

    while True:
        params = {
            "part": "contentDetails,snippet", "playlistId": playlist_id,
            "maxResults": 50, "key": api_key,
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        try:
            resp = requests.get(f"{BASE_URL}/playlistItems", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"[ERROR] {e}", file=sys.stderr)
            break
        if "error" in data:
            print(f"[ERROR] {data['error']}", file=sys.stderr)
            break

        items = data.get("items", [])
        if not items:
            break
        total_fetched += len(items)

        stop_early = False
        for item in items:
            vid   = item["contentDetails"]["videoId"]
            title = item["snippet"].get("title", "")
            dt    = _parse_dt(
                item["contentDetails"].get("videoPublishedAt") or item["snippet"].get("publishedAt")
            )
            if vid in existing_ids:
                consecutive_known += 1
                if consecutive_known >= stop_threshold:
                    stop_early = True
                    break
            else:
                consecutive_known = 0
                new_videos.append({"video_id": vid, "video_title": title, "published_at": dt})

        if stop_early:
            print(f"[INFO] Stop-early: {consecutive_known} consecutive known IDs hit.")
            break

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    new_videos.sort(key=lambda v: v["published_at"] or datetime.min, reverse=True)
    return new_videos, total_fetched


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Collect YouTube video IDs into PostgreSQL.")
    p.add_argument("--api-key",        default=DEFAULT_API_KEY)
    p.add_argument("--channel-id",     default=DEFAULT_CHANNEL_ID)
    p.add_argument("--stop-threshold", type=int, default=STOP_THRESHOLD)
    p.add_argument("--dump-json",      action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    if not args.api_key:
        print("[ERROR] No API key provided.", file=sys.stderr)
        sys.exit(1)

    try:
        init_pool()
    except psycopg2.Error as e:
        print(f"[ERROR] PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

    init_db()
    migrate_db()

    print("[INFO] Fetching channel info...")
    playlist_id, channel_name = fetch_channel_info(args.api_key, args.channel_id)
    if not playlist_id:
        sys.exit(1)

    print(f"[INFO] Channel        : {args.channel_id}")
    print(f"[INFO] Channel name   : {channel_name}")
    upsert_channel(args.channel_id, channel_name)

    existing_ids = get_existing_video_ids_for_channel(args.channel_id)
    print(f"[INFO] Already stored : {len(existing_ids)} video(s)")
    print("[INFO] Fetching new videos from YouTube...")

    new_videos, total_fetched = fetch_new_videos_incremental(
        args.api_key, playlist_id, existing_ids, args.stop_threshold,
    )

    if new_videos:
        print(f"[INFO] Fetching durations for {len(new_videos)} new video(s)...")
        durations = fetch_durations(args.api_key, [v["video_id"] for v in new_videos])
        for v in new_videos:
            v["duration"] = durations.get(v["video_id"], "00:00:00")
        print(f"[INFO] Durations fetched: {len(durations)}")

    inserted = insert_videos(args.channel_id, new_videos)

    print()
    print(f"[RESULT] Channel name               : {channel_name}")
    print(f"[RESULT] API items fetched this run : {total_fetched}")
    print(f"[RESULT] New videos inserted        : {inserted}")
    if new_videos:
        print(f"[RESULT] Newest published_at        : {new_videos[0]['published_at']}")
        print(f"[RESULT] Oldest published_at        : {new_videos[-1]['published_at']}")
    else:
        print("[RESULT] No new videos found — database is already up to date.")

    if args.dump_json and new_videos:
        import os
        dump_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "videos_data_channel.json")
        with open(dump_path, "w", encoding="utf-8") as f:
            json.dump([{
                "video_id":     v["video_id"],
                "video_title":  v["video_title"],
                "published_at": v["published_at"].isoformat() if v["published_at"] else None,
                "duration":     v.get("duration"),
            } for v in new_videos], f, indent=4, ensure_ascii=False)
        print(f"[INFO] JSON dump written to {dump_path}")


if __name__ == "__main__":
    main()
