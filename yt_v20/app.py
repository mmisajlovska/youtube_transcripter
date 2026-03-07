"""
app.py — Flask backend for YouTube Pipeline Orchestrator v20

Routes:
  GET  /                              → Main UI
  GET  /api/config                    → Load current config
  POST /api/config                    → Save config (reinitializes DB pool)
  POST /api/config/reset              → Reset config to defaults
  POST /api/config/test_db            → Test a DB connection with given credentials
  GET  /api/channels                  → List all channels with stats
  GET  /api/channels/<id>/videos      → List videos for a channel
  GET  /api/global_stats              → Aggregate stats across all channels
  POST /run_collector                 → Start a collector task (SSE)
  POST /run_chunker                   → Start a chunker task (SSE)
  POST /run_chunker_minio             → Start a MinIO chunker task (SSE)
  POST /api/tasks/<task_id>/stop      → Stop a running task gracefully
  GET  /status/<task_id>              → SSE stream for a running task
"""

import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
import uuid
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

from flask import Flask, Response, jsonify, render_template, request

from config import get_config, save_config, reset_config
from db import init_pool, PooledConn

app = Flask(__name__)

PYTHON = sys.executable

# Task registry: task_id → { type, status, log_lines, result, progress, queue, proc, created_at }
tasks: dict = {}
_tasks_lock = threading.Lock()

TASK_TTL_SECONDS = 7200  # evict completed tasks after 2 hours


# ── Task cleanup ───────────────────────────────────────────────────────────────

def _evict_old_tasks():
    """Remove completed tasks older than TASK_TTL_SECONDS."""
    now = time.time()
    with _tasks_lock:
        stale = [
            tid for tid, t in tasks.items()
            if t["status"] in ("done", "error", "stopped")
            and now - t.get("created_at", now) > TASK_TTL_SECONDS
        ]
        for tid in stale:
            del tasks[tid]


# ── URL helpers ────────────────────────────────────────────────────────────────

def parse_channel_url(url: str) -> dict:
    value = url.strip()

    if "/" not in value:
        if re.match(r"^UC[\w-]{22}$", value):
            return {"type": "id", "value": value}
        if value.startswith("@"):
            return {"type": "handle", "value": value.lstrip("@")}
        return {"type": "raw", "value": value}

    parsed = urlparse(value)
    path   = parsed.path.rstrip("/")

    m = re.match(r"^/channel/(UC[\w-]{22})$", path)
    if m:
        return {"type": "id", "value": m.group(1)}

    m = re.match(r"^/@([^/]+)$", path)
    if m:
        return {"type": "handle", "value": m.group(1)}

    m = re.match(r"^/user/([^/]+)$", path)
    if m:
        return {"type": "user", "value": m.group(1)}

    m = re.match(r"^/c/([^/]+)$", path)
    if m:
        return {"type": "custom", "value": m.group(1)}

    return {"type": "raw", "value": value}


def get_utf8_env() -> dict:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    return env


# ── Task helpers ───────────────────────────────────────────────────────────────

def create_task(task_type: str) -> str:
    _evict_old_tasks()
    task_id = str(uuid.uuid4())
    with _tasks_lock:
        tasks[task_id] = {
            "type":       task_type,
            "status":     "running",
            "log_lines":  [],
            "result":     {},
            "progress":   0,
            "queue":      queue.Queue(),
            "proc":       None,
            "created_at": time.time(),
        }
    return task_id


def enqueue(task_id: str, event: str, data: dict):
    if task_id in tasks:
        tasks[task_id]["queue"].put({"event": event, "data": data})
        if "line" in data:
            tasks[task_id]["log_lines"].append(data["line"])


def _parse_progress(line: str, task_id: str, _total: int) -> int:
    try:
        parts     = line.split()[1].split("/")
        completed = int(parts[0])
        _total    = int(parts[1])
        pct       = min(int(completed / _total * 95), 95)
        tasks[task_id]["progress"] = pct
        enqueue(task_id, "progress",
                {"pct": pct, "processed": completed, "total": _total})
    except Exception:
        pass
    return _total


# ── Pages ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── Config API ─────────────────────────────────────────────────────────────────

@app.route("/api/config", methods=["GET"])
def api_config_get():
    cfg = get_config()
    cfg["LANGS"] = ",".join(cfg["LANGS"]) if isinstance(cfg["LANGS"], list) else cfg["LANGS"]
    return jsonify(cfg)


@app.route("/api/config", methods=["POST"])
def api_config_save():
    body = request.get_json(force=True)
    try:
        updates = {
            "PG_HOST":           str(body.get("PG_HOST",           "localhost")).strip(),
            "PG_PORT":           int(body.get("PG_PORT",           5432)),
            "PG_USER":           str(body.get("PG_USER",           "postgres")).strip(),
            "PG_PASSWORD":       str(body.get("PG_PASSWORD",       "")),
            "PG_DB":             str(body.get("PG_DB",             "youtube_db")).strip(),
            "CHUNK_SIZE":        int(body.get("CHUNK_SIZE",        30)),
            "LANGS":             [l.strip() for l in str(body.get("LANGS", "mk")).split(",") if l.strip()],
            "MAX_WORKERS":       int(body.get("MAX_WORKERS",       2)),
            "MAX_VIDEO_WORKERS": int(body.get("MAX_VIDEO_WORKERS", 3)),
        }
        save_config(updates)
        # Re-initialize the pool so new DB settings take effect immediately
        init_pool()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/config/reset", methods=["POST"])
def api_config_reset():
    reset_config()
    init_pool()
    return jsonify({"ok": True})


@app.route("/api/config/test_db", methods=["POST"])
def api_config_test_db():
    body = request.get_json(force=True)
    try:
        conn = psycopg2.connect(
            host=str(body.get("PG_HOST", "localhost")).strip(),
            port=int(body.get("PG_PORT", 5432)),
            user=str(body.get("PG_USER", "postgres")).strip(),
            password=str(body.get("PG_PASSWORD", "")),
            dbname=str(body.get("PG_DB", "youtube_db")).strip(),
            connect_timeout=5,
        )
        conn.close()
        return jsonify({"ok": True, "message": "Connection successful ✓"})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})


# ── Channel URL Resolution ─────────────────────────────────────────────────────

@app.route("/api/resolve_channel", methods=["POST"])
def api_resolve_channel():
    import urllib.request
    import urllib.parse

    body        = request.get_json(force=True)
    api_key     = body.get("api_key",     "").strip()
    channel_url = body.get("channel_url", "").strip()

    if not api_key:
        return jsonify({"error": "api_key is required"}), 400
    if not channel_url:
        return jsonify({"error": "channel_url is required"}), 400

    parsed = parse_channel_url(channel_url)
    kind   = parsed["type"]
    value  = parsed["value"]

    if kind == "id":
        return jsonify({"channel_id": value, "channel_name": ""})

    if "youtube.com" in channel_url or "youtu.be" in channel_url:
        fetch_url = channel_url.split("?")[0].rstrip("/")
    elif kind == "handle":
        fetch_url = f"https://www.youtube.com/@{value}"
    elif kind == "user":
        fetch_url = f"https://www.youtube.com/user/{value}"
    elif kind == "custom":
        fetch_url = f"https://www.youtube.com/c/{value}"
    else:
        fetch_url = f"https://www.youtube.com/@{value}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        req = urllib.request.Request(fetch_url, headers=headers)
        with urllib.request.urlopen(req, timeout=12) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        m = re.search(r'"channelId"\s*:\s*"(UC[\w-]{22})"', html)
        if m:
            channel_id   = m.group(1)
            nm           = re.search(r'"title"\s*:\s*"([^"]{1,120})"', html)
            channel_name = nm.group(1) if nm else ""
            return jsonify({"channel_id": channel_id, "channel_name": channel_name})

        m2 = re.search(r'<meta\s+itemprop="identifier"\s+content="(UC[\w-]{22})"', html)
        if m2:
            return jsonify({"channel_id": m2.group(1), "channel_name": ""})

    except Exception:
        pass

    base = "https://www.googleapis.com/youtube/v3/channels"

    def yt_api_get(url: str):
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.loads(r.read().decode("utf-8"))

    if kind in ("handle", "raw", "custom"):
        candidate_url = (
            f"{base}?part=id,snippet"
            f"&forHandle={urllib.parse.quote(value)}"
            f"&key={api_key}"
        )
    else:
        candidate_url = (
            f"{base}?part=id,snippet"
            f"&forUsername={urllib.parse.quote(value)}"
            f"&key={api_key}"
        )

    try:
        data  = yt_api_get(candidate_url)
        items = data.get("items", [])
        if items:
            return jsonify({
                "channel_id":   items[0]["id"],
                "channel_name": items[0].get("snippet", {}).get("title", ""),
            })
    except Exception as exc:
        return jsonify({"error": f"YouTube API request failed: {exc}"}), 502

    if kind == "custom":
        search_url = (
            f"https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet&type=channel"
            f"&q={urllib.parse.quote(value)}&maxResults=1&key={api_key}"
        )
        try:
            sdata  = yt_api_get(search_url)
            sitems = sdata.get("items", [])
            if sitems:
                return jsonify({
                    "channel_id":   sitems[0]["snippet"]["channelId"],
                    "channel_name": sitems[0]["snippet"]["channelTitle"],
                })
        except Exception as exc:
            return jsonify({"error": f"YouTube search API failed: {exc}"}), 502

    return jsonify({"error": f"Could not resolve channel: {value}"}), 404


# ── Channels & Videos API ──────────────────────────────────────────────────────

@app.route("/api/channels")
def api_channels():
    try:
        with PooledConn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        c.channel_id,
                        c.channel_name,
                        COUNT(v.video_id)                                        AS video_count,
                        SUM(CASE WHEN v.is_chunked THEN 1 ELSE 0 END)           AS chunked_count,
                        SUM(
                            CASE WHEN v.is_chunked AND v.duration IS NOT NULL
                                 THEN EXTRACT(EPOCH FROM v.duration::interval)::int
                                 ELSE 0 END
                        )                                                        AS total_chunked_seconds
                    FROM channels c
                    LEFT JOIN videos v ON v.channel_id = c.channel_id
                    GROUP BY c.channel_id, c.channel_name
                    ORDER BY c.channel_name
                """)
                rows = list(cur.fetchall())
        for row in rows:
            row["video_count"]           = int(row["video_count"]           or 0)
            row["chunked_count"]         = int(row["chunked_count"]         or 0)
            row["total_chunked_seconds"] = int(row["total_chunked_seconds"] or 0)
        return jsonify(rows)
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/channels/<channel_id>/videos")
def api_channel_videos(channel_id: str):
    try:
        with PooledConn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT video_id, video_title, published_at, is_chunked, duration
                    FROM videos
                    WHERE channel_id = %s
                    ORDER BY published_at DESC
                """, (channel_id,))
                rows = list(cur.fetchall())
        for row in rows:
            if row["published_at"]:
                row["published_at"] = row["published_at"].strftime("%Y-%m-%d %H:%M")
            row["is_chunked"] = bool(row["is_chunked"])
            row["duration"]   = row["duration"] or "—"
        return jsonify(rows)
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/global_stats")
def api_global_stats():
    try:
        with PooledConn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                                              AS total_videos,
                        SUM(CASE WHEN is_chunked THEN 1 ELSE 0 END)          AS chunked_count,
                        SUM(
                            CASE WHEN is_chunked AND duration IS NOT NULL
                                 THEN EXTRACT(EPOCH FROM duration::interval)::int
                                 ELSE 0 END
                        )                                                     AS total_chunked_seconds
                    FROM videos
                """)
                row = cur.fetchone()
        total   = int(row["total_videos"]         or 0)
        chunked = int(row["chunked_count"]         or 0)
        secs    = int(row["total_chunked_seconds"] or 0)
        return jsonify({
            "total_videos":          total,
            "chunked_count":         chunked,
            "pending_count":         total - chunked,
            "total_chunked_seconds": secs,
        })
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500


# ── Stop Task ──────────────────────────────────────────────────────────────────

@app.route("/api/tasks/<task_id>/stop", methods=["POST"])
def stop_task(task_id: str):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Unknown task"}), 404
    if task["status"] != "running":
        return jsonify({"ok": True, "message": "Task already finished"})

    proc = task.get("proc")
    if proc and proc.poll() is None:
        try:
            proc.terminate()
        except Exception as e:
            return jsonify({"error": f"Could not terminate process: {e}"}), 500

    task["status"] = "stopped"
    enqueue(task_id, "stopped", {
        "message":    "Stopped by user.",
        "successful": task["result"].get("successful", 0),
        "total":      task["result"].get("total",      0),
        "output_path": task["result"].get("output_path", ""),
    })
    return jsonify({"ok": True})


# ── Collector Task ─────────────────────────────────────────────────────────────

@app.route("/run_collector", methods=["POST"])
def run_collector():
    body       = request.get_json(force=True)
    api_key    = body.get("api_key",    "").strip()
    channel_id = body.get("channel_id", "").strip()
    if not api_key or not channel_id:
        return jsonify({"error": "api_key and channel_id are required"}), 400

    task_id = create_task("collector")

    def run():
        cmd = [PYTHON, "youtube_api_db.py",
               "--api-key", api_key, "--channel-id", channel_id, "--dump-json"]
        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, encoding="utf-8", env=get_utf8_env(),
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )
            tasks[task_id]["proc"] = proc
            inserted, api_items, channel_name = 0, 0, ""

            for raw_line in proc.stdout:
                line = raw_line.rstrip()
                enqueue(task_id, "log", {"line": line})
                if "[RESULT] New videos inserted" in line:
                    try: inserted     = int(line.split(":")[-1].strip())
                    except ValueError: pass
                if "[RESULT] API items fetched" in line:
                    try: api_items    = int(line.split(":")[-1].strip())
                    except ValueError: pass
                if "[RESULT] Channel name" in line:
                    channel_name = line.split(":", 1)[-1].strip()

            proc.wait(timeout=120)

            # Read the JSON dump from the known absolute path
            dump_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "videos_data_channel.json")
            new_ids = []
            if os.path.exists(dump_path):
                try:
                    with open(dump_path, encoding="utf-8") as f:
                        new_ids = [v["video_id"] for v in json.load(f)]
                except Exception:
                    pass

            if proc.returncode == 0:
                payload = {"inserted": inserted, "api_items": api_items,
                           "new_ids": new_ids, "channel_name": channel_name}
                tasks[task_id]["status"] = "done"
                tasks[task_id]["result"] = payload
                enqueue(task_id, "done", payload)
            else:
                tasks[task_id]["status"] = "error"
                enqueue(task_id, "error", {"message": "Collector exited with non-zero code."})

        except subprocess.TimeoutExpired:
            proc.kill()
            tasks[task_id]["status"] = "error"
            enqueue(task_id, "error", {"message": "Collector timed out."})
        except Exception as e:
            tasks[task_id]["status"] = "error"
            enqueue(task_id, "error", {"message": str(e)})

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"task_id": task_id})


# ── Shared chunker subprocess runner ──────────────────────────────────────────

def _run_chunker_process(task_id: str, cmd: list, total: int, extra_result_parser=None):
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, encoding="utf-8", env=get_utf8_env(),
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        tasks[task_id]["proc"] = proc

        _total        = total
        successful    = 0
        deleted_count = 0
        extra_result  = {}
        ip_ban_msg    = ""

        for raw_line in proc.stdout:
            line = raw_line.rstrip()

            if line.startswith("[PROGRESS]"):
                _total = _parse_progress(line, task_id, _total)
                continue

            enqueue(task_id, "log", {"line": line})

            if line.startswith("Processing:"):
                title = line[len("Processing:"):].strip()
                enqueue(task_id, "current_video", {"title": title})

            if "[RESULT] Videos chunked successfully" in line:
                try: successful    = int(line.split(":")[-1].strip())
                except ValueError: pass
            if "[RESULT] Deleted (no transcript)" in line:
                try: deleted_count = int(line.split(":")[-1].strip())
                except ValueError: pass
            if "[RESULT] IP ban detected" in line:
                ip_ban_msg = line.split(":", 1)[-1].strip()

            if extra_result_parser:
                extra_result_parser(line, extra_result)

        try:
            proc.wait(timeout=1800)
        except subprocess.TimeoutExpired:
            proc.kill()
            tasks[task_id]["status"] = "error"
            enqueue(task_id, "error", {"message": "Chunker timed out after 30 min."})
            return

        payload = {
            "successful":    successful,
            "total":         _total,
            "deleted_count": deleted_count,
            **extra_result,
        }
        tasks[task_id]["result"].update(payload)

        was_stopped = tasks[task_id]["status"] == "stopped"

        if ip_ban_msg:
            tasks[task_id]["status"] = "error"
            enqueue(task_id, "error", {
                "message": f"YouTube is blocking your IP — stopped after {ip_ban_msg}. "
                           f"{successful} video(s) were successfully chunked before the ban.",
                "ip_ban":  True,
            })
        elif was_stopped:
            enqueue(task_id, "stopped", {
                "message":     "Stopped by user.",
                "successful":  successful,
                "total":       _total,
                "output_path": tasks[task_id]["result"].get("output_path", ""),
            })
        elif proc.returncode == 0:
            tasks[task_id]["status"]   = "done"
            tasks[task_id]["progress"] = 100
            enqueue(task_id, "done", tasks[task_id]["result"])
        else:
            tasks[task_id]["status"] = "error"
            enqueue(task_id, "error", {"message": "Chunker exited with non-zero code."})

    except Exception as e:
        tasks[task_id]["status"] = "error"
        enqueue(task_id, "error", {"message": str(e)})


# ── Chunker Task (local) ───────────────────────────────────────────────────────

@app.route("/run_chunker", methods=["POST"])
def run_chunker():
    body        = request.get_json(force=True)
    channel_id  = body.get("channel_id",  "").strip()
    output_path = body.get("output_path", "./youtubechunks").strip()
    video_ids   = body.get("video_ids",   [])
    mode        = body.get("mode",        "channel")

    if not channel_id:
        return jsonify({"error": "channel_id is required"}), 400

    from pathlib import Path
    Path(output_path).mkdir(parents=True, exist_ok=True)

    task_id = create_task("chunker")
    total   = int(body.get("total_videos", len(video_ids) if video_ids else 1))
    tasks[task_id]["result"]["output_path"] = output_path

    def run():
        cmd = [PYTHON, "yt_chunked_db.py",
               "--channel-id", channel_id,
               "--output-dir", output_path]
        if mode == "selected" and video_ids:
            cmd += ["--video-ids", ",".join(video_ids)]
        _run_chunker_process(task_id, cmd, total)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"task_id": task_id})


# ── MinIO Chunker Task ─────────────────────────────────────────────────────────

@app.route("/run_chunker_minio", methods=["POST"])
def run_chunker_minio():
    body             = request.get_json(force=True)
    channel_id       = body.get("channel_id",       "").strip()
    video_ids        = body.get("video_ids",         [])
    mode             = body.get("mode",              "channel")
    minio_endpoint   = body.get("minio_endpoint",   "localhost:9000").strip()
    minio_access_key = body.get("minio_access_key", "minioadmin").strip()
    minio_secret_key = body.get("minio_secret_key", "minioadmin").strip()
    minio_bucket     = body.get("minio_bucket",     "youtube-chunks").strip()

    if not channel_id:
        return jsonify({"error": "channel_id is required"}), 400
    if not minio_endpoint:
        return jsonify({"error": "minio_endpoint is required"}), 400

    task_id = create_task("chunker_minio")
    total   = int(body.get("total_videos", len(video_ids) if video_ids else 1))
    tasks[task_id]["result"]["output_path"]  = f"minio://{minio_endpoint}/{minio_bucket}"
    tasks[task_id]["result"]["minio_bucket"] = minio_bucket

    def run():
        cmd = [
            PYTHON, "yt_chunked_db_minio.py",
            "--channel-id",       channel_id,
            "--minio-endpoint",   minio_endpoint,
            "--minio-access-key", minio_access_key,
            "--minio-secret-key", minio_secret_key,
            "--minio-bucket",     minio_bucket,
        ]
        if mode == "selected" and video_ids:
            cmd += ["--video-ids", ",".join(video_ids)]

        def extra_parser(line, extra):
            if "[RESULT] MinIO bucket" in line:
                bucket = line.split(":", 1)[-1].strip()
                extra["output_path"]  = f"minio://{minio_endpoint}/{bucket}"
                extra["minio_bucket"] = bucket

        _run_chunker_process(task_id, cmd, total, extra_parser)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"task_id": task_id})


# ── SSE stream ─────────────────────────────────────────────────────────────────

@app.route("/status/<task_id>")
def status(task_id: str):
    if task_id not in tasks:
        return jsonify({"error": "Unknown task"}), 404

    def generate():
        task = tasks[task_id]
        q    = task["queue"]
        while True:
            try:
                item  = q.get(timeout=30)
                event = item["event"]
                data  = json.dumps(item["data"])
                yield f"event: {event}\ndata: {data}\n\n"
                if event in ("done", "error", "stopped"):
                    break
            except queue.Empty:
                yield ": heartbeat\n\n"
                if task["status"] in ("done", "error", "stopped"):
                    break

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Startup ────────────────────────────────────────────────────────────────────

# Initialize the DB pool on startup (best-effort — won't crash if DB is offline)
try:
    init_pool()
except Exception:
    pass

if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True)
