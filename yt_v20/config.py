"""
config.py — Configuration for YouTube Pipeline Orchestrator v20

get_config() reads config_user.json fresh on every call so changes
saved in the UI apply immediately — no restart needed.
"""

import json
import os

DEFAULTS = {
    "PG_HOST":           "localhost",
    "PG_PORT":           5432,
    "PG_USER":           "postgres",
    "PG_PASSWORD":       "",
    "PG_DB":             "yt_postgres",
    "CHUNK_SIZE":        30,
    "LANGS":             ["mk"],
    "MAX_WORKERS":       2,
    "MAX_VIDEO_WORKERS": 1,
}

USER_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_user.json")

# Constants (not user-configurable)
BASE_URL           = "https://www.googleapis.com/youtube/v3"
STOP_THRESHOLD     = 25
DEFAULT_OUTPUT_DIR = "./youtubechunks"


def get_config() -> dict:
    """Return current config merged with user overrides from config_user.json."""
    cfg = dict(DEFAULTS)
    if os.path.exists(USER_CONFIG_PATH):
        try:
            with open(USER_CONFIG_PATH, "r", encoding="utf-8") as f:
                user = json.load(f)
            # Accept only known keys — drop any stale MYSQL_* leftovers
            for k in DEFAULTS:
                if k in user:
                    cfg[k] = user[k]
        except Exception:
            pass

    cfg["PG_PORT"]          = int(cfg["PG_PORT"])
    cfg["CHUNK_SIZE"]       = int(cfg["CHUNK_SIZE"])
    cfg["MAX_WORKERS"]      = int(cfg["MAX_WORKERS"])
    cfg["MAX_VIDEO_WORKERS"] = int(cfg["MAX_VIDEO_WORKERS"])
    if isinstance(cfg["LANGS"], str):
        cfg["LANGS"] = [l.strip() for l in cfg["LANGS"].split(",") if l.strip()]

    return cfg


def save_config(updates: dict) -> None:
    """Persist updates to config_user.json (only known keys)."""
    current = get_config()
    for k in DEFAULTS:
        if k in updates:
            current[k] = updates[k]
    if isinstance(current["LANGS"], str):
        current["LANGS"] = [l.strip() for l in current["LANGS"].split(",") if l.strip()]
    with open(USER_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(current, f, indent=2)


def reset_config() -> None:
    """Delete config_user.json, reverting everything to defaults."""
    if os.path.exists(USER_CONFIG_PATH):
        os.remove(USER_CONFIG_PATH)
