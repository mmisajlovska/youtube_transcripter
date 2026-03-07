# YouTube Pipeline Orchestrator

A self-hosted web application for collecting YouTube video metadata, fetching transcripts, and chunking them into time-stamped segments — with either local filesystem or MinIO object storage output. Built on Flask + PostgreSQL.

---

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Step 0 — Config](#step-0--config)
  - [Step 1 — Collect Video IDs](#step-1--collect-video-ids)
  - [Step 2 — Smart Chunker](#step-2--smart-chunker)
- [Storage Backends](#storage-backends)
- [Output Format](#output-format)
- [Project Structure](#project-structure)
- [Obtaining a YouTube Data API v3 Key](#obtaining-a-youtube-data-api-v3-key)
- [Installing ffmpeg](#installing-ffmpeg)
- [Setting Up MinIO](#setting-up-minio)
- [Troubleshooting](#troubleshooting)

---

## Overview

YouTube Pipeline Orchestrator automates the end-to-end process of building a structured, searchable transcript dataset from any YouTube channel or set of individual videos.

**Pipeline at a glance:**

```
YouTube Channel / Video IDs
        │
        ▼
 [01] Collect Video IDs          ←  YouTube Data API v3  →  PostgreSQL
        │
        ▼
 [02] Smart Chunker              ←  youtube-transcript-api + yt-dlp
        │
        ├──▶  Local Filesystem  (JSON + MP3 per chunk)
        └──▶  MinIO Bucket      (same structure, cloud-ready)
```

---

## Prerequisites

Before installing Python dependencies, ensure the following are present on your system:

| Requirement | Version    | Notes |
|---|------------|---|
| Python | 3.10+      | |
| PostgreSQL | 13+        | Must be running and accessible |
| ffmpeg | Any recent | Required for audio extraction |
| yt-dlp | Latest     | Installed via pip (see below) |

> **ffmpeg** must be installed at the **OS level**, not via pip.
> See [Installing ffmpeg](#installing-ffmpeg) below for platform-specific instructions.

> **PostgreSQL** must be running before you launch the app.
> Download: [https://www.postgresql.org/download/](https://www.postgresql.org/download/)

---

## Installation

```bash
# 1. Clone or unzip the project
cd yt_v20

# 2. Install all Python dependencies in one command
pip install -r requirements.txt

# 3. Start the application
python app.py

# 4. Open in your browser
# http://localhost:5000
```

---

## Configuration

### Step 0 — Config

> ![App screenshot](readme_images/00config.png)

The **Config** tab is the starting point. All settings are persisted to `config_user.json` and take effect immediately — no restart required.

| Field | Description | Default      |
|---|---|--------------|
| `PG_HOST` | PostgreSQL host | `localhost`  |
| `PG_PORT` | PostgreSQL port | `5432`       |
| `PG_USER` | PostgreSQL user | `postgres`   |
| `PG_PASSWORD` | PostgreSQL password | *(empty)*    |
| `PG_DB` | Database name | `youtube_db` |
| `CHUNK_SIZE` | Seconds per transcript chunk | `30`         |
| `LANGS` | Transcript language codes (comma-separated) | `mk`         |
| `MAX_WORKERS` | Parallel chunk-download threads per video | `2`          |
| `MAX_VIDEO_WORKERS` | Videos processed in parallel | `1`          |

**Test Connection** — Use the built-in button to verify your PostgreSQL credentials before running any pipeline step.

**Reset** — Reverts all fields to their defaults by deleting `config_user.json`.

---

## Usage

### Step 1 — Collect Video IDs

> ![App screenshot](readme_images/01api.png)

This step queries the YouTube Data API v3 to retrieve all video IDs, titles, publish dates, and durations for a given channel, then stores them in your PostgreSQL database.

**Inputs required:**

- **YouTube Data API v3 Key** — See [Obtaining a YouTube Data API v3 Key](#obtaining-a-youtube-data-api-v3-key) below.
- **Channel URL :

```
https://www.youtube.com/@ChannelHandle
```

**Behavior:**

- On first run, all videos in the channel are fetched and inserted.
- On subsequent runs, only **new** videos are added — already-stored videos are skipped via incremental detection (stop-early after 25 consecutive known IDs).
- Duration is fetched for every new video using the `contentDetails` endpoint.

---

### Step 2 — Smart Chunker

> ![App screenshot](readme_images/02chunk.png)

The **Smart Chunker** fetches transcripts and audio for every video collected in Step 1 and splits them into fixed-size chunks (default: 30 seconds each).

Each chunk produces:
- A **JSON file** with timestamped transcript entries
- An **MP3 audio file** extracted via `yt-dlp` + `ffmpeg`

#### Channel Mode vs. Individual Video Mode

| Mode | When to use | How to trigger |
|---|---|---|
| **Full Channel** | Process every un-chunked video in the channel | Leave video selection empty |
| **Individual Videos** | Process a specific subset of videos | Select videos from the channel table before running |

> ![App screenshot](readme_images/02multi.png)

---

## Storage Backends

### Local Filesystem

Output is written to a directory on your machine (default: `./youtubechunks`).

```
youtubechunks/
└── Channel_Name/
    └── Video_Title/
        ├── 0.0-30.0s.json
        ├── 0.0-30.0s_audio.mp3
        ├── 30.0-60.0s.json
        ├── 30.0-60.0s_audio.mp3
        └── ...
```

**Best for:** Local development, single-machine setups, or when you want direct filesystem access to chunks.

> ![App screenshot](readme_images/local_output.png)

---

### MinIO Object Storage

Output is uploaded directly to a MinIO bucket. The same folder structure as local mode is used, but objects are stored under:

```
<bucket>/
└── Channel_Name/
    └── Video_Title/
        ├── 0.0-30.0s.json
        ├── 0.0-30.0s_audio.mp3
        └── ...
```

**Best for:** Multi-machine setups, integration with vector databases or ML pipelines, or when you need S3-compatible object storage.

**MinIO-specific fields:**

| Field | Default | Description |
|---|---|---|
| `Endpoint` | `localhost:9000` | MinIO server address (no `http://`) |
| `Access Key` | `minioadmin` | MinIO root user or access key |
| `Secret Key` | `minioadmin` | MinIO root password or secret key |
| `Bucket` | `youtube-chunks` | Bucket name (created automatically if missing) |

See [Setting Up MinIO](#setting-up-minio) for installation instructions.

> ![App screenshot](readme_images/minio_output.png)

---

## Output Format

Each `.json` chunk file contains an array of transcript entries:

```json
[
  {
    "text": "Здраво на сите денес",
    "start": 57.32,
    "duration": 5.879,
    "end": 60.519
  },
  {
    "text": "ќе одиме на прошетка во",
    "start": 60.519,
    "duration": 3.561,
    "end": 63.199
  }
]
```

---

## Project Structure

```
yt_v20/
├── app.py                    # Flask backend — all API routes and SSE task runner
├── config.py                 # Config load / save / reset (reads config_user.json)
├── config_user.json          # User overrides (auto-generated, safe to delete)
├── db.py                     # Centralized PostgreSQL connection pool (ThreadedConnectionPool)
├── chunker_core.py           # Shared chunker engine: IP-ban protection, rate guard,
│                             #   transcript fetch, chunking logic, DB helpers, audio download
├── yt_chunked_db.py          # Local filesystem chunker (implements LocalBackend)
├── yt_chunked_db_minio.py    # MinIO chunker (implements MinioBackend)
├── youtube_api_db.py         # YouTube Data API v3 collector → PostgreSQL
├── requirements.txt          # All Python dependencies (direct + transitive)
├── static/
│   ├── script.js             # Frontend logic (SSE handling, UI state)
│   └── style.css             # Application styles
└── templates/
    └── index.html            # Single-page application shell
```

---

## Obtaining a YouTube Data API v3 Key

>  Video tutorial https://www.youtube.com/watch?v=QY8dhl1EQfI

**Step-by-step:**

1. **Create or sign in** to a Google account at [https://accounts.google.com](https://accounts.google.com).

2. **Open Google Cloud Console** at [https://console.cloud.google.com](https://console.cloud.google.com).

3. **Create a new project:**
   - Click the project dropdown at the top of the page → **New Project**
   - Give it a name (e.g., `youtube-pipeline`) and click **Create**

4. **Enable the YouTube Data API v3:**
   - In the left sidebar, go to **APIs & Services → Library**
   - Search for `YouTube Data API v3`
   - Click the result and press **Enable**

5. **Create an API key:**
   - Go to **APIs & Services → Credentials**
   - Click **Create Credentials → API key**
   - Copy the generated key

6. **(Recommended) Restrict the key:**
   - Click **Edit** on your new key
   - Under **API restrictions**, select **Restrict key** → choose `YouTube Data API v3`
   - Click **Save**

7. **Paste the key** into the YouTube API Key field in Step 1 of the application.

> ⚠️ The free quota is **10,000 units/day**. 
> You can see the cost of quotas here https://developers.google.com/youtube/v3/getting-started

---

## Installing ffmpeg

ffmpeg must be installed at the OS level. It is **not** a Python package and cannot be installed via `pip`.

### Windows (Recommended — WinGet)

The fastest and cleanest way to install ffmpeg on Windows is via **WinGet**, the Windows Package Manager built into Windows 10/11.

Open **PowerShell** and run:

```powershell
winget install Gyan.FFmpeg
```

This installs the full GPL build (version `8.0.1-full_build` by `www.gyan.dev`) and automatically adds `ffmpeg.exe` to your system `PATH`.

After installation, verify it works by opening a new PowerShell window and running:

```powershell
ffmpeg -version
```

You should see output beginning with:
```
ffmpeg version 8.0.1-full_build-www.gyan.dev ...
```
> ⚠️ **Open a new terminal after installation.** WinGet updates your PATH, but the change only takes effect in newly opened terminal windows.
### Linux

On Linux, FFmpeg is usually installed directly from the system package manager.

```bash
  sudo apt update
  sudo apt install ffmpeg
```

Check installation:
```bash
  ffmpeg -version
```

---

## Setting Up MinIO

> Video tutorial https://www.youtube.com/watch?v=jlJHAI3nOFc&t=1s

MinIO is an open-source, S3-compatible object storage server. It runs as a single binary — no installer, no Docker required.

---

### Download

#### Windows

Download the MinIO server binary for Windows from the official page:

**[https://www.min.io/download/aistor-server?platform=windows](https://www.min.io/download/aistor-server?platform=windows)**

Save the downloaded `minio.exe` to a folder of your choice, for example `C:\minio\`.

#### Linux

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/
```

---

### Starting the Server

#### Windows

Open **PowerShell** in the folder where `minio.exe` is saved, then run:

```powershell
.\minio.exe server C:\minio-data --console-address ":9001"
```

- `C:\minio-data` — the folder where MinIO will store all uploaded objects. It will be created automatically if it does not exist. You can change this path to any folder you prefer.
- `--console-address ":9001"` — makes the web management console available at `http://localhost:9001`.

#### Linux

```bash
minio server ~/minio-data --console-address ":9001"
```

Once the server starts, you will see output like:

```
API: http://127.0.0.1:9000
WebUI: http://127.0.0.1:9001
```

Leave this terminal open. MinIO runs in the foreground.

---

### Changing the Default Username and Password

By default, MinIO uses `minioadmin` / `minioadmin` as root credentials. **Change these before storing any real data or exposing MinIO on a network.**

Set the credentials as environment variables **before** starting the server:

#### Windows (PowerShell)

```powershell
$env:MINIO_ROOT_USER = "your_username"
$env:MINIO_ROOT_PASSWORD = "your_strong_password"
.\minio.exe server C:\minio-data --console-address ":9001"
```

#### Linux (bash)

```bash
export MINIO_ROOT_USER=your_username
export MINIO_ROOT_PASSWORD=your_strong_password
minio server ~/minio-data --console-address ":9001"
```

> ⚠️ The password must be **at least 8 characters** or MinIO will refuse to start.

After changing credentials, update the **Access Key** and **Secret Key** fields in the application's Smart Chunker tab to match.

---

### Default Credentials (unchanged)

| Field | Value |
|---|---|
| API Endpoint | `localhost:9000` |
| Web Console | `http://localhost:9001` |
| Root User | `minioadmin` |
| Root Password | `minioadmin` |

### Verify

Open [http://localhost:9001](http://localhost:9001) in your browser. Log in with your credentials. If the MinIO console loads and you can see the **Buckets** section, the server is ready.

---

### Channel Examples
| Channel Name                          | Rating |
|---------------------------------------|--------|
| vasko eftov                           | 1      |
| snezhe velkov                         | 1      |
| infomaks                              | 1      |
| prva tv                               | 1      |
| bajkerot i krosfiterot                | 1      |
| mario arangelovski                    | 0      |
| boki 13                               | 0      |
| lazarov                               | 1      |
| stefanator                            | 0      |
| telma                                 | 1      |
| sitel                                 | 1      |
| kanal5                                | 1      |
| zivotna prikazna                      | 1      |
| Stars Show by Ivana Jankovska         | 0      |
| POPOVIC                               | 0      |
| Mimi Markovski                        | 0      |
| A1 ONmkd                              | 1      |
| BEZ RACNA                             | 0      |
| ALFA TV                               | 1      |
| Glamur Tv                             | 0      |
| hype tv mk                            | 0      |
| Televizija 24Vesti                    | 1      |
| bmg network                           | 1      |
| mandar                                | 0      |
| Backstage Media                       | 0      |
| Stef Steady                           | 1      |
| FUTURIVA                              | 0      |
| Ivan Nedelkovski                      | 1      |
| ZaSe Makedonija                       | 1      |
| Kanal 77                              | 1      |
| NextGen.Podcast                       | 1      |
| Ivan Ivanovski                        | 1      |
| Kako si? podkast                      | 1      |
| Pari                                  | 1      |
| Sto go vrti svetot                    | 1      |
| Dr. Chadikovski Podcast               | 1      |
| Tapshanov                             | 0      |
| Investigative Reporting Lab           | 1      |
| KOD Lupevska                          | 1      |
| Издиши се / Izdishi se                | 1      |
| Marija vo Amerika                     | 1      |
| Pocetna Presmetka                     | 1      |
| Makedonski koreni JB                  | 1      |
| Startup Revolution AI                 | 1      |
| nustarz                               | 1      |
| Bigorski Manastir                     | 1      |


## Troubleshooting

**`ffmpeg not found` error**
Install ffmpeg using the instructions in [Installing ffmpeg](#installing-ffmpeg) above. After installation, open a **new** terminal window before running the app — the PATH change will not apply to already-open terminals.

**`PostgreSQL connection refused`**
Ensure PostgreSQL is running (`pg_isready` on Linux/macOS, or check Services on Windows) and the credentials in the Config tab are correct. Use the **Test Connection** button to verify before starting any pipeline step.

**`No transcript found` for many videos**
The video has no available transcript in the language(s) configured in `LANGS`. Videos with no transcript are automatically removed from the database. Add additional language codes (e.g., `en,mk,de`) to broaden the search.

**YouTube is blocking your IP**
The chunker has a built-in IP-ban circuit breaker. If 3 consecutive transcript requests are blocked, the run stops automatically and all successfully processed videos are saved. Wait several hours before retrying, or use a different network or VPN.

**`yt-dlp` fails to download audio**
Run `yt-dlp --update` to ensure you have the latest version. YouTube frequently changes its internal API, and yt-dlp releases fixes regularly.

**MinIO server exits immediately on Windows**
Ensure you are setting `MINIO_ROOT_PASSWORD` to at least 8 characters. A password that is too short will cause MinIO to exit with an error at startup.

---

*YouTube Pipeline Orchestrator v20*
