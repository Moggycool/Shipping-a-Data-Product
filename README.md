# Shipping a Data Product: From Raw Telegram Data to an Analytical API

End-to-end data pipeline for Ethiopian medical/business intelligence using **Telethon** (Extract), **PostgreSQL** (Load), **dbt** (Transform + Tests), **YOLOv8** (Enrichment), **FastAPI** (Analytics API), and **Dagster** (Orchestration).

> **Week 8 Challenge (10 Academy):** Shipping a Data Product — 14 Jan to 20 Jan 2026  
> **Data domain:** public Telegram medical/pharma-related channels  
> **Pattern:** ELT (raw-first, schema-on-read + curated marts)

---

## Table of Contents

- [1. Architecture (Task 1–5)](#1-architecture-task-15)
- [2. Repository Structure](#2-repository-structure)
- [3. Task 1 — Telegram Scraping & Raw Data Lake](#3-task-1--telegram-scraping--raw-data-lake)
- [4. Task 2 — Load to PostgreSQL + dbt Modeling & Tests](#4-task-2--load-to-postgresql--dbt-modeling--tests)
- [5. Task 3 — YOLOv8 Enrichment (Object Detection)](#5-task-3--yolov8-enrichment-object-detection)
- [6. Task 4 — FastAPI Analytical API](#6-task-4--fastapi-analytical-api)
- [7. Task 5 — Dagster Orchestration (Daily Schedule)](#7-task-5--dagster-orchestration-daily-schedule)
- [8. Setup & Configuration](#8-setup--configuration)
- [9. How to Run End-to-End](#9-how-to-run-end-to-end)
- [10. Data Quality & Observability](#10-data-quality--observability)
- [11. Screenshots / Evidence for Submission](#11-screenshots--evidence-for-submission)
- [12. Troubleshooting](#12-troubleshooting)

---

## 1. Architecture (Task 1–5)

**Extract (Task 1)**

- Telegram channels → `src/scraper.py` (Telethon)
- Writes partitioned JSON + downloads images to the raw data lake

**Load (Task 2)**

- Raw JSON partitions → `src/load_raw_to_postgres.py`
- Loads into PostgreSQL `raw` schema (lossless storage)

**Transform (Task 2)**

- dbt project: `medical_warehouse/`
- `raw` → `staging` → `marts` (star schema)

**Enrich (Task 3)**

- Images → `src/yolo_detect.py` (YOLOv8)
- Produces detection outputs (CSV and/or DB table)
- dbt mart: `fct_image_detections`

**Serve (Task 4)**

- FastAPI service in `api/` reads curated marts and serves analytical endpoints

**Orchestrate (Task 5)**

- Dagster pipeline in `pipeline.py`
- Daily schedule (cron) runs: scrape → load → dbt → yolo (→ optional API stays running separately)

---

## 2. Repository Structure

```
Shipping-a-Data-Product
├─ api
│  ├─ channels
│  ├─ config.py
│  ├─ database.py
│  ├─ docs
│  ├─ main.py
│  ├─ queries.py
│  ├─ reports
│  ├─ schemas.py
│  ├─ search
│  └─ __init__.py
├─ channels.txt
├─ data
│  ├─ processed
│  │  └─ yolo
│  ├─ README.md
│  └─ sample
│     ├─ images
│     │  └─ sample_channel
│     │     └─ 2.jpg
│     └─ telegram_messages
│        └─ 2021-10-28
│           └─ sample_channel.json
├─ docker-compose.yml
├─ Dockerfile
├─ logs
│  └─ README.md
├─ medical_warehouse
│  ├─ dbt_project.yml
│  ├─ logs
│  ├─ models
│  │  ├─ marts
│  │  │  ├─ dim_channels.sql
│  │  │  ├─ dim_dates.sql
│  │  │  ├─ fct_image_detections.sql
│  │  │  ├─ fct_messages.sql
│  │  │  └─ schema.yml
│  │  └─ staging
│  │     ├─ schema.yml
│  │     ├─ stg_telegram_messages.sql
│  │     └─ stg_yolo_detections.sql
│  ├─ package-lock.yml
│  ├─ packages.yml
│  ├─ seeds
│  │  └─ yolo_detections.csv
│  └─ tests
│     ├─ assert_non_negative_views.sql
│     └─ assert_no_future_messages.sql
├─ notebooks
│  ├─ task02_data_modeling_and_transformation.ipynb
│  ├─ task1_telegram_scraping.ipynb
│  ├─ task3_YOLOv8_enrichment_evidence.ipynb
│  ├─ yolov8n.pt
│  └─ __init__.py
├─ pipeline.py
├─ README.md
├─ requirements.txt
├─ src
│  ├─ load_raw_to_postgres.py
│  ├─ login_test.py
│  ├─ scraper.py
│  ├─ yolo_detect.py
│  └─ __init__.py
├─ tests
│  ├─ tests
│  └─ __init__.py
└─ yolov8n.pt

```

> Note: Large raw data is typically **not committed** to git; keep folder structure with `.gitkeep` or provide `data/sample/`.

---

## 3. Task 1 — Telegram Scraping & Raw Data Lake

### Goal

Scrape messages (and images when available) from selected Telegram channels and store them in a partitioned raw data lake.

### Implementation

- Script: `src/scraper.py`
- Channel list: `channels.txt` (or configured in code via `CHANNELS_FILE`)
- Output:
  - Messages: `data/raw/telegram_messages/YYYY-MM-DD/channel_name.json`
  - Images: `data/raw/images/{channel_name}/{message_id}.jpg`

### Raw Message Schema (minimum fields)

Each record includes at least:

- `message_id`
- `channel_name`
- `message_date`
- `message_text`
- `has_media`
- `image_path` (relative or absolute path)
- `views`
- `forwards`

### Notes (robustness)

- Handles Telegram rate limiting (FloodWait) via sleep/retry
- Logs progress/errors to `logs/`
- Optional incremental resume via a state file (if enabled in your implementation)

---

## 4. Task 2 — Load to PostgreSQL + dbt Modeling & Tests

### 4.1 Load Raw JSON to PostgreSQL (raw schema)

- Script: `src/load_raw_to_postgres.py`
- Input: `data/raw/telegram_messages/`
- Output: PostgreSQL tables in a `raw` schema (commonly `raw.telegram_messages`)

**Why JSONB/raw-first?**

- Preserves source fidelity for auditing/backfills
- Enables schema-on-read and iterative modeling

### 4.2 dbt Project (Transformation)

- dbt project directory: `medical_warehouse/`
- Layering:
  - **staging**: cleanup/casting/standardized naming
  - **marts**: analytics-ready star schema

#### Staging model(s)

- `stg_telegram_messages`
  - casting and cleanup
  - calculated fields such as `message_length`, `has_image`

#### Mart models (Star Schema)

- `dim_channels`
  - `channel_key`, `channel_name`, optional metadata
- `dim_dates`
  - `date_key`, `full_date`, day/week/month/quarter/year attributes
- `fct_messages`
  - facts: views/forwards/text/image flags keyed to dimensions

### 4.3 Data Quality (dbt tests)

- Generic tests in:
  - `medical_warehouse/models/staging/schema.yml`
  - `medical_warehouse/models/marts/schema.yml`
- Custom SQL tests in:
  - `medical_warehouse/tests/`
  - Examples:
    - `assert_no_future_messages.sql`
    - `assert_non_negative_views.sql`

---

## 5. Task 3 — YOLOv8 Enrichment (Object Detection)

### Goal

Enrich image data by detecting objects on downloaded Telegram images and publish results for analytics.

### Implementation

- Script: `src/yolo_detect.py`
- Input images:
  - `data/raw/images/{channel_name}/{message_id}.jpg`
- Output:
  - `data/processed/yolo/detections.csv` (recommended)
  - (optional) load detections into PostgreSQL (e.g., `raw.image_detections`)

### Detection schema (recommended columns)

- `image_path`
- `channel_name`
- `message_id`
- `object_class`
- `confidence`
- bounding box: `x1`, `y1`, `x2`, `y2`
- optional: `image_category` (e.g., `product_display` vs `other`)
- `model_name` (e.g., `yolov8n`)
- `detected_at` timestamp

### Analytics model (dbt)

- `medical_warehouse/models/marts/fct_image_detections.sql`
  - fact table used by API endpoints (visual stats, product display rate, etc.)

---

## 6. Task 4 — FastAPI Analytical API

### Goal

Expose curated analytics via an API for downstream consumers.

### App location

- `api/main.py` (FastAPI app)
- `api/schemas.py` (Pydantic response models)
- `api/database.py` (DB connection/session)

### Run the API

uvicorn api.main:app --reload --port 8000

## Task 5 — Pipeline Orchestration (Dagster)

This project uses **Dagster** to orchestrate the full workflow end-to-end with observability and a **daily schedule**.

### What Dagster Orchestrates

The Dagster job runs the pipeline in a dependency chain:

1. **Scrape** Telegram channels (Telethon) → writes raw JSON + images to the data lake  
2. **Load** raw JSON → PostgreSQL `raw` schema  
3. **Transform** with **dbt** → staging + marts (star schema)  
4. **Enrich** images with **YOLOv8** → detections output (CSV and/or DB) and mart model updates

### Where the Dagster Code Lives

- `pipeline.py`

**Expected contents (rubric evidence):**

- `@op` functions for each stage (scrape, load, dbt, yolo)
- A `@job` that wires op dependencies (e.g., `scrape_op -> load_op -> dbt_op -> yolo_op`)
- A daily schedule (cron), e.g. `0 2 * * *`
- `Definitions(...)` registration so Dagster UI discovers the job + schedule:
  - `defs = Definitions(jobs=[shipping_data_product_job], schedules=[daily_schedule])`

> If the schedule does not show in Dagster UI, it is almost always because `Definitions(...)` is missing or not named `defs`.

---

### How to Run Dagster Locally

#### 1) Start required services

Make sure PostgreSQL is running:

bash

- docker compose up -d
- dagster dev -f pipeline.py
Open the Dagster UI: <http://127.0.0.1:3000>
