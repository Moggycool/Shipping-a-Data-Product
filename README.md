# Shipping a Data Product — Telegram ELT Pipeline (Telethon + PostgreSQL + dbt)

This project implements an end-to-end ELT pipeline that:

1) Scrapes messages from selected Telegram channels using **Telethon**
2) Stores raw data in a local **data lake** (JSON partitions + downloaded images)
3) Loads raw JSON into **PostgreSQL** (raw schema)
4) Transforms data into an analytics-ready **star schema** using **dbt** with tests and custom data quality checks

---

## Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Lake Layout](#data-lake-layout)
- [Setup](#setup)
- [Environment Variables](#environment-variables)
- [How to Run (End-to-End)](#how-to-run-end-to-end)
- [dbt Models](#dbt-models)
- [Data Quality (dbt Tests)](#data-quality-dbt-tests)
- [Operational Notes (Logging, Retries, Resume)](#operational-notes-logging-retries-resume)
- [API (Optional)](#api-optional)

---

## Architecture

**Extract (Telethon)**  
Telegram Channels → `src/scraper.py` → `data/raw/telegram_messages/...` + `data/raw/images/...`

**Load (Python)**  
Raw JSON → `src/load_raw_to_postgres.py` → PostgreSQL `raw` schema tables

**Transform (dbt)**  
`raw` → `staging` → `analytics` (marts star schema)

**Quality**  
dbt generic tests + custom SQL tests in `medical_warehouse/tests/`

---

## Project Structure

```
Shipping-a-Data-Product
├─ api
│  ├─ database.py
│  ├─ main.py
│  ├─ schemas.py
│  └─ __init__.py
├─ channels.txt
├─ data
│  ├─ processed
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
│  │  │  ├─ fct_messages.sql
│  │  │  └─ schema.yml
│  │  └─ staging
│  │     ├─ schema.yml
│  │     └─ stg_telegram_messages.sql
│  ├─ package-lock.yml
│  ├─ packages.yml
│  └─ tests
│     ├─ assert_non_negative_views.sql
│     └─ assert_no_future_messages.sql
├─ notebooks
│  ├─ task02_data_modeling_and_transformation.ipynb
│  ├─ task1_telegram_scraping.ipynb
│  └─ __init__.py
├─ README.md
├─ requirements.txt
├─ src
│  ├─ load_raw_to_postgres.py
│  ├─ login_test.py
│  ├─ scraper.py
│  └─ __init__.py
└─ tests
   ├─ tests
   └─ __init__.py

```

> Note: Large raw data is not committed to git; a small sample may be provided under `data/sample/`.

---

## Data Lake Layout

### Raw Telegram messages (partitioned by date)

- data/raw/telegram_messages/YYYY-MM-DD/channel_name.json

### Downloaded images

- data/raw/images/{channel_name}/{message_id}.jpg

### Required fields in the raw JSON

Each scraped message record includes at least:

- `message_id`
- `channel_name`
- `message_date`
- `message_text`
- `has_media`
- `image_path`
- `views`
- `forwards`

---

## Setup

### 1) Create and activate a virtual environment

**Windows (PowerShell)**

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d

### Environment Variables

Create a .env file in the project root (do not commit it):

# Telegram

TELEGRAM_API_ID=<your_api_id>
TELEGRAM_API_HASH=<your_api_hash>
TELEGRAM_PHONE=<your_phone_or_blank_if_not_needed>

# Postgres

DB_HOST=localhost
DB_PORT=5432
DB_NAME=<db_name>
DB_USER=<db_user>
DB_PASSWORD=<db_password>

# Optional

CHANNELS_FILE=channels.txt

## How to Run (End-to-End)

Step 1 — Scrape Telegram data (Extract)
Bash
copy
python src/scraper.py
Logs are written to:

logs/scraper.log
logs/scrape_<date>.log (if enabled)
Step 2 — Load raw JSON into Postgres (Load)
Bash
copy
python src/load_raw_to_postgres.py
This loads JSON partitions from data/raw/telegram_messages/ into the PostgreSQL raw schema.

Step 3 — Transform with dbt (Transform)
Bash
copy
cd medical_warehouse
dbt deps
dbt build
dbt test
dbt docs generate

### To view docs locally

Bash
copy
dbt docs serve --port 8081
dbt Models
Staging
stg_telegram_messages
casting and cleanup
calculated fields (e.g., message_length, has_image)

### Marts (Star Schema)

dim_channels
channel_key, channel_name, channel_type
first_post_date, last_post_date
total_posts, avg_views
dim_dates
date_key (YYYYMMDD), full_date, day_of_week, day_name
week_of_year, month, quarter, year, is_weekend
fct_messages
message_id, channel_key, date_key
message_text, message_length
view_count, forward_count, has_image

### Data Quality (dbt Tests)

1) Generic tests
Located in:

medical_warehouse/models/staging/schema.yml
medical_warehouse/models/marts/schema.yml
Includes checks like:

not_null, unique
relationships (FK integrity from facts to dimensions)
2) Custom tests
Located in:

medical_warehouse/tests/ Examples:
assert_no_future_messages.sql
assert_non_negative_views.sql

## Operational Notes (Logging, Retries, Resume)

- Logging: Scraper writes logs to logs/ including channels processed, counts, and exceptions.
- Rate limiting / FloodWait: Scraper handles Telegram rate limits by sleeping and continuing.
- Retries: Network issues are handled via retry/backoff to avoid job failure.
- Resume state: data/raw/scrape_state.json stores progress (last processed date/message) to support incremental scraping.

## API (Task 4 — Analytical FastAPI)

This project exposes curated analytics from the dbt marts (PostgreSQL) via a FastAPI service.

### Requirements

- PostgreSQL running and populated (raw loaded + dbt marts built)
- Python dependencies installed (see `requirements.txt`)

### Environment Variables (API)

Set these before running the API:

- `DATABASE_URL` — SQLAlchemy connection string  
  Example:
  `postgresql+psycopg2://postgres:<password>@localhost:5432/medical_warehouse`

- `MART_SCHEMA` — schema containing dbt marts  
  Example: `analytics_analytics`

### Run the API

bash
uvicorn api.main:app --reload --port 8000

### API Documentation

Swagger UI: <http://127.0.0.1:8000/docs>
OpenAPI JSON: <http://127.0.0.1:8000/openapi.json>

### Example Requests

Bash

- Health
curl "<http://127.0.0.1:8000/health>"
- Endpoint 1: Top Products
curl "<http://127.0.0.1:8000/api/reports/top-products?limit=10>"
- Endpoint 2: Channel Activity
curl "<http://127.0.0.1:8000/api/channels/><channel_name>/activity?grain=day&days=30"
- Endpoint 3: Message Search
curl "<http://127.0.0.1:8000/api/search/messages?q=paracetamol&limit=20&offset=0>"
- Endpoint 4: Visual Content Stats
curl "<http://127.0.0.1:8000/api/reports/visual-content>"

### Screenshots are stored in docs/screenshots/

- task4_01_docs.png — Swagger UI landing page
- task4_02_top_products.png — /api/reports/top-products?limit=10
- task4_03_channel_activity.png — /api/channels/{channel_name}/activity
- task4_04_message_search.png — /api/search/messages?q=paracetamol&limit=20
- task4_05_visual_content.png — /api/reports/visual-content
- task4_06_validation_422.png- /api/reports/validation
