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
├─ docker-compose.yml
├─ Dockerfile
├─ medical_warehouse
│  ├─ dbt_project.yml
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
├─ scripts
├─ src
│  ├─ load_raw_to_postgres.py
│  ├─ login_test.py
│  ├─ README.md
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

data/raw/telegram_messages/YYYY-MM-DD/channel_name.json

### Downloaded images

data/raw/images/{channel_name}/{message_id}.jpg

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
