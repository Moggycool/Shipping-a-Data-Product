"""
load_raw_to_postgres.py

Reads Telegram JSON files from a local "data lake" directory and loads them into
PostgreSQL raw schema/table: raw.telegram_messages

It stores ALL scraped fields losslessly in payload (jsonb) and also extracts
common fields into typed columns.

Run:
  python -m pip install psycopg2-binary python-dotenv
Then:
  python src\\load_raw_to_postgres.py

Recommended: create .env (same folder you run from) or set env vars:
  PGHOST=127.0.0.1
  PGPORT=5432
  PGDATABASE=medical_warehouse
  PGUSER=postgres
  PGPASSWORD=YOUR_PASSWORD
  DATA_LAKE_DIR=D:\\Python\\Week 8\\Shipping-a-Data-Product\\data\\raw\\telegram_messages
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None  # optional


# -----------------------------
# Config
# -----------------------------

@dataclass(frozen=True)
class Config:
    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str
    data_lake_dir: Path

    raw_schema: str = "raw"
    raw_table: str = "telegram_messages"
    batch_size: int = 1000
    verbose: bool = True


def load_config() -> Config:
    # Load .env if present, but do not override already-set environment variables
    if load_dotenv is not None:
        try:
            load_dotenv(override=False)
        except TypeError:
            # older python-dotenv versions may not support override kwarg
            load_dotenv()

    # Prefer IPv4 by default to avoid localhost -> ::1 surprises
    pghost = os.getenv("PGHOST", "127.0.0.1")
    pgport = int(os.getenv("PGPORT", "5432"))
    pgdatabase = os.getenv("PGDATABASE", "medical_warehouse")
    pguser = os.getenv("PGUSER", "postgres")

    # Do NOT hardcode real passwords in code; rely on env var or .env
    pgpassword = os.getenv("PGPASSWORD", "")

    default_lake = r"D:\Python\Week 8\Shipping-a-Data-Product\data\raw\telegram_messages"
    data_lake_dir = Path(os.getenv("DATA_LAKE_DIR", default_lake)).resolve()

    return Config(
        pghost=pghost,
        pgport=pgport,
        pgdatabase=pgdatabase,
        pguser=pguser,
        pgpassword=pgpassword,
        data_lake_dir=data_lake_dir,
    )


# -----------------------------
# Utilities: JSON + parsing
# -----------------------------

DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def iter_json_files(data_lake_dir: Path) -> Iterable[Tuple[Optional[date], Path]]:
    """
    Yields (partition_date, json_path).

    Supports:
      data_lake_dir/YYYY-MM-DD/*.json
    Also supports:
      data_lake_dir/*.json
    """
    if not data_lake_dir.exists():
        raise FileNotFoundError(f"DATA_LAKE_DIR not found: {data_lake_dir}")

    # Partitioned layout
    for day_dir in sorted([p for p in data_lake_dir.glob("*") if p.is_dir()]):
        part_date: Optional[date] = None
        if DATE_DIR_RE.match(day_dir.name):
            try:
                part_date = datetime.strptime(day_dir.name, "%Y-%m-%d").date()
            except Exception:
                part_date = None

        for f in sorted(day_dir.glob("*.json")):
            yield (part_date, f)

    # Flat layout fallback
    for f in sorted(data_lake_dir.glob("*.json")):
        yield (None, f)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def normalize_messages(obj: Any) -> List[Dict[str, Any]]:
    """
    Convert a loaded JSON object into a list of message dicts.
    Handles:
      - list[dict]
      - {"messages": list[dict]}
      - single dict
    """
    if obj is None:
        return []
    if isinstance(obj, list):
        return [m for m in obj if isinstance(m, dict)]
    if isinstance(obj, dict):
        if "messages" in obj and isinstance(obj["messages"], list):
            return [m for m in obj["messages"] if isinstance(m, dict)]
        return [obj]
    return []


def safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            try:
                return int(s.replace(",", ""))
            except Exception:
                return None
    return None


def safe_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip()
        return s if s else None
    return str(x)


def safe_bool(x: Any) -> Optional[bool]:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, int):
        return bool(x)
    if isinstance(x, str):
        s = x.strip().lower()
        if s in ("true", "t", "1", "yes", "y"):
            return True
        if s in ("false", "f", "0", "no", "n"):
            return False
    return None


def safe_dt(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x
    if isinstance(x, (int, float)):
        try:
            return datetime.fromtimestamp(x)
        except Exception:
            return None
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
    return None


def json_default(o: Any) -> Any:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Path):
        return str(o)
    if isinstance(o, (bytes, bytearray)):
        import base64
        return base64.b64encode(o).decode("ascii")
    return str(o)


# -----------------------------
# Extractors tuned to your JSON
# -----------------------------

def extract_channel_username(msg: Dict[str, Any]) -> Optional[str]:
    v = msg.get("channel")
    if isinstance(v, str) and v.strip():
        return v.strip().lstrip("@")

    for key in ("channel_username", "chat_username", "username"):
        v = msg.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip().lstrip("@")

    raw = msg.get("raw")
    if isinstance(raw, dict):
        chat = raw.get("chat")
        if isinstance(chat, dict):
            v = chat.get("username")
            if isinstance(v, str) and v.strip():
                return v.strip().lstrip("@")

    return None


def extract_channel_title(msg: Dict[str, Any]) -> Optional[str]:
    for key in ("channel_title", "chat_title", "title"):
        v = msg.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    raw = msg.get("raw")
    if isinstance(raw, dict):
        chat = raw.get("chat")
        if isinstance(chat, dict):
            v = chat.get("title")
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def extract_message_id(msg: Dict[str, Any]) -> Optional[int]:
    i = safe_int(msg.get("message_id"))
    if i is not None:
        return i

    raw = msg.get("raw")
    if isinstance(raw, dict):
        i = safe_int(raw.get("id"))
        if i is not None:
            return i

    for key in ("id", "msg_id"):
        i = safe_int(msg.get(key))
        if i is not None:
            return i

    return None


def extract_message_datetime(msg: Dict[str, Any]) -> Optional[datetime]:
    dt = safe_dt(msg.get("date"))
    if dt is not None:
        return dt

    raw = msg.get("raw")
    if isinstance(raw, dict):
        dt = safe_dt(raw.get("date"))
        if dt is not None:
            return dt

    return None


def extract_message_text(msg: Dict[str, Any]) -> Optional[str]:
    v = msg.get("text")
    if isinstance(v, str):
        return safe_text(v)

    raw = msg.get("raw")
    if isinstance(raw, dict):
        v = raw.get("message")
        if isinstance(v, str):
            return safe_text(v)

    for key in ("message", "caption"):
        v = msg.get(key)
        if isinstance(v, str):
            return safe_text(v)

    return None


def extract_views(msg: Dict[str, Any]) -> Optional[int]:
    raw = msg.get("raw")
    if isinstance(raw, dict):
        i = safe_int(raw.get("views"))
        if i is not None:
            return i
    return safe_int(msg.get("views"))


def extract_forwards(msg: Dict[str, Any]) -> Optional[int]:
    raw = msg.get("raw")
    if isinstance(raw, dict):
        i = safe_int(raw.get("forwards"))
        if i is not None:
            return i
    return safe_int(msg.get("forwards"))


def extract_reply_count(msg: Dict[str, Any]) -> Optional[int]:
    raw = msg.get("raw")
    if isinstance(raw, dict):
        replies = raw.get("replies")
        if isinstance(replies, dict):
            i = safe_int(replies.get("replies") or replies.get("count"))
            if i is not None:
                return i
    for key in ("reply_count", "replies_count"):
        i = safe_int(msg.get(key))
        if i is not None:
            return i
    return None


def extract_media_flags(msg: Dict[str, Any]) -> Tuple[Optional[bool], Optional[bool], Optional[str]]:
    has_media = safe_bool(msg.get("has_media"))
    media_type: Optional[str] = None
    has_image: Optional[bool] = None

    raw = msg.get("raw")
    if isinstance(raw, dict):
        media = raw.get("media")
        if isinstance(media, dict):
            mt = media.get("_")
            if isinstance(mt, str) and mt.strip():
                media_type = mt
                if mt == "MessageMediaPhoto":
                    has_image = True
                elif mt in ("MessageMediaDocument", "MessageMediaWebPage"):
                    has_image = False

    if has_media is None:
        has_media = True if isinstance(raw, dict) and raw.get(
            "media") is not None else None

    if has_image is None and isinstance(media_type, str):
        has_image = ("Photo" in media_type)

    if has_media is None and msg.get("media_path"):
        has_media = True

    return has_media, has_image, media_type


def extract_media_path(msg: Dict[str, Any]) -> Optional[str]:
    v = msg.get("media_path")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


# -----------------------------
# Postgres DDL + load
# -----------------------------

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    channel_username      TEXT        NOT NULL,
    message_id            BIGINT      NOT NULL,

    channel_title         TEXT        NULL,
    message_ts            TIMESTAMPTZ NULL,
    message_text          TEXT        NULL,

    views                 INTEGER     NULL,
    forwards              INTEGER     NULL,
    reply_count           INTEGER     NULL,

    has_media             BOOLEAN     NULL,
    has_image             BOOLEAN     NULL,
    media_type            TEXT        NULL,
    media_path            TEXT        NULL,

    partition_date        DATE        NULL,
    source_file           TEXT        NULL,
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    payload               JSONB       NOT NULL,

    CONSTRAINT telegram_messages_pk PRIMARY KEY (channel_username, message_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_telegram_messages_ts
    ON raw.telegram_messages (message_ts);

CREATE INDEX IF NOT EXISTS idx_raw_telegram_messages_partition_date
    ON raw.telegram_messages (partition_date);

CREATE INDEX IF NOT EXISTS idx_raw_telegram_messages_payload_gin
    ON raw.telegram_messages USING GIN (payload);
"""

UPSERT_SQL = """
INSERT INTO raw.telegram_messages (
    channel_username,
    message_id,
    channel_title,
    message_ts,
    message_text,
    views,
    forwards,
    reply_count,
    has_media,
    has_image,
    media_type,
    media_path,
    partition_date,
    source_file,
    payload
)
VALUES %s
ON CONFLICT (channel_username, message_id) DO UPDATE SET
    channel_title   = EXCLUDED.channel_title,
    message_ts      = EXCLUDED.message_ts,
    message_text    = EXCLUDED.message_text,
    views           = EXCLUDED.views,
    forwards        = EXCLUDED.forwards,
    reply_count     = EXCLUDED.reply_count,
    has_media       = EXCLUDED.has_media,
    has_image       = EXCLUDED.has_image,
    media_type      = EXCLUDED.media_type,
    media_path      = EXCLUDED.media_path,
    partition_date  = COALESCE(EXCLUDED.partition_date, raw.telegram_messages.partition_date),
    source_file     = COALESCE(EXCLUDED.source_file, raw.telegram_messages.source_file),
    payload         = EXCLUDED.payload;
"""


def connect(cfg: Config):
    try:
        return psycopg2.connect(
            host=cfg.pghost,
            port=cfg.pgport,
            dbname=cfg.pgdatabase,
            user=cfg.pguser,
            password=cfg.pgpassword,
        )
    except psycopg2.OperationalError as e:
        raise psycopg2.OperationalError(
            f"{e}\n\n[Connection details]\n"
            f"  host={cfg.pghost}\n  port={cfg.pgport}\n  dbname={cfg.pgdatabase}\n  user={cfg.pguser}\n"
            f"  (password provided? {'yes' if bool(cfg.pgpassword) else 'no'})\n"
        )


def ensure_schema_and_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def make_row(
    msg: Dict[str, Any],
    partition_date: Optional[date],
    source_file: Path
) -> Optional[Tuple[Any, ...]]:
    channel_username = extract_channel_username(msg)
    message_id = extract_message_id(msg)

    # If channel missing, infer from filename
    if not channel_username:
        channel_username = source_file.stem.strip().lstrip(
            "@") if source_file.stem else None

    if not channel_username or message_id is None:
        return None

    channel_title = extract_channel_title(msg)
    message_ts = extract_message_datetime(msg)
    message_text = extract_message_text(msg)
    views = extract_views(msg)
    forwards = extract_forwards(msg)
    reply_count = extract_reply_count(msg)
    has_media, has_image, media_type = extract_media_flags(msg)
    media_path = extract_media_path(msg)

    payload_json = json.loads(json.dumps(
        msg, default=json_default, ensure_ascii=False))

    return (
        channel_username,
        message_id,
        channel_title,
        message_ts,
        message_text,
        views,
        forwards,
        reply_count,
        has_media,
        has_image,
        media_type,
        media_path,
        partition_date,
        str(source_file),
        psycopg2.extras.Json(payload_json),
    )


def chunked(items: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), n):
        yield items[i: i + n]


def load_files(cfg: Config) -> None:
    total_files = 0
    total_msgs_seen = 0
    total_rows_written = 0
    total_rows_skipped = 0

    conn = connect(cfg)
    try:
        ensure_schema_and_table(conn)

        for part_date, path in iter_json_files(cfg.data_lake_dir):
            total_files += 1

            try:
                obj = read_json(path)
            except Exception as e:
                print(f"[WARN] Failed to read JSON: {path} -> {e}")
                continue

            messages = normalize_messages(obj)
            total_msgs_seen += len(messages)

            rows: List[Tuple[Any, ...]] = []
            for m in messages:
                row = make_row(m, part_date, path)
                if row is None:
                    total_rows_skipped += 1
                else:
                    rows.append(row)

            if not rows:
                if cfg.verbose:
                    print(
                        f"[INFO] {path.name}: 0 rows to write (messages={len(messages)})")
                continue

            with conn.cursor() as cur:
                for batch in chunked(rows, cfg.batch_size):
                    psycopg2.extras.execute_values(
                        cur,
                        UPSERT_SQL,
                        batch,
                        template=None,
                        page_size=len(batch),
                    )
                    total_rows_written += len(batch)

            conn.commit()

            if cfg.verbose:
                print(
                    f"[OK] Loaded {path.name} | partition_date={part_date} | "
                    f"messages={len(messages)} | upserted={len(rows)} | skipped={len(messages)-len(rows)}"
                )

        print("\n=== Load Summary ===")
        print("DATA_LAKE_DIR:", cfg.data_lake_dir)
        print("Files processed:", total_files)
        print("Messages seen:", total_msgs_seen)
        print("Rows upserted:", total_rows_written)
        print("Rows skipped (missing channel/message_id):", total_rows_skipped)

    finally:
        conn.close()


def main() -> None:
    cfg = load_config()
    print("Postgres target:",
          f"{cfg.pguser}@{cfg.pghost}:{cfg.pgport}/{cfg.pgdatabase}")
    print("Data lake dir:", cfg.data_lake_dir)
    load_files(cfg)


if __name__ == "__main__":
    main()
