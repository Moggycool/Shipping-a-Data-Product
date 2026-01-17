"""Telegram scraper module."""
from __future__ import annotations

import os
import json
import csv
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import base64

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import Message


# ============================================================
# Paths (Project layout)
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_LAKE_DIR = PROJECT_ROOT / "data" / "raw" / "telegram_messages"
IMAGES_DIR = PROJECT_ROOT / "data" / "raw" / "images"
CSV_DIR = PROJECT_ROOT / "data" / "raw" / "csv"
STATE_PATH = PROJECT_ROOT / "data" / "raw" / "scrape_state.json"
LOG_DIR = PROJECT_ROOT / "logs"
CHANNELS_TXT = PROJECT_ROOT / "channels.txt"


# ============================================================
# Performance controls
# ============================================================
MAX_MESSAGES_PER_CHANNEL = int(os.getenv("MAX_MESSAGES_PER_CHANNEL", "300"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))

DOWNLOAD_MEDIA = os.getenv(
    "DOWNLOAD_MEDIA", "1").lower() not in ("0", "false", "no")
MAX_MEDIA_PER_CHANNEL = int(os.getenv("MAX_MEDIA_PER_CHANNEL", "50"))

PER_CHANNEL_TIMEOUT_SEC = int(os.getenv("PER_CHANNEL_TIMEOUT_SEC", "180"))
FLOODWAIT_MAX_SLEEP_SEC = int(os.getenv("FLOODWAIT_MAX_SLEEP_SEC", "60"))

RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_BASE_DELAY_SEC = float(os.getenv("RETRY_BASE_DELAY_SEC", "2.0"))


# ============================================================
# Logging (daily log file + console)
# ============================================================
def setup_logging() -> logging.Logger:
    """Setup logging to file and console."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).date().isoformat()
    log_path = LOG_DIR / f"scrape_{today}.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)

    return logging.getLogger("telegram_scraper")


logger = setup_logging()


# ============================================================
# JSON helpers
# ============================================================
def json_default(o: Any) -> Any:
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(o, datetime):
        return o.astimezone(timezone.utc).isoformat()
    if isinstance(o, Path):
        return str(o)
    if isinstance(o, bytes):
        return {"_type": "bytes", "base64": base64.b64encode(o).decode()}
    return str(o)


# ============================================================
# Helpers
# ============================================================
def ensure_dirs() -> None:
    """Ensure necessary directories exist."""
    DATA_LAKE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


def normalize_channel(s: str) -> str:
    """Normalize a Telegram channel string to just the username."""
    return s.replace("https://t.me/", "").replace("@", "").split("?")[0].strip()


def load_channels() -> List[str]:
    """Load the list of Telegram channels to scrape."""
    required = ["CheMed123", "lobelia4cosmetics",
                "tikvahpharma", "rayapharmaceuticals"]
    extra: List[str] = []

    if CHANNELS_TXT.exists():
        extra = [
            normalize_channel(l)
            for l in CHANNELS_TXT.read_text(encoding="utf-8").splitlines()
            if l and not l.startswith("#")
        ]

    return sorted(set(map(normalize_channel, required + extra)))


def load_state() -> Dict[str, Any]:
    """Load state dict from the state JSON file."""
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {"channels": {}}


def save_state(state: Dict[str, Any]) -> None:
    """Save state dict to the state JSON file."""
    STATE_PATH.write_text(json.dumps(
        state, indent=2, default=json_default), encoding="utf-8")


def get_last_message_id(state: Dict[str, Any], channel: str) -> int:
    """Get last message ID for a channel from the state dict."""
    channels = state.get("channels", {})
    value = channels.get(channel, 0)

    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    if isinstance(value, dict):
        for key in ("last_message_id", "last_id", "lastMessageId"):
            if key in value:
                try:
                    return int(value[key])
                except Exception:
                    return 0
        return 0
    return 0


def set_last_message_id(state: Dict[str, Any], channel: str, msg_id: int) -> None:
    """Set last message ID for a channel in the state dict."""
    state.setdefault("channels", {})[channel] = msg_id


def msg_day(dt: Optional[datetime]) -> str:
    """Get the ISO date string (YYYY-MM-DD) for a message datetime."""
    return (dt or datetime.now(timezone.utc)).date().isoformat()


async def safe_sleep(seconds: int) -> None:
    await asyncio.sleep(max(0, seconds))


async def run_with_retries(coro_factory, *, label: str) -> Any:
    """
    Retries a coroutine factory with exponential backoff for transient errors.
    coro_factory: callable returning an awaitable
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return await coro_factory()
        except FloodWaitError as e:
            wait_s = int(getattr(e, "seconds", 0) or 0)
            sleep_sec = min(
                wait_s, FLOODWAIT_MAX_SLEEP_SEC) if wait_s > 0 else FLOODWAIT_MAX_SLEEP_SEC
            logger.warning(
                f"[{label}] FloodWaitError: sleeping {sleep_sec}s (attempt {attempt})")
            await safe_sleep(sleep_sec)
        except (RPCError, asyncio.TimeoutError, OSError) as e:
            if attempt >= RETRY_MAX_ATTEMPTS:
                logger.exception(
                    f"[{label}] giving up after {attempt} attempts: {e}")
                raise
            delay = RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1))
            logger.warning(
                f"[{label}] error={type(e).__name__} retrying in {delay:.1f}s (attempt {attempt})"
            )
            await safe_sleep(int(delay))


# ============================================================
# CSV backup
# ============================================================
def append_to_csv(day: str, records: List[Dict[str, Any]]) -> None:
    """Append records to a CSV file for a specific day."""
    day_dir = CSV_DIR / day
    day_dir.mkdir(parents=True, exist_ok=True)
    csv_path = day_dir / "telegram_data.csv"

    write_header = not csv_path.exists()

    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "channel",
                "message_id",
                "date",
                "text",
                "has_media",
                "media_path",
            ],
        )
        if write_header:
            writer.writeheader()
        for r in records:
            writer.writerow({k: r.get(k) for k in writer.fieldnames})


# ============================================================
# Media download
# ============================================================
async def maybe_download_media(client: TelegramClient, msg: Message, channel: str) -> Optional[str]:
    """
    Download media for a message if enabled.
    Returns a relative path string (from PROJECT_ROOT) or None.
    """
    if not DOWNLOAD_MEDIA or not msg.media:
        return None

    channel_dir = IMAGES_DIR / channel
    channel_dir.mkdir(parents=True, exist_ok=True)

    out_base = channel_dir / f"{msg.id}"
    try:
        saved_path = await client.download_media(msg, file=str(out_base))
        if not saved_path:
            return None

        # store relative path to keep portability across machines
        try:
            return str(Path(saved_path).resolve().relative_to(PROJECT_ROOT.resolve()))
        except Exception:
            return str(saved_path)
    except Exception as e:
        logger.warning(
            f"Media download failed @{channel} msg_id={msg.id}: {type(e).__name__}: {e}")
        return None


# ============================================================
# Message processing
# ============================================================
def msg_to_record(msg: Message, channel: str, media_path: Optional[str]) -> Dict[str, Any]:
    """Convert a Telegram message to a dict record (loader-compatible + rubric aliases)."""
    dt_iso = msg.date.astimezone(
        timezone.utc).isoformat() if msg.date else None
    text = msg.message or ""

    views = int(getattr(msg, "views", 0) or 0)
    forwards = int(getattr(msg, "forwards", 0) or 0)

    # rubric-friendly alias names (do not break existing loader)
    return {
        # === loader expects these ===
        "channel": f"@{channel}",
        "message_id": msg.id,
        "date": dt_iso,
        "text": text,
        "has_media": bool(msg.media),
        "media_path": media_path,

        # === rubric-friendly aliases ===
        "channel_name": f"@{channel}",
        "message_date": dt_iso,
        "message_text": text,
        "image_path": media_path,
        "views": views,
        "forwards": forwards,

        # raw backup for lossless load
        "raw": msg.to_dict(),
    }


# ============================================================
# Scraping
# ============================================================
@dataclass
class ChannelResult:
    """Result of scraping a channel."""
    channel: str
    new_messages: int
    new_last_message_id: int
    media_downloaded: int


async def scrape_channel(client: TelegramClient, channel: str, last_id: int) -> ChannelResult:
    """Scrape messages from a Telegram channel since the last message ID."""
    logger.info(f"Scraping @{channel} (last_id={last_id})")

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    records_by_day: Dict[str, List[Dict[str, Any]]] = {}
    new_last_id = last_id

    media_downloaded = 0
    seen_messages = 0

    # NOTE: iter_messages yields newest -> oldest
    async for msg in client.iter_messages(channel, min_id=last_id):
        seen_messages += 1
        if seen_messages > MAX_MESSAGES_PER_CHANNEL:
            break

        if msg.date and msg.date < cutoff:
            break

        if msg.id and msg.id > new_last_id:
            new_last_id = msg.id

        media_path: Optional[str] = None
        if DOWNLOAD_MEDIA and msg.media and media_downloaded < MAX_MEDIA_PER_CHANNEL:
            media_path = await maybe_download_media(client, msg, channel)
            if media_path:
                media_downloaded += 1

        rec = msg_to_record(msg, channel, media_path)
        records_by_day.setdefault(msg_day(msg.date), []).append(rec)

    for day, recs in records_by_day.items():
        # JSON partition
        day_dir = DATA_LAKE_DIR / day
        day_dir.mkdir(parents=True, exist_ok=True)
        fp = day_dir / f"{channel}.json"

        # FIX: correctly load existing
        existing: List[Dict[str, Any]] = []
        if fp.exists():
            existing = json.loads(fp.read_text(encoding="utf-8"))

        merged: Dict[int, Dict[str, Any]] = {}
        for r in existing:
            mid = r.get("message_id")
            if isinstance(mid, int):
                merged[mid] = r
            else:
                try:
                    merged[int(mid)] = r
                except Exception:
                    continue

        for r in recs:
            merged[int(r["message_id"])] = r

        fp.write_text(
            json.dumps(list(merged.values()), indent=2, default=json_default),
            encoding="utf-8",
        )

        # CSV backup
        append_to_csv(day, recs)

    new_messages = sum(len(v) for v in records_by_day.values())
    return ChannelResult(
        channel=channel,
        new_messages=new_messages,
        new_last_message_id=new_last_id,
        media_downloaded=media_downloaded,
    )


# ============================================================
# Main
# ============================================================
async def main() -> None:
    """Main scraping routine."""
    logger.info("=== Starting Telegram scraping ===")
    ensure_dirs()
    load_dotenv(PROJECT_ROOT / ".env")

    api_id_raw = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")

    if not api_id_raw or not api_hash:
        raise ValueError(
            "Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in .env")

    api_id = int(api_id_raw)
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_scraper")

    channels = load_channels()
    state = load_state()

    logger.info(
        f"Config: channels={len(channels)} lookback_days={LOOKBACK_DAYS} "
        f"max_messages_per_channel={MAX_MESSAGES_PER_CHANNEL} download_media={DOWNLOAD_MEDIA} "
        f"max_media_per_channel={MAX_MEDIA_PER_CHANNEL} per_channel_timeout={PER_CHANNEL_TIMEOUT_SEC}s"
    )

    async with TelegramClient(session_name, api_id, api_hash) as client:
        for ch in channels:
            last_id = get_last_message_id(state, ch)

            try:
                async def _do():
                    return await asyncio.wait_for(
                        scrape_channel(client, ch, last_id),
                        timeout=PER_CHANNEL_TIMEOUT_SEC,
                    )

                result: ChannelResult = await run_with_retries(_do, label=f"channel @{ch}")

                logger.info(
                    f"Done @{ch}: new_messages={result.new_messages} "
                    f"media_downloaded={result.media_downloaded} new_last_id={result.new_last_message_id}"
                )

                if result.new_last_message_id > last_id:
                    set_last_message_id(state, ch, result.new_last_message_id)
                    save_state(state)

            except Exception as e:
                # log-and-continue so one channel doesn't kill the job
                logger.exception(
                    f"Failed channel @{ch} (continuing): {type(e).__name__}: {e}")
                continue

    logger.info("=== Scraping complete ===")


if __name__ == "__main__":
    asyncio.run(main())
