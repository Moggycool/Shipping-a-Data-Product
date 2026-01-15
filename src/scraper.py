"""
Task 1 - Telegram Scraper (Extract & Load)

Features:
- Scrape messages from public Telegram channels (Telethon)
- Extract required fields: message_id, date, text, views, forwards, media info
- Preserve raw API structure (msg.to_dict())
- Download photos to: data/raw/images/{channel_name}/{message_id}.jpg
- Store raw JSON in partitioned data lake:
    data/raw/telegram_messages/YYYY-MM-DD/channel_name.json
- Incremental scraping with merge/no-duplicates using:
    data/raw/scrape_state.json
- Logging to logs/scraper.log
"""

import os
import json
import logging
import asyncio
import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from collections.abc import Mapping

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    RPCError,
    FloodWaitError,
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import Message


# -----------------------
# Paths / Config
# -----------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_LAKE_MESSAGES_DIR = PROJECT_ROOT / "data" / "raw" / "telegram_messages"
DATA_LAKE_IMAGES_DIR = PROJECT_ROOT / "data" / "raw" / "images"
STATE_PATH = PROJECT_ROOT / "data" / "raw" / "scrape_state.json"

LOG_DIR = PROJECT_ROOT / "logs"

# Required channels (minimum)
DEFAULT_CHANNELS = [
    "CheMed123",            # @CheMed123
    "lobelia4cosmetics",    # https://t.me/lobelia4cosmetics
    "tikvahpharma",         # https://t.me/tikvahpharma
    "rayapharmaceuticals",  # https://t.me/rayapharmaceuticals
]

# Optional: add more channels by putting them (one per line) in:
# <project_root>/channels.txt
CHANNELS_TXT_PATH = PROJECT_ROOT / "channels.txt"


# -----------------------
# Logging
# -----------------------
def setup_logging() -> logging.Logger:
    """Sets up logging to file and console."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("telegram_scraper")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    if not logger.handlers:
        file_handler = logging.FileHandler(
            LOG_DIR / "scraper.log", encoding="utf-8")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)

    return logger


# -----------------------
# Utility helpers
# -----------------------
def safe_channel_name(ch: str) -> str:
    """Normalize channel identifier into a username-like slug."""
    ch = ch.strip()
    if not ch:
        return ch
    if ch.startswith("@"):
        ch = ch[1:]
    if ch.startswith("https://t.me/"):
        ch = ch.replace("https://t.me/", "")
    # Remove any trailing URL parameters/fragments
    ch = ch.split("?")[0].split("#")[0].strip("/")
    return ch


def utc_iso(dt: Optional[datetime]) -> Optional[str]:
    """Convert datetime to UTC ISO-8601 string."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def partition_date(dt: Optional[datetime]) -> str:
    """Partition key: YYYY-MM-DD (UTC)."""
    if dt is None:
        return "unknown-date"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dtu = dt.astimezone(timezone.utc)
    return dtu.strftime("%Y-%m-%d")


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def json_sanitize(obj: Any) -> Any:
    """
    Convert objects that are not JSON-serializable into safe representations.
    - datetime -> ISO string (UTC)
    - bytes -> base64 string wrapper
    - Path -> string
    - set/tuple -> list
    Recurses through dicts/lists.
    """
    if obj is None:
        return None

    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat()

    if isinstance(obj, (bytes, bytearray)):
        return {
            "__type__": "bytes_base64",
            "data": base64.b64encode(bytes(obj)).decode("ascii"),
        }

    if isinstance(obj, Path):
        return str(obj)

    if isinstance(obj, Mapping):
        return {str(k): json_sanitize(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [json_sanitize(v) for v in obj]

    if isinstance(obj, (str, int, float, bool)):
        return obj

    # Fallback: keep pipeline running, preserve info as string
    return str(obj)


def load_channels(logger: logging.Logger) -> List[str]:
    """
    Load channels from channels.txt if present, otherwise use DEFAULT_CHANNELS.
    Always includes DEFAULT_CHANNELS (deduped).
    """
    channels: List[str] = []
    channels.extend(DEFAULT_CHANNELS)

    if CHANNELS_TXT_PATH.exists():
        try:
            txt = CHANNELS_TXT_PATH.read_text(encoding="utf-8")
            for line in txt.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                channels.append(line)
            logger.info("Loaded additional channels from %s",
                        CHANNELS_TXT_PATH)
        except Exception as e:
            logger.exception(
                "Failed reading channels.txt (%s): %s", CHANNELS_TXT_PATH, e)

    # Normalize + dedupe, keep stable order
    seen = set()
    out: List[str] = []
    for ch in channels:
        name = safe_channel_name(ch)
        if not name:
            continue
        if name not in seen:
            out.append(name)
            seen.add(name)
    return out


# -----------------------
# State (incremental)
# -----------------------
def load_state() -> Dict[str, Any]:
    """
    State format:
    {
      "channels": {
        "lobelia4cosmetics": {"last_message_id": 12345, "updated_at": "..."},
        ...
      }
    }
    """
    if not STATE_PATH.exists():
        return {"channels": {}}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"channels": {}}


def save_state(state: Dict[str, Any]) -> None:
    ensure_parent_dir(STATE_PATH)
    # sanitize defensively in case any non-JSON sneaks into state
    STATE_PATH.write_text(
        json.dumps(json_sanitize(state), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def get_last_message_id(state: Dict[str, Any], channel: str) -> int:
    try:
        return int(state.get("channels", {}).get(channel, {}).get("last_message_id") or 0)
    except Exception:
        return 0


def set_last_message_id(state: Dict[str, Any], channel: str, last_id: int) -> None:
    state.setdefault("channels", {}).setdefault(channel, {})
    state["channels"][channel]["last_message_id"] = int(last_id)
    state["channels"][channel]["updated_at"] = datetime.now(
        timezone.utc).isoformat()


# -----------------------
# Extraction helpers
# -----------------------
def extract_media_info(msg: Message) -> Optional[Dict[str, Any]]:
    """Media summary required by Task 1 (kept small), separate from raw msg.to_dict()."""
    if not msg.media:
        return None

    info: Dict[str, Any] = {
        "has_media": True,
        "type": type(msg.media).__name__,
    }

    photo = getattr(msg, "photo", None)
    video = getattr(msg, "video", None)
    voice = getattr(msg, "voice", None)
    document = getattr(msg, "document", None)

    if photo:
        info["kind"] = "photo"
    elif video:
        info["kind"] = "video"
        info["duration"] = getattr(video, "duration", None)
        info["mime_type"] = getattr(video, "mime_type", None)
        info["size"] = getattr(video, "size", None)
    elif voice:
        info["kind"] = "voice"
        info["duration"] = getattr(voice, "duration", None)
        info["mime_type"] = getattr(voice, "mime_type", None)
        info["size"] = getattr(voice, "size", None)
    elif document:
        info["kind"] = "document"
        info["mime_type"] = getattr(document, "mime_type", None)
        info["size"] = getattr(document, "size", None)
    else:
        info["kind"] = "other"

    return info


def message_to_record(msg: Message, channel: str) -> Dict[str, Any]:
    """
    Record contains:
    - required extracted fields (easy access)
    - raw structure preserved via msg.to_dict(), sanitized for JSON storage
    """
    raw = json_sanitize(msg.to_dict())

    return {
        "channel": channel,

        # required fields
        "message_id": msg.id,
        "date": utc_iso(msg.date),
        "text": msg.message or "",
        "views": msg.views,
        "forwards": msg.forwards,
        "media": extract_media_info(msg),

        # raw API-ish structure (JSON-safe)
        "raw": raw,
    }


# -----------------------
# Data lake write (merge w/o duplicates)
# -----------------------
def load_existing_records(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # Defensive: ensure it's JSON-safe / consistent
        return json_sanitize(data)
    except Exception:
        # If a file is corrupted, don't destroy it; start a new list in memory
        return []


def merge_records_no_dupes(existing: List[Dict[str, Any]], new: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge by message_id; keep the newest version if duplicates exist.
    Output sorted ascending by message_id for consistency.
    """
    by_id: Dict[int, Dict[str, Any]] = {}

    def put(rec: Dict[str, Any]) -> None:
        mid = rec.get("message_id")
        if mid is None:
            return
        try:
            mid_int = int(mid)
        except Exception:
            return
        by_id[mid_int] = rec

    for r in existing:
        put(r)
    for r in new:
        put(r)

    merged = [by_id[k] for k in sorted(by_id.keys())]
    return merged


def write_json_array(path: Path, records: List[Dict[str, Any]]) -> None:
    ensure_parent_dir(path)
    # sanitize defensively (covers any remaining datetime/bytes)
    path.write_text(
        json.dumps(json_sanitize(records), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def day_channel_path(day: str, channel: str) -> Path:
    return DATA_LAKE_MESSAGES_DIR / day / f"{channel}.json"


# -----------------------
# Media download
# -----------------------
async def download_photo_if_present(
    client: TelegramClient,
    msg: Message,
    channel: str,
    logger: logging.Logger,
) -> Optional[Path]:
    """
    If message has a photo, download to:
      data/raw/images/{channel}/{message_id}.jpg

    Returns the saved path, or None if no photo / failed.
    """
    photo = getattr(msg, "photo", None)
    if not photo:
        return None

    channel_dir = DATA_LAKE_IMAGES_DIR / channel
    channel_dir.mkdir(parents=True, exist_ok=True)

    out_path = channel_dir / f"{msg.id}.jpg"
    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path

    try:
        downloaded = await client.download_media(photo, file=str(out_path))
        # download_media returns the path (string/bytes) or None
        if not downloaded:
            return None

        # If it returned bytes, persist them
        if isinstance(downloaded, (bytes, bytearray)):
            try:
                out_path.write_bytes(bytes(downloaded))
                return out_path
            except Exception as e:
                logger.exception(
                    "Failed writing downloaded bytes for @%s msg %s: %s",
                    channel, msg.id, e
                )
                return None

        downloaded_path = Path(str(downloaded))
        if downloaded_path != out_path and downloaded_path.exists():
            # rename/move to required name
            try:
                if out_path.exists():
                    out_path.unlink()
                downloaded_path.replace(out_path)
            except Exception:
                logger.warning(
                    "Downloaded photo for @%s msg %s but could not rename %s -> %s",
                    channel, msg.id, downloaded_path, out_path
                )
                return downloaded_path

        return out_path
    except Exception as e:
        logger.exception(
            "Failed downloading photo for @%s msg %s: %s", channel, msg.id, e)
        return None


# -----------------------
# Scrape channel (incremental)
# -----------------------
async def scrape_channel_incremental(
    client: TelegramClient,
    channel: str,
    state: Dict[str, Any],
    logger: logging.Logger,
) -> Tuple[int, int, int]:
    """
    Scrape only messages with id > last_message_id.
    For each message:
      - build record (with raw)
      - download photo if present
      - group by day partition
    Then merge into existing day files without duplicates.

    Returns: (messages_new, images_downloaded, last_message_id_new)
    """
    last_id = get_last_message_id(state, channel)
    logger.info("Channel @%s last_message_id=%s", channel, last_id)

    buffered: Dict[str, List[Dict[str, Any]]] = {}
    messages_new = 0
    images_downloaded = 0
    max_seen_id = last_id

    async for msg in client.iter_messages(channel, min_id=last_id):
        if msg is None:
            continue

        if msg.id and msg.id > max_seen_id:
            max_seen_id = msg.id

        rec = message_to_record(msg, channel)
        day = partition_date(msg.date)
        buffered.setdefault(day, []).append(rec)
        messages_new += 1

        saved = await download_photo_if_present(client, msg, channel, logger)
        if saved is not None:
            images_downloaded += 1

    partitions_written = 0
    for day, new_records in buffered.items():
        path = day_channel_path(day, channel)
        existing = load_existing_records(path)
        merged = merge_records_no_dupes(existing, new_records)
        write_json_array(path, merged)
        partitions_written += 1
        logger.info(
            "Wrote partition day=%s channel=@%s file=%s (existing=%d new=%d merged=%d)",
            day, channel, path, len(existing), len(new_records), len(merged)
        )

    if max_seen_id > last_id:
        set_last_message_id(state, channel, max_seen_id)

    logger.info(
        "Channel @%s done: new_messages=%d, images_downloaded=%d, partitions=%d, new_last_message_id=%d",
        channel, messages_new, images_downloaded, partitions_written, get_last_message_id(
            state, channel)
    )

    return messages_new, images_downloaded, get_last_message_id(state, channel)


# -----------------------
# Main
# -----------------------
async def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")

    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_scrapper")

    if not api_id or not api_hash:
        raise RuntimeError(
            "Missing TELEGRAM_API_ID / TELEGRAM_API_HASH in .env")

    logger = setup_logging()
    logger.info("Starting Telegram scraping pipeline (Task 1 compliant)")

    DATA_LAKE_MESSAGES_DIR.mkdir(parents=True, exist_ok=True)
    DATA_LAKE_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

    channels = load_channels(logger)
    logger.info("Total channels to scrape: %d", len(channels))

    state = load_state()

    async with TelegramClient(str(PROJECT_ROOT / session_name), int(api_id), api_hash) as client:
        for ch in channels:
            channel = safe_channel_name(ch)
            if not channel:
                continue

            logger.info("Scraping channel: @%s", channel)

            try:
                await scrape_channel_incremental(client, channel, state, logger)
                save_state(state)  # persist after each channel
            except FloodWaitError as e:
                logger.warning(
                    "FloodWait while scraping @%s; sleeping %s seconds then continuing",
                    channel, e.seconds
                )
                await asyncio.sleep(e.seconds)
                save_state(state)
                continue
            except (
                ChannelInvalidError,
                ChannelPrivateError,
                UsernameInvalidError,
                UsernameNotOccupiedError,
            ) as e:
                logger.error("Channel access error for @%s: %s", channel, e)
                save_state(state)
            except (RPCError, OSError, ConnectionError, ValueError) as e:
                logger.exception("Failed scraping @%s: %s", channel, e)
                save_state(state)

    logger.info("Done. State saved at %s", STATE_PATH)


if __name__ == "__main__":
    asyncio.run(main())
