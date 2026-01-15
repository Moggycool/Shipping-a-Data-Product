from __future__ import annotations

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
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
STATE_PATH = PROJECT_ROOT / "data" / "raw" / "scrape_state.json"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_PATH = LOG_DIR / "scraper.log"
CHANNELS_TXT = PROJECT_ROOT / "channels.txt"


# ============================================================
# Performance controls (override via environment variables)
# ============================================================
MAX_MESSAGES_PER_CHANNEL = int(os.getenv("MAX_MESSAGES_PER_CHANNEL", "300"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))

DOWNLOAD_MEDIA = os.getenv("DOWNLOAD_MEDIA", "1").strip(
).lower() not in ("0", "false", "no")
MAX_MEDIA_PER_CHANNEL = int(os.getenv("MAX_MEDIA_PER_CHANNEL", "50"))

PER_CHANNEL_TIMEOUT_SEC = int(os.getenv("PER_CHANNEL_TIMEOUT_SEC", "180"))
FLOODWAIT_MAX_SLEEP_SEC = int(os.getenv("FLOODWAIT_MAX_SLEEP_SEC", "60"))


# ============================================================
# Logging (robust: always prints to terminal + writes file)
# ============================================================
def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    has_stream = any(isinstance(h, logging.StreamHandler)
                     for h in root.handlers)
    has_file = any(
        isinstance(h, logging.FileHandler) and getattr(
            h, "baseFilename", "") == str(LOG_PATH)
        for h in root.handlers
    )

    if not has_file:
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        root.addHandler(fh)

    if not has_stream:
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(fmt)
        root.addHandler(sh)
    else:
        for h in root.handlers:
            if isinstance(h, logging.StreamHandler):
                h.setLevel(logging.INFO)
                if h.formatter is None:
                    h.setFormatter(fmt)

    return logging.getLogger("telegram_scraper")


logger = setup_logging()


# ============================================================
# JSON serialization helpers
# ============================================================
def json_default(o: Any) -> Any:
    """Make Telegram/Telethon objects JSON-safe (notably datetime in msg.to_dict())."""
    if isinstance(o, datetime):
        # preserve timezone if present
        if o.tzinfo is None:
            o = o.replace(tzinfo=timezone.utc)
        return o.astimezone(timezone.utc).isoformat()
    if isinstance(o, Path):
        return str(o)
    if isinstance(o, bytes):
        # bytes occasionally appear in Telethon dicts; base64 keeps it lossless
        return {"_type": "bytes", "base64": base64.b64encode(o).decode("ascii")}
    return str(o)


# ============================================================
# Helpers
# ============================================================
def ensure_dirs() -> None:
    DATA_LAKE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


def normalize_channel(s: str) -> str:
    s = s.strip()
    s = s.replace("https://t.me/", "").replace("http://t.me/", "")
    s = s.replace("@", "")
    s = s.split("?")[0].strip()
    return s


def load_channels() -> List[str]:
    required = ["CheMed123", "lobelia4cosmetics",
                "tikvahpharma", "rayapharmaceuticals"]
    extra: List[str] = []

    if CHANNELS_TXT.exists():
        for line in CHANNELS_TXT.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            extra.append(line)
        logger.info(f"Loaded additional channels from {CHANNELS_TXT}")

    out: List[str] = []
    seen = set()
    for ch in required + extra:
        chn = normalize_channel(ch)
        if chn and chn not in seen:
            out.append(chn)
            seen.add(chn)
    return out


def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        try:
            st = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(st, dict):
                st.setdefault("channels", {})
                return st
        except Exception as e:
            logger.warning(f"Failed to parse state file {STATE_PATH}: {e}")
    return {"channels": {}}


def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(
        state, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")


def get_last_message_id(state: Dict[str, Any], channel: str) -> int:
    channels = state.get("channels", {})
    v = channels.get(channel, 0)

    if v is None:
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        try:
            return int(v)
        except Exception:
            return 0
    if isinstance(v, dict):
        for key in ("last_message_id", "last_id", "lastMessageId", "lastMessageID"):
            if key in v:
                try:
                    return int(v.get(key) or 0)
                except Exception:
                    return 0
        return 0
    return 0


def set_last_message_id(state: Dict[str, Any], channel: str, last_message_id: int) -> None:
    state.setdefault("channels", {})
    existing = state["channels"].get(channel)

    if isinstance(existing, dict):
        existing["last_message_id"] = int(last_message_id)
        state["channels"][channel] = existing
    else:
        state["channels"][channel] = int(last_message_id)


def safe_dt_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def msg_day(dt: Optional[datetime]) -> str:
    if not dt:
        return datetime.now(timezone.utc).date().isoformat()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date().isoformat()


def upsert_partition(day: str, channel: str, new_records: List[Dict[str, Any]]) -> Tuple[Path, int, int, int]:
    day_dir = DATA_LAKE_DIR / day
    day_dir.mkdir(parents=True, exist_ok=True)
    fp = day_dir / f"{channel}.json"

    existing: List[Dict[str, Any]] = []
    if fp.exists():
        try:
            existing = json.loads(fp.read_text(encoding="utf-8"))
            if not isinstance(existing, list):
                existing = []
        except Exception:
            existing = []

    by_id: Dict[str, Dict[str, Any]] = {}
    for r in existing:
        mid = r.get("message_id")
        if mid is not None:
            by_id[str(mid)] = r
    for r in new_records:
        mid = r.get("message_id")
        if mid is not None:
            by_id[str(mid)] = r

    merged = list(by_id.values())
    merged.sort(key=lambda x: x.get("message_id", 0))

    fp.write_text(
        json.dumps(merged, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )
    return fp, len(existing), len(new_records), len(merged)


async def download_photo(client: TelegramClient, msg: Message, channel: str) -> Optional[str]:
    if not DOWNLOAD_MEDIA:
        return None
    if not getattr(msg, "photo", None):
        return None

    out_dir = IMAGES_DIR / channel
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{msg.id}.jpg"

    if out_path.exists():
        return str(out_path)

    try:
        await client.download_media(msg, file=str(out_path))
        return str(out_path)
    except FloodWaitError as e:
        sleep_s = min(int(e.seconds), FLOODWAIT_MAX_SLEEP_SEC)
        logger.warning(
            f"FloodWait while downloading media: sleeping {sleep_s}s (reported {e.seconds}s)")
        await asyncio.sleep(sleep_s)
        return None
    except RPCError as e:
        logger.warning(
            f"RPCError downloading media for @{channel} msg={msg.id}: {e}")
        return None
    except Exception as e:
        logger.warning(
            f"Unexpected media download error for @{channel} msg={msg.id}: {e}")
        return None


def msg_to_record(msg: Message, channel: str, media_path: Optional[str]) -> Dict[str, Any]:
    raw: Dict[str, Any] = {}
    try:
        raw = msg.to_dict()
    except Exception:
        raw = {"_note": "msg.to_dict() failed"}

    return {
        "channel": f"@{channel}",
        "message_id": msg.id,
        "date": safe_dt_iso(getattr(msg, "date", None)),
        "text": getattr(msg, "message", None),
        "has_media": bool(getattr(msg, "media", None)),
        "media_path": media_path,
        "raw": raw,  # may contain datetimes; json_default handles them when dumping
    }


# ============================================================
# Scraping
# ============================================================
@dataclass
class ChannelResult:
    channel: str
    new_messages: int
    images_downloaded: int
    partitions: int
    new_last_message_id: int


async def scrape_channel(client: TelegramClient, channel: str, last_message_id: int) -> ChannelResult:
    logger.info(f"Scraping channel: @{channel}")
    logger.info(f"Channel @{channel} last_message_id={last_message_id}")

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    new_last_id = last_message_id
    images_downloaded = 0
    media_attempts = 0
    by_day: Dict[str, List[Dict[str, Any]]] = {}

    async for msg in client.iter_messages(
        entity=channel,
        min_id=last_message_id if last_message_id > 0 else 0,
        limit=MAX_MESSAGES_PER_CHANNEL,
    ):
        if not isinstance(msg, Message):
            continue

        # First run: enforce lookback cutoff
        if last_message_id == 0:
            dt = getattr(msg, "date", None)
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < cutoff:
                    break

        if msg.id and msg.id > new_last_id:
            new_last_id = msg.id

        media_path = None
        if DOWNLOAD_MEDIA and getattr(msg, "photo", None) and media_attempts < MAX_MEDIA_PER_CHANNEL:
            media_path = await download_photo(client, msg, channel)
            if media_path:
                images_downloaded += 1
            media_attempts += 1

        rec = msg_to_record(msg, channel, media_path)
        day = msg_day(getattr(msg, "date", None))
        by_day.setdefault(day, []).append(rec)

    partitions = 0
    new_messages = 0
    for day, recs in sorted(by_day.items(), key=lambda kv: kv[0]):
        if not recs:
            continue
        fp, existing, new, merged = upsert_partition(day, channel, recs)
        partitions += 1
        new_messages += new
        logger.info(
            f"Wrote partition day={day} channel=@{channel} file={fp} "
            f"(existing={existing} new={new} merged={merged})"
        )

    logger.info(
        f"Channel @{channel} done: new_messages={new_messages}, images_downloaded={images_downloaded}, "
        f"partitions={partitions}, new_last_message_id={new_last_id}"
    )

    return ChannelResult(channel, new_messages, images_downloaded, partitions, new_last_id)


async def scrape_channel_with_guards(client: TelegramClient, channel: str, last_message_id: int) -> ChannelResult:
    try:
        return await asyncio.wait_for(
            scrape_channel(client, channel, last_message_id),
            timeout=PER_CHANNEL_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        logger.warning(
            f"Timeout scraping @{channel} after {PER_CHANNEL_TIMEOUT_SEC}s; moving on.")
        return ChannelResult(channel, 0, 0, 0, last_message_id)
    except FloodWaitError as e:
        sleep_s = min(int(e.seconds), FLOODWAIT_MAX_SLEEP_SEC)
        logger.warning(
            f"FloodWait scraping @{channel}: sleeping {sleep_s}s (reported {e.seconds}s)")
        await asyncio.sleep(sleep_s)
        return ChannelResult(channel, 0, 0, 0, last_message_id)
    except RPCError as e:
        logger.warning(f"RPCError scraping @{channel}: {e}")
        return ChannelResult(channel, 0, 0, 0, last_message_id)
    except Exception as e:
        logger.exception(f"Unexpected error scraping @{channel}: {e}")
        return ChannelResult(channel, 0, 0, 0, last_message_id)


# ============================================================
# Main
# ============================================================
async def main() -> None:
    ensure_dirs()
    load_dotenv(PROJECT_ROOT / ".env")

    logger.info("=== Telegram scraper starting ===")
    logger.info(f"PROJECT_ROOT={PROJECT_ROOT}")
    logger.info(f"STATE_PATH={STATE_PATH}")
    logger.info(f"DATA_LAKE_DIR={DATA_LAKE_DIR}")
    logger.info(f"IMAGES_DIR={IMAGES_DIR}")
    logger.info(
        f"CONFIG: MAX_MESSAGES_PER_CHANNEL={MAX_MESSAGES_PER_CHANNEL}, LOOKBACK_DAYS={LOOKBACK_DAYS}, "
        f"DOWNLOAD_MEDIA={DOWNLOAD_MEDIA}, MAX_MEDIA_PER_CHANNEL={MAX_MEDIA_PER_CHANNEL}, "
        f"PER_CHANNEL_TIMEOUT_SEC={PER_CHANNEL_TIMEOUT_SEC}"
    )

    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_scrapper")

    if not api_id or not api_hash:
        raise ValueError("Missing TELEGRAM_API_ID / TELEGRAM_API_HASH in .env")

    api_id_int = int(api_id)

    channels = load_channels()
    logger.info(
        f"Total channels to scrape: {len(channels)} -> {', '.join('@' + c for c in channels)}")

    state = load_state()
    state.setdefault("channels", {})

    async with TelegramClient(session_name, api_id_int, api_hash) as client:
        logger.info("Telegram client connected.")
        for channel in channels:
            last_id = get_last_message_id(state, channel)
            result = await scrape_channel_with_guards(client, channel, last_id)

            if result.new_last_message_id > last_id:
                set_last_message_id(state, channel, result.new_last_message_id)
                save_state(state)

    logger.info("=== Telegram scraper finished ===")


if __name__ == "__main__":
    asyncio.run(main())
