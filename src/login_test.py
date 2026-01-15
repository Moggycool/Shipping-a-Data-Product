"""A simple script to test Telegram login using Telethon."""
import os
import inspect
from pathlib import Path
from typing import Awaitable, cast

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import User

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")  # secrets file


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


api_id = int(_require_env("TELEGRAM_API_ID"))
api_hash = _require_env("TELEGRAM_API_HASH")
session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_scraper")

client = TelegramClient(str(PROJECT_ROOT / session_name), api_id, api_hash)


async def main():
    """Main function to handle login and print user info."""
    started = client.start()   # <-- this triggers the login prompt if no session exists
    if inspect.isawaitable(started):
        await cast(Awaitable[TelegramClient], started)

    me = cast(User, await client.get_me())
    print("Logged in as:", me.username or me.first_name, me.id)

with client:
    client.loop.run_until_complete(main())
