import os
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")  # secrets file

api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")
session_name = os.getenv("TELEGRAM_SESSION_NAME", "telegram_scraper")

client = TelegramClient(str(PROJECT_ROOT / session_name), api_id, api_hash)

async def main():
    await client.start()   # <-- this triggers the login prompt if no session exists
    me = await client.get_me()
    print("Logged in as:", me.username or me.first_name, me.id)

with client:
    client.loop.run_until_complete(main())