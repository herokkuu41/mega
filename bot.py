import os
import asyncio
import logging
import time
from aiohttp import web
from pyrogram import Client, filters
from smart_mega_leech import SmartMegaLeecher
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PORT = int(os.environ.get("PORT", 8080))
DOWNLOAD_DIR = "downloads"

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

app = Client("mega_leech_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

async def web_server():
    async def handle(request):
        return web.Response(text="Bot is Running")

    webapp = web.Application()
    webapp.router.add_get("/", handle)
    runner = web.AppRunner(webapp)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    LOGGER.info(f"Web Server running on port {PORT}")

@app.on_message(filters.command("leech") & filters.private)
async def leech_handler(client, message):
    if len(message.command) < 2:
        await message.reply("Usage: /leech [Mega Link]")
        return

    link = message.command[1]
    msg = await message.reply("Initializing Smart Leech...")
    
    leecher = SmartMegaLeecher(DOWNLOAD_DIR, "proxies.txt")

    last_update = 0
    async def update_status(text):
        nonlocal last_update
        if time.time() - last_update > 5:
            try:
                await msg.edit(text)
                last_update = time.time()
            except:
                pass

    success, result = await leecher.download(link, update_status)

    if success:
        await msg.edit("Download Complete! Uploading...")
        try:
            await app.send_document(
                chat_id=message.chat.id,
                document=result,
                caption="Here is your file."
            )
            os.remove(result)
            await msg.delete()
        except Exception as e:
            await msg.edit(f"Upload failed: {e}")
    else:
        await msg.edit(f"Download Failed: {result}")

async def main():
    await web_server()
    await app.start()
    LOGGER.info("Bot Started!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
