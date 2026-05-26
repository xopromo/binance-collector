import asyncio

from telethon import TelegramClient, events

from .config import config
from .logger import get_logger
from .processor import process_message

logger = get_logger(__name__)


async def run() -> None:
    """Connect to Telegram and listen for new messages in the target channel."""
    client = TelegramClient(
        "tg_signal_session",
        config.tg_api_id,
        config.tg_api_hash,
    )

    await client.start()
    me = await client.get_me()
    logger.info(f"Logged in as: {me.first_name} (@{me.username})")

    channel = await client.get_entity(config.tg_channel)
    channel_title = getattr(channel, "title", config.tg_channel)
    logger.info(f"Watching channel: {channel_title}")

    @client.on(events.NewMessage(chats=channel))
    async def on_new_message(event: events.NewMessage.Event) -> None:
        msg = event.message
        logger.info(f"New message id={msg.id} | text_len={len(msg.text or '')}")
        try:
            await process_message(msg)
        except Exception as exc:
            logger.error(f"Unhandled error processing msg {msg.id}: {exc}", exc_info=True)

    logger.info("Listening for signals... (Ctrl+C to stop)")
    await client.run_until_disconnected()
