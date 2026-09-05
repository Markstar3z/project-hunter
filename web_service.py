"""
Project Hunter - Koyeb Web Service Wrapper

This file keeps the existing project_hunter.py code untouched and runs:
1. A small aiohttp web server for Koyeb.
2. The existing Telethon bot client.
3. The existing Telegram user client.
4. The existing user-client keepalive task.

The main Project Hunter logic remains inside project_hunter.py.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from contextlib import suppress

from aiohttp import web

import project_hunter as hunter


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

LOGGER = logging.getLogger("project-hunter-web")


# =========================================================
# WEB CONFIGURATION
# =========================================================

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8000"))


# =========================================================
# HEALTH ENDPOINTS
# =========================================================

async def root_handler(request: web.Request) -> web.Response:
    """
    Basic root endpoint.

    Useful for checking whether the Koyeb service is currently awake.
    """

    bot_connected = hunter.bot_client.is_connected()
    user_connected = hunter.user_client.is_connected()

    return web.json_response(
        {
            "service": "Project Hunter",
            "status": "online",
            "bot_client": (
                "connected"
                if bot_connected
                else "disconnected"
            ),
            "user_client": (
                "connected"
                if user_connected
                else "disconnected"
            ),
        }
    )


async def health_handler(request: web.Request) -> web.Response:
    """
    Health endpoint for Koyeb.
    """

    return web.json_response(
        {
            "status": "ok",
            "service": "project-hunter",
        }
    )


async def status_handler(request: web.Request) -> web.Response:
    """
    More detailed Project Hunter status endpoint.
    """

    result = {
        "service": "Project Hunter",
        "bot_connected": hunter.bot_client.is_connected(),
        "user_connected": hunter.user_client.is_connected(),
        "active_jobs": len(hunter.active_jobs),
    }

    return web.json_response(result)


# =========================================================
# APPLICATION
# =========================================================

def create_web_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/", root_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/status", status_handler)

    return app


async def start_web_server() -> web.AppRunner:
    """
    Start the HTTP server required by Koyeb Web Services.
    """

    app = create_web_app()

    runner = web.AppRunner(app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        host=HOST,
        port=PORT,
    )

    await site.start()

    LOGGER.info(
        "Project Hunter web server listening on %s:%s",
        HOST,
        PORT,
    )

    return runner


# =========================================================
# TELEGRAM STARTUP
# =========================================================

async def start_project_hunter() -> None:
    """
    Start Project Hunter's existing Telethon clients.
    """

    LOGGER.info(
        "Starting Project Hunter Telegram clients..."
    )

    if not hunter.user_client.is_connected():
        await hunter.user_client.start()

    if not hunter.bot_client.is_connected():
        await hunter.bot_client.start(
            bot_token=hunter.BOT_TOKEN
        )

    bot = await hunter.bot_client.get_me()

    LOGGER.info(
        "Project Hunter connected as @%s",
        bot.username,
    )

    LOGGER.info(
        "Discovery sources: "
        "coingecko=%s "
        "mobula=%s "
        "birdeye=%s "
        "dex=true",
        bool(hunter.COINGECKO_API_KEY),
        bool(hunter.MOBULA_API_KEY),
        bool(hunter.BIRDEYE_API_KEY),
    )


# =========================================================
# SHUTDOWN
# =========================================================

async def disconnect_telegram() -> None:
    """
    Gracefully close Telegram connections.
    """

    if hunter.bot_client.is_connected():
        LOGGER.info(
            "Disconnecting Project Hunter bot client..."
        )

        await hunter.bot_client.disconnect()

    if hunter.user_client.is_connected():
        LOGGER.info(
            "Disconnecting Project Hunter user client..."
        )

        await hunter.user_client.disconnect()


# =========================================================
# MAIN SERVICE
# =========================================================

async def main() -> None:
    web_runner = None
    keepalive_task = None

    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        shutdown_event.set()

    for sig in (
        signal.SIGTERM,
        signal.SIGINT,
    ):
        with suppress(
            NotImplementedError,
            RuntimeError,
        ):
            loop.add_signal_handler(
                sig,
                request_shutdown,
            )

    try:
        # -------------------------------------------------
        # Start HTTP server first.
        #
        # This is important because Koyeb expects the
        # application to bind to its PORT.
        # -------------------------------------------------

        web_runner = await start_web_server()

        # -------------------------------------------------
        # Start existing Project Hunter clients.
        # -------------------------------------------------

        await start_project_hunter()

        # -------------------------------------------------
        # Start the existing Telethon user-client keepalive.
        # -------------------------------------------------

        keepalive_task = asyncio.create_task(
            hunter.user_client_keepalive(),
            name="project-hunter-user-keepalive",
        )

        LOGGER.info(
            "Project Hunter is ready."
        )

        # -------------------------------------------------
        # Wait for either:
        #
        # 1. Koyeb/system shutdown
        # 2. Telegram bot disconnection
        # -------------------------------------------------

        telegram_disconnected = asyncio.create_task(
            hunter.bot_client.disconnected,
            name="telegram-disconnected",
        )

        system_shutdown = asyncio.create_task(
            shutdown_event.wait(),
            name="system-shutdown",
        )

        done, pending = await asyncio.wait(
            {
                telegram_disconnected,
                system_shutdown,
            },
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        await asyncio.gather(
            *pending,
            return_exceptions=True,
        )

        for task in done:
            if task.cancelled():
                continue

            try:
                task.result()
            except Exception:
                LOGGER.exception(
                    "Project Hunter service task failed."
                )

    except asyncio.CancelledError:
        LOGGER.info(
            "Project Hunter service cancelled."
        )

        raise

    except Exception:
        LOGGER.exception(
            "Project Hunter failed to start or crashed."
        )

        raise

    finally:
        LOGGER.info(
            "Shutting down Project Hunter..."
        )

        if keepalive_task is not None:
            keepalive_task.cancel()

            await asyncio.gather(
                keepalive_task,
                return_exceptions=True,
            )

        await disconnect_telegram()

        if web_runner is not None:
            await web_runner.cleanup()

        LOGGER.info(
            "Project Hunter shutdown complete."
        )


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        LOGGER.info(
            "Project Hunter stopped manually."
        )
