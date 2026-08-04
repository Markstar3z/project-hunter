"""Project Hunter v2: two-stage CoinGecko + X + Telegram research bot."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from telethon import TelegramClient, events
from telethon.errors import (
    ChannelPrivateError,
    ChatAdminRequiredError,
    FloodWaitError,
    RPCError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.sessions import MemorySession, StringSession
from telethon.tl.types import (
    ChannelParticipantAdmin,
    ChannelParticipantCreator,
    ChannelParticipantsAdmins,
    UserStatusEmpty,
    UserStatusLastMonth,
    UserStatusLastWeek,
    UserStatusOffline,
    UserStatusOnline,
    UserStatusRecently,
)
from urllib3.util.retry import Retry


# =========================================================
# CONFIGURATION
# =========================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
STRING_SESSION = os.environ["STRING_SESSION"]

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "").strip()

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/project_hunter.db")
ALLOWED_CHAT_ID_RAW = os.getenv("ALLOWED_CHAT_ID", "").strip()
ALLOWED_CHAT_ID = int(ALLOWED_CHAT_ID_RAW) if ALLOWED_CHAT_ID_RAW else None

MIN_MARKET_CAP = int(os.getenv("MIN_MARKET_CAP", "10000"))
MAX_MARKET_CAP = int(os.getenv("MAX_MARKET_CAP", "1000000000"))

FAST_SCAN_MAX_INSPECTED = int(os.getenv("FAST_SCAN_MAX_INSPECTED", "500"))
MAX_PAGES_PER_FAST_SCAN = int(os.getenv("MAX_PAGES_PER_FAST_SCAN", "5"))
PAGE_SIZE = min(int(os.getenv("PAGE_SIZE", "250")), 250)

MAX_X_INACTIVE_DAYS = int(os.getenv("MAX_X_INACTIVE_DAYS", "30"))
MAX_TG_INACTIVE_DAYS = int(os.getenv("MAX_TG_INACTIVE_DAYS", "30"))
TG_LOOKBACK_DAYS = int(os.getenv("TG_LOOKBACK_DAYS", "7"))
TG_MIN_MESSAGES_7D = int(os.getenv("TG_MIN_MESSAGES_7D", "5"))
TG_MIN_HUMAN_SENDERS_7D = int(os.getenv("TG_MIN_HUMAN_SENDERS_7D", "2"))

MAX_ACTIVE_ADMINS = 3
MESSAGE_LIMIT = 3800

COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"
X_API_BASE = "https://api.x.com/2"


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)


# =========================================================
# CLIENTS
# =========================================================

user_client = TelegramClient(
    StringSession(STRING_SESSION),
    API_ID,
    API_HASH,
    auto_reconnect=True,
    connection_retries=10,
    retry_delay=3,
    request_retries=5,
)

bot_client = TelegramClient(
    MemorySession(),
    API_ID,
    API_HASH,
    auto_reconnect=True,
    connection_retries=10,
    retry_delay=3,
    request_retries=5,
)

active_jobs: set[int] = set()
user_connection_lock = asyncio.Lock()


# =========================================================
# HELPERS
# =========================================================

@dataclass
class FastScanParams:
    target_count: int = 50
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    sort_mode: str = "market_cap_desc"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def display_datetime(value: Optional[datetime]) -> str:
    if value is None:
        return "Unavailable"
    return value.strftime("%d %b %Y, %H:%M UTC")


def split_text(text: str, limit: int = MESSAGE_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""

    for block in text.split("\n\n"):
        candidate = block if not current else f"{current}\n\n{block}"

        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)

        if len(block) <= limit:
            current = block
        else:
            for start in range(0, len(block), limit):
                chunks.append(block[start:start + limit])
            current = ""

    if current:
        chunks.append(current)

    return chunks


async def send_long(event: events.NewMessage.Event, text: str) -> None:
    for chunk in split_text(text):
        await event.reply(chunk, link_preview=False)
        await asyncio.sleep(0.4)


def authorized(event: events.NewMessage.Event) -> bool:
    return ALLOWED_CHAT_ID is None or event.chat_id == ALLOWED_CHAT_ID


def build_http_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=20,
        pool_maxsize=20,
    )

    session = requests.Session()
    session.mount("https://", adapter)
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "ProjectHunterV2/1.0",
        }
    )
    return session


HTTP = build_http_session()


# =========================================================
# DATABASE
# =========================================================

class Storage:
    def __init__(self, path: str) -> None:
        self.path = path

        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        self.initialize()
        self.migrate_database()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    symbol TEXT,
                    market_cap INTEGER,
                    category TEXT,
                    website TEXT,
                    x_username TEXT,
                    x_url TEXT,
                    telegram_url TEXT,
                    stage TEXT NOT NULL DEFAULT 'pending',
                    score INTEGER DEFAULT 0,
                    classification TEXT,
                    rejection_reason TEXT,
                    x_last_post_at TEXT,
                    x_status TEXT,
                    telegram_last_message_at TEXT,
                    telegram_messages_7d INTEGER DEFAULT 0,
                    telegram_unique_humans_7d INTEGER DEFAULT 0,
                    telegram_status TEXT,
                    owner_username TEXT,
                    owner_activity TEXT,
                    admin_1 TEXT,
                    admin_2 TEXT,
                    admin_3 TEXT,
                    discovered_at TEXT NOT NULL,
                    analyzed_at TEXT
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_type TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    requested_count INTEGER NOT NULL,
                    inspected_count INTEGER DEFAULT 0,
                    found_count INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    error_message TEXT
                )
                """
            )

            connection.commit()

    def migrate_database(self) -> None:
        """Upgrade older Railway SQLite databases without deleting data."""

        required_columns = {
            "category": "TEXT",
            "website": "TEXT",
            "x_username": "TEXT",
            "x_url": "TEXT",
            "telegram_url": "TEXT",
            "stage": "TEXT NOT NULL DEFAULT 'pending'",
            "score": "INTEGER DEFAULT 0",
            "classification": "TEXT",
            "rejection_reason": "TEXT",
            "x_last_post_at": "TEXT",
            "x_status": "TEXT",
            "telegram_last_message_at": "TEXT",
            "telegram_messages_7d": "INTEGER DEFAULT 0",
            "telegram_unique_humans_7d": "INTEGER DEFAULT 0",
            "telegram_status": "TEXT",
            "owner_username": "TEXT",
            "owner_activity": "TEXT",
            "admin_1": "TEXT",
            "admin_2": "TEXT",
            "admin_3": "TEXT",
            "discovered_at": "TEXT",
            "analyzed_at": "TEXT",
        }

        with self.connect() as connection:
            existing_columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(projects)"
                ).fetchall()
            }

            for column_name, column_definition in required_columns.items():
                if column_name in existing_columns:
                    continue

                connection.execute(
                    f"ALTER TABLE projects "
                    f"ADD COLUMN {column_name} "
                    f"{column_definition}"
                )

            connection.execute(
                """
                UPDATE projects
                SET stage = COALESCE(NULLIF(stage, ''), 'pending')
                """
            )

            connection.execute(
                """
                UPDATE projects
                SET score = COALESCE(score, 0)
                """
            )

            connection.execute(
                """
                UPDATE projects
                SET discovered_at = COALESCE(
                    discovered_at,
                    ?
                )
                """,
                (utc_now().isoformat(),),
            )

            connection.commit()

    def exists(self, coin_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM projects WHERE coin_id = ? LIMIT 1",
                (coin_id,),
            ).fetchone()
        return row is not None

    def save_pending(self, project: dict[str, Any]) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO projects (
                    coin_id,
                    name,
                    symbol,
                    market_cap,
                    category,
                    website,
                    x_username,
                    x_url,
                    telegram_url,
                    stage,
                    discovered_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                ON CONFLICT(coin_id) DO NOTHING
                """,
                (
                    project["coin_id"],
                    project["name"],
                    project["symbol"],
                    project["market_cap"],
                    project["category"],
                    project.get("website"),
                    project["x_username"],
                    project["x_url"],
                    project["telegram_url"],
                    utc_now().isoformat(),
                ),
            )
            connection.commit()

    def pending_projects(self, limit: int) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM projects
                WHERE stage = 'pending'
                ORDER BY id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [dict(row) for row in rows]

    def save_analysis(self, result: dict[str, Any]) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE projects
                SET stage = ?,
                    score = ?,
                    classification = ?,
                    rejection_reason = ?,
                    x_last_post_at = ?,
                    x_status = ?,
                    telegram_last_message_at = ?,
                    telegram_messages_7d = ?,
                    telegram_unique_humans_7d = ?,
                    telegram_status = ?,
                    owner_username = ?,
                    owner_activity = ?,
                    admin_1 = ?,
                    admin_2 = ?,
                    admin_3 = ?,
                    analyzed_at = ?
                WHERE coin_id = ?
                """,
                (
                    result["stage"],
                    result["score"],
                    result["classification"],
                    result.get("rejection_reason"),
                    result.get("x_last_post_at"),
                    result.get("x_status"),
                    result.get("telegram_last_message_at"),
                    result.get("telegram_messages_7d", 0),
                    result.get("telegram_unique_humans_7d", 0),
                    result.get("telegram_status"),
                    result.get("owner_username"),
                    result.get("owner_activity"),
                    result.get("admin_1"),
                    result.get("admin_2"),
                    result.get("admin_3"),
                    utc_now().isoformat(),
                    result["coin_id"],
                ),
            )
            connection.commit()

    def list_projects(
        self,
        stage: Optional[str],
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        with self.connect() as connection:
            if stage:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM projects
                    WHERE stage = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (stage, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM projects
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()

        return [dict(row) for row in rows]

    def counts(self) -> dict[str, int]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT stage, COUNT(*) AS total
                FROM projects
                GROUP BY stage
                """
            ).fetchall()

        result = {
            "pending": 0,
            "priority": 0,
            "qualified": 0,
            "watchlist": 0,
            "rejected": 0,
        }

        for row in rows:
            result[row["stage"]] = int(row["total"])

        result["total"] = sum(result.values())
        return result

    def create_history(
        self,
        scan_type: str,
        requested_count: int,
    ) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO scan_history (
                    scan_type,
                    started_at,
                    requested_count,
                    status
                )
                VALUES (?, ?, ?, 'running')
                """,
                (
                    scan_type,
                    utc_now().isoformat(),
                    requested_count,
                ),
            )
            connection.commit()
            return int(cursor.lastrowid)

    def finish_history(
        self,
        history_id: int,
        *,
        inspected: int,
        found: int,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scan_history
                SET completed_at = ?,
                    inspected_count = ?,
                    found_count = ?,
                    status = ?,
                    error_message = ?
                WHERE id = ?
                """,
                (
                    utc_now().isoformat(),
                    inspected,
                    found,
                    status,
                    error_message,
                    history_id,
                ),
            )
            connection.commit()


STORAGE = Storage(DATABASE_PATH)


# =========================================================
# COINGECKO FAST DISCOVERY
# =========================================================

class CoinGeckoClient:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

        if COINGECKO_API_KEY:
            self.headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    def market_page(
        self,
        page: int,
        category_id: Optional[str],
        sort_mode: str,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "vs_currency": "usd",
            "order": sort_mode,
            "per_page": PAGE_SIZE,
            "page": page,
        }

        if category_id:
            params["category"] = category_id

        response = HTTP.get(
            f"{COINGECKO_API_BASE}/coins/markets",
            params=params,
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        return data if isinstance(data, list) else []

    def details(self, coin_id: str) -> dict[str, Any]:
        response = HTTP.get(
            f"{COINGECKO_API_BASE}/coins/{coin_id}",
            params={
                "localization": "false",
                "tickers": "false",
                "market_data": "false",
                "community_data": "false",
                "developer_data": "false",
                "sparkline": "false",
            },
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        return data if isinstance(data, dict) else {}

    @staticmethod
    def telegram_url(value: Any) -> Optional[str]:
        if not value:
            return None

        if isinstance(value, list):
            for item in value:
                result = CoinGeckoClient.telegram_url(item)
                if result:
                    return result
            return None

        text = str(value).strip()

        if not text:
            return None

        if text.startswith(("http://", "https://")):
            return text

        return f"https://t.me/{text.lstrip('@')}"

    @staticmethod
    def website_url(value: Any) -> Optional[str]:
        if not value:
            return None

        if isinstance(value, list):
            for item in value:
                if item:
                    return str(item).strip()
            return None

        return str(value).strip() or None


COINGECKO = CoinGeckoClient()


# =========================================================
# X ACTIVITY
# =========================================================

class XClient:
    def latest_original_post(
        self,
        username: str,
    ) -> dict[str, Any]:
        if not X_BEARER_TOKEN:
            return {
                "available": False,
                "reason": "X bearer token is missing",
            }

        headers = {
            "Authorization": f"Bearer {X_BEARER_TOKEN}"
        }

        user_response = HTTP.get(
            f"{X_API_BASE}/users/by/username/{username}",
            headers=headers,
            timeout=30,
        )
        user_response.raise_for_status()

        user_data = user_response.json().get("data")

        if not user_data:
            return {
                "available": False,
                "reason": "X account not found",
            }

        timeline_response = HTTP.get(
            f"{X_API_BASE}/users/{user_data['id']}/tweets",
            params={
                "max_results": 5,
                "exclude": "retweets,replies",
                "tweet.fields": "created_at",
            },
            headers=headers,
            timeout=30,
        )
        timeline_response.raise_for_status()

        posts = timeline_response.json().get("data") or []

        if not posts:
            return {
                "available": False,
                "reason": "No original posts were found",
            }

        latest = max(
            posts,
            key=lambda post: post.get("created_at", ""),
        )

        created_at = parse_iso_datetime(latest["created_at"])
        age_days = (
            utc_now() - created_at
        ).total_seconds() / 86400

        return {
            "available": True,
            "created_at": created_at,
            "active": age_days <= MAX_X_INACTIVE_DAYS,
            "age_days": age_days,
        }


X_API = XClient()


async def ensure_user_client_connected() -> None:
    """Ensure the personal Telethon session is connected and authorized."""

    async with user_connection_lock:
        for attempt in range(1, 4):
            try:
                if not user_client.is_connected():
                    LOGGER.warning(
                        "Personal Telegram client disconnected. "
                        "Reconnect attempt %s/3.",
                        attempt,
                    )
                    await user_client.connect()

                if not await user_client.is_user_authorized():
                    raise RuntimeError(
                        "STRING_SESSION is invalid or no longer authorized."
                    )

                # A lightweight API call confirms the sender is usable.
                await user_client.get_me()
                return

            except Exception as error:
                LOGGER.warning(
                    "Personal Telegram connection attempt %s failed: %s",
                    attempt,
                    error,
                )

                try:
                    if user_client.is_connected():
                        await user_client.disconnect()
                except Exception:
                    pass

                if attempt < 3:
                    await asyncio.sleep(attempt * 2)

        raise RuntimeError(
            "Personal Telegram account could not reconnect after 3 attempts."
        )


# =========================================================
# TELEGRAM DEEP ANALYSIS
# =========================================================

def user_activity(status: Any) -> dict[str, Any]:
    now = utc_now()

    if isinstance(status, UserStatusOnline):
        return {
            "text": "Online now",
            "rank": 6,
            "timestamp": now.timestamp(),
        }

    if isinstance(status, UserStatusOffline):
        last_seen = ensure_utc(status.was_online)
        age = now - last_seen

        if age <= timedelta(days=1):
            rank = 5
        elif age <= timedelta(days=7):
            rank = 4
        elif age <= timedelta(days=30):
            rank = 3
        else:
            rank = 2

        return {
            "text": f"Last seen {display_datetime(last_seen)}",
            "rank": rank,
            "timestamp": last_seen.timestamp(),
        }

    if isinstance(status, UserStatusRecently):
        return {
            "text": "Recently active",
            "rank": 4,
            "timestamp": 0,
        }

    if isinstance(status, UserStatusLastWeek):
        return {
            "text": "Active within a week",
            "rank": 3,
            "timestamp": 0,
        }

    if isinstance(status, UserStatusLastMonth):
        return {
            "text": "Active within a month",
            "rank": 2,
            "timestamp": 0,
        }

    if isinstance(status, UserStatusEmpty):
        return {
            "text": "Activity hidden",
            "rank": 1,
            "timestamp": 0,
        }

    return {
        "text": "Activity unavailable",
        "rank": 0,
        "timestamp": 0,
    }


async def analyze_telegram(telegram_url: str) -> dict[str, Any]:
    await ensure_user_client_connected()

    entity = await user_client.get_entity(telegram_url)

    owner: Optional[dict[str, Any]] = None
    admins: list[dict[str, Any]] = []

    async for user in user_client.iter_participants(
        entity,
        filter=ChannelParticipantsAdmins(),
    ):
        participant = getattr(user, "participant", None)

        is_owner = isinstance(
            participant,
            ChannelParticipantCreator,
        )
        is_admin = isinstance(
            participant,
            ChannelParticipantAdmin,
        )

        if not is_owner and not is_admin:
            continue

        if getattr(user, "bot", False):
            continue

        username = getattr(user, "username", None)

        if not username:
            continue

        activity = user_activity(
            getattr(user, "status", None)
        )

        person = {
            "username": f"@{username}",
            "activity": activity["text"],
            "rank": activity["rank"],
            "timestamp": activity["timestamp"],
        }

        if is_owner:
            owner = person
        else:
            admins.append(person)

    admins.sort(
        key=lambda item: (
            item["rank"],
            item["timestamp"],
        ),
        reverse=True,
    )

    admins = admins[:MAX_ACTIVE_ADMINS]

    cutoff = utc_now() - timedelta(days=TG_LOOKBACK_DAYS)
    last_message: Optional[datetime] = None
    message_count = 0
    human_senders: set[int] = set()

    async for message in user_client.iter_messages(
        entity,
        limit=500,
    ):
        if not message.date:
            continue

        message_time = ensure_utc(message.date)

        if last_message is None:
            last_message = message_time

        if message_time < cutoff:
            break

        message_count += 1

        sender = await message.get_sender()

        if (
            sender is not None
            and not getattr(sender, "bot", False)
            and getattr(sender, "id", None)
        ):
            human_senders.add(sender.id)

    if last_message is None:
        status = "Unknown"
        active = False
    else:
        age_days = (
            utc_now() - last_message
        ).total_seconds() / 86400

        if age_days <= 1 and message_count >= 50:
            status = "Very active"
        elif age_days <= 3 and message_count >= 15:
            status = "Active"
        elif age_days <= 7 and message_count >= TG_MIN_MESSAGES_7D:
            status = "Moderately active"
        elif age_days <= MAX_TG_INACTIVE_DAYS:
            status = "Low activity"
        else:
            status = "Inactive"

        active = (
            age_days <= MAX_TG_INACTIVE_DAYS
            and message_count >= TG_MIN_MESSAGES_7D
            and len(human_senders) >= TG_MIN_HUMAN_SENDERS_7D
        )

    return {
        "owner": owner,
        "admins": admins,
        "last_message": last_message,
        "messages_7d": message_count,
        "unique_humans_7d": len(human_senders),
        "status": status,
        "active": active,
    }


# =========================================================
# FAST SCAN
# =========================================================

async def run_fast_scan(
    event: events.NewMessage.Event,
    params: FastScanParams,
) -> None:
    chat_id = event.chat_id

    if chat_id in active_jobs:
        await event.reply("A job is already running in this chat.")
        return

    active_jobs.add(chat_id)
    history_id = STORAGE.create_history(
        "fast_scan",
        params.target_count,
    )

    progress = await event.reply(
        (
            "⚡ Fast scan started\n\n"
            f"Target candidates: {params.target_count}\n"
            f"Maximum inspected: {FAST_SCAN_MAX_INSPECTED}"
        )
    )

    inspected = 0
    found: list[dict[str, Any]] = []

    try:
        for page in range(1, MAX_PAGES_PER_FAST_SCAN + 1):
            market_page = await asyncio.to_thread(
                COINGECKO.market_page,
                page,
                params.category_id,
                params.sort_mode,
            )

            if not market_page:
                break

            for coin in market_page:
                if inspected >= FAST_SCAN_MAX_INSPECTED:
                    break

                inspected += 1

                market_cap = int(
                    coin.get("market_cap") or 0
                )

                if (
                    market_cap < MIN_MARKET_CAP
                    or market_cap > MAX_MARKET_CAP
                ):
                    continue

                coin_id = coin.get("id")

                if not coin_id or STORAGE.exists(coin_id):
                    continue

                try:
                    details = await asyncio.to_thread(
                        COINGECKO.details,
                        coin_id,
                    )
                except Exception as error:
                    LOGGER.warning(
                        "CoinGecko details failed for %s: %s",
                        coin_id,
                        error,
                    )
                    continue

                links = details.get("links") or {}

                x_username = str(
                    links.get("twitter_screen_name") or ""
                ).strip().lstrip("@")

                telegram_url = COINGECKO.telegram_url(
                    links.get(
                        "telegram_channel_identifier"
                    )
                )

                if not x_username or not telegram_url:
                    continue

                website = COINGECKO.website_url(
                    links.get("homepage")
                )

                project = {
                    "coin_id": coin_id,
                    "name": str(coin.get("name") or ""),
                    "symbol": str(
                        coin.get("symbol") or ""
                    ).upper(),
                    "market_cap": market_cap,
                    "category": params.category_name or "All",
                    "website": website,
                    "x_username": x_username,
                    "x_url": f"https://x.com/{x_username}",
                    "telegram_url": telegram_url,
                }

                STORAGE.save_pending(project)
                found.append(project)

                await progress.edit(
                    (
                        "⚡ Fast scan running\n\n"
                        f"Inspected: {inspected}/"
                        f"{FAST_SCAN_MAX_INSPECTED}\n"
                        f"Candidates found: {len(found)}/"
                        f"{params.target_count}\n"
                        f"Current: {project['name']}"
                    )
                )

                if len(found) >= params.target_count:
                    break

                await asyncio.sleep(0.5)

            if (
                len(found) >= params.target_count
                or inspected >= FAST_SCAN_MAX_INSPECTED
            ):
                break

        STORAGE.finish_history(
            history_id,
            inspected=inspected,
            found=len(found),
            status="completed",
        )

        await progress.edit(
            (
                "✅ Fast scan completed\n\n"
                f"Inspected: {inspected}\n"
                f"Candidates saved: {len(found)}\n\n"
                "Run /analyze to perform the deep checks."
            )
        )

        if found:
            output = "NEW CANDIDATES\n\n" + "\n\n".join(
                (
                    f"{number}.\n"
                    f"Project: {project['name']} "
                    f"(${project['symbol']})\n"
                    f"Market cap: "
                    f"${project['market_cap']:,}\n"
                    f"X: {project['x_url']}\n"
                    f"TG: {project['telegram_url']}"
                )
                for number, project in enumerate(found, start=1)
            )

            await send_long(event, output)

    except Exception as error:
        LOGGER.exception("Fast scan failed")

        STORAGE.finish_history(
            history_id,
            inspected=inspected,
            found=len(found),
            status="failed",
            error_message=str(error),
        )

        await progress.edit(
            f"❌ Fast scan failed\n\n{type(error).__name__}: {error}"
        )

    finally:
        active_jobs.discard(chat_id)


# =========================================================
# DEEP ANALYSIS AND SCORING
# =========================================================

def admin_text(admin: dict[str, Any]) -> str:
    return f"{admin['username']} ({admin['activity']})"


async def analyze_one(
    project: dict[str, Any],
) -> dict[str, Any]:
    """
    Deep-check one pending project.

    X API payment or rate-limit errors do not automatically reject the
    project. Telegram and project-quality signals can still qualify it.
    """

    # Passed market-cap and social-link discovery filters.
    score = 30
    reasons: list[str] = []

    result: dict[str, Any] = {
        "coin_id": project["coin_id"],
        "name": project["name"],
        "symbol": project["symbol"],
        "market_cap": int(project["market_cap"] or 0),
        "x_url": project["x_url"],
        "telegram_url": project["telegram_url"],
        "website": project.get("website"),
    }

    # -----------------------------------------------------
    # X ACTIVITY
    # -----------------------------------------------------

    try:
        x_result = await asyncio.to_thread(
            X_API.latest_original_post,
            project["x_username"],
        )

        if x_result.get("available"):
            created_at = x_result["created_at"]

            result["x_last_post_at"] = created_at.isoformat()
            result["x_status"] = (
                "Active"
                if x_result["active"]
                else "Inactive"
            )
            result["x_last_post_display"] = display_datetime(
                created_at
            )

            if x_result["active"]:
                score += 20
            else:
                reasons.append(
                    f"Latest X post is older than "
                    f"{MAX_X_INACTIVE_DAYS} days"
                )

        else:
            result["x_status"] = "Not checked"
            result["x_last_post_display"] = "Unavailable"

            reason = x_result.get(
                "reason",
                "X activity could not be checked",
            )

            LOGGER.warning(
                "X activity unavailable for @%s: %s",
                project["x_username"],
                reason,
            )

    except requests.HTTPError as error:
        status_code = (
            error.response.status_code
            if error.response is not None
            else None
        )

        result["x_last_post_display"] = "Unavailable"

        if status_code == 402:
            result["x_status"] = "Not checked"

            LOGGER.warning(
                "X API payment required for @%s. "
                "Continuing without X scoring.",
                project["x_username"],
            )

        elif status_code == 429:
            result["x_status"] = "Rate limited"

            LOGGER.warning(
                "X API rate limited while checking @%s. "
                "Continuing without X scoring.",
                project["x_username"],
            )

        else:
            result["x_status"] = "API error"

            LOGGER.warning(
                "X API returned HTTP %s for @%s.",
                status_code,
                project["x_username"],
            )

    except Exception as error:
        result["x_status"] = "Error"
        result["x_last_post_display"] = "Unavailable"

        LOGGER.warning(
            "X check failed for @%s: %s",
            project["x_username"],
            error,
        )

    # -----------------------------------------------------
    # TELEGRAM ACTIVITY, OWNER AND ADMINS
    # -----------------------------------------------------

    try:
        await ensure_user_client_connected()

        tg = await analyze_telegram(
            project["telegram_url"]
        )

        result["telegram_last_message_at"] = (
            tg["last_message"].isoformat()
            if tg["last_message"]
            else None
        )
        result["telegram_last_message_display"] = (
            display_datetime(tg["last_message"])
        )
        result["telegram_messages_7d"] = tg["messages_7d"]
        result["telegram_unique_humans_7d"] = (
            tg["unique_humans_7d"]
        )
        result["telegram_status"] = tg["status"]
        result["owner"] = tg["owner"]
        result["admins"] = tg["admins"]

        if tg["active"]:
            score += 20
        else:
            reasons.append(
                "Telegram community did not meet "
                "the activity threshold"
            )

        if tg["owner"]:
            score += 20
        else:
            reasons.append(
                "Owner is not publicly visible"
            )

        score += min(
            len(tg["admins"]) * 4,
            10,
        )

        if not tg["owner"] and not tg["admins"]:
            reasons.append(
                "No public human owner or admins"
            )

    except (
        ChannelPrivateError,
        ChatAdminRequiredError,
    ):
        result["telegram_status"] = "Inaccessible"
        result["admins"] = []
        result["owner"] = None
        reasons.append(
            "Telegram group is inaccessible"
        )

    except (
        UsernameInvalidError,
        UsernameNotOccupiedError,
        ValueError,
    ):
        result["telegram_status"] = "Invalid"
        result["admins"] = []
        result["owner"] = None
        reasons.append(
            "Telegram group is invalid"
        )

    except RPCError as error:
        result["telegram_status"] = "Error"
        result["admins"] = []
        result["owner"] = None
        reasons.append(
            f"Telegram error: {error}"
        )

    except Exception as error:
        # Retry once when Telegram disconnected during a request.
        if "disconnected" in str(error).lower():
            try:
                await ensure_user_client_connected()

                tg = await analyze_telegram(
                    project["telegram_url"]
                )

                result["telegram_last_message_at"] = (
                    tg["last_message"].isoformat()
                    if tg["last_message"]
                    else None
                )
                result[
                    "telegram_last_message_display"
                ] = display_datetime(
                    tg["last_message"]
                )
                result["telegram_messages_7d"] = (
                    tg["messages_7d"]
                )
                result[
                    "telegram_unique_humans_7d"
                ] = tg["unique_humans_7d"]
                result["telegram_status"] = tg["status"]
                result["owner"] = tg["owner"]
                result["admins"] = tg["admins"]

                if tg["active"]:
                    score += 20
                else:
                    reasons.append(
                        "Telegram community did not meet "
                        "the activity threshold"
                    )

                if tg["owner"]:
                    score += 20
                else:
                    reasons.append(
                        "Owner is not publicly visible"
                    )

                score += min(
                    len(tg["admins"]) * 4,
                    10,
                )

                if not tg["owner"] and not tg["admins"]:
                    reasons.append(
                        "No public human owner or admins"
                    )

            except Exception as retry_error:
                result["telegram_status"] = "Disconnected"
                result["admins"] = []
                result["owner"] = None
                reasons.append(
                    "Telegram analysis failed after reconnect: "
                    f"{retry_error}"
                )

        else:
            result["telegram_status"] = "Error"
            result["admins"] = []
            result["owner"] = None
            reasons.append(
                f"Telegram analysis failed: {error}"
            )

    # -----------------------------------------------------
    # WEBSITE AND FINAL SCORE
    # -----------------------------------------------------

    if project.get("website"):
        score += 10

    if score >= 80:
        stage = "priority"
        classification = "🔥 Priority"
    elif score >= 60:
        stage = "qualified"
        classification = "✅ Qualified"
    elif score >= 40:
        stage = "watchlist"
        classification = "🟡 Watchlist"
    else:
        stage = "rejected"
        classification = "❌ Rejected"

    owner = result.get("owner")
    admins = result.get("admins") or []

    result.update(
        {
            "score": score,
            "stage": stage,
            "classification": classification,
            "rejection_reason": (
                "; ".join(reasons)
                if reasons
                else None
            ),
            "owner_username": (
                owner["username"]
                if owner
                else None
            ),
            "owner_activity": (
                owner["activity"]
                if owner
                else None
            ),
            "admin_1": (
                admin_text(admins[0])
                if len(admins) > 0
                else None
            ),
            "admin_2": (
                admin_text(admins[1])
                if len(admins) > 1
                else None
            ),
            "admin_3": (
                admin_text(admins[2])
                if len(admins) > 2
                else None
            ),
        }
    )

    return result

def format_analysis(result: dict[str, Any]) -> str:
    lines = [
        f"Project: {result['name']} (${result['symbol']})",
        f"Score: {result['score']}/100",
        f"Classification: {result['classification']}",
        f"Market cap: ${result['market_cap']:,}",
        f"X: {result['x_url']}",
        f"X status: {result.get('x_status', 'Unavailable')}",
        (
            "X last post: "
            f"{result.get('x_last_post_display', 'Unavailable')}"
        ),
        f"TG: {result['telegram_url']}",
        (
            "TG status: "
            f"{result.get('telegram_status', 'Unavailable')}"
        ),
        (
            "TG last message: "
            f"{result.get('telegram_last_message_display', 'Unavailable')}"
        ),
        (
            "Messages in 7 days: "
            f"{result.get('telegram_messages_7d', 0)}"
        ),
        (
            "Unique humans in 7 days: "
            f"{result.get('telegram_unique_humans_7d', 0)}"
        ),
    ]

    owner = result.get("owner")
    if owner:
        lines.append(f"Owner: {admin_text(owner)}")
    else:
        lines.append("Owner: Not publicly visible")

    admins = result.get("admins") or []

    if admins:
        lines.append("Top active admins:")
        lines.extend(admin_text(admin) for admin in admins)
    else:
        lines.append("Top active admins: None available")

    if result.get("rejection_reason"):
        lines.append(f"Notes: {result['rejection_reason']}")

    return "\n".join(lines)


async def run_deep_analysis(
    event: events.NewMessage.Event,
    limit: int,
) -> None:
    await ensure_user_client_connected()

    chat_id = event.chat_id

    if chat_id in active_jobs:
        await event.reply("A job is already running in this chat.")
        return

    pending = STORAGE.pending_projects(limit)

    if not pending:
        await event.reply(
            "No pending candidates. Run /scan first."
        )
        return

    active_jobs.add(chat_id)
    history_id = STORAGE.create_history(
        "deep_analysis",
        len(pending),
    )

    progress = await event.reply(
        (
            "🔬 Deep analysis started\n\n"
            f"Candidates: {len(pending)}"
        )
    )

    results: list[dict[str, Any]] = []

    try:
        for number, project in enumerate(pending, start=1):
            await progress.edit(
                (
                    "🔬 Deep analysis running\n\n"
                    f"Progress: {number - 1}/{len(pending)}\n"
                    f"Current: {project['name']}"
                )
            )

            try:
                result = await analyze_one(project)
            except FloodWaitError as error:
                await progress.edit(
                    (
                        "⏳ Telegram rate limit\n\n"
                        f"Waiting {error.seconds} seconds..."
                    )
                )
                await asyncio.sleep(error.seconds + 1)
                result = await analyze_one(project)

            STORAGE.save_analysis(result)
            results.append(result)

            await progress.edit(
                (
                    "🔬 Deep analysis running\n\n"
                    f"Progress: {number}/{len(pending)}\n"
                    f"Current: {project['name']}\n"
                    f"Result: {result['classification']} "
                    f"({result['score']}/100)"
                )
            )

            await asyncio.sleep(1)

        STORAGE.finish_history(
            history_id,
            inspected=len(pending),
            found=len(results),
            status="completed",
        )

        priority = [
            item for item in results
            if item["stage"] == "priority"
        ]
        qualified = [
            item for item in results
            if item["stage"] == "qualified"
        ]
        watchlist = [
            item for item in results
            if item["stage"] == "watchlist"
        ]
        rejected = [
            item for item in results
            if item["stage"] == "rejected"
        ]

        await progress.edit(
            (
                "✅ Deep analysis completed\n\n"
                f"Priority: {len(priority)}\n"
                f"Qualified: {len(qualified)}\n"
                f"Watchlist: {len(watchlist)}\n"
                f"Rejected: {len(rejected)}"
            )
        )

        for heading, group in (
            ("🔥 PRIORITY PROJECTS", priority),
            ("✅ QUALIFIED PROJECTS", qualified),
            ("🟡 WATCHLIST", watchlist),
        ):
            if group:
                await send_long(
                    event,
                    f"{heading}\n\n"
                    + "\n\n".join(
                        format_analysis(item)
                        for item in group
                    ),
                )

        if rejected:
            await event.reply(
                (
                    f"❌ Rejected projects: {len(rejected)}\n\n"
                    "Use /rejected to view them."
                )
            )

    except Exception as error:
        LOGGER.exception("Deep analysis failed")

        STORAGE.finish_history(
            history_id,
            inspected=len(results),
            found=len(results),
            status="failed",
            error_message=str(error),
        )

        await progress.edit(
            (
                "❌ Deep analysis failed\n\n"
                f"{type(error).__name__}: {error}"
            )
        )

    finally:
        active_jobs.discard(chat_id)


# =========================================================
# COMMAND PARSERS AND SAVED OUTPUT
# =========================================================

def parse_fast_scan(text: str) -> FastScanParams:
    parts = text.strip().split()
    target = 50
    category = None

    if len(parts) >= 2:
        target = int(parts[1])

    if target < 1 or target > 100:
        raise ValueError(
            "Target must be between 1 and 100."
        )

    if len(parts) >= 3:
        category = parts[2]

    return FastScanParams(
        target_count=target,
        category_id=category,
        category_name=category,
    )


def parse_limit(text: str, default: int = 20) -> int:
    parts = text.strip().split()

    if len(parts) < 2:
        return default

    value = int(parts[1])

    if value < 1 or value > 100:
        raise ValueError(
            "Limit must be between 1 and 100."
        )

    return value


def format_saved(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No projects found."

    blocks: list[str] = []

    for row in rows:
        block = (
            f"Project: {row['name']} (${row['symbol']})\n"
            f"Stage: {row['stage']}\n"
            f"Score: {row['score'] or 0}/100\n"
            f"Market cap: ${int(row['market_cap'] or 0):,}\n"
            f"X: {row['x_url']}\n"
            f"TG: {row['telegram_url']}"
        )

        if row.get("owner_username"):
            block += (
                f"\nOwner: {row['owner_username']} "
                f"({row.get('owner_activity') or 'Unknown'})"
            )

        admins = [
            row.get("admin_1"),
            row.get("admin_2"),
            row.get("admin_3"),
        ]
        admins = [admin for admin in admins if admin]

        if admins:
            block += "\nTop admins:\n" + "\n".join(admins)

        if row.get("rejection_reason"):
            block += f"\nNotes: {row['rejection_reason']}"

        blocks.append(block)

    return "\n\n".join(blocks)


async def safe_event_reply(
    event: events.NewMessage.Event,
    text: str,
) -> Optional[Any]:
    """Reply to commands while logging delivery failures."""

    try:
        return await event.reply(
            text,
            link_preview=False,
        )
    except FloodWaitError as error:
        await asyncio.sleep(error.seconds + 1)
        return await event.reply(
            text,
            link_preview=False,
        )
    except Exception as error:
        LOGGER.exception(
            "Could not reply to chat %s: %s",
            event.chat_id,
            error,
        )
        return None


# =========================================================
# BOT COMMANDS
# =========================================================

@bot_client.on(events.NewMessage(pattern=r"^/start(?:@\w+)?$"))
async def start_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        await event.reply("This bot is private.")
        return

    personal_status = (
        "connected"
        if user_client.is_connected()
        else "temporarily disconnected"
    )

    await safe_event_reply(
        event,
        (
            "Project Hunter v2 is online ✅\n\n"
            f"Personal Telegram session: {personal_status}\n\n"
            "Fast discovery:\n"
            "/scan\n"
            "/scan 50\n"
            "/scan 50 artificial-intelligence\n\n"
            "Deep analysis:\n"
            "/analyze\n"
            "/analyze 20\n\n"
            "Saved results:\n"
            "/pending\n"
            "/priority\n"
            "/qualified\n"
            "/watchlist\n"
            "/rejected\n"
            "/latest\n"
            "/count\n"
            "/status"
        ),
    )


@bot_client.on(events.NewMessage(pattern=r"^/scan(?:@\w+)?(?:\s+.*)?$"))
async def scan_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        await event.reply("This bot is private.")
        return

    try:
        params = parse_fast_scan(event.raw_text)
    except (ValueError, TypeError) as error:
        await event.reply(f"❌ {error}")
        return

    asyncio.create_task(
        run_fast_scan(event, params)
    )


@bot_client.on(events.NewMessage(pattern=r"^/analyze(?:@\w+)?(?:\s+\d+)?$"))
async def analyze_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        await event.reply("This bot is private.")
        return

    try:
        limit = parse_limit(event.raw_text, 20)
    except (ValueError, TypeError) as error:
        await event.reply(f"❌ {error}")
        return

    asyncio.create_task(
        run_deep_analysis(event, limit)
    )


async def send_stage(
    event: events.NewMessage.Event,
    stage: Optional[str],
    heading: str,
) -> None:
    rows = STORAGE.list_projects(stage, 20)
    await send_long(
        event,
        f"{heading}\n\n{format_saved(rows)}",
    )


@bot_client.on(events.NewMessage(pattern=r"^/pending(?:@\w+)?$"))
async def pending_handler(event: events.NewMessage.Event) -> None:
    if authorized(event):
        await send_stage(event, "pending", "PENDING CANDIDATES")


@bot_client.on(events.NewMessage(pattern=r"^/priority(?:@\w+)?$"))
async def priority_handler(event: events.NewMessage.Event) -> None:
    if authorized(event):
        await send_stage(event, "priority", "🔥 PRIORITY PROJECTS")


@bot_client.on(events.NewMessage(pattern=r"^/qualified(?:@\w+)?$"))
async def qualified_handler(event: events.NewMessage.Event) -> None:
    if authorized(event):
        await send_stage(event, "qualified", "✅ QUALIFIED PROJECTS")


@bot_client.on(events.NewMessage(pattern=r"^/watchlist(?:@\w+)?$"))
async def watchlist_handler(event: events.NewMessage.Event) -> None:
    if authorized(event):
        await send_stage(event, "watchlist", "🟡 WATCHLIST")


@bot_client.on(events.NewMessage(pattern=r"^/rejected(?:@\w+)?$"))
async def rejected_handler(event: events.NewMessage.Event) -> None:
    if authorized(event):
        await send_stage(event, "rejected", "❌ REJECTED PROJECTS")


@bot_client.on(events.NewMessage(pattern=r"^/latest(?:@\w+)?$"))
async def latest_handler(event: events.NewMessage.Event) -> None:
    if authorized(event):
        await send_stage(event, None, "LATEST PROJECTS")


@bot_client.on(events.NewMessage(pattern=r"^/count(?:@\w+)?$"))
async def count_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        return

    counts = STORAGE.counts()

    await event.reply(
        (
            f"Total: {counts['total']}\n"
            f"Pending: {counts['pending']}\n"
            f"Priority: {counts['priority']}\n"
            f"Qualified: {counts['qualified']}\n"
            f"Watchlist: {counts['watchlist']}\n"
            f"Rejected: {counts['rejected']}"
        )
    )


@bot_client.on(events.NewMessage(pattern=r"^/status(?:@\w+)?$"))
async def status_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        await safe_event_reply(event, "This bot is private.")
        return

    bot_ok = bot_client.is_connected()
    user_ok = user_client.is_connected()
    authorization = "unknown"

    try:
        await ensure_user_client_connected()
        user_ok = True
        authorization = "authorized"
    except Exception as error:
        user_ok = False
        authorization = f"error: {error}"

    counts = STORAGE.counts()

    await safe_event_reply(
        event,
        (
            "Project Hunter status\n\n"
            f"Bot connection: {'online' if bot_ok else 'offline'}\n"
            f"Personal session: {'online' if user_ok else 'offline'}\n"
            f"Authorization: {authorization}\n"
            f"Database total: {counts['total']}\n"
            f"Pending: {counts['pending']}"
        ),
    )


@bot_client.on(events.NewMessage)
async def unknown_command_handler(event: events.NewMessage.Event) -> None:
    """Confirm that the bot is receiving messages."""

    text = (event.raw_text or "").strip()

    if not text or not text.startswith("/"):
        return

    known = (
        "/start",
        "/scan",
        "/analyze",
        "/pending",
        "/priority",
        "/qualified",
        "/watchlist",
        "/rejected",
        "/latest",
        "/count",
        "/status",
    )

    command = text.split()[0].split("@")[0].lower()

    if command not in known and authorized(event):
        await safe_event_reply(
            event,
            "Unknown command. Send /start to view available commands.",
        )


# =========================================================
# STARTUP
# =========================================================

async def user_client_keepalive() -> None:
    """Keep the personal Telegram session connected in Railway."""

    while True:
        try:
            await ensure_user_client_connected()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            LOGGER.error(
                "Personal Telegram keepalive failed: %s",
                error,
            )

        await asyncio.sleep(30)


async def main() -> None:
    # Start the BotFather bot first so /start and /status work even when
    # the personal Telegram session has a temporary connection problem.
    await bot_client.start(bot_token=BOT_TOKEN)

    bot = await bot_client.get_me()

    LOGGER.info(
        "Project Hunter bot connected as @%s",
        bot.username,
    )

    try:
        await ensure_user_client_connected()
        personal = await user_client.get_me()
        LOGGER.info(
            "Personal Telegram session connected as %s",
            (
                f"@{personal.username}"
                if getattr(personal, "username", None)
                else getattr(personal, "first_name", "Unknown")
            ),
        )
    except Exception as error:
        LOGGER.error(
            "Personal Telegram session did not connect at startup: %s. "
            "The bot will remain online and keep retrying.",
            error,
        )

    keepalive_task = asyncio.create_task(
        user_client_keepalive()
    )

    try:
        await bot_client.run_until_disconnected()
    finally:
        keepalive_task.cancel()

        await asyncio.gather(
            keepalive_task,
            return_exceptions=True,
        )

        if bot_client.is_connected():
            await bot_client.disconnect()

        if user_client.is_connected():
            await user_client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("Bot stopped manually.")
    except Exception:
        LOGGER.exception("Project Hunter stopped unexpectedly.")
        raise
