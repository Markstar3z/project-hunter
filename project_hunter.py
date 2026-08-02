"""Project Hunter bot.

Pipeline:
1. Discover projects from CoinGecko.
2. Require X and Telegram links.
3. Filter by market cap.
4. Check the latest X post through the official X API.
5. Check recent Telegram community activity through Telethon.
6. Find the owner and three most recently active human admins.
7. Save qualified and rejected projects to SQLite.
8. Send reports back through a Telegram bot.

Railway variables:
BOT_TOKEN
API_ID
API_HASH
STRING_SESSION
COINGECKO_API_KEY
X_BEARER_TOKEN
DATABASE_PATH=/data/project_hunter.db
ALLOWED_CHAT_ID
"""

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

DATABASE_PATH = os.getenv(
    "DATABASE_PATH",
    "/data/project_hunter.db",
)

ALLOWED_CHAT_ID_RAW = os.getenv("ALLOWED_CHAT_ID", "").strip()
ALLOWED_CHAT_ID = (
    int(ALLOWED_CHAT_ID_RAW)
    if ALLOWED_CHAT_ID_RAW
    else None
)

MIN_MARKET_CAP = int(
    os.getenv("MIN_MARKET_CAP", "10000")
)
MAX_MARKET_CAP = int(
    os.getenv("MAX_MARKET_CAP", "1000000000")
)

MAX_PAGES_PER_SCAN = int(
    os.getenv("MAX_PAGES_PER_SCAN", "10")
)
PAGE_SIZE = min(
    int(os.getenv("PAGE_SIZE", "250")),
    250,
)

MAX_X_INACTIVE_DAYS = int(
    os.getenv("MAX_X_INACTIVE_DAYS", "30")
)
MAX_TG_INACTIVE_DAYS = int(
    os.getenv("MAX_TG_INACTIVE_DAYS", "30")
)

TG_ACTIVITY_LOOKBACK_DAYS = int(
    os.getenv("TG_ACTIVITY_LOOKBACK_DAYS", "7")
)
TG_MIN_MESSAGES_7D = int(
    os.getenv("TG_MIN_MESSAGES_7D", "5")
)
TG_MIN_HUMAN_SENDERS_7D = int(
    os.getenv("TG_MIN_HUMAN_SENDERS_7D", "2")
)

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
)

bot_client = TelegramClient(
    MemorySession(),
    API_ID,
    API_HASH,
)

active_jobs: set[int] = set()


# =========================================================
# DATA MODELS
# =========================================================

@dataclass
class ScanParams:
    target_count: int = 20
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    sort_mode: str = "market_cap_desc"


# =========================================================
# HELPERS
# =========================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def format_datetime(value: Optional[datetime]) -> str:
    if value is None:
        return "Unavailable"

    return value.astimezone(timezone.utc).strftime(
        "%d %b %Y, %H:%M UTC"
    )


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def split_text(
    text: str,
    limit: int = MESSAGE_LIMIT,
) -> list[str]:
    if len(text) <= limit:
        return [text]

    blocks = text.split("\n\n")
    chunks: list[str] = []
    current = ""

    for block in blocks:
        candidate = (
            block
            if not current
            else f"{current}\n\n{block}"
        )

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


def extract_x_username(url: str) -> Optional[str]:
    match = re.match(
        r"https?://(?:www\.)?(?:x\.com|twitter\.com)/"
        r"([A-Za-z0-9_]+)",
        url,
        re.IGNORECASE,
    )

    return match.group(1) if match else None


# =========================================================
# HTTP CLIENT
# =========================================================

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
            "User-Agent": "ProjectHunterBot/1.0",
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

        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    symbol TEXT,
                    market_cap INTEGER,
                    category TEXT,
                    x_url TEXT,
                    x_username TEXT,
                    x_last_post_at TEXT,
                    x_status TEXT,
                    telegram_url TEXT,
                    telegram_last_message_at TEXT,
                    telegram_messages_7d INTEGER DEFAULT 0,
                    telegram_unique_humans_7d INTEGER DEFAULT 0,
                    telegram_status TEXT,
                    owner_username TEXT,
                    owner_activity TEXT,
                    admin_1 TEXT,
                    admin_2 TEXT,
                    admin_3 TEXT,
                    qualification_status TEXT NOT NULL,
                    rejection_reason TEXT,
                    discovered_at TEXT NOT NULL,
                    UNIQUE(coin_id)
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    target_count INTEGER NOT NULL,
                    category TEXT,
                    inspected_count INTEGER DEFAULT 0,
                    qualified_count INTEGER DEFAULT 0,
                    rejected_count INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    error_message TEXT
                )
                """
            )

            connection.commit()

    def project_exists(self, coin_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM projects
                WHERE coin_id = ?
                LIMIT 1
                """,
                (coin_id,),
            ).fetchone()

        return row is not None

    def save_project(
        self,
        project: dict[str, Any],
    ) -> None:
        now = utc_now().isoformat()

        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO projects (
                    coin_id,
                    name,
                    symbol,
                    market_cap,
                    category,
                    x_url,
                    x_username,
                    x_last_post_at,
                    x_status,
                    telegram_url,
                    telegram_last_message_at,
                    telegram_messages_7d,
                    telegram_unique_humans_7d,
                    telegram_status,
                    owner_username,
                    owner_activity,
                    admin_1,
                    admin_2,
                    admin_3,
                    qualification_status,
                    rejection_reason,
                    discovered_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(coin_id)
                DO UPDATE SET
                    name = excluded.name,
                    symbol = excluded.symbol,
                    market_cap = excluded.market_cap,
                    category = excluded.category,
                    x_url = excluded.x_url,
                    x_username = excluded.x_username,
                    x_last_post_at = excluded.x_last_post_at,
                    x_status = excluded.x_status,
                    telegram_url = excluded.telegram_url,
                    telegram_last_message_at =
                        excluded.telegram_last_message_at,
                    telegram_messages_7d =
                        excluded.telegram_messages_7d,
                    telegram_unique_humans_7d =
                        excluded.telegram_unique_humans_7d,
                    telegram_status =
                        excluded.telegram_status,
                    owner_username =
                        excluded.owner_username,
                    owner_activity =
                        excluded.owner_activity,
                    admin_1 = excluded.admin_1,
                    admin_2 = excluded.admin_2,
                    admin_3 = excluded.admin_3,
                    qualification_status =
                        excluded.qualification_status,
                    rejection_reason =
                        excluded.rejection_reason
                """,
                (
                    project["coin_id"],
                    project["name"],
                    project["symbol"],
                    project["market_cap"],
                    project["category"],
                    project["x_url"],
                    project["x_username"],
                    project.get("x_last_post_at"),
                    project.get("x_status"),
                    project["telegram_url"],
                    project.get("telegram_last_message_at"),
                    project.get("telegram_messages_7d", 0),
                    project.get("telegram_unique_humans_7d", 0),
                    project.get("telegram_status"),
                    project.get("owner_username"),
                    project.get("owner_activity"),
                    project.get("admin_1"),
                    project.get("admin_2"),
                    project.get("admin_3"),
                    project["qualification_status"],
                    project.get("rejection_reason"),
                    now,
                ),
            )

            connection.commit()

    def create_scan(
        self,
        params: ScanParams,
    ) -> int:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO scan_history (
                    started_at,
                    target_count,
                    category,
                    status
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    utc_now().isoformat(),
                    params.target_count,
                    params.category_name or "All",
                    "running",
                ),
            )

            connection.commit()
            return int(cursor.lastrowid)

    def finish_scan(
        self,
        scan_id: int,
        *,
        inspected: int,
        qualified: int,
        rejected: int,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE scan_history
                SET completed_at = ?,
                    inspected_count = ?,
                    qualified_count = ?,
                    rejected_count = ?,
                    status = ?,
                    error_message = ?
                WHERE id = ?
                """,
                (
                    utc_now().isoformat(),
                    inspected,
                    qualified,
                    rejected,
                    status,
                    error_message,
                    scan_id,
                ),
            )
            connection.commit()

    def list_projects(
        self,
        status: Optional[str],
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        with self.connect() as connection:
            if status:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM projects
                    WHERE qualification_status = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (status, limit),
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

    def count_projects(self) -> dict[str, int]:
        with self.connect() as connection:
            total = int(
                connection.execute(
                    "SELECT COUNT(*) FROM projects"
                ).fetchone()[0]
            )

            qualified = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM projects
                    WHERE qualification_status = 'qualified'
                    """
                ).fetchone()[0]
            )

            rejected = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM projects
                    WHERE qualification_status = 'rejected'
                    """
                ).fetchone()[0]
            )

        return {
            "total": total,
            "qualified": qualified,
            "rejected": rejected,
        }


STORAGE = Storage(DATABASE_PATH)


# =========================================================
# COINGECKO
# =========================================================

class CoinGeckoClient:
    def __init__(self) -> None:
        self.headers = {}

        if COINGECKO_API_KEY:
            self.headers[
                "x-cg-demo-api-key"
            ] = COINGECKO_API_KEY

    def get_market_page(
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

    def get_details(
        self,
        coin_id: str,
    ) -> dict[str, Any]:
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
    def extract_telegram_url(
        value: Any,
    ) -> Optional[str]:
        if not value:
            return None

        if isinstance(value, list):
            for item in value:
                result = (
                    CoinGeckoClient
                    .extract_telegram_url(item)
                )
                if result:
                    return result
            return None

        text = str(value).strip()

        if not text:
            return None

        if text.startswith(
            ("http://", "https://")
        ):
            return text

        return f"https://t.me/{text.lstrip('@')}"


COINGECKO = CoinGeckoClient()


# =========================================================
# X API
# =========================================================

class XClient:
    def __init__(self) -> None:
        self.headers = {
            "Authorization": f"Bearer {X_BEARER_TOKEN}"
        }

    def latest_post(
        self,
        username: str,
    ) -> dict[str, Any]:
        if not X_BEARER_TOKEN:
            return {
                "available": False,
                "reason": "X_BEARER_TOKEN is missing",
            }

        user_response = HTTP.get(
            f"{X_API_BASE}/users/by/username/{username}",
            headers=self.headers,
            timeout=30,
        )
        user_response.raise_for_status()
        user_data = user_response.json().get("data")

        if not user_data:
            return {
                "available": False,
                "reason": "X account not found",
            }

        user_id = user_data["id"]

        posts_response = HTTP.get(
            f"{X_API_BASE}/users/{user_id}/tweets",
            params={
                "max_results": 5,
                "exclude": "retweets,replies",
                "tweet.fields": "created_at",
            },
            headers=self.headers,
            timeout=30,
        )
        posts_response.raise_for_status()
        posts = posts_response.json().get("data") or []

        if not posts:
            return {
                "available": False,
                "reason": "No original X posts found",
            }

        latest = max(
            posts,
            key=lambda post: post.get(
                "created_at",
                "",
            ),
        )

        created_at = parse_iso_datetime(
            latest["created_at"]
        )

        age_days = (
            utc_now() - created_at
        ).total_seconds() / 86400

        return {
            "available": True,
            "created_at": created_at,
            "age_days": age_days,
            "active": (
                age_days <= MAX_X_INACTIVE_DAYS
            ),
        }


X_API = XClient()


# =========================================================
# TELEGRAM ANALYSIS
# =========================================================

def describe_user_activity(status: Any) -> dict[str, Any]:
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
            "text": (
                "Last seen "
                f"{format_datetime(last_seen)}"
            ),
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


async def analyze_telegram(
    telegram_url: str,
) -> dict[str, Any]:
    entity = await user_client.get_entity(
        telegram_url
    )

    owner: Optional[dict[str, Any]] = None
    admins: list[dict[str, Any]] = []

    async for user in user_client.iter_participants(
        entity,
        filter=ChannelParticipantsAdmins(),
    ):
        participant = getattr(
            user,
            "participant",
            None,
        )

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

        username = getattr(
            user,
            "username",
            None,
        )

        if not username:
            continue

        activity = describe_user_activity(
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

    cutoff = utc_now() - timedelta(
        days=TG_ACTIVITY_LOOKBACK_DAYS
    )

    last_message_at: Optional[datetime] = None
    messages_7d = 0
    human_senders: set[int] = set()

    async for message in user_client.iter_messages(
        entity,
        limit=500,
    ):
        if not message.date:
            continue

        message_time = ensure_utc(
            message.date
        )

        if last_message_at is None:
            last_message_at = message_time

        if message_time < cutoff:
            break

        messages_7d += 1

        sender = await message.get_sender()

        if (
            sender is not None
            and not getattr(sender, "bot", False)
            and getattr(sender, "id", None)
        ):
            human_senders.add(sender.id)

    if last_message_at is None:
        tg_status = "Unknown"
        active = False
    else:
        age_days = (
            utc_now() - last_message_at
        ).total_seconds() / 86400

        if (
            age_days <= 1
            and messages_7d >= 50
        ):
            tg_status = "Very active"
        elif (
            age_days <= 3
            and messages_7d >= 15
        ):
            tg_status = "Active"
        elif (
            age_days <= 7
            and messages_7d >= TG_MIN_MESSAGES_7D
        ):
            tg_status = "Moderately active"
        elif age_days <= MAX_TG_INACTIVE_DAYS:
            tg_status = "Low activity"
        else:
            tg_status = "Inactive"

        active = (
            age_days <= MAX_TG_INACTIVE_DAYS
            and messages_7d >= TG_MIN_MESSAGES_7D
            and len(human_senders)
                >= TG_MIN_HUMAN_SENDERS_7D
        )

    return {
        "owner": owner,
        "admins": admins,
        "last_message_at": last_message_at,
        "messages_7d": messages_7d,
        "unique_humans_7d": len(human_senders),
        "status": tg_status,
        "active": active,
    }


# =========================================================
# FORMATTERS
# =========================================================

def format_admin(person: dict[str, Any]) -> str:
    return (
        f"{person['username']} "
        f"({person['activity']})"
    )


def format_project(
    project: dict[str, Any],
) -> str:
    lines = [
        f"Project: {project['name']} "
        f"(${project['symbol']})",
        f"Market cap: ${project['market_cap']:,}",
        f"X link: {project['x_url']}",
        f"X status: {project.get('x_status') or 'Unavailable'}",
        (
            "X last post: "
            f"{project.get('x_last_post_display') or 'Unavailable'}"
        ),
        f"TG link: {project['telegram_url']}",
        (
            "TG status: "
            f"{project.get('telegram_status') or 'Unavailable'}"
        ),
        (
            "TG last message: "
            f"{project.get('telegram_last_message_display') or 'Unavailable'}"
        ),
        (
            "Messages in 7 days: "
            f"{project.get('telegram_messages_7d', 0)}"
        ),
        (
            "Unique humans in 7 days: "
            f"{project.get('telegram_unique_humans_7d', 0)}"
        ),
    ]

    owner = project.get("owner")
    if owner:
        lines.append(
            f"Owner: {format_admin(owner)}"
        )
    else:
        lines.append("Owner: Not publicly visible")

    admins = project.get("admins") or []
    if admins:
        lines.append("Top active admins:")
        lines.extend(
            format_admin(admin)
            for admin in admins
        )
    else:
        lines.append(
            "Top active admins: None available"
        )

    lines.append(
        "Status: "
        f"{project['qualification_status'].upper()}"
    )

    if project.get("rejection_reason"):
        lines.append(
            "Reason: "
            f"{project['rejection_reason']}"
        )

    return "\n".join(lines)


async def send_long(
    event: events.NewMessage.Event,
    text: str,
) -> None:
    for chunk in split_text(text):
        await event.reply(
            chunk,
            link_preview=False,
        )
        await asyncio.sleep(0.5)


# =========================================================
# SCANNING PIPELINE
# =========================================================

async def analyze_candidate(
    coin: dict[str, Any],
    details: dict[str, Any],
    category_name: Optional[str],
) -> dict[str, Any]:
    links = details.get("links") or {}

    x_username = str(
        links.get("twitter_screen_name")
        or ""
    ).strip().lstrip("@")

    telegram_url = (
        COINGECKO.extract_telegram_url(
            links.get(
                "telegram_channel_identifier"
            )
        )
    )

    project = {
        "coin_id": coin["id"],
        "name": str(coin.get("name") or ""),
        "symbol": str(
            coin.get("symbol") or ""
        ).upper(),
        "market_cap": int(
            coin.get("market_cap") or 0
        ),
        "category": category_name or "All",
        "x_username": x_username,
        "x_url": (
            f"https://x.com/{x_username}"
            if x_username
            else ""
        ),
        "telegram_url": telegram_url or "",
        "qualification_status": "rejected",
        "rejection_reason": None,
    }

    reasons: list[str] = []

    if not x_username:
        reasons.append("Missing X account")

    if not telegram_url:
        reasons.append(
            "Missing Telegram group"
        )

    if reasons:
        project["rejection_reason"] = "; ".join(
            reasons
        )
        return project

    try:
        x_result = await asyncio.to_thread(
            X_API.latest_post,
            x_username,
        )
    except Exception as error:
        x_result = {
            "available": False,
            "reason": f"X API error: {error}",
        }

    if x_result.get("available"):
        x_last_post = x_result["created_at"]

        project["x_last_post_at"] = (
            x_last_post.isoformat()
        )
        project["x_last_post_display"] = (
            format_datetime(x_last_post)
        )
        project["x_status"] = (
            "Active"
            if x_result["active"]
            else "Inactive"
        )

        if not x_result["active"]:
            reasons.append(
                "Latest X post is older than "
                f"{MAX_X_INACTIVE_DAYS} days"
            )
    else:
        project["x_status"] = "Unavailable"
        project["x_last_post_display"] = (
            "Unavailable"
        )
        reasons.append(
            x_result.get(
                "reason",
                "Could not check X activity",
            )
        )

    try:
        tg_result = await analyze_telegram(
            telegram_url
        )

        project["owner"] = tg_result["owner"]
        project["admins"] = tg_result["admins"]
        project["telegram_status"] = (
            tg_result["status"]
        )
        project["telegram_messages_7d"] = (
            tg_result["messages_7d"]
        )
        project[
            "telegram_unique_humans_7d"
        ] = tg_result["unique_humans_7d"]

        last_message = tg_result[
            "last_message_at"
        ]

        project[
            "telegram_last_message_at"
        ] = (
            last_message.isoformat()
            if last_message
            else None
        )

        project[
            "telegram_last_message_display"
        ] = format_datetime(last_message)

        if not tg_result["active"]:
            reasons.append(
                "Telegram community did not meet "
                "the activity threshold"
            )

        if not (
            tg_result["owner"]
            or tg_result["admins"]
        ):
            reasons.append(
                "No public human owner or admins"
            )

    except (
        ChannelPrivateError,
        ChatAdminRequiredError,
    ):
        project["telegram_status"] = (
            "Inaccessible"
        )
        reasons.append(
            "Telegram group is inaccessible"
        )

    except (
        UsernameInvalidError,
        UsernameNotOccupiedError,
        ValueError,
    ):
        project["telegram_status"] = "Invalid"
        reasons.append(
            "Telegram group is invalid or missing"
        )

    except RPCError as error:
        project["telegram_status"] = "Error"
        reasons.append(
            f"Telegram error: {error}"
        )

    except Exception as error:
        project["telegram_status"] = "Error"
        reasons.append(
            f"Telegram analysis failed: {error}"
        )

    if reasons:
        project["qualification_status"] = "rejected"
        project["rejection_reason"] = "; ".join(
            reasons
        )
    else:
        project["qualification_status"] = "qualified"
        project["rejection_reason"] = None

    return project


def storage_payload(
    project: dict[str, Any],
) -> dict[str, Any]:
    owner = project.get("owner")
    admins = project.get("admins") or []

    return {
        **project,
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
            format_admin(admins[0])
            if len(admins) > 0
            else None
        ),
        "admin_2": (
            format_admin(admins[1])
            if len(admins) > 1
            else None
        ),
        "admin_3": (
            format_admin(admins[2])
            if len(admins) > 2
            else None
        ),
    }


async def run_scan(
    event: events.NewMessage.Event,
    params: ScanParams,
) -> None:
    chat_id = event.chat_id

    if chat_id in active_jobs:
        await event.reply(
            "A scan is already running in this chat."
        )
        return

    active_jobs.add(chat_id)
    scan_id = STORAGE.create_scan(params)

    progress = await event.reply(
        (
            "⏳ Project Hunter scan started\n\n"
            f"Target qualified projects: "
            f"{params.target_count}\n"
            f"Category: "
            f"{params.category_name or 'All'}"
        )
    )

    qualified: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    inspected = 0

    try:
        for page in range(
            1,
            MAX_PAGES_PER_SCAN + 1,
        ):
            await progress.edit(
                (
                    "⏳ Project Hunter scan running\n\n"
                    f"Page: {page}/"
                    f"{MAX_PAGES_PER_SCAN}\n"
                    f"Inspected: {inspected}\n"
                    f"Qualified: {len(qualified)}\n"
                    f"Rejected: {len(rejected)}"
                )
            )

            market_page = await asyncio.to_thread(
                COINGECKO.get_market_page,
                page,
                params.category_id,
                params.sort_mode,
            )

            if not market_page:
                break

            for coin in market_page:
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

                if not coin_id:
                    continue

                if STORAGE.project_exists(
                    coin_id
                ):
                    continue

                try:
                    details = await asyncio.to_thread(
                        COINGECKO.get_details,
                        coin_id,
                    )
                except Exception as error:
                    LOGGER.warning(
                        "CoinGecko details failed for %s: %s",
                        coin_id,
                        error,
                    )
                    continue

                project = await analyze_candidate(
                    coin,
                    details,
                    params.category_name,
                )

                STORAGE.save_project(
                    storage_payload(project)
                )

                if (
                    project[
                        "qualification_status"
                    ] == "qualified"
                ):
                    qualified.append(project)
                else:
                    rejected.append(project)

                await progress.edit(
                    (
                        "⏳ Project Hunter scan running\n\n"
                        f"Page: {page}/"
                        f"{MAX_PAGES_PER_SCAN}\n"
                        f"Inspected: {inspected}\n"
                        f"Qualified: "
                        f"{len(qualified)}/"
                        f"{params.target_count}\n"
                        f"Rejected: {len(rejected)}\n\n"
                        f"Current: "
                        f"{project['name']}"
                    )
                )

                if (
                    len(qualified)
                    >= params.target_count
                ):
                    break

                await asyncio.sleep(1)

            if len(qualified) >= params.target_count:
                break

        STORAGE.finish_scan(
            scan_id,
            inspected=inspected,
            qualified=len(qualified),
            rejected=len(rejected),
            status="completed",
        )

        await progress.edit(
            (
                "✅ Scan completed\n\n"
                f"Inspected: {inspected}\n"
                f"Qualified: {len(qualified)}\n"
                f"Rejected: {len(rejected)}"
            )
        )

        if qualified:
            text = (
                "✅ QUALIFIED PROJECTS\n\n"
                + "\n\n".join(
                    format_project(project)
                    for project in qualified
                )
            )
            await send_long(event, text)

        if rejected:
            text = (
                "❌ REJECTED PROJECTS\n\n"
                + "\n\n".join(
                    format_project(project)
                    for project in rejected
                )
            )
            await send_long(event, text)

        if not qualified and not rejected:
            await event.reply(
                "No new projects were processed."
            )

    except FloodWaitError as error:
        STORAGE.finish_scan(
            scan_id,
            inspected=inspected,
            qualified=len(qualified),
            rejected=len(rejected),
            status="failed",
            error_message=str(error),
        )

        await progress.edit(
            (
                "❌ Telegram rate limit reached\n\n"
                f"Wait: {error.seconds} seconds"
            )
        )

    except Exception as error:
        LOGGER.exception("Project Hunter scan failed")

        STORAGE.finish_scan(
            scan_id,
            inspected=inspected,
            qualified=len(qualified),
            rejected=len(rejected),
            status="failed",
            error_message=str(error),
        )

        await progress.edit(
            (
                "❌ Scan failed\n\n"
                f"{type(error).__name__}: {error}"
            )
        )

    finally:
        active_jobs.discard(chat_id)


# =========================================================
# COMMANDS
# =========================================================

def parse_scan_command(text: str) -> ScanParams:
    parts = text.strip().split()

    target = 20
    category_id = None

    if len(parts) >= 2:
        target = int(parts[1])

    if target < 1 or target > 100:
        raise ValueError(
            "Target must be between 1 and 100."
        )

    if len(parts) >= 3:
        category_id = parts[2]

    return ScanParams(
        target_count=target,
        category_id=category_id,
        category_name=category_id,
    )


def format_saved_rows(
    rows: list[dict[str, Any]],
) -> str:
    if not rows:
        return "No saved projects found."

    blocks: list[str] = []

    for row in rows:
        block = (
            f"Project: {row['name']} "
            f"(${row['symbol']})\n"
            f"Market cap: "
            f"${int(row['market_cap'] or 0):,}\n"
            f"X: {row['x_url']}\n"
            f"TG: {row['telegram_url']}\n"
            f"Status: "
            f"{row['qualification_status']}"
        )

        if row.get("rejection_reason"):
            block += (
                "\nReason: "
                f"{row['rejection_reason']}"
            )

        blocks.append(block)

    return "\n\n".join(blocks)


def authorized(
    event: events.NewMessage.Event,
) -> bool:
    return (
        ALLOWED_CHAT_ID is None
        or event.chat_id == ALLOWED_CHAT_ID
    )


@bot_client.on(
    events.NewMessage(
        pattern=r"^/start(?:@\w+)?$"
    )
)
async def start_handler(
    event: events.NewMessage.Event,
) -> None:
    if not authorized(event):
        await event.reply("This bot is private.")
        return

    await event.reply(
        (
            "Project Hunter Bot\n\n"
            "Commands:\n"
            "/scan\n"
            "/scan 20\n"
            "/scan 20 artificial-intelligence\n"
            "/qualified\n"
            "/rejected\n"
            "/latest\n"
            "/count\n\n"
            "The bot discovers CoinGecko projects, "
            "checks X activity, checks Telegram "
            "community activity, finds the owner "
            "and top active admins, then saves "
            "qualified and rejected results."
        ),
        link_preview=False,
    )


@bot_client.on(
    events.NewMessage(
        pattern=r"^/scan(?:@\w+)?(?:\s+.*)?$"
    )
)
async def scan_handler(
    event: events.NewMessage.Event,
) -> None:
    if not authorized(event):
        await event.reply("This bot is private.")
        return

    try:
        params = parse_scan_command(
            event.raw_text
        )
    except (ValueError, TypeError) as error:
        await event.reply(f"❌ {error}")
        return

    asyncio.create_task(
        run_scan(event, params)
    )


@bot_client.on(
    events.NewMessage(
        pattern=r"^/qualified(?:@\w+)?$"
    )
)
async def qualified_handler(
    event: events.NewMessage.Event,
) -> None:
    if not authorized(event):
        return

    rows = STORAGE.list_projects(
        "qualified",
        20,
    )
    await send_long(
        event,
        "✅ QUALIFIED PROJECTS\n\n"
        + format_saved_rows(rows),
    )


@bot_client.on(
    events.NewMessage(
        pattern=r"^/rejected(?:@\w+)?$"
    )
)
async def rejected_handler(
    event: events.NewMessage.Event,
) -> None:
    if not authorized(event):
        return

    rows = STORAGE.list_projects(
        "rejected",
        20,
    )
    await send_long(
        event,
        "❌ REJECTED PROJECTS\n\n"
        + format_saved_rows(rows),
    )


@bot_client.on(
    events.NewMessage(
        pattern=r"^/latest(?:@\w+)?$"
    )
)
async def latest_handler(
    event: events.NewMessage.Event,
) -> None:
    if not authorized(event):
        return

    rows = STORAGE.list_projects(
        None,
        20,
    )
    await send_long(
        event,
        "LATEST PROJECTS\n\n"
        + format_saved_rows(rows),
    )


@bot_client.on(
    events.NewMessage(
        pattern=r"^/count(?:@\w+)?$"
    )
)
async def count_handler(
    event: events.NewMessage.Event,
) -> None:
    if not authorized(event):
        return

    counts = STORAGE.count_projects()

    await event.reply(
        (
            f"Total: {counts['total']}\n"
            f"Qualified: "
            f"{counts['qualified']}\n"
            f"Rejected: {counts['rejected']}"
        )
    )


# =========================================================
# STARTUP
# =========================================================

async def main() -> None:
    await user_client.start()

    await bot_client.start(
        bot_token=BOT_TOKEN
    )

    me = await bot_client.get_me()

    LOGGER.info(
        "Project Hunter bot connected as @%s",
        me.username,
    )

    await bot_client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("Bot stopped manually.")
