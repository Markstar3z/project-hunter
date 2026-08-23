"""Project Hunter v2: two-stage CoinGecko + X + Telegram research bot."""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from telethon import TelegramClient, events, Button
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
MOBULA_API_KEY = os.getenv("MOBULA_API_KEY", "").strip()
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "").strip()

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/project_hunter.db")
ALLOWED_CHAT_ID_RAW = os.getenv("ALLOWED_CHAT_ID", "").strip()
ALLOWED_CHAT_ID = int(ALLOWED_CHAT_ID_RAW) if ALLOWED_CHAT_ID_RAW else None

MIN_MARKET_CAP = int(os.getenv("MIN_MARKET_CAP", "10000"))
MAX_MARKET_CAP = int(os.getenv("MAX_MARKET_CAP", "1000000000"))

MEME_MIN_MARKET_CAP = int(os.getenv("MEME_MIN_MARKET_CAP", "10000"))
MEME_MAX_MARKET_CAP = int(os.getenv("MEME_MAX_MARKET_CAP", "50000000"))
MEME_MIN_LIQUIDITY = float(os.getenv("MEME_MIN_LIQUIDITY", "5000"))
MEME_MIN_VOLUME_24H = float(os.getenv("MEME_MIN_VOLUME_24H", "5000"))
MEME_DEFAULT_CHAIN = os.getenv("MEME_DEFAULT_CHAIN", "solana").strip().lower()

FAST_SCAN_MAX_INSPECTED = int(os.getenv("FAST_SCAN_MAX_INSPECTED", "250"))
DISCOVERY_OVERFETCH_MULTIPLIER = int(os.getenv("DISCOVERY_OVERFETCH_MULTIPLIER", "5"))
DISCOVERY_BATCH_MAX = int(os.getenv("DISCOVERY_BATCH_MAX", "100"))
MAX_PAGES_PER_FAST_SCAN = int(os.getenv("MAX_PAGES_PER_FAST_SCAN", "3"))
PAGE_SIZE = min(int(os.getenv("PAGE_SIZE", "100")), 100)

MAX_X_INACTIVE_DAYS = int(os.getenv("MAX_X_INACTIVE_DAYS", "30"))
MAX_TG_INACTIVE_DAYS = int(os.getenv("MAX_TG_INACTIVE_DAYS", "30"))
TG_LOOKBACK_DAYS = int(os.getenv("TG_LOOKBACK_DAYS", "7"))
TG_MIN_MESSAGES_7D = int(os.getenv("TG_MIN_MESSAGES_7D", "5"))
TG_MIN_HUMAN_SENDERS_7D = int(os.getenv("TG_MIN_HUMAN_SENDERS_7D", "2"))

MAX_ACTIVE_ADMINS = 3
MESSAGE_LIMIT = 3800
TG_MESSAGE_SCAN_LIMIT = int(os.getenv("TG_MESSAGE_SCAN_LIMIT", "150"))
ENTITY_CACHE_LIMIT_USER = int(os.getenv("ENTITY_CACHE_LIMIT_USER", "200"))
ENTITY_CACHE_LIMIT_BOT = int(os.getenv("ENTITY_CACHE_LIMIT_BOT", "100"))
GC_EVERY_N_PROJECTS = int(os.getenv("GC_EVERY_N_PROJECTS", "10"))

COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"
X_API_BASE = "https://api.x.com/2"
MOBULA_API_BASE = "https://api.mobula.io/api"
BIRDEYE_API_BASE = "https://public-api.birdeye.so"
DEXSCREENER_API_BASE = "https://api.dexscreener.com"


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

# Per-chat temporary menu state. It only stores the current scan choices,
# not project data or API responses.
scan_ui_state: dict[int, dict[str, Any]] = {}


# =========================================================
# HELPERS
# =========================================================

@dataclass
class FastScanParams:
    target_count: int = 50
    asset_type: str = "alt"
    sector: str = "all"
    source: str = "coingecko"
    launchpad: str = "all"
    chain: str = "all"
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    sort_mode: str = "market_cap_desc"


@dataclass
class DiscoveryCandidate:
    unique_id: str
    name: str
    symbol: str
    market_cap: int = 0
    source: str = ""
    asset_type: str = "alt"
    sector: str = "all"
    chain: str = ""
    contract_address: str = ""
    launchpad: str = ""
    website: Optional[str] = None
    x_username: str = ""
    x_url: str = ""
    telegram_url: str = ""
    external_url: str = ""
    liquidity: float = 0.0
    volume_24h: float = 0.0
    pair_created_at: Optional[int] = None
    metadata: Optional[dict[str, Any]] = None

    def to_project(self) -> dict[str, Any]:
        return {
            "coin_id": self.unique_id,
            "name": self.name,
            "symbol": self.symbol,
            "market_cap": int(self.market_cap or 0),
            "category": self.sector or "all",
            "sector": self.sector or "all",
            "source": self.source,
            "sources": self.source,
            "asset_type": self.asset_type,
            "chain": self.chain,
            "contract_address": self.contract_address,
            "launchpad": self.launchpad,
            "website": self.website,
            "x_username": self.x_username,
            "x_url": self.x_url,
            "telegram_url": self.telegram_url,
            "external_url": self.external_url,
            "liquidity": float(self.liquidity or 0),
            "volume_24h": float(self.volume_24h or 0),
            "pair_created_at": self.pair_created_at,
        }


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


async def send_event_message(event: Any, text: str, **kwargs: Any) -> Any:
    """Send a message from either a NewMessage or CallbackQuery event."""
    if isinstance(event, events.CallbackQuery.Event):
        return await event.respond(text, **kwargs)
    return await event.reply(text, **kwargs)


async def send_long(event: Any, text: str) -> None:
    for chunk in split_text(text):
        await send_event_message(
            event,
            chunk,
            link_preview=False,
        )
        await asyncio.sleep(0.4)


def authorized(event: Any) -> bool:
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
        pool_connections=5,
        pool_maxsize=5,
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
# DISCOVERY CONFIGURATION / NORMALIZATION
# =========================================================

ALTCOIN_SECTORS: dict[str, str] = {
    "all": "All Altcoins",
    "ai": "Artificial Intelligence",
    "artificial-intelligence": "Artificial Intelligence",
    "gamefi": "GameFi",
    "gaming": "GameFi",
    "defi": "DeFi",
    "depin": "DePIN",
    "rwa": "Real World Assets",
    "real-world-assets": "Real World Assets",
    "layer1": "Layer 1",
    "layer-1": "Layer 1",
    "layer2": "Layer 2",
    "layer-2": "Layer 2",
    "infrastructure": "Infrastructure",
    "privacy": "Privacy",
    "interoperability": "Interoperability",
    "nft": "NFT",
    "dex": "DEX",
    "stablecoin": "Stablecoin",
}

# Project Hunter labels mapped to CoinGecko category IDs.
COINGECKO_SECTOR_IDS: dict[str, str] = {
    "ai": "artificial-intelligence",
    "artificial-intelligence": "artificial-intelligence",
    "gamefi": "gaming",
    "gaming": "gaming",
    "defi": "decentralized-finance-defi",
    "depin": "depin",
    "rwa": "real-world-assets-rwa",
    "real-world-assets": "real-world-assets-rwa",
    "layer1": "layer-1",
    "layer-1": "layer-1",
    "layer2": "layer-2",
    "layer-2": "layer-2",
    "privacy": "privacy-coins",
    "interoperability": "interoperability",
    "nft": "non-fungible-tokens-nft",
    "dex": "decentralized-exchange",
    "stablecoin": "stablecoins",
}

ALT_SOURCES = {"all", "coingecko", "mobula", "dex", "dexscreener"}
MEME_SOURCES = {"all", "birdeye", "mobula", "dex", "dexscreener"}
MEME_LAUNCHPADS = {
    "all": "all",
    "pumpfun": "pump_dot_fun",
    "pump.fun": "pump_dot_fun",
    "pump_dot_fun": "pump_dot_fun",
    "fourmeme": "four.meme",
    "four.meme": "four.meme",
    "moonshot": "moonshot",
    "raydium": "raydium_launchlab",
    "raydium-launchlab": "raydium_launchlab",
    "meteora": "meteora_dynamic_bonding_curve",
    "nadfun": "nad.fun",
    "nad.fun": "nad.fun",
}

MOBULA_POOL_TYPES = {
    "pumpfun": "pumpfun",
    "pump.fun": "pumpfun",
    "pump_dot_fun": "pumpfun",
    "moonshot": "moonshot",
}

CHAIN_ALIASES = {
    "sol": "solana",
    "solana": "solana",
    "solana:solana": "solana",
    "bsc": "bsc",
    "bnb": "bsc",
    "bnb-chain": "bsc",
    "binance-smart-chain": "bsc",
    "evm:56": "bsc",
    "eth": "ethereum",
    "ethereum": "ethereum",
    "evm:1": "ethereum",
    "base": "base",
    "evm:8453": "base",
    "monad": "monad",
}

MOBULA_CHAIN_IDS = {
    "solana": "solana:solana",
    "ethereum": "evm:1",
    "bsc": "evm:56",
    "base": "evm:8453",
}

def clean_source_name(value: str) -> str:
    value = (value or "").strip().lower()
    return "dex" if value == "dexscreener" else value


def normalize_chain(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("_", "-")
    return CHAIN_ALIASES.get(text, text)


def first_number(data: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def first_text(data: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_dicts(child)


def extract_items(payload: Any) -> list[dict[str, Any]]:
    """Tolerate provider response wrappers without depending on one schema revision."""
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    for key in ("items", "list", "tokens", "results", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = extract_items(value)
            if nested:
                return nested

    # Mobula Pulse nests token rows inside payload/views.
    candidates: list[dict[str, Any]] = []
    seen: set[int] = set()
    for item in walk_dicts(payload):
        marker = id(item)
        if marker in seen:
            continue
        seen.add(marker)
        if (
            ("address" in item or "contract" in item or "tokenAddress" in item)
            and ("name" in item or "symbol" in item)
        ):
            candidates.append(item)
    return candidates


def extract_socials(value: Any) -> dict[str, str]:
    result = {
        "website": "",
        "x_username": "",
        "x_url": "",
        "telegram_url": "",
    }

    url_pattern = re.compile(r"https?://[^\s\"'<>]+", re.I)

    def consume_url(url: str, label: str = "") -> None:
        if not url:
            return
        url = url.strip().rstrip(".,)")
        lowered = url.lower()
        label = label.lower()

        if "t.me/" in lowered or "telegram.me/" in lowered:
            if not result["telegram_url"]:
                result["telegram_url"] = url
            return

        if "x.com/" in lowered or "twitter.com/" in lowered:
            if not result["x_url"]:
                result["x_url"] = url
            username = url.rstrip("/").split("/")[-1].split("?")[0].lstrip("@")
            if username and username not in {"home", "share", "intent"}:
                result["x_username"] = username
            return

        if label in {"twitter", "x"} and not url.startswith("http"):
            username = url.lstrip("@")
            result["x_username"] = username
            result["x_url"] = f"https://x.com/{username}"
            return

        if label in {"telegram", "tg"} and not url.startswith("http"):
            result["telegram_url"] = f"https://t.me/{url.lstrip('@')}"
            return

        if not result["website"]:
            result["website"] = url

    for item in walk_dicts(value):
        platform = first_text(item, "platform", "type", "label", "name")
        direct_url = first_text(item, "url", "link", "value", "website")
        if direct_url:
            consume_url(direct_url, platform)

        handle = first_text(item, "handle", "username")
        if handle and platform.lower() in {"twitter", "x"}:
            consume_url(handle, platform)
        elif handle and platform.lower() in {"telegram", "tg"}:
            consume_url(handle, platform)

        for raw in item.values():
            if isinstance(raw, str):
                for url in url_pattern.findall(raw):
                    consume_url(url)

    return result



def normalize_x_username(value: Any) -> str:
    text = str(value or "").strip().lower()

    if not text:
        return ""

    text = re.sub(r"^https?://(www\.)?(x\.com|twitter\.com)/", "", text, flags=re.I)
    text = text.split("?")[0].split("#")[0].strip("/")
    text = text.lstrip("@")

    if "/" in text:
        text = text.split("/")[0]

    if text in {"home", "share", "intent", "search"}:
        return ""

    return text


def normalize_telegram_url(value: Any) -> str:
    text = str(value or "").strip().lower()

    if not text:
        return ""

    text = text.replace("http://", "https://")
    text = re.sub(
        r"^https://(www\.)?(telegram\.me|telegram\.org|t\.me)/",
        "https://t.me/",
        text,
        flags=re.I,
    )
    text = text.split("?")[0].split("#")[0].rstrip("/")

    if not text.startswith("http"):
        text = f"https://t.me/{text.lstrip('@').lstrip('/')}"

    return text


def normalize_website(value: Any) -> str:
    text = str(value or "").strip().lower()

    if not text:
        return ""

    text = re.sub(r"^https?://", "", text, flags=re.I)
    text = re.sub(r"^www\.", "", text, flags=re.I)
    text = text.split("#")[0].split("?")[0].rstrip("/")

    return text


def project_identity_keys(project: dict[str, Any]) -> set[str]:
    keys: set[str] = set()

    chain = normalize_chain(project.get("chain"))
    contract = str(project.get("contract_address") or "").strip().lower()

    if chain and contract:
        keys.add(f"contract:{chain}:{contract}")

    x_username = normalize_x_username(
        project.get("x_username") or project.get("x_url")
    )
    if x_username:
        keys.add(f"x:{x_username}")

    telegram = normalize_telegram_url(project.get("telegram_url"))
    if telegram:
        keys.add(f"tg:{telegram}")

    website = normalize_website(project.get("website"))
    if website:
        keys.add(f"web:{website}")

    coin_id = str(project.get("coin_id") or "").strip().lower()
    if coin_id:
        keys.add(f"id:{coin_id}")

    return keys


def candidates_match(
    left: DiscoveryCandidate,
    right: DiscoveryCandidate,
) -> bool:
    left_project = left.to_project()
    right_project = right.to_project()

    return bool(
        project_identity_keys(left_project)
        & project_identity_keys(right_project)
    )


def candidate_unique_id(
    source: str,
    chain: str,
    contract_address: str,
    fallback: str,
) -> str:
    chain = normalize_chain(chain)
    contract_address = (contract_address or "").strip()
    if chain and contract_address:
        return f"{chain}:{contract_address.lower()}"
    return f"{source}:{fallback.strip().lower()}"


def merge_candidate(primary: DiscoveryCandidate, incoming: DiscoveryCandidate) -> DiscoveryCandidate:
    """Merge provider observations for the same contract without losing richer fields."""
    source_set = {
        part.strip()
        for part in (primary.source + "," + incoming.source).split(",")
        if part.strip()
    }
    primary.source = ",".join(sorted(source_set))

    for attr in (
        "name", "symbol", "chain", "contract_address", "launchpad",
        "website", "x_username", "x_url", "telegram_url", "external_url",
    ):
        if not getattr(primary, attr) and getattr(incoming, attr):
            setattr(primary, attr, getattr(incoming, attr))

    primary.market_cap = max(primary.market_cap, incoming.market_cap)
    primary.liquidity = max(primary.liquidity, incoming.liquidity)
    primary.volume_24h = max(primary.volume_24h, incoming.volume_24h)

    if not primary.pair_created_at and incoming.pair_created_at:
        primary.pair_created_at = incoming.pair_created_at

    return primary


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
                    sector TEXT,
                    source TEXT,
                    sources TEXT,
                    asset_type TEXT DEFAULT 'alt',
                    chain TEXT,
                    contract_address TEXT,
                    launchpad TEXT,
                    liquidity REAL DEFAULT 0,
                    volume_24h REAL DEFAULT 0,
                    external_url TEXT,
                    pair_created_at INTEGER,
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
            "sector": "TEXT",
            "source": "TEXT",
            "sources": "TEXT",
            "asset_type": "TEXT DEFAULT 'alt'",
            "chain": "TEXT",
            "contract_address": "TEXT",
            "launchpad": "TEXT",
            "liquidity": "REAL DEFAULT 0",
            "volume_24h": "REAL DEFAULT 0",
            "external_url": "TEXT",
            "pair_created_at": "INTEGER",
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

            connection.execute(
                """
                UPDATE projects
                SET asset_type = COALESCE(NULLIF(asset_type, ''), 'alt'),
                    sector = COALESCE(NULLIF(sector, ''), category, 'all'),
                    source = COALESCE(NULLIF(source, ''), 'coingecko'),
                    sources = COALESCE(NULLIF(sources, ''), source, 'coingecko'),
                    liquidity = COALESCE(liquidity, 0),
                    volume_24h = COALESCE(volume_24h, 0)
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_projects_contract
                ON projects(chain, contract_address)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_projects_type_sector
                ON projects(asset_type, sector)
                """
            )

            connection.commit()

    def exists(self, coin_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM projects WHERE coin_id = ? LIMIT 1",
                (coin_id,),
            ).fetchone()
        return row is not None

    def find_by_contract(
        self,
        chain: str,
        contract_address: str,
    ) -> Optional[dict[str, Any]]:
        if not chain or not contract_address:
            return None

        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM projects
                WHERE LOWER(chain) = LOWER(?)
                  AND LOWER(contract_address) = LOWER(?)
                LIMIT 1
                """,
                (chain, contract_address),
            ).fetchone()

        return dict(row) if row else None

    def find_duplicate(
        self,
        project: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        """
        Match the same project across providers using the strongest available
        identifiers. Contract match wins, then X, Telegram, website, then coin_id.
        """

        chain = normalize_chain(project.get("chain"))
        contract = str(project.get("contract_address") or "").strip().lower()
        x_username = normalize_x_username(
            project.get("x_username") or project.get("x_url")
        )
        telegram_url = normalize_telegram_url(project.get("telegram_url"))
        website = normalize_website(project.get("website"))
        coin_id = str(project.get("coin_id") or "").strip().lower()

        with self.connect() as connection:
            if chain and contract:
                row = connection.execute(
                    """
                    SELECT *
                    FROM projects
                    WHERE LOWER(chain) = LOWER(?)
                      AND LOWER(contract_address) = LOWER(?)
                    LIMIT 1
                    """,
                    (chain, contract),
                ).fetchone()
                if row:
                    return dict(row)

            if x_username:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM projects
                    WHERE COALESCE(x_username, '') != ''
                       OR COALESCE(x_url, '') != ''
                    """
                ).fetchall()

                for row in rows:
                    existing = dict(row)
                    existing_x = normalize_x_username(
                        existing.get("x_username") or existing.get("x_url")
                    )
                    if existing_x and existing_x == x_username:
                        return existing

            if telegram_url:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM projects
                    WHERE COALESCE(telegram_url, '') != ''
                    """
                ).fetchall()

                for row in rows:
                    existing = dict(row)
                    existing_tg = normalize_telegram_url(
                        existing.get("telegram_url")
                    )
                    if existing_tg and existing_tg == telegram_url:
                        return existing

            if website:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM projects
                    WHERE COALESCE(website, '') != ''
                    """
                ).fetchall()

                for row in rows:
                    existing = dict(row)
                    existing_web = normalize_website(existing.get("website"))
                    if existing_web and existing_web == website:
                        return existing

            if coin_id:
                row = connection.execute(
                    """
                    SELECT *
                    FROM projects
                    WHERE LOWER(coin_id) = LOWER(?)
                    LIMIT 1
                    """,
                    (coin_id,),
                ).fetchone()
                if row:
                    return dict(row)

        return None

    def candidate_exists(self, project: dict[str, Any]) -> bool:
        return self.find_duplicate(project) is not None

    def save_pending(self, project: dict[str, Any]) -> bool:
        """Insert a new candidate or merge another provider into an existing contract."""
        existing = self.find_duplicate(project)

        if existing:
            existing_sources = {
                item.strip()
                for item in str(existing.get("sources") or existing.get("source") or "").split(",")
                if item.strip()
            }
            incoming_sources = {
                item.strip()
                for item in str(project.get("sources") or project.get("source") or "").split(",")
                if item.strip()
            }
            merged_sources = ",".join(sorted(existing_sources | incoming_sources))

            with self.connect() as connection:
                connection.execute(
                    """
                    UPDATE projects
                    SET sources = ?,
                        source = COALESCE(NULLIF(source, ''), ?),
                        chain = COALESCE(NULLIF(chain, ''), ?),
                        contract_address = COALESCE(NULLIF(contract_address, ''), ?),
                        website = COALESCE(NULLIF(website, ''), ?),
                        x_username = COALESCE(NULLIF(x_username, ''), ?),
                        x_url = COALESCE(NULLIF(x_url, ''), ?),
                        telegram_url = COALESCE(NULLIF(telegram_url, ''), ?),
                        market_cap = MAX(COALESCE(market_cap, 0), ?),
                        liquidity = MAX(COALESCE(liquidity, 0), ?),
                        volume_24h = MAX(COALESCE(volume_24h, 0), ?),
                        launchpad = COALESCE(NULLIF(launchpad, ''), ?),
                        external_url = COALESCE(NULLIF(external_url, ''), ?)
                    WHERE id = ?
                    """,
                    (
                        merged_sources,
                        project.get("source"),
                        normalize_chain(project.get("chain")),
                        str(project.get("contract_address") or "").strip(),
                        project.get("website"),
                        project.get("x_username"),
                        project.get("x_url"),
                        project.get("telegram_url"),
                        int(project.get("market_cap") or 0),
                        float(project.get("liquidity") or 0),
                        float(project.get("volume_24h") or 0),
                        project.get("launchpad"),
                        project.get("external_url"),
                        existing["id"],
                    ),
                )
                connection.commit()
            return False

        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO projects (
                    coin_id,
                    name,
                    symbol,
                    market_cap,
                    category,
                    sector,
                    source,
                    sources,
                    asset_type,
                    chain,
                    contract_address,
                    launchpad,
                    liquidity,
                    volume_24h,
                    external_url,
                    pair_created_at,
                    website,
                    x_username,
                    x_url,
                    telegram_url,
                    stage,
                    discovered_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    'pending', ?
                )
                ON CONFLICT(coin_id) DO NOTHING
                """,
                (
                    project["coin_id"],
                    project["name"],
                    project["symbol"],
                    int(project.get("market_cap") or 0),
                    project.get("category") or "all",
                    project.get("sector") or project.get("category") or "all",
                    project.get("source") or "",
                    project.get("sources") or project.get("source") or "",
                    project.get("asset_type") or "alt",
                    normalize_chain(project.get("chain")),
                    str(project.get("contract_address") or "").strip(),
                    project.get("launchpad") or "",
                    float(project.get("liquidity") or 0),
                    float(project.get("volume_24h") or 0),
                    project.get("external_url") or "",
                    project.get("pair_created_at"),
                    project.get("website"),
                    project.get("x_username") or "",
                    project.get("x_url") or "",
                    project.get("telegram_url") or "",
                    utc_now().isoformat(),
                ),
            )
            connection.commit()
            return cursor.rowcount > 0

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
# MOBULA / BIRDEYE / DEX SCREENER DISCOVERY
# =========================================================

class MobulaClient:
    @property
    def headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if MOBULA_API_KEY:
            headers["Authorization"] = MOBULA_API_KEY
        return headers

    def trendings(self, blockchain: Optional[str] = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if blockchain and blockchain != "all":
            params["blockchain"] = blockchain

        response = HTTP.get(
            f"{MOBULA_API_BASE}/1/metadata/trendings",
            params=params,
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else extract_items(data)

    def pulse(
        self,
        *,
        chain: str = "solana",
        launchpad: str = "all",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        params: list[tuple[str, Any]] = [
            ("assetMode", "true"),
            ("limit", max(1, min(limit, 100))),
            ("excludeDuplicates", "true"),
        ]

        mobula_chain = MOBULA_CHAIN_IDS.get(normalize_chain(chain))
        if mobula_chain:
            params.append(("chainId", mobula_chain))

        pool_type = MOBULA_POOL_TYPES.get(launchpad.lower())
        if pool_type:
            params.append(("poolTypes", pool_type))

        response = HTTP.get(
            f"{MOBULA_API_BASE}/2/pulse",
            params=params,
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()
        return extract_items(response.json())


class BirdeyeClient:
    @property
    def headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "x-chain": MEME_DEFAULT_CHAIN,
        }
        if BIRDEYE_API_KEY:
            headers["X-API-KEY"] = BIRDEYE_API_KEY
        return headers

    def meme_list(
        self,
        *,
        chain: str,
        launchpad: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        headers = dict(self.headers)
        headers["x-chain"] = normalize_chain(chain)

        params: dict[str, Any] = {
            "sort_by": "volume_24h_usd",
            "sort_type": "desc",
            "limit": max(1, min(limit, 100)),
            "offset": 0,
            "min_market_cap": MEME_MIN_MARKET_CAP,
            "max_market_cap": MEME_MAX_MARKET_CAP,
            "min_liquidity": MEME_MIN_LIQUIDITY,
            "min_volume_24h_usd": MEME_MIN_VOLUME_24H,
        }

        source_name = MEME_LAUNCHPADS.get(launchpad.lower(), launchpad.lower())
        if source_name and source_name != "all":
            params["source"] = source_name

        response = HTTP.get(
            f"{BIRDEYE_API_BASE}/defi/v3/token/meme/list",
            params=params,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        return extract_items(response.json())

    def meme_detail(self, address: str, chain: str) -> dict[str, Any]:
        headers = dict(self.headers)
        headers["x-chain"] = normalize_chain(chain)

        response = HTTP.get(
            f"{BIRDEYE_API_BASE}/defi/v3/token/meme/detail/single",
            params={"address": address},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            inner = data.get("data")
            return inner if isinstance(inner, dict) else data
        return {}


class DexScreenerClient:
    def latest_profiles(self) -> list[dict[str, Any]]:
        response = HTTP.get(
            f"{DEXSCREENER_API_BASE}/token-profiles/latest/v1",
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []

    def token_pairs(self, chain: str, address: str) -> list[dict[str, Any]]:
        response = HTTP.get(
            f"{DEXSCREENER_API_BASE}/token-pairs/v1/{chain}/{address}",
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []

    def best_pair(self, chain: str, address: str) -> Optional[dict[str, Any]]:
        pairs = self.token_pairs(chain, address)
        if not pairs:
            return None

        return max(
            pairs,
            key=lambda pair: first_number(
                pair.get("liquidity") or {},
                "usd",
            ),
        )


MOBULA = MobulaClient()
BIRDEYE = BirdeyeClient()
DEX = DexScreenerClient()


def candidate_from_dex_pair(
    pair: dict[str, Any],
    *,
    asset_type: str,
    sector: str,
    source: str = "dex",
    launchpad: str = "",
) -> Optional[DiscoveryCandidate]:
    base = pair.get("baseToken") or {}
    address = first_text(base, "address")
    name = first_text(base, "name")
    symbol = first_text(base, "symbol").upper()
    chain = normalize_chain(first_text(pair, "chainId"))

    if not address or not name or not symbol or not chain:
        return None

    info = pair.get("info") or {}
    socials = extract_socials(info)

    market_cap = int(first_number(pair, "marketCap", "fdv"))
    liquidity = first_number(pair.get("liquidity") or {}, "usd")
    volume_24h = first_number(pair.get("volume") or {}, "h24", "24h")

    return DiscoveryCandidate(
        unique_id=candidate_unique_id(source, chain, address, name),
        name=name,
        symbol=symbol,
        market_cap=market_cap,
        source=source,
        asset_type=asset_type,
        sector=sector,
        chain=normalize_chain(chain),
        contract_address=address.strip() if address else "",
        launchpad=launchpad or first_text(pair, "dexId"),
        website=socials["website"] or None,
        x_username=socials["x_username"],
        x_url=socials["x_url"],
        telegram_url=socials["telegram_url"],
        external_url=first_text(pair, "url"),
        liquidity=liquidity,
        volume_24h=volume_24h,
        pair_created_at=int(first_number(pair, "pairCreatedAt")) or None,
        metadata={"provider": "dex"},
    )


def enrich_with_dex(candidate: DiscoveryCandidate) -> DiscoveryCandidate:
    if not candidate.chain or not candidate.contract_address:
        return candidate

    try:
        pair = DEX.best_pair(candidate.chain, candidate.contract_address)
    except Exception as error:
        LOGGER.warning(
            "DEX Screener enrichment failed for %s:%s: %s",
            candidate.chain,
            candidate.contract_address,
            error,
        )
        return candidate

    if not pair:
        return candidate

    dex_candidate = candidate_from_dex_pair(
        pair,
        asset_type=candidate.asset_type,
        sector=candidate.sector,
        launchpad=candidate.launchpad,
    )

    if dex_candidate:
        candidate = merge_candidate(candidate, dex_candidate)

    return candidate


def candidate_from_mobula(
    item: dict[str, Any],
    *,
    asset_type: str,
    sector: str,
    launchpad: str = "",
) -> Optional[DiscoveryCandidate]:
    token = item.get("token") if isinstance(item.get("token"), dict) else item

    address = first_text(
        token,
        "address",
        "contract",
        "contractAddress",
        "tokenAddress",
    )
    name = first_text(token, "name", "tokenName")
    symbol = first_text(token, "symbol", "tokenSymbol").upper()
    chain = normalize_chain(
        first_text(token, "blockchain", "chain", "chainId")
    )

    # Trendings use a contracts array.
    contracts = token.get("contracts")
    if isinstance(contracts, list) and contracts:
        contract = contracts[0] if isinstance(contracts[0], dict) else {}
        address = address or first_text(contract, "address")
        chain = chain or normalize_chain(first_text(contract, "blockchain", "chain"))

    if ":" in chain:
        if chain == "solana:solana":
            chain = "solana"
        elif chain.startswith("evm:"):
            evm_map = {"evm:1": "ethereum", "evm:56": "bsc", "evm:8453": "base"}
            chain = evm_map.get(chain, chain)

    if not name or not symbol:
        return None

    socials = extract_socials(item)

    market_cap = int(first_number(
        token,
        "marketCap", "market_cap", "market_cap_usd", "fdv",
    ))
    liquidity = first_number(
        token,
        "liquidity", "liquidityUSD", "liquidity_usd", "liquidityMax",
    )
    volume_24h = first_number(
        token,
        "volume24h", "volume_24h", "volume24hUSD", "volume_24h_usd",
    )
    detected_launchpad = (
        first_text(token, "source", "poolType", "pool_type")
        or launchpad
    )

    fallback = address or f"{name}-{symbol}"
    return DiscoveryCandidate(
        unique_id=candidate_unique_id("mobula", chain, address, fallback),
        name=name,
        symbol=symbol,
        market_cap=market_cap,
        source="mobula",
        asset_type=asset_type,
        sector=sector,
        chain=normalize_chain(chain),
        contract_address=address.strip(),
        launchpad=detected_launchpad,
        website=socials["website"] or None,
        x_username=socials["x_username"],
        x_url=socials["x_url"],
        telegram_url=socials["telegram_url"],
        liquidity=liquidity,
        volume_24h=volume_24h,
        metadata={"provider": "mobula"},
    )


def candidate_from_birdeye(
    item: dict[str, Any],
    *,
    chain: str,
    launchpad: str,
) -> Optional[DiscoveryCandidate]:
    address = first_text(
        item,
        "address", "token_address", "tokenAddress", "mint",
    )
    name = first_text(item, "name", "token_name", "tokenName")
    symbol = first_text(item, "symbol", "token_symbol", "tokenSymbol").upper()

    if not address or not name or not symbol:
        return None

    socials = extract_socials(item)

    return DiscoveryCandidate(
        unique_id=candidate_unique_id("birdeye", chain, address, name),
        name=name,
        symbol=symbol,
        market_cap=int(first_number(
            item,
            "market_cap", "marketCap", "marketcap", "fdv",
        )),
        source="birdeye",
        asset_type="meme",
        sector="memecoin",
        chain=normalize_chain(chain),
        contract_address=address.strip(),
        launchpad=first_text(item, "source", "launchpad") or launchpad,
        website=socials["website"] or None,
        x_username=socials["x_username"],
        x_url=socials["x_url"],
        telegram_url=socials["telegram_url"],
        liquidity=first_number(item, "liquidity", "liquidity_usd"),
        volume_24h=first_number(
            item,
            "volume_24h_usd", "volume24h", "volume_24h",
        ),
        metadata={"provider": "birdeye"},
    )


async def discover_coingecko(params: FastScanParams) -> list[DiscoveryCandidate]:
    candidates: list[DiscoveryCandidate] = []
    inspected = 0

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

            market_cap = int(coin.get("market_cap") or 0)
            if market_cap < MIN_MARKET_CAP or market_cap > MAX_MARKET_CAP:
                continue

            coin_id = coin.get("id")
            if not coin_id or STORAGE.exists(str(coin_id)):
                continue

            try:
                details = await asyncio.to_thread(COINGECKO.details, coin_id)
            except Exception as error:
                LOGGER.warning("CoinGecko details failed for %s: %s", coin_id, error)
                continue

            links = details.get("links") or {}
            x_username = str(links.get("twitter_screen_name") or "").strip().lstrip("@")
            telegram_url = COINGECKO.telegram_url(
                links.get("telegram_channel_identifier")
            )
            website = COINGECKO.website_url(links.get("homepage"))

            platforms = details.get("platforms") or {}
            chain = ""
            address = ""
            if isinstance(platforms, dict):
                for raw_chain, raw_address in platforms.items():
                    if raw_address:
                        chain = normalize_chain(raw_chain)
                        address = str(raw_address).strip()
                        break

            candidate = DiscoveryCandidate(
                unique_id=str(coin_id),
                name=str(coin.get("name") or ""),
                symbol=str(coin.get("symbol") or "").upper(),
                market_cap=market_cap,
                source="coingecko",
                asset_type="alt",
                sector=params.sector,
                chain=chain,
                contract_address=address,
                website=website,
                x_username=x_username,
                x_url=f"https://x.com/{x_username}" if x_username else "",
                telegram_url=telegram_url or "",
                external_url=f"https://www.coingecko.com/en/coins/{coin_id}",
                metadata={"provider": "coingecko"},
            )

            if address and chain:
                candidate = await asyncio.to_thread(enrich_with_dex, candidate)

            candidates.append(candidate)
            if len(candidates) >= min(
                params.target_count * DISCOVERY_OVERFETCH_MULTIPLIER,
                FAST_SCAN_MAX_INSPECTED,
            ):
                return candidates

        if inspected >= FAST_SCAN_MAX_INSPECTED:
            break

    return candidates


async def discover_mobula(params: FastScanParams) -> list[DiscoveryCandidate]:
    if not MOBULA_API_KEY:
        LOGGER.warning("Mobula source requested but MOBULA_API_KEY is missing.")
        return []

    if params.asset_type == "meme":
        chain = MEME_DEFAULT_CHAIN if params.chain == "all" else params.chain
        items = await asyncio.to_thread(
            MOBULA.pulse,
            chain=chain,
            launchpad=params.launchpad,
            limit=min(max(params.target_count * DISCOVERY_OVERFETCH_MULTIPLIER, 30), DISCOVERY_BATCH_MAX),
        )
        asset_type = "meme"
        sector = "memecoin"
    else:
        blockchain = None if params.chain == "all" else params.chain
        items = await asyncio.to_thread(MOBULA.trendings, blockchain)
        asset_type = "alt"
        sector = params.sector

    candidates: list[DiscoveryCandidate] = []

    for item in items:
        candidate = candidate_from_mobula(
            item,
            asset_type=asset_type,
            sector=sector,
            launchpad=params.launchpad,
        )
        if not candidate:
            continue

        candidate = await asyncio.to_thread(enrich_with_dex, candidate)

        if asset_type == "meme":
            if candidate.market_cap and not (
                MEME_MIN_MARKET_CAP <= candidate.market_cap <= MEME_MAX_MARKET_CAP
            ):
                continue
            if candidate.liquidity and candidate.liquidity < MEME_MIN_LIQUIDITY:
                continue

        candidates.append(candidate)
        if len(candidates) >= min(
            params.target_count * DISCOVERY_OVERFETCH_MULTIPLIER,
            FAST_SCAN_MAX_INSPECTED,
        ):
            break

    return candidates


async def discover_birdeye(params: FastScanParams) -> list[DiscoveryCandidate]:
    if not BIRDEYE_API_KEY:
        LOGGER.warning("Birdeye source requested but BIRDEYE_API_KEY is missing.")
        return []

    chain = MEME_DEFAULT_CHAIN if params.chain == "all" else params.chain

    if chain not in {"solana", "bsc", "monad"}:
        LOGGER.warning("Birdeye meme list does not support chain=%s", chain)
        return []

    items = await asyncio.to_thread(
        BIRDEYE.meme_list,
        chain=chain,
        launchpad=params.launchpad,
        limit=min(max(params.target_count * DISCOVERY_OVERFETCH_MULTIPLIER, 30), DISCOVERY_BATCH_MAX),
    )

    candidates: list[DiscoveryCandidate] = []

    for item in items:
        candidate = candidate_from_birdeye(
            item,
            chain=chain,
            launchpad=params.launchpad,
        )
        if not candidate:
            continue

        # The list is intentionally cheap; detail is fetched only when socials
        # are missing and the candidate survives the market filters.
        if not candidate.x_username or not candidate.telegram_url:
            try:
                detail = await asyncio.to_thread(
                    BIRDEYE.meme_detail,
                    candidate.contract_address,
                    chain,
                )
                richer = candidate_from_birdeye(
                    detail,
                    chain=chain,
                    launchpad=candidate.launchpad or params.launchpad,
                )
                if richer:
                    candidate = merge_candidate(candidate, richer)
            except Exception as error:
                LOGGER.warning(
                    "Birdeye detail failed for %s: %s",
                    candidate.contract_address,
                    error,
                )

        candidate = await asyncio.to_thread(enrich_with_dex, candidate)

        candidates.append(candidate)
        if len(candidates) >= min(
            params.target_count * DISCOVERY_OVERFETCH_MULTIPLIER,
            FAST_SCAN_MAX_INSPECTED,
        ):
            break

    return candidates


async def discover_dex(params: FastScanParams) -> list[DiscoveryCandidate]:
    profiles = await asyncio.to_thread(DEX.latest_profiles)
    candidates: list[DiscoveryCandidate] = []

    for profile in profiles:
        chain = normalize_chain(first_text(profile, "chainId"))
        address = first_text(profile, "tokenAddress")

        if not chain or not address:
            continue

        if params.chain != "all" and chain != normalize_chain(params.chain):
            continue

        try:
            pair = await asyncio.to_thread(DEX.best_pair, chain, address)
        except Exception as error:
            LOGGER.warning("DEX pair lookup failed for %s:%s: %s", chain, address, error)
            continue

        if not pair:
            continue

        candidate = candidate_from_dex_pair(
            pair,
            asset_type=params.asset_type,
            sector="memecoin" if params.asset_type == "meme" else params.sector,
        )
        if not candidate:
            continue

        if params.asset_type == "meme":
            if candidate.market_cap and not (
                MEME_MIN_MARKET_CAP <= candidate.market_cap <= MEME_MAX_MARKET_CAP
            ):
                continue
            if candidate.liquidity < MEME_MIN_LIQUIDITY:
                continue
            if candidate.volume_24h < MEME_MIN_VOLUME_24H:
                continue

        candidates.append(candidate)
        if len(candidates) >= min(
            params.target_count * DISCOVERY_OVERFETCH_MULTIPLIER,
            FAST_SCAN_MAX_INSPECTED,
        ):
            break

    return candidates


async def discover_candidates(
    params: FastScanParams,
    progress_callback: Optional[Any] = None,
) -> tuple[list[DiscoveryCandidate], list[str]]:
    """Run selected providers, normalize results, and deduplicate by chain+contract."""
    requested = clean_source_name(params.source)
    warnings: list[str] = []
    providers = []

    if params.asset_type == "alt":
        if params.sector != "all":
            if requested == "all":
                # CoinGecko provides the explicit category taxonomy. DEX still
                # enriches the resulting CoinGecko contracts inside the provider.
                available = ["coingecko"]
                warnings.append(
                    "Sector-specific altcoin scans use CoinGecko for discovery "
                    "because Mobula/Dex do not expose the same category taxonomy."
                )
            elif requested != "coingecko":
                available = []
                warnings.append(
                    f"{requested} cannot guarantee the requested altcoin sector. "
                    "Use source=coingecko or source=all for sector scans."
                )
            else:
                available = ["coingecko"]
        else:
            available = ["coingecko", "mobula", "dex"] if requested == "all" else [requested]
    else:
        if params.launchpad != "all":
            if requested == "all":
                available = ["birdeye"]
                if params.launchpad in MOBULA_POOL_TYPES:
                    available.append("mobula")
            elif requested == "dex":
                available = []
                warnings.append(
                    "DEX Screener cannot reliably identify the original meme "
                    "launchpad. Use Birdeye or Mobula for launchpad-specific scans."
                )
            elif requested == "mobula" and params.launchpad not in MOBULA_POOL_TYPES:
                available = []
                warnings.append(
                    f"Mobula launchpad mapping is not configured for {params.launchpad}. "
                    "Use Birdeye for this launchpad."
                )
            else:
                available = [requested]
        else:
            available = ["birdeye", "mobula", "dex"] if requested == "all" else [requested]

    for source in available:
        if source == "coingecko" and params.asset_type == "alt":
            providers.append(("coingecko", discover_coingecko(params)))
        elif source == "mobula":
            providers.append(("mobula", discover_mobula(params)))
        elif source == "birdeye" and params.asset_type == "meme":
            providers.append(("birdeye", discover_birdeye(params)))
        elif source == "dex":
            providers.append(("dex", discover_dex(params)))
        else:
            warnings.append(f"Source '{source}' is not valid for {params.asset_type} scans.")

    merged: dict[str, DiscoveryCandidate] = {}

    total_providers = len(providers)

    for provider_number, (source_name, coroutine) in enumerate(providers, start=1):
        if progress_callback:
            await progress_callback(
                stage="discovering",
                provider=source_name,
                provider_number=provider_number,
                provider_total=total_providers,
                discovered=len(merged),
            )

        try:
            rows = await coroutine
        except Exception as error:
            LOGGER.exception("%s discovery failed", source_name)
            warnings.append(f"{source_name}: {type(error).__name__}: {error}")
            if progress_callback:
                await progress_callback(
                    stage="provider_failed",
                    provider=source_name,
                    provider_number=provider_number,
                    provider_total=total_providers,
                    discovered=len(merged),
                )
            continue

        for candidate in rows:
            matched_key: Optional[str] = None

            # First try exact normalized contract identity.
            if candidate.chain and candidate.contract_address:
                contract_key = (
                    f"contract:{normalize_chain(candidate.chain)}:"
                    f"{candidate.contract_address.strip().lower()}"
                )
                if contract_key in merged:
                    matched_key = contract_key

            # Fallback to social/website identities when provider contract
            # metadata is missing or represented differently.
            if matched_key is None:
                candidate_keys = project_identity_keys(candidate.to_project())

                for existing_key, existing_candidate in merged.items():
                    existing_keys = project_identity_keys(
                        existing_candidate.to_project()
                    )
                    if candidate_keys & existing_keys:
                        matched_key = existing_key
                        break

            if matched_key is not None:
                merged[matched_key] = merge_candidate(
                    merged[matched_key],
                    candidate,
                )
                continue

            if candidate.chain and candidate.contract_address:
                new_key = (
                    f"contract:{normalize_chain(candidate.chain)}:"
                    f"{candidate.contract_address.strip().lower()}"
                )
            else:
                new_key = candidate.unique_id

            merged[new_key] = candidate

        if progress_callback:
            await progress_callback(
                stage="provider_complete",
                provider=source_name,
                provider_number=provider_number,
                provider_total=total_providers,
                discovered=len(merged),
                provider_found=len(rows),
            )

    # Favor candidates that already have actionable contact/social data.
    ordered = sorted(
        merged.values(),
        key=lambda c: (
            bool(c.x_username and c.telegram_url),
            c.volume_24h,
            c.liquidity,
            c.market_cap,
        ),
        reverse=True,
    )

    discovery_limit = min(
        max(
            params.target_count * DISCOVERY_OVERFETCH_MULTIPLIER,
            params.target_count,
        ),
        FAST_SCAN_MAX_INSPECTED,
    )
    return ordered[:discovery_limit], warnings


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
    """Reconnect the personal Telethon client when Railway drops it."""

    if not user_client.is_connected():
        LOGGER.warning(
            "Telethon user client disconnected. Reconnecting..."
        )
        await user_client.connect()

    if not await user_client.is_user_authorized():
        raise RuntimeError(
            "Telethon StringSession is no longer authorized."
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
        limit=TG_MESSAGE_SCAN_LIMIT,
    ):
        if not message.date:
            continue

        message_time = ensure_utc(message.date)

        if last_message is None:
            last_message = message_time

        if message_time < cutoff:
            break

        message_count += 1

        # Low-memory mode: use sender_id directly instead of fetching
        # every sender entity with message.get_sender().
        if message.sender_id:
            human_senders.add(int(message.sender_id))

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
    event: Any,
    params: FastScanParams,
) -> None:
    chat_id = event.chat_id

    if active_jobs:
        await send_event_message(
            event,
            "A job is already running. Wait for it to finish.",
        )
        return

    active_jobs.add(chat_id)

    scan_label = (
        f"{params.asset_type}:{params.sector}:{params.source}"
        if params.asset_type == "alt"
        else f"meme:{params.launchpad}:{params.source}:{params.chain}"
    )

    history_id = STORAGE.create_history(
        f"fast_scan:{scan_label}",
        params.target_count,
    )

    progress = await send_event_message(
        event,
        (
            "⚡ Project Hunter scan started\n\n"
            f"Type: {params.asset_type.upper()}\n"
            f"Sector: {params.sector}\n"
            f"Source: {params.source}\n"
            + (
                f"Launchpad: {params.launchpad}\n"
                f"Chain: {params.chain}\n"
                if params.asset_type == "meme"
                else ""
            )
            + f"Target: {params.target_count}"
        )
    )

    inspected = 0
    inserted_count = 0
    merged_count = 0
    skipped_socials = 0
    preview_blocks: list[str] = []

    try:
        async def update_discovery_progress(**info: Any) -> None:
            stage = info.get("stage", "discovering")
            provider = str(info.get("provider") or "sources")
            provider_number = int(info.get("provider_number") or 0)
            provider_total = int(info.get("provider_total") or 0)
            discovered = int(info.get("discovered") or 0)
            provider_found = int(info.get("provider_found") or 0)

            if stage == "discovering":
                stage_text = f"🔍 Searching {provider}..."
            elif stage == "provider_complete":
                stage_text = (
                    f"✅ {provider} checked"
                    + (f" • {provider_found} returned" if provider_found else "")
                )
            else:
                stage_text = f"⚠️ {provider} could not be checked"

            await progress.edit(
                (
                    "⚡ Project Hunter scan running\n\n"
                    f"Type: {params.asset_type.upper()}\n"
                    + (
                        f"Launchpad: {params.launchpad}\n"
                        if params.asset_type == "meme"
                        else f"Sector: {params.sector}\n"
                    )
                    + f"Source: {params.source}\n\n"
                    f"Stage: {stage_text}\n"
                    f"Sources checked: {provider_number - (1 if stage == 'discovering' else 0)}/{provider_total}\n"
                    f"Discovered so far: {discovered}\n"
                    f"Saved: {inserted_count}/{params.target_count}\n"
                    f"Skipped: {skipped_socials}"
                )
            )

        candidates, warnings = await discover_candidates(
            params,
            progress_callback=update_discovery_progress,
        )
        inspected = len(candidates)

        await progress.edit(
            (
                "⚡ Project Hunter scan running\n\n"
                "Stage: 🧹 Filtering & saving\n"
                f"Discovered: {inspected}\n"
                f"Saved: {inserted_count}/{params.target_count}\n"
                f"Skipped: {skipped_socials}"
            )
        )

        for number, candidate in enumerate(candidates, start=1):
            # Preserve the current deep-analysis contract:
            # only candidates with both X and Telegram enter pending analysis.
            if not candidate.x_username or not candidate.telegram_url:
                skipped_socials += 1
                continue

            project = candidate.to_project()
            is_new = STORAGE.save_pending(project)

            if is_new:
                inserted_count += 1
                preview_blocks.append(
                    (
                        f"{inserted_count}.\n"
                        f"Project: {project['name']} (${project['symbol']})\n"
                        f"Type: {project['asset_type']}\n"
                        f"Sector: {project['sector']}\n"
                        f"Source(s): {project['sources']}\n"
                        + (
                            f"Chain: {project['chain']}\n"
                            if project.get("chain")
                            else ""
                        )
                        + (
                            f"Launchpad: {project['launchpad']}\n"
                            if project.get("launchpad")
                            else ""
                        )
                        + f"Market cap: ${project['market_cap']:,}\n"
                        + (
                            f"Liquidity: ${project['liquidity']:,.0f}\n"
                            if project.get("liquidity")
                            else ""
                        )
                        + (
                            f"24h volume: ${project['volume_24h']:,.0f}\n"
                            if project.get("volume_24h")
                            else ""
                        )
                        + f"X: {project['x_url']}\n"
                        f"TG: {project['telegram_url']}"
                    )
                )
            else:
                merged_count += 1

            await progress.edit(
                (
                    "⚡ Project Hunter scan running\n\n"
                    "Stage: 💾 Filtering & saving\n"
                    f"Discovered: {len(candidates)}\n"
                    f"Processed: {number}/{len(candidates)}\n"
                    f"Saved: {inserted_count}/{params.target_count}\n"
                    f"Duplicates/known merged: {merged_count}\n"
                    f"Skipped: {skipped_socials}\n"
                    f"Current: {candidate.name}"
                )
            )

            if inserted_count >= params.target_count:
                break

            if GC_EVERY_N_PROJECTS > 0 and number % GC_EVERY_N_PROJECTS == 0:
                gc.collect()

            await asyncio.sleep(0.15)

        STORAGE.finish_history(
            history_id,
            inspected=inspected,
            found=inserted_count,
            status="completed",
        )

        warning_text = ""
        if warnings:
            warning_text = "\n\nNotes:\n" + "\n".join(f"• {w}" for w in warnings[:5])

        target_reached = inserted_count >= params.target_count

        if target_reached:
            completion_title = "✅ Scan target reached"
            target_note = (
                f"Requested: {params.target_count}\n"
                f"Saved: {inserted_count}/{params.target_count}"
            )
        else:
            completion_title = "⚠️ Scan finished before target"
            target_note = (
                f"Requested: {params.target_count}\n"
                f"Saved: {inserted_count}/{params.target_count}\n"
                "The selected sources were exhausted or the inspection "
                "safety limit was reached before enough valid new projects survived."
            )

        await progress.edit(
            (
                f"{completion_title}\n\n"
                f"{target_note}\n"
                f"Candidates inspected: {inspected}\n"
                f"Duplicates/known merged: {merged_count}\n"
                f"Missing X or Telegram: {skipped_socials}\n\n"
                "Run /analyze to perform the deep checks."
                + warning_text
            )
        )

        if preview_blocks:
            await send_long(
                event,
                "NEW CANDIDATES\n\n" + "\n\n".join(preview_blocks),
            )

    except Exception as error:
        LOGGER.exception("Fast scan failed")

        STORAGE.finish_history(
            history_id,
            inspected=inspected,
            found=inserted_count,
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
        preview_blocks.clear()
        gc.collect()
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
        "source": project.get("source") or "",
        "sources": project.get("sources") or project.get("source") or "",
        "asset_type": project.get("asset_type") or "alt",
        "sector": project.get("sector") or project.get("category") or "all",
        "chain": project.get("chain") or "",
        "contract_address": project.get("contract_address") or "",
        "launchpad": project.get("launchpad") or "",
        "liquidity": float(project.get("liquidity") or 0),
        "volume_24h": float(project.get("volume_24h") or 0),
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
        f"Type: {result.get('asset_type', 'alt')}",
        f"Sector: {result.get('sector', 'all')}",
        f"Source(s): {result.get('sources') or result.get('source') or 'unknown'}",
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

    if active_jobs:
        await event.reply(
            "A job is already running. Wait for it to finish."
        )
        return

    pending = STORAGE.pending_projects(limit)

    if not pending:
        await event.reply(
            "No pending candidates. Run /scan first."
        )
        return

    active_jobs.add(chat_id)

    total_pending = len(pending)

    history_id = STORAGE.create_history(
        "deep_analysis",
        total_pending,
    )

    progress = await event.reply(
        (
            "🔬 Deep analysis started\n\n"
            f"Candidates: {total_pending}"
        )
    )

    priority_count = 0
    qualified_count = 0
    watchlist_count = 0
    rejected_count = 0
    processed_count = 0

    # Store only the report text for non-rejected projects.
    # Full result dictionaries are written to SQLite immediately.
    report_blocks: dict[str, list[str]] = {
        "priority": [],
        "qualified": [],
        "watchlist": [],
    }

    try:
        for number, project in enumerate(
            pending,
            start=1,
        ):
            await progress.edit(
                (
                    "🔬 Deep analysis running\n\n"
                    f"Progress: "
                    f"{number - 1}/{total_pending}\n"
                    f"Current: {project['name']}"
                )
            )

            try:
                result = await analyze_one(project)

            except FloodWaitError as error:
                await progress.edit(
                    (
                        "⏳ Telegram rate limit\n\n"
                        f"Waiting "
                        f"{error.seconds} seconds..."
                    )
                )
                await asyncio.sleep(
                    error.seconds + 1
                )
                result = await analyze_one(project)

            STORAGE.save_analysis(result)
            processed_count += 1

            stage = result["stage"]

            if stage == "priority":
                priority_count += 1
                report_blocks["priority"].append(
                    format_analysis(result)
                )

            elif stage == "qualified":
                qualified_count += 1
                report_blocks["qualified"].append(
                    format_analysis(result)
                )

            elif stage == "watchlist":
                watchlist_count += 1
                report_blocks["watchlist"].append(
                    format_analysis(result)
                )

            else:
                rejected_count += 1

            await progress.edit(
                (
                    "🔬 Deep analysis running\n\n"
                    f"Progress: "
                    f"{number}/{total_pending}\n"
                    f"Current: {project['name']}\n"
                    f"Result: "
                    f"{result['classification']} "
                    f"({result['score']}/100)"
                )
            )

            # Release large Telegram/CoinGecko-derived structures
            # as soon as they have been saved and formatted.
            result = None
            project = None

            if (
                GC_EVERY_N_PROJECTS > 0
                and processed_count
                % GC_EVERY_N_PROJECTS
                == 0
            ):
                gc.collect()

            await asyncio.sleep(0.5)

        STORAGE.finish_history(
            history_id,
            inspected=processed_count,
            found=processed_count,
            status="completed",
        )

        await progress.edit(
            (
                "✅ Deep analysis completed\n\n"
                f"Priority: {priority_count}\n"
                f"Qualified: {qualified_count}\n"
                f"Watchlist: {watchlist_count}\n"
                f"Rejected: {rejected_count}"
            )
        )

        headings = {
            "priority": "🔥 PRIORITY PROJECTS",
            "qualified": "✅ QUALIFIED PROJECTS",
            "watchlist": "🟡 WATCHLIST",
        }

        for stage, heading in headings.items():
            blocks = report_blocks[stage]

            if blocks:
                await send_long(
                    event,
                    f"{heading}\n\n"
                    + "\n\n".join(blocks),
                )

                blocks.clear()
                gc.collect()

        if rejected_count:
            await event.reply(
                (
                    f"❌ Rejected projects: "
                    f"{rejected_count}\n\n"
                    "Use /rejected to view them."
                )
            )

    except Exception as error:
        LOGGER.exception(
            "Deep analysis failed"
        )

        STORAGE.finish_history(
            history_id,
            inspected=processed_count,
            found=processed_count,
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
        pending.clear()

        for blocks in report_blocks.values():
            blocks.clear()

        gc.collect()
        active_jobs.discard(chat_id)


# =========================================================
# COMMAND PARSERS AND SAVED OUTPUT
# =========================================================

def parse_fast_scan(text: str) -> FastScanParams:
    """
    Supported:
      /scan
      /scan 50
      /scan 50 artificial-intelligence          (legacy)
      /scan alt all all 50
      /scan alt ai coingecko 50
      /scan alt defi mobula 30
      /scan meme all birdeye 50
      /scan meme pumpfun all 50
      /scan meme pumpfun mobula 30 solana
      /scan all 50
    """
    parts = text.strip().split()
    args = parts[1:]

    if not args:
        return FastScanParams()

    # Backwards compatibility: /scan 50 [coingecko-category]
    if args[0].isdigit():
        target = int(args[0])
        if target < 1 or target > 100:
            raise ValueError("Target must be between 1 and 100.")

        category = args[1].lower() if len(args) >= 2 else None
        return FastScanParams(
            target_count=target,
            asset_type="alt",
            sector=category or "all",
            source="coingecko",
            category_id=category,
            category_name=category,
        )

    mode = args[0].lower()

    if mode == "all":
        target = int(args[1]) if len(args) >= 2 else 50
        if target < 1 or target > 100:
            raise ValueError("Target must be between 1 and 100.")
        # "all" is intentionally split by the handler into alt + meme jobs.
        return FastScanParams(
            target_count=target,
            asset_type="all",
            sector="all",
            source="all",
        )

    if mode in {"alt", "altcoin", "altcoins"}:
        sector = args[1].lower() if len(args) >= 2 else "all"
        source = clean_source_name(args[2]) if len(args) >= 3 else "all"
        target = int(args[3]) if len(args) >= 4 else 50

        if target < 1 or target > 100:
            raise ValueError("Target must be between 1 and 100.")
        if source not in ALT_SOURCES:
            raise ValueError(
                "Alt source must be: all, coingecko, mobula, or dex."
            )

        category_id = None
        if sector != "all":
            category_id = COINGECKO_SECTOR_IDS.get(sector, sector)

        return FastScanParams(
            target_count=target,
            asset_type="alt",
            sector=sector,
            source=source,
            category_id=category_id,
            category_name=ALTCOIN_SECTORS.get(sector, sector),
        )

    if mode in {"meme", "memecoin", "memecoins"}:
        launchpad = args[1].lower() if len(args) >= 2 else "all"
        source = clean_source_name(args[2]) if len(args) >= 3 else "all"
        target = int(args[3]) if len(args) >= 4 else 50

        if len(args) >= 5:
            chain = normalize_chain(args[4])
        elif launchpad in {"fourmeme", "four.meme"}:
            chain = "bsc"
        elif launchpad in {"nadfun", "nad.fun"}:
            chain = "monad"
        else:
            chain = MEME_DEFAULT_CHAIN

        if target < 1 or target > 100:
            raise ValueError("Target must be between 1 and 100.")
        if source not in MEME_SOURCES:
            raise ValueError(
                "Meme source must be: all, birdeye, mobula, or dex."
            )
        if launchpad not in MEME_LAUNCHPADS:
            raise ValueError(
                "Launchpad must be: all, pumpfun, fourmeme, moonshot, "
                "raydium, meteora, or nadfun."
            )

        return FastScanParams(
            target_count=target,
            asset_type="meme",
            sector="memecoin",
            source=source,
            launchpad=launchpad,
            chain=chain,
        )

    raise ValueError(
        "Use /scan alt ..., /scan meme ..., /scan all, "
        "or the legacy /scan 50 [category]."
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
            f"Type: {row.get('asset_type') or 'alt'}\n"
            f"Sector: {row.get('sector') or row.get('category') or 'all'}\n"
            f"Source(s): {row.get('sources') or row.get('source') or 'unknown'}\n"
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



# =========================================================
# INLINE SCAN UI
# =========================================================

def default_scan_ui_state() -> dict[str, Any]:
    return {
        "asset_type": None,
        "sector": "all",
        "launchpad": "all",
        "source": "all",
        "target_count": 50,
        "chain": MEME_DEFAULT_CHAIN,
        "page": "root",
    }


def get_scan_ui_state(chat_id: int) -> dict[str, Any]:
    state = scan_ui_state.get(chat_id)
    if state is None:
        state = default_scan_ui_state()
        scan_ui_state[chat_id] = state
    return state


def scan_root_keyboard() -> list[list[Button]]:
    return [
        [
            Button.inline("💎 Altcoins", b"scan:type:alt"),
            Button.inline("🐸 Memecoins", b"scan:type:meme"),
        ],
        [
            Button.inline("🌐 Scan Everything", b"scan:type:all"),
        ],
        [
            Button.inline("📊 Saved Projects", b"scan:saved"),
            Button.inline("ℹ️ Sources", b"scan:sources"),
        ],
        [
            Button.inline("❌ Close", b"scan:close"),
        ],
    ]


def alt_sector_keyboard() -> list[list[Button]]:
    return [
        [
            Button.inline("🤖 AI", b"scan:sector:ai"),
            Button.inline("🎮 GameFi", b"scan:sector:gamefi"),
        ],
        [
            Button.inline("💰 DeFi", b"scan:sector:defi"),
            Button.inline("📡 DePIN", b"scan:sector:depin"),
        ],
        [
            Button.inline("🪙 RWA", b"scan:sector:rwa"),
            Button.inline("🏗 Layer 1", b"scan:sector:layer1"),
        ],
        [
            Button.inline("🧱 Layer 2", b"scan:sector:layer2"),
            Button.inline("⚡ Infrastructure", b"scan:sector:infrastructure"),
        ],
        [
            Button.inline("🔐 Privacy", b"scan:sector:privacy"),
            Button.inline("🔗 Interop", b"scan:sector:interoperability"),
        ],
        [
            Button.inline("🖼 NFT", b"scan:sector:nft"),
            Button.inline("💱 DEX", b"scan:sector:dex"),
        ],
        [
            Button.inline("💵 Stablecoin", b"scan:sector:stablecoin"),
            Button.inline("🌐 All Altcoins", b"scan:sector:all"),
        ],
        [
            Button.inline("◀️ Back", b"scan:back:root"),
        ],
    ]


def meme_launchpad_keyboard() -> list[list[Button]]:
    return [
        [
            Button.inline("🚀 Pump.fun", b"scan:launchpad:pumpfun"),
            Button.inline("🟣 Four.meme", b"scan:launchpad:fourmeme"),
        ],
        [
            Button.inline("🌙 Moonshot", b"scan:launchpad:moonshot"),
            Button.inline("🌊 Raydium", b"scan:launchpad:raydium"),
        ],
        [
            Button.inline("🌋 Meteora", b"scan:launchpad:meteora"),
            Button.inline("🟢 Nad.fun", b"scan:launchpad:nadfun"),
        ],
        [
            Button.inline("🌐 All Launchpads", b"scan:launchpad:all"),
        ],
        [
            Button.inline("◀️ Back", b"scan:back:root"),
        ],
    ]


def source_keyboard(asset_type: str, sector_or_launchpad: str) -> list[list[Button]]:
    if asset_type == "alt":
        return [
            [Button.inline("🌐 All Sources", b"scan:source:all")],
            [
                Button.inline("🦎 CoinGecko", b"scan:source:coingecko"),
                Button.inline("📊 Mobula", b"scan:source:mobula"),
            ],
            [
                Button.inline("📈 DEX Screener", b"scan:source:dex"),
            ],
            [
                Button.inline("◀️ Back", b"scan:back:sector"),
            ],
        ]

    buttons: list[list[Button]] = [
        [Button.inline("🌐 All Sources", b"scan:source:all")],
        [
            Button.inline("👁 Birdeye", b"scan:source:birdeye"),
            Button.inline("📊 Mobula", b"scan:source:mobula"),
        ],
        [
            Button.inline("📈 DEX Screener", b"scan:source:dex"),
        ],
        [
            Button.inline("◀️ Back", b"scan:back:launchpad"),
        ],
    ]

    # DEX Screener does not reliably identify an original launchpad.
    if sector_or_launchpad != "all":
        buttons = [
            [Button.inline("🌐 Best Available", b"scan:source:all")],
            [
                Button.inline("👁 Birdeye", b"scan:source:birdeye"),
                Button.inline("📊 Mobula", b"scan:source:mobula"),
            ],
            [
                Button.inline("◀️ Back", b"scan:back:launchpad"),
            ],
        ]

    return buttons


def count_keyboard() -> list[list[Button]]:
    return [
        [
            Button.inline("10", b"scan:count:10"),
            Button.inline("25", b"scan:count:25"),
            Button.inline("50", b"scan:count:50"),
        ],
        [
            Button.inline("75", b"scan:count:75"),
            Button.inline("100", b"scan:count:100"),
        ],
        [
            Button.inline("◀️ Back", b"scan:back:source"),
        ],
    ]


def confirmation_keyboard() -> list[list[Button]]:
    return [
        [Button.inline("▶️ Start Scan", b"scan:start")],
        [
            Button.inline("🔢 Change Count", b"scan:back:count"),
            Button.inline("🛠 Change Source", b"scan:back:source"),
        ],
        [
            Button.inline("🏠 Main Menu", b"scan:back:root"),
            Button.inline("❌ Cancel", b"scan:close"),
        ],
    ]


def scan_summary_text(state: dict[str, Any]) -> str:
    if state["asset_type"] == "all":
        return (
            "🌐 SCAN EVERYTHING\n\n"
            f"Target: {state['target_count']} projects\n"
            "Sources: All available\n"
            "Altcoins + Memecoins"
        )

    if state["asset_type"] == "alt":
        return (
            "💎 ALTCOIN SCAN\n\n"
            f"Sector: {state['sector']}\n"
            f"Source: {state['source']}\n"
            f"Target: {state['target_count']}"
        )

    return (
        "🐸 MEMECOIN SCAN\n\n"
        f"Launchpad: {state['launchpad']}\n"
        f"Source: {state['source']}\n"
        f"Chain: {state['chain']}\n"
        f"Target: {state['target_count']}"
    )


async def show_scan_root(event: Any, *, edit: bool = False) -> None:
    text = (
        "🔎 PROJECT HUNTER\n\n"
        "What do you want to hunt?"
    )
    buttons = scan_root_keyboard()

    if edit and isinstance(event, events.CallbackQuery.Event):
        await event.edit(text, buttons=buttons, link_preview=False)
    else:
        await send_event_message(
            event,
            text,
            buttons=buttons,
            link_preview=False,
        )


async def edit_scan_menu(
    event: events.CallbackQuery.Event,
    text: str,
    buttons: list[list[Button]],
) -> None:
    await event.edit(
        text,
        buttons=buttons,
        link_preview=False,
    )


def state_to_params(state: dict[str, Any]) -> FastScanParams:
    if state["asset_type"] == "all":
        return FastScanParams(
            target_count=int(state["target_count"]),
            asset_type="all",
            sector="all",
            source="all",
        )

    if state["asset_type"] == "alt":
        sector = state["sector"]
        return FastScanParams(
            target_count=int(state["target_count"]),
            asset_type="alt",
            sector=sector,
            source=state["source"],
            category_id=(
                None
                if sector == "all"
                else COINGECKO_SECTOR_IDS.get(sector, sector)
            ),
            category_name=ALTCOIN_SECTORS.get(sector, sector),
        )

    return FastScanParams(
        target_count=int(state["target_count"]),
        asset_type="meme",
        sector="memecoin",
        source=state["source"],
        launchpad=state["launchpad"],
        chain=state["chain"],
    )


@bot_client.on(events.CallbackQuery(pattern=rb"^scan:"))
async def scan_menu_callback(event: events.CallbackQuery.Event) -> None:
    if not authorized(event):
        await event.answer("This bot is private.", alert=True)
        return

    chat_id = event.chat_id
    state = get_scan_ui_state(chat_id)

    try:
        data = event.data.decode("utf-8")
    except Exception:
        await event.answer("Invalid menu action.", alert=True)
        return

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    value = parts[2] if len(parts) > 2 else ""

    await event.answer()

    if action == "type":
        state.clear()
        state.update(default_scan_ui_state())
        state["asset_type"] = value

        if value == "alt":
            state["page"] = "sector"
            await edit_scan_menu(
                event,
                "💎 ALTCOINS\n\nChoose a sector:",
                alt_sector_keyboard(),
            )
            return

        if value == "meme":
            state["page"] = "launchpad"
            await edit_scan_menu(
                event,
                "🐸 MEMECOINS\n\nChoose a launchpad:",
                meme_launchpad_keyboard(),
            )
            return

        if value == "all":
            state["source"] = "all"
            state["page"] = "count"
            await edit_scan_menu(
                event,
                "🌐 SCAN EVERYTHING\n\nHow many projects should Hunter target?",
                count_keyboard(),
            )
            return

    if action == "sector":
        state["asset_type"] = "alt"
        state["sector"] = value
        state["page"] = "source"
        await edit_scan_menu(
            event,
            (
                "💎 ALTCOIN SCAN\n\n"
                f"Sector: {value}\n\n"
                "Choose a discovery source:"
            ),
            source_keyboard("alt", value),
        )
        return

    if action == "launchpad":
        state["asset_type"] = "meme"
        state["launchpad"] = value

        if value in {"fourmeme", "four.meme"}:
            state["chain"] = "bsc"
        elif value in {"nadfun", "nad.fun"}:
            state["chain"] = "monad"
        else:
            state["chain"] = MEME_DEFAULT_CHAIN

        state["page"] = "source"
        await edit_scan_menu(
            event,
            (
                "🐸 MEMECOIN SCAN\n\n"
                f"Launchpad: {value}\n"
                f"Chain: {state['chain']}\n\n"
                "Choose a discovery source:"
            ),
            source_keyboard("meme", value),
        )
        return

    if action == "source":
        state["source"] = value
        state["page"] = "count"
        await edit_scan_menu(
            event,
            (
                f"{scan_summary_text(state)}\n\n"
                "How many projects should Hunter target?"
            ),
            count_keyboard(),
        )
        return

    if action == "count":
        state["target_count"] = int(value)
        state["page"] = "confirm"
        await edit_scan_menu(
            event,
            (
                f"{scan_summary_text(state)}\n\n"
                "Ready to start?"
            ),
            confirmation_keyboard(),
        )
        return

    if action == "start":
        if active_jobs:
            await event.answer(
                "A scan or analysis job is already running.",
                alert=True,
            )
            return

        params = state_to_params(state)

        await event.edit(
            (
                f"{scan_summary_text(state)}\n\n"
                "⏳ Starting..."
            ),
            buttons=None,
            link_preview=False,
        )

        # Keep command behavior and UI behavior on the same backend.
        if params.asset_type == "all":
            async def run_everything_from_ui() -> None:
                half = max(1, params.target_count // 2)

                alt_params = FastScanParams(
                    target_count=half,
                    asset_type="alt",
                    sector="all",
                    source="all",
                )
                meme_params = FastScanParams(
                    target_count=max(1, params.target_count - half),
                    asset_type="meme",
                    sector="memecoin",
                    source="all",
                    launchpad="all",
                    chain=MEME_DEFAULT_CHAIN,
                )

                await run_fast_scan(event, alt_params)
                await run_fast_scan(event, meme_params)

            asyncio.create_task(run_everything_from_ui())
        else:
            asyncio.create_task(
                run_fast_scan(event, params)
            )
        return

    if action == "back":
        if value == "root":
            state.clear()
            state.update(default_scan_ui_state())
            await show_scan_root(event, edit=True)
            return

        if value == "sector":
            state["page"] = "sector"
            await edit_scan_menu(
                event,
                "💎 ALTCOINS\n\nChoose a sector:",
                alt_sector_keyboard(),
            )
            return

        if value == "launchpad":
            state["page"] = "launchpad"
            await edit_scan_menu(
                event,
                "🐸 MEMECOINS\n\nChoose a launchpad:",
                meme_launchpad_keyboard(),
            )
            return

        if value == "source":
            state["page"] = "source"
            if state["asset_type"] == "alt":
                await edit_scan_menu(
                    event,
                    (
                        "💎 ALTCOIN SCAN\n\n"
                        f"Sector: {state['sector']}\n\n"
                        "Choose a discovery source:"
                    ),
                    source_keyboard("alt", state["sector"]),
                )
            elif state["asset_type"] == "meme":
                await edit_scan_menu(
                    event,
                    (
                        "🐸 MEMECOIN SCAN\n\n"
                        f"Launchpad: {state['launchpad']}\n"
                        f"Chain: {state['chain']}\n\n"
                        "Choose a discovery source:"
                    ),
                    source_keyboard("meme", state["launchpad"]),
                )
            else:
                await edit_scan_menu(
                    event,
                    "🌐 SCAN EVERYTHING\n\nHow many projects should Hunter target?",
                    count_keyboard(),
                )
            return

        if value == "count":
            state["page"] = "count"
            await edit_scan_menu(
                event,
                (
                    f"{scan_summary_text(state)}\n\n"
                    "How many projects should Hunter target?"
                ),
                count_keyboard(),
            )
            return

    if action == "sources":
        await edit_scan_menu(
            event,
            (
                "🛠 DISCOVERY SOURCES\n\n"
                "Altcoins:\n"
                "• CoinGecko\n"
                "• Mobula\n"
                "• DEX Screener\n\n"
                "Memecoins:\n"
                "• Birdeye\n"
                "• Mobula\n"
                "• DEX Screener"
            ),
            [[Button.inline("◀️ Back", b"scan:back:root")]],
        )
        return

    if action == "saved":
        counts = STORAGE.counts()
        await edit_scan_menu(
            event,
            (
                "📊 SAVED PROJECTS\n\n"
                f"Total: {counts['total']}\n"
                f"Pending: {counts['pending']}\n"
                f"Priority: {counts['priority']}\n"
                f"Qualified: {counts['qualified']}\n"
                f"Watchlist: {counts['watchlist']}\n"
                f"Rejected: {counts['rejected']}"
            ),
            [
                [
                    Button.inline("🔥 Priority", b"scan:list:priority"),
                    Button.inline("✅ Qualified", b"scan:list:qualified"),
                ],
                [
                    Button.inline("🟡 Watchlist", b"scan:list:watchlist"),
                    Button.inline("⏳ Pending", b"scan:list:pending"),
                ],
                [Button.inline("◀️ Back", b"scan:back:root")],
            ],
        )
        return

    if action == "list":
        stage = value
        rows = STORAGE.list_projects(stage, 10)
        await edit_scan_menu(
            event,
            f"{stage.upper()}\n\n{format_saved(rows)}",
            [
                [Button.inline("📊 Saved Projects", b"scan:saved")],
                [Button.inline("🏠 Main Menu", b"scan:back:root")],
            ],
        )
        return

    if action == "close":
        scan_ui_state.pop(chat_id, None)
        await event.edit(
            "❌ Scanner menu closed.",
            buttons=None,
            link_preview=False,
        )
        return

    await event.answer("Unknown menu action.", alert=True)


# =========================================================
# BOT COMMANDS
# =========================================================

@bot_client.on(events.NewMessage(pattern=r"^/start(?:@\w+)?$"))
async def start_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        await event.reply("This bot is private.")
        return

    await event.reply(
        (
            "Project Hunter v3 Multi-Source\n\n"
            "Main scanner:\n"
            "/scan — open the interactive menu\n\n"
            "Altcoin discovery:\n"
            "/scan alt all all 50\n"
            "/scan alt ai coingecko 50\n"
            "/scan alt defi mobula 30\n\n"
            "Memecoin discovery:\n"
            "/scan meme all all 50\n"
            "/scan meme pumpfun birdeye 50\n"
            "/scan meme pumpfun mobula 30 solana\n\n"
            "Everything:\n"
            "/scan all 50\n\n"
            "Legacy syntax still works:\n"
            "/scan 50 artificial-intelligence\n\n"
            "Discovery help:\n"
            "/sources\n"
            "/sectors\n\n"
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
            "/count"
        ),
        link_preview=False,
    )


@bot_client.on(events.NewMessage(pattern=r"^/scan(?:@\w+)?(?:\s+.*)?$"))
async def scan_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        await event.reply("This bot is private.")
        return

    command_parts = event.raw_text.strip().split()

    # /scan is now the primary button-driven interface.
    if len(command_parts) == 1:
        scan_ui_state[event.chat_id] = default_scan_ui_state()
        await show_scan_root(event)
        return

    # Power-user / legacy commands remain available.
    try:
        params = parse_fast_scan(event.raw_text)
    except (ValueError, TypeError) as error:
        await event.reply(f"❌ {error}")
        return

    if params.asset_type == "all":
        async def run_everything() -> None:
            half = max(1, params.target_count // 2)

            alt_params = FastScanParams(
                target_count=half,
                asset_type="alt",
                sector="all",
                source="all",
            )
            meme_params = FastScanParams(
                target_count=max(1, params.target_count - half),
                asset_type="meme",
                sector="memecoin",
                source="all",
                launchpad="all",
                chain=MEME_DEFAULT_CHAIN,
            )

            await run_fast_scan(event, alt_params)
            await run_fast_scan(event, meme_params)

        asyncio.create_task(run_everything())
    else:
        asyncio.create_task(run_fast_scan(event, params))



@bot_client.on(events.NewMessage(pattern=r"^/sources(?:@\w+)?$"))
async def sources_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        return

    await event.reply(
        (
            "DISCOVERY SOURCES\n\n"
            "ALTCOINS\n"
            "• coingecko — category/sector discovery\n"
            "• mobula — on-chain/trending discovery\n"
            "• dex — latest DEX profiles + market validation\n"
            "• all — combine and deduplicate them\n\n"
            "MEMECOINS\n"
            "• birdeye — meme launchpad discovery\n"
            "• mobula — Pulse/bonding discovery\n"
            "• dex — latest DEX profiles + market validation\n"
            "• all — combine and deduplicate them"
        )
    )


@bot_client.on(events.NewMessage(pattern=r"^/sectors(?:@\w+)?$"))
async def sectors_handler(event: events.NewMessage.Event) -> None:
    if not authorized(event):
        return

    sectors = [
        "ai", "gamefi", "defi", "depin", "rwa", "layer1", "layer2",
        "infrastructure", "privacy", "interoperability", "nft", "dex",
        "stablecoin",
    ]

    await event.reply(
        (
            "ALTCOIN SECTORS\n\n"
            + ", ".join(sectors)
            + "\n\nMEME LAUNCHPADS\n\n"
            + "all, pumpfun, fourmeme, moonshot, raydium, meteora, nadfun"
        )
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


# =========================================================
# STARTUP
# =========================================================

async def user_client_keepalive() -> None:
    """Periodically reconnect the personal Telegram client."""

    while True:
        try:
            await ensure_user_client_connected()
        except Exception as error:
            LOGGER.error(
                "Telethon keepalive failed: %s",
                error,
            )

        await asyncio.sleep(60)


async def main() -> None:
    await user_client.start()
    await bot_client.start(bot_token=BOT_TOKEN)

    bot = await bot_client.get_me()

    LOGGER.info(
        "Project Hunter v2 connected as @%s",
        bot.username,
    )
    LOGGER.info(
        "Discovery sources: coingecko=%s mobula=%s birdeye=%s dex=true",
        bool(COINGECKO_API_KEY),
        bool(MOBULA_API_KEY),
        bool(BIRDEYE_API_KEY),
    )
    LOGGER.info(
        "Low-memory mode active: page_size=%s, "
        "max_inspected=%s, tg_message_limit=%s, "
        "user_entity_cache=%s",
        PAGE_SIZE,
        FAST_SCAN_MAX_INSPECTED,
        TG_MESSAGE_SCAN_LIMIT,
        ENTITY_CACHE_LIMIT_USER,
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