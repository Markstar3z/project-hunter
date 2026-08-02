PROJECT HUNTER BOT

FILES
project_hunter.py
requirements.txt
Procfile
.gitignore

RAILWAY VARIABLES
BOT_TOKEN=your BotFather token
API_ID=your Telegram API ID
API_HASH=your Telegram API hash
STRING_SESSION=your Telethon StringSession
COINGECKO_API_KEY=your CoinGecko demo API key
X_BEARER_TOKEN=your official X API bearer token
DATABASE_PATH=/data/project_hunter.db
ALLOWED_CHAT_ID=your Telegram numeric chat ID

OPTIONAL VARIABLES
MIN_MARKET_CAP=10000
MAX_MARKET_CAP=1000000000
MAX_PAGES_PER_SCAN=10
PAGE_SIZE=250
MAX_X_INACTIVE_DAYS=30
MAX_TG_INACTIVE_DAYS=30
TG_ACTIVITY_LOOKBACK_DAYS=7
TG_MIN_MESSAGES_7D=5
TG_MIN_HUMAN_SENDERS_7D=2

RAILWAY VOLUME
Mount a persistent volume at:
/data

START COMMAND
python project_hunter.py

COMMANDS
/start
/scan
/scan 20
/scan 20 artificial-intelligence
/qualified
/rejected
/latest
/count

IMPORTANT
The X activity filter uses the official X API and therefore requires
X_BEARER_TOKEN. Without it, projects will be rejected because X activity
cannot be verified.
