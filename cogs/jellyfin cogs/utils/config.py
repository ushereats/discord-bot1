"""
Shared configuration for all Jellyfin cogs.

Every cog imports from here instead of defining its own copy.
The dashboard cog uses DASHBOARD_API_KEY (different permissions for SSE);
everything else uses JELLYFIN_API_KEY.
"""

import os
from zoneinfo import ZoneInfo

# --- Jellyfin server ---
JELLYFIN_URL = "https://jellyfin.ushereats.com"
JELLYFIN_API_KEY = "9cb945d0d38d45cb84fcc8a2e1541bd8"
# Dashboard SSE needs a separate key with different Jellyfin permissions.
DASHBOARD_API_KEY = "a47ad7e52bd741b08e8b586fc6873fea"

# --- Discord IDs ---
OWNER_DISCORD_ID = 723417665087078451
TARGET_GUILD_ID = 1537023658323869757
LINKED_ROLE_ID = 1537338806943092767
TOKEN_CHANNEL_ID = 1538205460962738318
LOG_CHANNEL_ID = 1539141762600402994
ERROR_CHANNEL_ID = 1539140583744868463
WATCH_CHANNEL_ID = 1537028150783909999
COMMANDS_CHANNEL_ID = 1538371135362564096
UPTIME_CHANNEL_ID = 1539125023254511726
WATCHING_ROLE_ID = 1539199204017315911
BROWSING_ROLE_ID = 1540324328996872203

# --- Misc ---
SERVER_INVITE_URL = "https://discord.gg/D3sCdTCkNV"
TIMEZONE = ZoneInfo("Australia/Sydney")

# --- Database paths ---
DB_DIR = "/home/container/ushereats/DATABASES"
LINKS_DB_PATH = os.path.join(DB_DIR, "jellyfin_links.db")
DASHBOARD_DB_PATH = os.path.join(DB_DIR, "jellyfin.db")
STATS_DB_PATH = os.path.join(DB_DIR, "mystats.db")
DAILY_DB_PATH = os.path.join(DB_DIR, "daily_stats.db")
TRENDING_DB_PATH = os.path.join(DB_DIR, "trending.db")
PEAK_DB_PATH = os.path.join(DB_DIR, "peak_streams.db")
POLL_DB_PATH = os.path.join(DB_DIR, "poll.db")
SCHEDULE_DB_PATH = os.path.join(DB_DIR, "scheduled_posts.db")
REMOTE_DB_PATH = os.path.join(DB_DIR, "remote.db")
NEW_MEDIA_DB_PATH = os.path.join(DB_DIR, "new_media.db")

# --- Discord embed limits ---
EMBED_DESCRIPTION_LIMIT = 4096
EMBED_FIELD_VALUE_LIMIT = 1024
EMBED_TOTAL_LIMIT = 6000
EMBED_TOTAL_SAFETY_MARGIN = 64
MIN_FIELD_BUDGET = 150
MESSAGE_CONTENT_LIMIT = 2000
DM_BLOCKED_ERROR_CODE = 50007
