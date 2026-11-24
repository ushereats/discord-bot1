"""
User Settings Cog — per-user notification toggles + ALL notification logic.

Self-contained: owns event handlers and DM sending.
dashboard.py forwards SSE events here via bot.get_cog("UserSettings").
uptime.py forwards server state changes here.

TOGGLES (7):
  1. playback_dms      — DM on playback start/stop
  2. new_episodes      — DM when new content drops
  3. server_status     — DM on server up/down (forwarded from uptime cog)
  4. account_security  — DM on new device login (forwarded from dashboard SSE)
  5. maintenance       — DM for admin-triggered maintenance windows
  6. monthly_stats     — Weekly watch digest (fires Sunday 9 AM Sydney)
  7. poll_pings        — Grants/revokes poll ping role for new round notifications

EVENT FLOW (all instant, no polling):
  dashboard.py  →  handle_playback_event(event_type, data)
  dashboard.py  →  handle_library_event(event_type, data)
  dashboard.py  →  handle_session_event(event_type, data)   [device detection]
  uptime.py     →  handle_server_status(is_down, reason)    [server up/down]
  own task      →  _weekly_digest_loop() sleeps until Sunday 9 AM, fires, repeats
  /maintenance  →  admin command broadcasts a maintenance DM
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from .utils.config import (
    JELLYFIN_API_KEY,
    JELLYFIN_URL,
    LINKS_DB_PATH,
    OWNER_DISCORD_ID,
    SETTINGS_DB_PATH,
    TARGET_GUILD_ID,
    TIMEZONE,
)
from .utils.db import Database

logger = logging.getLogger("settings_cog")

# ------------------------------------------------------------------ schema

TOGGLES = {
    # key: (emoji, default, description)
    "playback_dms":     ("▶️", False, "Playback start & stop alerts"),
    "new_episodes":     ("🆕", False, "New content notifications"),
    "server_status":    ("🟢", False, "Server uptime alerts"),
    "account_security": ("🔒", True,  "Login & security alerts"),
    "maintenance":      ("🔧", False, "Maintenance announcements"),
    "monthly_stats":    ("📊", False, "Weekly watch digest"),
    "poll_pings":       ("🗳️", False, "Poll round ping notifications"),
}

ROW_1_KEYS = ["playback_dms", "new_episodes", "server_status"]
ROW_2_KEYS = ["account_security", "maintenance", "monthly_stats"]
ROW_3_KEYS = ["poll_pings"]

# Role granted/revoked when poll_pings is toggled
POLL_PING_ROLE_ID = 1574683570696364121

INIT_SQL = """
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    CREATE TABLE IF NOT EXISTS user_settings (
        discord_id       INTEGER PRIMARY KEY,
        playback_dms     INTEGER NOT NULL DEFAULT 0,
        new_episodes     INTEGER NOT NULL DEFAULT 0,
        server_status    INTEGER NOT NULL DEFAULT 0,
        account_security INTEGER NOT NULL DEFAULT 1,
        maintenance      INTEGER NOT NULL DEFAULT 0,
        monthly_stats    INTEGER NOT NULL DEFAULT 0,
        poll_pings       INTEGER NOT NULL DEFAULT 0
    );
"""

# Migration: add poll_pings column if the DB already exists without it
_MIGRATE_SQL = """
    ALTER TABLE user_settings ADD COLUMN poll_pings INTEGER NOT NULL DEFAULT 0;
"""

# Playback debounce — don't DM if someone scrubs / skips quickly
PLAYBACK_DEBOUNCE_SECONDS = 15
# New-episode debounce — bundle items added within this window
NEW_EPISODE_BUNDLE_SECONDS = 30
# Weekly digest — day and hour (Sydney time)
DIGEST_WEEKDAY = 6  # Sunday (0=Monday, 6=Sunday)
DIGEST_HOUR = 9     # 9 AM
# Rate-limited broadcast — avoids Discord DM rate limits at scale
DM_RATE_LIMIT_PER_SECOND = 1.0  # max DMs per second
DM_RATE_LIMIT_RETRY_AFTER_DEFAULT = 5.0  # fallback backoff on 429
# Jellyfin REST headers
_JF_HEADERS = {
    "Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"',
    "accept": "application/json",
}

# ------------------------------------------------------------------ DB singleton

_db: Database | None = None
_db_lock = asyncio.Lock()


async def _get_db() -> Database:
    global _db
    if _db is not None:
        return _db
    async with _db_lock:
        if _db is not None:
            return _db
        _db = Database(SETTINGS_DB_PATH)
        await _db.open()
        await _db.conn.executescript(INIT_SQL)
        # Migrate: add poll_pings column for existing DBs
        try:
            await _db.conn.execute(_MIGRATE_SQL)
            await _db.conn.commit()
        except Exception:
            pass  # column already exists
        return _db


async def _close_db():
    global _db
    if _db is not None:
        await _db.close()
        _db = None


# ------------------------------------------------------------------ links DB (read-only)

_links_db: Database | None = None
_links_db_lock = asyncio.Lock()


async def _get_links_db() -> Database:
    """Read-only access to the links database (owned by registry-commands).
    Used to map Jellyfin user IDs to Discord user IDs."""
    global _links_db
    if _links_db is not None:
        return _links_db
    async with _links_db_lock:
        if _links_db is not None:
            return _links_db
        _links_db = Database(LINKS_DB_PATH)
        await _links_db.open()
        return _links_db


async def _close_links_db():
    global _links_db
    if _links_db is not None:
        await _links_db.close()
        _links_db = None


async def _discord_id_for_jellyfin_user(jellyfin_user_id: str) -> int | None:
    """Lookup Discord ID from a Jellyfin user ID via the links table."""
    db = await _get_links_db()
    norm = jellyfin_user_id.replace("-", "").lower()
    rows = await db.fetch_all("SELECT discord_id, jellyfin_user_id FROM links")
    for r in rows:
        if r["jellyfin_user_id"].replace("-", "").lower() == norm:
            return r["discord_id"]
    return None


async def _all_discord_ids_linked() -> list[dict]:
    """Return all {discord_id, jellyfin_user_id} from the links table."""
    db = await _get_links_db()
    return await db.fetch_all("SELECT discord_id, jellyfin_user_id FROM links")


# ------------------------------------------------------------------ DM helper (rate-limit aware)

_dm_lock = asyncio.Lock()
_dm_last_send: float = 0.0


async def _send_dm(bot: commands.Bot, discord_id: int, content: str) -> bool:
    """Best-effort DM with per-send rate limiting.
    Waits if needed to stay under DM_RATE_LIMIT_PER_SECOND,
    and backs off on 429 responses."""
    global _dm_last_send
    async with _dm_lock:
        # Throttle: wait until enough time has passed since last send
        min_interval = 1.0 / DM_RATE_LIMIT_PER_SECOND
        now = time.time()
        wait = min_interval - (now - _dm_last_send)
        if wait > 0:
            await asyncio.sleep(wait)

        try:
            user = bot.get_user(discord_id) or await bot.fetch_user(discord_id)
        except (discord.NotFound, discord.HTTPException):
            return False

        for attempt in range(3):
            try:
                await user.send(content)
                _dm_last_send = time.time()
                return True
            except discord.Forbidden:
                return False
            except discord.HTTPException as e:
                if e.status == 429:
                    retry_after = getattr(e, "retry_after", DM_RATE_LIMIT_RETRY_AFTER_DEFAULT)
                    logger.warning(f"[Settings] DM rate limited, retrying in {retry_after:.1f}s (attempt {attempt + 1})")
                    await asyncio.sleep(retry_after)
                else:
                    return False
        return False


async def _broadcast_dm(bot: commands.Bot, recipients: list[int], content: str) -> int:
    """Send the same DM to many users with rate limiting. Returns count sent."""
    sent = 0
    for discord_id in recipients:
        if await _send_dm(bot, discord_id, content):
            sent += 1
    return sent


# ------------------------------------------------------------------ Jellyfin REST helpers

async def _jf_get(bot: commands.Bot, path: str, params: dict | None = None) -> dict | None:
    """GET a Jellyfin REST endpoint. Returns parsed JSON or None on error."""
    url = f"{JELLYFIN_URL}{path}"
    try:
        async with bot.http_session.get(url, headers=_JF_HEADERS, params=params,
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        logger.debug(f"[Settings] REST GET {path} failed: {e}")
    return None


async def _jf_get_item(bot: commands.Bot, item_id: str) -> dict | None:
    """Fetch a library item by ID."""
    return await _jf_get(bot, f"/Items/{item_id}")


async def _jf_get_sessions(bot: commands.Bot) -> list[dict] | None:
    """Fetch active sessions from Jellyfin."""
    data = await _jf_get(bot, "/Sessions")
    return data if isinstance(data, list) else None


async def _jf_query_playback(bot: commands.Bot, sql: str) -> list[list] | None:
    """Run a custom SQL query against the Playback Reporting plugin database.
    Returns a list of rows (each row is a list of values), or None on error."""
    url = f"{JELLYFIN_URL}/user_usage_stats/submit_custom_query"
    payload = {"CustomQueryString": sql, "ReplaceUserId": ""}
    try:
        async with bot.http_session.post(url, headers=_JF_HEADERS, json=payload,
                                          timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                # Response: {"colums": [...], "results": [[...], ...]}
                # (yes, the plugin misspells "columns" as "colums")
                return data.get("results", [])
    except Exception as e:
        logger.debug(f"[Settings] Playback Reporting custom query failed: {e}")
    return None


# ------------------------------------------------------------------ public API

async def get_user_settings(discord_id: int) -> dict:
    """Return the full settings row for a user, creating defaults if needed."""
    db = await _get_db()
    row = await db.fetch_one(
        "SELECT * FROM user_settings WHERE discord_id = ?", (discord_id,)
    )
    if row is not None:
        return row
    await db.execute(
        "INSERT OR IGNORE INTO user_settings (discord_id) VALUES (?)", (discord_id,)
    )
    row = await db.fetch_one(
        "SELECT * FROM user_settings WHERE discord_id = ?", (discord_id,)
    )
    return row


async def toggle_setting(discord_id: int, key: str) -> bool:
    """Flip a toggle and return the new value."""
    if key not in TOGGLES:
        raise ValueError(f"Unknown setting: {key}")
    settings = await get_user_settings(discord_id)
    new_val = 0 if settings[key] else 1
    db = await _get_db()
    await db.write((f"UPDATE user_settings SET {key} = ? WHERE discord_id = ?", (new_val, discord_id)))
    return bool(new_val)


async def is_enabled(discord_id: int, key: str) -> bool:
    """Check if a toggle is on."""
    settings = await get_user_settings(discord_id)
    return bool(settings.get(key, TOGGLES.get(key, (None, False))[1]))


async def get_all_users_with(key: str) -> list[int]:
    """Return all discord_ids that have a specific toggle enabled."""
    if key not in TOGGLES:
        return []
    db = await _get_db()
    rows = await db.fetch_all(
        f"SELECT discord_id FROM user_settings WHERE {key} = 1"
    )
    return [r["discord_id"] for r in rows]


# ------------------------------------------------------------------ UI: nav buttons

def build_nav_row(discord_id: int, page: int) -> discord.ui.ActionRow:
    """Build the navigation ActionRow. page=0 is account, page=1 is settings."""
    row = discord.ui.ActionRow()
    row.add_item(discord.ui.Button(
        emoji="◀️",
        style=discord.ButtonStyle.secondary,
        custom_id=f"jfnav:{discord_id}:0",
        disabled=(page == 0),
    ))
    row.add_item(discord.ui.Button(
        emoji="▶️",
        style=discord.ButtonStyle.secondary,
        custom_id=f"jfnav:{discord_id}:1",
        disabled=(page == 1),
    ))
    return row


# ------------------------------------------------------------------ UI: settings container

def _build_toggle_text(settings: dict) -> str:
    """Build a compact text summary of all toggle states."""
    lines = []
    for key, (emoji, _default, desc) in TOGGLES.items():
        state = "✅" if settings.get(key) else "❌"
        lines.append(f"{state} {emoji} **{desc}**")
    return "\n".join(lines)


async def build_settings_container(discord_id: int, include_nav: bool = True) -> discord.ui.Container:
    """Build the settings container with toggle buttons and optional nav."""
    settings = await get_user_settings(discord_id)

    container = discord.ui.Container()
    container.add_item(discord.ui.TextDisplay(
        "-# YOUR SETTINGS\n"
        "# ⚙️ Notification Preferences"
    ))
    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
    container.add_item(discord.ui.TextDisplay(_build_toggle_text(settings)))
    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

    # Row 1: first 3 toggles
    row1 = discord.ui.ActionRow()
    for key in ROW_1_KEYS:
        emoji, _default, _desc = TOGGLES[key]
        is_on = bool(settings.get(key))
        row1.add_item(discord.ui.Button(
            emoji=emoji,
            style=discord.ButtonStyle.primary if is_on else discord.ButtonStyle.secondary,
            custom_id=f"usettings:{discord_id}:{key}",
        ))
    container.add_item(row1)

    # Row 2: middle 3 toggles
    row2 = discord.ui.ActionRow()
    for key in ROW_2_KEYS:
        emoji, _default, _desc = TOGGLES[key]
        is_on = bool(settings.get(key))
        row2.add_item(discord.ui.Button(
            emoji=emoji,
            style=discord.ButtonStyle.primary if is_on else discord.ButtonStyle.secondary,
            custom_id=f"usettings:{discord_id}:{key}",
        ))
    container.add_item(row2)

    # Row 3: poll pings
    row3 = discord.ui.ActionRow()
    for key in ROW_3_KEYS:
        emoji, _default, _desc = TOGGLES[key]
        is_on = bool(settings.get(key))
        row3.add_item(discord.ui.Button(
            emoji=emoji,
            style=discord.ButtonStyle.primary if is_on else discord.ButtonStyle.secondary,
            custom_id=f"usettings:{discord_id}:{key}",
        ))
    container.add_item(row3)

    container.add_item(discord.ui.TextDisplay(
        "-# Tap a button to toggle · changes take effect immediately"
    ))

    if include_nav:
        container.add_item(build_nav_row(discord_id, page=1))

    return container


async def build_settings_view(discord_id: int) -> discord.ui.LayoutView:
    """Build a complete LayoutView for the settings page (page 1) with nav inside container."""
    view = discord.ui.LayoutView(timeout=None)
    view.add_item(await build_settings_container(discord_id, include_nav=True))
    return view


# ------------------------------------------------------------------ DynamicItem: page navigation

class PageNavButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"jfnav:(?P<owner>\d+):(?P<page>\d+)",
):
    """Persistent navigation button."""

    def __init__(self, owner_id: int = 0, page: int = 0):
        super().__init__(discord.ui.Button(
            emoji="◀️" if page == 0 else "▶️",
            custom_id=f"jfnav:{owner_id}:{page}",
        ))
        self.owner_id = owner_id
        self.page = page

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match):
        return cls(int(match["owner"]), int(match["page"]))

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            return await interaction.response.send_message(
                "❌ This isn't your panel.", ephemeral=True
            )
        await interaction.response.defer()
        try:
            if self.page == 1:
                new_view = await build_settings_view(self.owner_id)
                await interaction.edit_original_response(view=new_view)
            else:
                reg_cog = interaction.client.get_cog("JellyfinRegistryCommands")
                if reg_cog is None:
                    return await interaction.followup.send(
                        "❌ Jellyfin module isn't loaded right now.", ephemeral=True
                    )
                account_view = await reg_cog.build_account_page(self.owner_id)
                if account_view is None:
                    return await interaction.followup.send(
                        "❌ Couldn't load your account details. Try `/jellyfin` again.",
                        ephemeral=True,
                    )
                await interaction.edit_original_response(view=account_view)
        except Exception as e:
            logger.error(f"[PageNav] Failed for {self.owner_id} page={self.page}: {e}")
            await interaction.followup.send(
                "❌ Something went wrong. Try `/jellyfin` again.",
                ephemeral=True,
            )


# ------------------------------------------------------------------ DynamicItem: toggle buttons

class SettingsToggleButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"usettings:(?P<owner>\d+):(?P<key>[a-z_]+)",
):
    """Persistent toggle button."""

    def __init__(self, owner_id: int = 0, key: str = "playback_dms"):
        super().__init__(discord.ui.Button(
            emoji=TOGGLES.get(key, ("?",))[0],
            custom_id=f"usettings:{owner_id}:{key}",
        ))
        self.owner_id = owner_id
        self.key = key

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match):
        return cls(int(match["owner"]), match["key"])

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            return await interaction.response.send_message(
                "❌ These aren't your settings.", ephemeral=True
            )
        if self.key not in TOGGLES:
            return await interaction.response.send_message(
                "❌ That setting no longer exists.", ephemeral=True
            )
        await toggle_setting(self.owner_id, self.key)
        await interaction.response.defer()

        # Poll pings toggle: grant or revoke the ping role
        if self.key == "poll_pings" and POLL_PING_ROLE_ID:
            try:
                settings_now = await get_user_settings(self.owner_id)
                guild = interaction.client.get_guild(TARGET_GUILD_ID)
                if guild:
                    member = guild.get_member(self.owner_id) or await guild.fetch_member(self.owner_id)
                    role = guild.get_role(POLL_PING_ROLE_ID)
                    if member and role:
                        if settings_now.get("poll_pings"):
                            await member.add_roles(role, reason="Poll pings toggled ON")
                        else:
                            await member.remove_roles(role, reason="Poll pings toggled OFF")
            except discord.HTTPException as e:
                logger.warning(f"[Settings] Role grant/revoke failed for {self.owner_id}: {e}")

        try:
            new_view = await build_settings_view(self.owner_id)
            await interaction.edit_original_response(view=new_view)
        except Exception as e:
            logger.error(f"[Settings] Failed to re-render for {self.owner_id}: {e}")
            settings = await get_user_settings(self.owner_id)
            emoji, _, desc = TOGGLES[self.key]
            state = "✅ ON" if settings[self.key] else "❌ OFF"
            await interaction.followup.send(
                f"{emoji} **{desc}** is now {state}",
                ephemeral=True,
            )


# ------------------------------------------------------------------ format helpers

def _format_item_title(item: dict) -> str:
    """Human-readable title from a Jellyfin item dict."""
    item_type = item.get("Type", "")
    if item_type == "Episode" and item.get("SeriesName"):
        parts = [item["SeriesName"]]
        season = item.get("ParentIndexNumber")
        episode = item.get("IndexNumber")
        if season is not None and episode is not None:
            parts.append(f"S{season}E{episode}")
        name = item.get("Name")
        if name:
            parts.append(f'"{name}"')
        return " · ".join(parts)
    name = item.get("Name") or "Unknown title"
    year = item.get("ProductionYear")
    if year:
        return f"{name} ({year})"
    return name


def _format_duration_short(seconds: int) -> str:
    """E.g. '2h 15m' or '45m'."""
    h, m = divmod(seconds // 60, 60)
    if h:
        return f"{h}h {m}m" if m else f"{h}h"
    return f"{m}m"


def _ticks_to_seconds(ticks: int | None) -> int:
    """Jellyfin uses 100-nanosecond ticks."""
    return int((ticks or 0) / 10_000_000)


def _seconds_until_next(weekday: int, hour: int) -> float:
    """Seconds from now until the next occurrence of weekday at hour (Sydney time)."""
    now = datetime.now(TIMEZONE)
    days_ahead = weekday - now.weekday()
    if days_ahead < 0 or (days_ahead == 0 and now.hour >= hour):
        days_ahead += 7
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead)
    return (target - now).total_seconds()


# ================================================================== COG

class UserSettings(commands.Cog):
    """Per-user notification settings + all notification event handling."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Jellyfin"

        # Playback tracking: {session_id: {discord_id, item_title, started_at, notified}}
        self._playback_sessions: dict[str, dict] = {}

        # New-episode bundling: {discord_id: [item_title, ...]}
        self._new_episode_queue: dict[int, list[str]] = {}
        self._new_episode_task: asyncio.Task | None = None

        # Device tracking for account_security: {discord_id: set(device_id)}
        self._known_devices: dict[int, set[str]] = {}
        self._devices_seeded: bool = False

        # Weekly digest task handle
        self._digest_task: asyncio.Task | None = None

    async def cog_load(self):
        await _get_db()
        self.bot.add_dynamic_items(SettingsToggleButton)
        self.bot.add_dynamic_items(PageNavButton)
        self._digest_task = asyncio.ensure_future(self._weekly_digest_loop())
        asyncio.ensure_future(self._seed_known_devices())
        logger.info("[Settings] Loaded — %d toggles, event-driven (no polling)", len(TOGGLES))

    async def cog_unload(self):
        self.bot.remove_dynamic_items(SettingsToggleButton)
        self.bot.remove_dynamic_items(PageNavButton)
        if self._new_episode_task and not self._new_episode_task.done():
            self._new_episode_task.cancel()
        if self._digest_task and not self._digest_task.done():
            self._digest_task.cancel()
        await _close_db()
        await _close_links_db()

    # ============================================================== /settings

    @app_commands.command(
        name="settings",
        description="View and toggle your Jellyfin notification preferences",
    )
    async def settings_command(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        view = await build_settings_view(interaction.user.id)
        try:
            dm_channel = interaction.user.dm_channel or await interaction.user.create_dm()
            await dm_channel.send(view=view)
            confirm_container = discord.ui.Container()
            confirm_container.add_item(discord.ui.TextDisplay(
                f"✅ {interaction.user.mention}, I've DMed you your settings panel — check your private messages!"
            ))
            confirm_view = discord.ui.LayoutView(timeout=None)
            confirm_view.add_item(confirm_container)
            await interaction.followup.send(view=confirm_view, ephemeral=False)
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ Couldn't DM you — check that your DMs are open and try again.",
                ephemeral=True,
            )

    # ============================================================== /maintenance (admin only)

    @app_commands.command(
        name="maintenance",
        description="Announce planned maintenance to all opted-in users",
    )
    @app_commands.describe(message="What's happening and when")
    async def maintenance_command(self, interaction: discord.Interaction, message: str):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message(
                "❌ Only the server owner can send maintenance notices.", ephemeral=True
            )
        await interaction.response.defer(ephemeral=True)

        recipients = await get_all_users_with("maintenance")
        sent = 0
        for discord_id in recipients:
            ok = await _send_dm(self.bot, discord_id,
                f"🔧 **Maintenance Notice**\n{message}")
            if ok:
                sent += 1

        await interaction.followup.send(
            f"✅ Maintenance notice sent to {sent}/{len(recipients)} opted-in users.",
            ephemeral=True,
        )

    # ============================================================== PLAYBACK EVENTS
    # Called by dashboard.py: bot.get_cog("UserSettings").handle_playback_event(...)

    async def handle_playback_event(self, event_type: str, data: dict):
        """Process a playback SSE event (playing/stopped)."""
        session_id = data.get("sessionId")
        if not session_id:
            return

        if event_type in ("playing",):
            await self._handle_playback_start(session_id, data)
        elif event_type == "stopped":
            await self._handle_playback_stop(session_id)

    async def _handle_playback_start(self, session_id: str, data: dict):
        """A user started watching something."""
        sessions = await _jf_get_sessions(self.bot)
        if not sessions:
            return

        session = next((s for s in sessions if s.get("Id") == session_id), None)
        if session is None:
            return

        jellyfin_user_id = session.get("UserId")
        if not jellyfin_user_id:
            return

        discord_id = await _discord_id_for_jellyfin_user(jellyfin_user_id)
        if discord_id is None:
            return

        if not await is_enabled(discord_id, "playback_dms"):
            return

        now_playing = session.get("NowPlayingItem")
        if not now_playing:
            return

        title = _format_item_title(now_playing)

        self._playback_sessions[session_id] = {
            "discord_id": discord_id,
            "item_title": title,
            "started_at": time.time(),
            "notified": False,
            "item_type": now_playing.get("Type", "Video"),
        }

        # Debounce: wait before sending DM in case they're just skipping
        await asyncio.sleep(PLAYBACK_DEBOUNCE_SECONDS)

        tracked = self._playback_sessions.get(session_id)
        if tracked is None or tracked["notified"]:
            return

        tracked["notified"] = True
        device = session.get("DeviceName", "Unknown device")
        client = session.get("Client", "Unknown app")
        runtime = _ticks_to_seconds(now_playing.get("RunTimeTicks"))
        dur = _format_duration_short(runtime) if runtime else "?"
        msg = (
            f"▶️ **Now Playing**\n"
            f"**{title}**\n"
            f"📱 {client} on {device}\n"
            f"⏱️ {dur}"
        )
        await _send_dm(self.bot, discord_id, msg)

    async def _handle_playback_stop(self, session_id: str):
        """A user stopped watching."""
        tracked = self._playback_sessions.pop(session_id, None)
        if tracked is None:
            return

        discord_id = tracked["discord_id"]
        if not tracked["notified"]:
            return

        if not await is_enabled(discord_id, "playback_dms"):
            return

        elapsed = time.time() - tracked["started_at"]
        dur = _format_duration_short(int(elapsed))
        msg = (
            f"⏹️ **Playback Ended**\n"
            f"**{tracked['item_title']}**\n"
            f"Watched for {dur}"
        )
        await _send_dm(self.bot, discord_id, msg)

    # ============================================================== LIBRARY EVENTS
    # Called by dashboard.py: bot.get_cog("UserSettings").handle_library_event(...)

    async def handle_library_event(self, event_type: str, data: dict):
        """Process library.item.added SSE events."""
        item_id = data.get("itemId")
        if not item_id:
            return

        if event_type == "library.item.added":
            await self._queue_new_episode(item_id)

    async def _queue_new_episode(self, item_id: str):
        """Fetch item info and queue for bundled notification."""
        item = await _jf_get_item(self.bot, item_id)
        if item is None:
            return

        item_type = item.get("Type", "")
        if item_type not in ("Episode", "Movie", "Season", "Series"):
            return

        title = _format_item_title(item)
        recipients = await get_all_users_with("new_episodes")

        for discord_id in recipients:
            self._new_episode_queue.setdefault(discord_id, []).append(title)

        # Start a flush task if one isn't already running. The flush
        # sleeps for the bundle window then sends whatever accumulated.
        # Do NOT cancel-and-restart — that resets the timer on every
        # item and prevents the flush from ever firing during scans.
        if self._new_episode_task is None or self._new_episode_task.done():
            self._new_episode_task = asyncio.ensure_future(self._flush_new_episodes())

    async def _flush_new_episodes(self):
        """Wait for bundling window then send DMs."""
        await asyncio.sleep(NEW_EPISODE_BUNDLE_SECONDS)

        queue = self._new_episode_queue.copy()
        self._new_episode_queue.clear()

        for discord_id, titles in queue.items():
            if not titles:
                continue
            if len(titles) == 1:
                msg = f"🆕 **New on Jellyfin**\n{titles[0]}"
            else:
                listed = "\n".join(f"• {t}" for t in titles[:15])
                extra = f"\n…and {len(titles) - 15} more" if len(titles) > 15 else ""
                msg = f"🆕 **{len(titles)} New Items on Jellyfin**\n{listed}{extra}"

            await _send_dm(self.bot, discord_id, msg)

    # ============================================================== SERVER STATUS
    # Called by uptime.py on state transitions — no polling needed.

    async def handle_server_status(self, is_down: bool, reason: str | None = None):
        """Instant server up/down notification. Called by the uptime cog."""
        recipients = await get_all_users_with("server_status")
        if not recipients:
            return

        if is_down:
            detail = f" — {reason}" if reason else ""
            msg = f"🔴 **Jellyfin is down**{detail}"
        else:
            msg = "🟢 **Jellyfin is back online**"

        sent = 0
        for discord_id in recipients:
            if await _send_dm(self.bot, discord_id, msg):
                sent += 1
        logger.info(f"[Settings] Server {'DOWN' if is_down else 'UP'} — notified {sent}/{len(recipients)} users")

    # ============================================================== ACCOUNT SECURITY (device detection)
    # Called by dashboard.py on session.start SSE — no polling needed.

    async def _seed_known_devices(self):
        """On startup, snapshot current devices so restarts don't false-alert."""
        await self.bot.wait_until_ready()
        await asyncio.sleep(10)  # let Jellyfin settle
        try:
            recipients = await get_all_users_with("account_security")
            if not recipients:
                self._devices_seeded = True
                return

            links = await _all_discord_ids_linked()
            enabled_set = set(recipients)

            for link in links:
                discord_id = link["discord_id"]
                if discord_id not in enabled_set:
                    continue

                jf_uid = link["jellyfin_user_id"]
                data = await _jf_get(self.bot, "/Devices", params={"userId": jf_uid})
                if not data or "Items" not in data:
                    continue

                norm_uid = jf_uid.replace("-", "").lower()
                devices = set()
                for d in data["Items"]:
                    did = d.get("Id")
                    if not did:
                        continue
                    last_user = (d.get("LastUserId") or "").replace("-", "").lower()
                    if last_user == norm_uid:
                        devices.add(did)

                self._known_devices[discord_id] = devices

            self._devices_seeded = True
            logger.info(f"[Settings] Seeded known devices for {len(self._known_devices)} users")
        except Exception as e:
            logger.error(f"[Settings] Device seeding failed: {e}", exc_info=True)
            self._devices_seeded = True  # don't block detection forever

    async def handle_session_event(self, event_type: str, data: dict):
        """Process session.start SSE events for new-device detection. Instant."""
        if event_type != "session.start":
            return
        if not self._devices_seeded:
            return  # still seeding, skip to avoid false alerts

        session_id = data.get("sessionId")
        if not session_id:
            return

        # Hydrate the session to get UserId, DeviceId, DeviceName, Client
        sessions = await _jf_get_sessions(self.bot)
        if not sessions:
            return

        session = next((s for s in sessions if s.get("Id") == session_id), None)
        if session is None:
            return

        jellyfin_user_id = session.get("UserId")
        if not jellyfin_user_id:
            return

        discord_id = await _discord_id_for_jellyfin_user(jellyfin_user_id)
        if discord_id is None:
            return

        if not await is_enabled(discord_id, "account_security"):
            return

        device_id = session.get("DeviceId")
        if not device_id:
            return

        known = self._known_devices.get(discord_id)
        if known is None:
            # First time seeing this user — seed and don't alert
            self._known_devices[discord_id] = {device_id}
            return

        if device_id in known:
            return  # known device

        # New device detected
        known.add(device_id)
        device_name = session.get("DeviceName", "Unknown device")
        client = session.get("Client", "Unknown app")
        msg = (
            f"🔒 **New Device Login**\n"
            f"**{device_name}**\n"
            f"App: {client}\n"
            f"-# If this wasn't you, change your password with `/jellyfin`"
        )
        await _send_dm(self.bot, discord_id, msg)
        logger.info(f"[Settings] New device alert for {discord_id}: {device_name} ({client})")

    # ============================================================== WEEKLY DIGEST
    # Sleeps until Sunday 9 AM Sydney, fires, repeats. No hourly polling.

    async def _weekly_digest_loop(self):
        """Sleep until Sunday 9 AM Sydney, send digests, repeat."""
        await self.bot.wait_until_ready()
        while True:
            try:
                wait = _seconds_until_next(DIGEST_WEEKDAY, DIGEST_HOUR)
                logger.info(f"[Settings] Weekly digest sleeping {wait / 3600:.1f}h until next Sunday 9 AM")
                await asyncio.sleep(wait)
                await self._send_weekly_digest()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[Settings] Weekly digest error: {e}", exc_info=True)
                await asyncio.sleep(3600)  # retry in an hour on error

    async def _send_weekly_digest(self):
        """Send weekly watch summaries to opted-in users.

        Queries the Playback Reporting plugin's database directly via
        submit_custom_query.  The PlaybackActivity table schema:
            DateCreated, UserId, ItemId, ItemType, ItemName,
            PlaybackDuration (seconds), PlayMethod, ClientName
        """
        recipients = await get_all_users_with("monthly_stats")
        if not recipients:
            return

        links = await _all_discord_ids_linked()
        enabled_set = set(recipients)

        now = datetime.now(TIMEZONE)
        week_ago = now - timedelta(days=7)
        start_date = week_ago.strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
        week_label = f"{week_ago.strftime('%d %b')} – {now.strftime('%d %b %Y')}"

        sent = 0
        for link in links:
            discord_id = link["discord_id"]
            if discord_id not in enabled_set:
                continue

            jf_uid = link["jellyfin_user_id"]

            # Query total play time, item count, movies, and episodes for this user
            sql = (
                "SELECT "
                "  COUNT(*) as total_items, "
                "  SUM(PlayDuration) as total_seconds, "
                "  SUM(CASE WHEN ItemType = 'Movie' THEN 1 ELSE 0 END) as movies, "
                "  SUM(CASE WHEN ItemType = 'Episode' THEN 1 ELSE 0 END) as episodes "
                "FROM PlaybackActivity "
                f"WHERE UserId = '{jf_uid}' "
                f"AND DateCreated >= '{start_date}' "
                f"AND DateCreated <= '{end_date}' "
                "AND PlayDuration > 0 AND PlayDuration < 86400"
            )
            rows = await _jf_query_playback(self.bot, sql)

            if not rows or not rows[0]:
                msg = f"\U0001f4ca **Weekly Digest** ({week_label})\nNo watch activity this week."
            else:
                row = rows[0]
                total_items = int(row[0] or 0)
                total_seconds = int(row[1] or 0)
                movies = int(row[2] or 0)
                episodes = int(row[3] or 0)

                if total_items == 0:
                    msg = f"\U0001f4ca **Weekly Digest** ({week_label})\nNo watch activity this week."
                else:
                    dur = _format_duration_short(total_seconds)
                    lines = [
                        f"\U0001f4ca **Weekly Digest** ({week_label})",
                        f"⏱️ Watch time: **{dur}**",
                        f"\U0001f3ac Items: **{total_items}**",
                    ]
                    if movies:
                        lines.append(f"\U0001f3a5 Movies: **{movies}**")
                    if episodes:
                        lines.append(f"\U0001f4fa Episodes: **{episodes}**")
                    msg = "\n".join(lines)

            if await _send_dm(self.bot, discord_id, msg):
                sent += 1

        logger.info(f"[Settings] Weekly digest sent to {sent} users")


async def setup(bot: commands.Bot):
    await bot.add_cog(UserSettings(bot))
