"""
Jellyfin Registry Commands Cog

Commands:
  - /manage [Owner]: paginated admin panel. Prev/Next walk ONE flat page
      sequence made of three sections:
        Accounts & Tokens pages (TOKENS_PER_PAGE pending tokens each):
          Link / Unlink / Register / Unregister, grant or revoke a signup
          token, toggle auto-token-on-join. Expired tokens are hidden
          from the list (expire_tokens_loop clears them within
          TOKEN_EXPIRY_CHECK_MINUTES).
        Left Users pages (LEFT_USERS_PER_PAGE queued rows each):
          add/remove exemptions, Preview Deletions, Delete all left users
          now, Sync Everything. The Exempt / Unlinked lists are embed
          fields, each sized against Discord's 6000-character total embed
          budget.
        Inactive Users pages (INACTIVE_PER_PAGE users each):
          linked users still in the server with no Jellyfin activity in
          INACTIVE_LINKED_DAYS+ days. Preview / Delete (throttled at
          INACTIVE_DELETE_DELAY_SECONDS between each) / Refresh. Data
          comes from the last Sync; Refresh triggers a new one.
  - /jellyfin [Linked users]: DMs an account summary with Reset Password,
      Change Username and My Devices buttons. My Devices lists every
      device signed into their account (name, app, last active) with a
      picker to sign one out remotely, or a confirmed "sign out all".
  - /leaveserver [Linked users]: self-serve deletion of their own account,
      behind a confirmation button.
  - /exempt [Owner]: add/remove a leave-deletion exemption by raw Discord
      ID (works for people who aren't in the server).

  Auto-token on join: when enabled via the toggle in /manage, every human
  member who joins TARGET_GUILD_ID with no existing link/token, and who
  isn't exempt, is issued a signup token exactly as if an admin had
  granted one. When disabled, joins are ignored. A member who rejoins
  while they still have a queued `left_users` row has that row cleared
  instead — their account survives and they are not a token candidate.

  Leaving the server (AUTO_DELETE_ON_LEAVE): when a linked member leaves,
  their Jellyfin account is deleted IMMEDIATELY in on_member_remove —
  matching the signup message's promise ("If you leave, your account will
  be terminated immediately"). Any pending signup token they held is
  revoked at the same moment. Exceptions, all of which stay queued in
  `left_users` for the "Delete all left users now" button:
    - exempt users (never deleted for leaving, button skips them too);
    - a deletion that failed (Jellyfin down/erroring);
    - leaves past the burst cap: more than AUTO_DELETE_BURST_LIMIT
      auto-deletions within AUTO_DELETE_BURST_WINDOW_SECONDS pauses
      auto-delete (protects against a prune / mass kick wiping accounts).
      Pressing the delete button processes the queue and resumes it;
    - leaves the bot never saw (it was offline) — caught later by Sync
      or the startup backfill, which only queue, never delete.

  Reminders: remind_pending_loop DMs every pending user once per
  REMIND_INTERVAL_HOURS while their token is between REMIND_MIN_AGE_HOURS
  and TOKEN_EXPIRY_HOURS old (3 reminders over a 24h token). A token is
  expired early only when Discord PERMANENTLY refuses the DM (user gone,
  or error 50007); transient failures leave it pending for the next cycle.

  Every command that mutates a link/account calls into the registry
  dashboard cog (registry_dashboard.py) to force a refresh afterward, but
  never touches dashboard rendering or the 5-minute poll itself — that
  lives entirely in the dashboard cog.

  All database access goes through aiosqlite. This is not about
  concurrent writes — SQLite still only allows one writer at a time
  regardless of driver — it's about not blocking the bot's single event
  loop for the duration of each query.

  All Jellyfin API calls share the bot-wide aiohttp.ClientSession created
  in main.py's setup_hook (bot.http_session). Lookups and duplicate-name
  checks reuse a /Users list for JELLYFIN_USERS_CACHE_SECONDS; every
  create/rename/delete made here invalidates it immediately.

  Performance and safety:
    - Every DB write goes through _write: serialized on one lock, each
      call one atomic commit (multi-row changes land together or not at
      all). Age filters and page slices run in SQL against indexes.
    - Action-log lines are batched into combined messages every
      ACTION_LOG_FLUSH_SECONDS (mentions render, never ping).
    - Dashboard refresh requests within DASHBOARD_REFRESH_DEBOUNCE_SECONDS
      collapse into one refresh.
    - Background loops catch their own exceptions, so one bad pass never
      stops a loop. The weekly sync's last-run time is persisted, so
      restarts neither trigger nor postpone it.

Config: set JELLYFIN_URL and JELLYFIN_API_KEY below (same values as the
other Jellyfin cogs).
"""

import io
import os
import math
import time
import asyncio
import logging
from collections import deque

import aiohttp
import aiosqlite
import discord
from datetime import datetime, timezone
from discord import app_commands
from discord.ext import commands, tasks

logger = logging.getLogger("registry_commands_cog")

OWNER_DISCORD_ID = 723417665087078451
TARGET_GUILD_ID = 1537023658323869757
JELLYFIN_URL = "https://jellyfin.ushereats.com"
JELLYFIN_API_KEY = "9cb945d0d38d45cb84fcc8a2e1541bd8"
LINKED_ROLE_ID = 1537338806943092767
TOKEN_CHANNEL_ID = 1538205460962738318
SERVER_INVITE_URL = "https://discord.gg/D3sCdTCkNV"
LOG_CHANNEL_ID = 1539141762600402994

DB_DIR = "/home/container/ushereats/DATABASES"
DB_PATH = os.path.join(DB_DIR, "jellyfin_links.db")

TOKEN_EXPIRY_HOURS = 24
REMIND_MIN_AGE_HOURS = 6  # a token younger than this hasn't had a real chance to be seen yet — skip it this cycle
REMIND_INTERVAL_HOURS = 6  # how often remind_pending_loop fires — reminds the SAME pending user every cycle until they sign up or their token expires
INACTIVE_LINKED_DAYS = 30  # linked + still-in-server users with no Jellyfin activity in this long get flagged by Sync
STALE_SYNC_WARNING_HOURS = 24  # Delete/Preview warn (not block) if Sync hasn't run this recently
WEEKLY_SYNC_INTERVAL_HOURS = 168  # automatic Sync cadence, independent of anyone pressing the button
SYNC_DUE_CHECK_MINUTES = 60  # how often weekly_sync_loop checks whether a sync is due (last sync time is persisted, so restarts don't trigger one)
TOKEN_EXPIRY_CHECK_MINUTES = 10  # how often the background loop sweeps for expired tokens

DASHBOARD_REFRESH_DEBOUNCE_SECONDS = 3  # refresh requests arriving within this window collapse into one dashboard refresh
ACTION_LOG_FLUSH_SECONDS = 2  # log lines are batched and posted at most once per this interval
JELLYFIN_USERS_CACHE_SECONDS = 30  # /Users list reuse window for lookups and duplicate-name checks

PENDING_SIGNUP_RECHECK_MINUTES = 15  # how often orphan-recovery re-checks flagged/unresolved signup markers

# One-time backfill: on startup, scan every existing `links` row against
# actual guild membership and insert anyone already gone into
# `left_users` (skips anyone already tracked, so it's safe to leave on
# across restarts — but set back to False once you've confirmed the
# backfill ran, so it isn't rescanning every linked user on every boot).
# Backfilled rows are only QUEUED, never auto-deleted.
BACKFILL_LEFT_USERS_ON_STARTUP = True

BATCH_CONCURRENCY = 5  # max simultaneous in-flight operations for batch DM/delete/remind passes

# --- /manage pagination ---
TOKENS_PER_PAGE = 20  # pending-token rows per Accounts & Tokens page
LEFT_USERS_PER_PAGE = 20  # queued left-user rows per Left Users page
INACTIVE_PER_PAGE = 20  # inactive linked users per Inactive Users page
INACTIVE_DELETE_DELAY_SECONDS = 2.0  # pause between each Jellyfin account deletion (avoid hammering the API)

# --- Instant auto-delete on leave ---
AUTO_DELETE_ON_LEAVE = True
AUTO_DELETE_BURST_LIMIT = 5  # auto-deletions allowed inside one window; the next one pauses auto-delete
AUTO_DELETE_BURST_WINDOW_SECONDS = 600

# --- Discord limits ---
EMBED_DESCRIPTION_LIMIT = 4096
EMBED_FIELD_VALUE_LIMIT = 1024
EMBED_TOTAL_LIMIT = 6000  # title + description + field names/values + footer, across the whole embed
EMBED_TOTAL_SAFETY_MARGIN = 64
MIN_FIELD_BUDGET = 150  # below this a field can't hold even one line plus the "…and N more" suffix
MESSAGE_CONTENT_LIMIT = 2000
DM_BLOCKED_ERROR_CODE = 50007  # "Cannot send messages to this user" — permanent, not transient


# --- MESSAGE TEMPLATES ---

# Token-channel card: this header + thumbnail never changes; only the line
# under the divider (and whether the Sign up button is shown) changes
# between states. Every state's text is built by one of the _token_*_text
# functions below, so the wording lives in exactly one place.
TOKEN_CARD_HEADER = (
    "**Welcome to the Usher Eats Jellyfin server**\n"
    "> <#1539538061526958120>\n"
    "> <#1537023659665793056>\n"
    f"-# Service: {JELLYFIN_URL}"
)
# The card's thumbnail is the token owner's own Discord avatar (server
# avatar if they set one, else their global one), resolved by
# _avatar_url(). This permanent Discord default avatar is the fallback
# when the user can't be resolved at all.
DEFAULT_AVATAR_URL = "https://cdn.discordapp.com/embed/avatars/0.png"


def _token_pending_text(discord_id: int, expires_ts: int | None) -> str:
    expiry = f"<t:{expires_ts}:R>" if expires_ts is not None else "soon"
    return (
        f"<@{discord_id}> you can now register — click below to sign up.\n"
        f"-# This link expires {expiry}."
    )


def _token_signed_up_text(discord_id: int, username: str, created_ts: int | None = None) -> str:
    created_ts = created_ts if created_ts is not None else int(time.time())
    return f"✅ <@{discord_id}> signed up as **{username}**.\n-# Account created <t:{created_ts}:R>."


def _token_expired_text(discord_id: int) -> str:
    return f"⌛ <@{discord_id}>'s signup link has expired.\n-# Ask an admin for a new one."


def _token_revoked_text(discord_id: int) -> str:
    return f"🚫 <@{discord_id}>'s signup link was revoked by an admin."


def _token_left_text(discord_id: int) -> str:
    return f"🚪 <@{discord_id}> left the server.\n-# Signup link revoked."


def _token_unreachable_text(discord_id: int) -> str:
    return (
        f"⌛ <@{discord_id}>'s signup link expired — we couldn't reach them by DM.\n"
        "-# Ask an admin for a new one once DMs are open."
    )

def _build_signup_success_message() -> str:
    """Sent both as the ephemeral reply after a successful signup AND as a
    DM to the same user, verbatim identical text in both places."""
    return (
        "✅ You have successfully signed up for the UsherEats Jellyfin media server.\n\n"
        "⚠️ **THE ONE RULE — READ THIS:**\n"
        "**You must stay in this Discord server.**\n"
        "**If you leave, your account will be terminated immediately.**\n\n"
        f"Issues? DM <@{OWNER_DISCORD_ID}>."
    )


def _build_pending_reminder_message(message_link: str, expires_ts: int | None) -> str:
    """Sent to a user with an unused signup token by remind_pending_loop.
    Points at the actual token message, not the server invite, since a
    pending-token user is already in the server."""
    expiry_line = f"This token expires <t:{expires_ts}:R>." if expires_ts is not None else "This token has an unknown expiry — sign up soon."
    return (
        "🔔 You have a pending signup token for the UsherEats Jellyfin media server.\n\n"
        f"Sign up here: {message_link}\n\n"
        f"{expiry_line}"
    )


# --- DATABASE SETUP ---
# A single shared aiosqlite.Connection, opened once in cog_load and reused
# for every query for the lifetime of the process. WAL journaling +
# NORMAL synchronous are set once here so the shared connection doesn't
# fsync-block on every write.
_db_conn: aiosqlite.Connection | None = None


async def _get_db() -> aiosqlite.Connection:
    return _db_conn


async def _open_db():
    global _db_conn
    os.makedirs(DB_DIR, exist_ok=True)
    _db_conn = await aiosqlite.connect(DB_PATH)
    _db_conn.row_factory = aiosqlite.Row
    await _db_conn.execute("PRAGMA journal_mode=WAL")
    await _db_conn.execute("PRAGMA synchronous=NORMAL")
    await _db_conn.commit()


async def _close_db():
    global _db_conn
    if _db_conn is not None:
        await _db_conn.close()
        _db_conn = None


# Every write in this module goes through _write. The shared connection
# means statements from different coroutines would otherwise interleave
# inside one implicit transaction, so one coroutine's commit could commit
# another's half-finished multi-statement change. Holding this lock for
# execute(s) + commit makes each _write call atomic: all of its
# statements land together, or (on error) none do.
_db_write_lock = asyncio.Lock()


async def _write(*statements: tuple[str, tuple]) -> list[int]:
    """Executes one or more (sql, params) statements as a single atomic
    commit. Returns each statement's rowcount, in order."""
    conn = await _get_db()
    async with _db_write_lock:
        try:
            counts = []
            for sql, params in statements:
                cur = await conn.execute(sql, params)
                counts.append(cur.rowcount)
            await conn.commit()
            return counts
        except Exception:
            await conn.rollback()
            raise


async def _get_meta(key: str) -> str | None:
    conn = await _get_db()
    async with conn.execute("SELECT value FROM registry_meta WHERE key = ?", (key,)) as cursor:
        row = await cursor.fetchone()
    return row[0] if row else None


async def _set_meta(key: str, value: str):
    await _write((
        "INSERT INTO registry_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    ))


async def _ensure_db():
    """Creates all registry tables, same as registry_dashboard.py. Both
    cogs call this independently on load — CREATE TABLE IF NOT EXISTS is
    idempotent, so it's safe regardless of which cog loads first."""
    conn = await _get_db()
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS links (
            discord_id INTEGER PRIMARY KEY,
            jellyfin_user_id TEXT NOT NULL,
            linked_by INTEGER NOT NULL,
            linked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS registry_dashboard_config (
            guild_id INTEGER PRIMARY KEY,
            dashboard_channel_id INTEGER,
            dashboard_message_id INTEGER
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS signup_tokens (
            discord_id INTEGER PRIMARY KEY,
            issued_by INTEGER NOT NULL,
            issued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            message_id INTEGER,
            reminded INTEGER NOT NULL DEFAULT 0
        )
    """)
    # Migration path for DBs created before `reminded` existed.
    cursor = await conn.execute("PRAGMA table_info(signup_tokens)")
    existing_cols = [row[1] for row in await cursor.fetchall()]
    if "reminded" not in existing_cols:
        await conn.execute("ALTER TABLE signup_tokens ADD COLUMN reminded INTEGER NOT NULL DEFAULT 0")
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS auto_token_settings (
            guild_id INTEGER PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 0
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS left_users (
            discord_id INTEGER PRIMARY KEY,
            jellyfin_user_id TEXT NOT NULL,
            left_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            notified INTEGER NOT NULL DEFAULT 0,
            notified_at TEXT
        )
    """)
    # notified/notified_at columns are legacy — kept in the schema so an
    # existing DB doesn't need a migration, but nothing writes to them.
    # left_users is now the QUEUE of leavers whose account still exists:
    # exempt users, failed auto-deletions, leaves past the burst cap, and
    # leaves found later by Sync/backfill.
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS exempt_users (
            discord_id INTEGER PRIMARY KEY,
            added_by INTEGER NOT NULL,
            added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            reason TEXT
        )
    """)
    cursor = await conn.execute("PRAGMA table_info(exempt_users)")
    exempt_cols = [row[1] for row in await cursor.fetchall()]
    if "reason" not in exempt_cols:
        await conn.execute("ALTER TABLE exempt_users ADD COLUMN reason TEXT")
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_signups (
            discord_id INTEGER PRIMARY KEY,
            jellyfin_username TEXT NOT NULL,
            jellyfin_user_id TEXT,
            flagged INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migration path for DBs created before jellyfin_user_id/flagged existed.
    cursor = await conn.execute("PRAGMA table_info(pending_signups)")
    pending_cols = [row[1] for row in await cursor.fetchall()]
    if "jellyfin_user_id" not in pending_cols:
        await conn.execute("ALTER TABLE pending_signups ADD COLUMN jellyfin_user_id TEXT")
    if "flagged" not in pending_cols:
        await conn.execute("ALTER TABLE pending_signups ADD COLUMN flagged INTEGER NOT NULL DEFAULT 0")

    await conn.execute("CREATE INDEX IF NOT EXISTS idx_links_jfid ON links(jellyfin_user_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_left_users_jfid ON left_users(jellyfin_user_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_tokens_issued_at ON signup_tokens(issued_at)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_left_users_left_at ON left_users(left_at)")
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS registry_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    await conn.commit()


# --- LINKS HELPERS ---

async def _get_link_by_discord_id(discord_id: int) -> dict | None:
    conn = await _get_db()
    async with conn.execute("SELECT * FROM links WHERE discord_id = ?", (discord_id,)) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


_UPSERT_LINK_SQL = """INSERT INTO links (discord_id, jellyfin_user_id, linked_by)
           VALUES (?, ?, ?)
           ON CONFLICT(discord_id) DO UPDATE SET
             jellyfin_user_id=excluded.jellyfin_user_id,
             linked_by=excluded.linked_by,
             linked_at=CURRENT_TIMESTAMP"""


async def _add_link(discord_id: int, jellyfin_user_id: str, linked_by: int):
    await _write((_UPSERT_LINK_SQL, (discord_id, jellyfin_user_id, linked_by)))


async def _commit_signup_link(discord_id: int, jellyfin_user_id: str, linked_by: int):
    """Link + clear the pending-signup marker in ONE commit — there is no
    moment where the link exists but the marker still says 'orphan', or
    vice versa."""
    await _write(
        (_UPSERT_LINK_SQL, (discord_id, jellyfin_user_id, linked_by)),
        ("DELETE FROM pending_signups WHERE discord_id = ?", (discord_id,)),
    )


async def _remove_link(discord_id: int) -> bool:
    (count,) = await _write(("DELETE FROM links WHERE discord_id = ?", (discord_id,)))
    return count > 0


async def _purge_account_rows(jellyfin_user_id: str, discord_id: int | None = None) -> bool:
    """After a Jellyfin account is deleted: removes its link and any
    left_users queue row in ONE commit, matched by Jellyfin ID (and by
    Discord ID too when known). Returns whether a link row existed."""
    link_count, _ = await _write(
        ("DELETE FROM links WHERE jellyfin_user_id = ?", (jellyfin_user_id,)),
        (
            "DELETE FROM left_users WHERE jellyfin_user_id = ? OR discord_id = ?",
            (jellyfin_user_id, discord_id if discord_id is not None else -1),
        ),
    )
    return link_count > 0


async def _get_all_links() -> list[dict]:
    conn = await _get_db()
    async with conn.execute("SELECT * FROM links") as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


# --- SIGNUP TOKEN HELPERS ---
# issued_at is SQLite CURRENT_TIMESTAMP ('YYYY-MM-DD HH:MM:SS', UTC) — the
# same format datetime('now', ...) produces, so age filters run in SQL as
# plain string comparisons against an index instead of loading every row.

_TOKEN_EXPIRY_MODIFIER = f"-{TOKEN_EXPIRY_HOURS} hours"


async def _get_token(discord_id: int) -> dict | None:
    conn = await _get_db()
    async with conn.execute("SELECT * FROM signup_tokens WHERE discord_id = ?", (discord_id,)) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def _get_token_by_message_id(message_id: int) -> dict | None:
    conn = await _get_db()
    async with conn.execute("SELECT * FROM signup_tokens WHERE message_id = ?", (message_id,)) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def _get_all_tokens() -> list[dict]:
    conn = await _get_db()
    async with conn.execute("SELECT * FROM signup_tokens") as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _get_expired_tokens() -> list[dict]:
    conn = await _get_db()
    async with conn.execute(
        "SELECT * FROM signup_tokens WHERE issued_at <= datetime('now', ?)", (_TOKEN_EXPIRY_MODIFIER,)
    ) as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _count_tokens() -> tuple[int, int]:
    """(visible, expired) in one query. Visible = not yet expired; expired
    rows are hidden from the panel and cleared by expire_tokens_loop
    within TOKEN_EXPIRY_CHECK_MINUTES."""
    conn = await _get_db()
    async with conn.execute(
        "SELECT COALESCE(SUM(issued_at > datetime('now', ?)), 0), COUNT(*) FROM signup_tokens",
        (_TOKEN_EXPIRY_MODIFIER,),
    ) as cursor:
        visible, total = await cursor.fetchone()
    return int(visible), int(total) - int(visible)


async def _get_visible_tokens_page(offset: int, limit: int) -> list[dict]:
    """One page of non-expired tokens, oldest first — stable ordering so
    page boundaries don't shuffle between renders."""
    conn = await _get_db()
    async with conn.execute(
        "SELECT * FROM signup_tokens WHERE issued_at > datetime('now', ?) "
        "ORDER BY issued_at, discord_id LIMIT ? OFFSET ?",
        (_TOKEN_EXPIRY_MODIFIER, limit, offset),
    ) as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _get_remindable_tokens(min_age_hours: float) -> list[dict]:
    """Every posted, non-expired token at least min_age_hours old. Does NOT
    exclude tokens already reminded — remind_pending_loop reminds the SAME
    pending user every cycle until they sign up (row deleted) or the token
    expires. `reminded` is display-only."""
    conn = await _get_db()
    async with conn.execute(
        "SELECT * FROM signup_tokens WHERE message_id IS NOT NULL "
        "AND issued_at <= datetime('now', ?) AND issued_at > datetime('now', ?)",
        (f"-{min_age_hours} hours", _TOKEN_EXPIRY_MODIFIER),
    ) as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _issue_token(discord_id: int, issued_by: int) -> bool:
    """Returns False without inserting if a token already exists for this
    user — refuse-and-tell-them rather than silently orphaning the old one.
    INSERT OR IGNORE makes the check-and-insert a single atomic statement."""
    (count,) = await _write((
        "INSERT OR IGNORE INTO signup_tokens (discord_id, issued_by) VALUES (?, ?)", (discord_id, issued_by),
    ))
    return count > 0


async def _set_token_message_id(discord_id: int, message_id: int):
    await _write(("UPDATE signup_tokens SET message_id = ? WHERE discord_id = ?", (message_id, discord_id)))


async def _set_token_reminded(discord_id: int):
    await _write(("UPDATE signup_tokens SET reminded = 1 WHERE discord_id = ?", (discord_id,)))


async def _delete_token(discord_id: int) -> bool:
    """Returns whether THIS call removed the row — callers racing over the
    same token (expiry sweep vs revoke vs leave) use it so only the winner
    edits the message and DMs."""
    (count,) = await _write(("DELETE FROM signup_tokens WHERE discord_id = ?", (discord_id,)))
    return count > 0


def _parse_utc_ts(raw: str | None) -> int | None:
    """SQLite CURRENT_TIMESTAMP is a naive 'YYYY-MM-DD HH:MM:SS' string
    that is already UTC. UTC tzinfo is attached explicitly before
    converting to epoch, or .timestamp() would assume the host's local
    timezone."""
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return None


def _token_issued_ts(token: dict) -> int | None:
    return _parse_utc_ts(token.get("issued_at"))


def _token_expires_ts(token: dict) -> int | None:
    issued_ts = _token_issued_ts(token)
    if issued_ts is None:
        return None
    return issued_ts + TOKEN_EXPIRY_HOURS * 3600


def _is_token_expired(token: dict) -> bool:
    issued_ts = _token_issued_ts(token)
    if issued_ts is None:
        return False  # can't determine — don't punish the user for ambiguous/corrupt data
    return (time.time() - issued_ts) >= TOKEN_EXPIRY_HOURS * 3600


# --- AUTO-TOKEN SETTING HELPERS ---

async def _get_auto_token_enabled(guild_id: int) -> bool:
    conn = await _get_db()
    async with conn.execute("SELECT enabled FROM auto_token_settings WHERE guild_id = ?", (guild_id,)) as cursor:
        row = await cursor.fetchone()
    return bool(row[0]) if row else False


async def _set_auto_token_enabled(guild_id: int, enabled: bool):
    await _write((
        """INSERT INTO auto_token_settings (guild_id, enabled) VALUES (?, ?)
           ON CONFLICT(guild_id) DO UPDATE SET enabled=excluded.enabled""",
        (guild_id, int(enabled)),
    ))


# --- LEFT-USERS HELPERS ---

async def _get_left_user(discord_id: int) -> dict | None:
    conn = await _get_db()
    async with conn.execute("SELECT * FROM left_users WHERE discord_id = ?", (discord_id,)) as cursor:
        row = await cursor.fetchone()
    return dict(row) if row else None


async def _add_left_user(discord_id: int, jellyfin_user_id: str):
    """INSERT OR IGNORE — if a row already exists, keep its original left_at."""
    await _write((
        "INSERT OR IGNORE INTO left_users (discord_id, jellyfin_user_id) VALUES (?, ?)",
        (discord_id, jellyfin_user_id),
    ))


async def _remove_left_user(discord_id: int) -> bool:
    (count,) = await _write(("DELETE FROM left_users WHERE discord_id = ?", (discord_id,)))
    return count > 0


async def _get_all_left_users() -> list[dict]:
    """Oldest leaver first."""
    conn = await _get_db()
    async with conn.execute("SELECT * FROM left_users ORDER BY left_at, discord_id") as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _count_left_users() -> int:
    conn = await _get_db()
    async with conn.execute("SELECT COUNT(*) FROM left_users") as cursor:
        (count,) = await cursor.fetchone()
    return int(count)


async def _get_left_users_page(offset: int, limit: int) -> list[dict]:
    conn = await _get_db()
    async with conn.execute(
        "SELECT * FROM left_users ORDER BY left_at, discord_id LIMIT ? OFFSET ?", (limit, offset),
    ) as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


def _left_user_left_ts(row: dict) -> int | None:
    return _parse_utc_ts(row.get("left_at"))


# --- EXEMPTION HELPERS ---
# A user on this list can leave the Discord server and their linked
# Jellyfin account is NEVER deleted for it — checked by on_member_remove's
# auto-delete AND by every Delete-button press, so changes take effect
# immediately.

async def _get_all_exempt_users() -> list[dict]:
    conn = await _get_db()
    async with conn.execute("SELECT * FROM exempt_users") as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _is_exempt(discord_id: int) -> bool:
    conn = await _get_db()
    async with conn.execute("SELECT 1 FROM exempt_users WHERE discord_id = ?", (discord_id,)) as cursor:
        return (await cursor.fetchone()) is not None


async def _add_exempt_user(discord_id: int, added_by: int, reason: str | None = None):
    await _write((
        "INSERT OR IGNORE INTO exempt_users (discord_id, added_by, reason) VALUES (?, ?, ?)",
        (discord_id, added_by, reason),
    ))


async def _remove_exempt_user(discord_id: int) -> bool:
    (count,) = await _write(("DELETE FROM exempt_users WHERE discord_id = ?", (discord_id,)))
    return count > 0


# --- PENDING-SIGNUP HELPERS (crash-recovery for orphaned Jellyfin accounts) ---

async def _set_pending_signup(discord_id: int, jellyfin_username: str):
    """Upsert — a retry after a failed attempt overwrites the previous
    marker. jellyfin_user_id/flagged reset to NULL/0 on a fresh attempt."""
    await _write((
        """INSERT INTO pending_signups (discord_id, jellyfin_username) VALUES (?, ?)
           ON CONFLICT(discord_id) DO UPDATE SET
             jellyfin_username=excluded.jellyfin_username,
             jellyfin_user_id=NULL,
             flagged=0,
             started_at=CURRENT_TIMESTAMP""",
        (discord_id, jellyfin_username),
    ))


async def _mark_pending_signup_created(discord_id: int, jellyfin_user_id: str):
    """Called immediately after Jellyfin confirms account creation — shrinks
    the crash-recovery blind spot to the gap between _create_jellyfin_user's
    response landing and this one fast local write."""
    await _write(("UPDATE pending_signups SET jellyfin_user_id = ? WHERE discord_id = ?", (jellyfin_user_id, discord_id)))


async def _flag_pending_signup(discord_id: int):
    """Marks a marker as 'seen ambiguous at least once' instead of
    clearing it outright — gives reconciliation one more pass before
    concluding nothing was created."""
    await _write(("UPDATE pending_signups SET flagged = 1 WHERE discord_id = ?", (discord_id,)))


async def _clear_pending_signup(discord_id: int):
    await _write(("DELETE FROM pending_signups WHERE discord_id = ?", (discord_id,)))


async def _get_all_pending_signups() -> list[dict]:
    conn = await _get_db()
    async with conn.execute("SELECT * FROM pending_signups") as cursor:
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


# --- SHARED JELLYFIN HTTP SESSION ---
# The whole bot process shares ONE aiohttp.ClientSession, created in
# main.py's setup_hook and attached as bot.http_session. _bot_ref is set
# once in cog_load.
_bot_ref = None


async def _get_jellyfin_session() -> aiohttp.ClientSession:
    return _bot_ref.http_session


# --- JELLYFIN API HELPERS ---
# Built once at import instead of on every call.
_JF_HEADERS = {"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"', "accept": "application/json"}
_JF_TIMEOUT = aiohttp.ClientTimeout(total=10)

# Short-lived /Users cache. Every lookup and duplicate-name check used to
# download the full user list. Now they reuse one fetch for
# JELLYFIN_USERS_CACHE_SECONDS, and every create/rename/delete made by this
# module invalidates it immediately, so this module never reads a list that
# misses its own changes. (A clash with a change made directly in
# Jellyfin's UI inside that window is still rejected by Jellyfin itself.)
_jf_users_cache: tuple[float, list[dict]] | None = None
_jf_users_lock = asyncio.Lock()


async def _fetch_jellyfin_users() -> list[dict]:
    """Always hits Jellyfin (used where freshness matters: Sync and
    reconciliation). Refreshes the cache as a side effect."""
    global _jf_users_cache
    session = await _get_jellyfin_session()
    async with session.get(f"{JELLYFIN_URL}/Users", headers=_JF_HEADERS, timeout=_JF_TIMEOUT) as resp:
        resp.raise_for_status()
        users = await resp.json()
    _jf_users_cache = (time.monotonic(), users)
    return users


async def _get_jellyfin_users_cached() -> list[dict]:
    """Cached /Users for lookups and duplicate checks. Concurrent callers
    on a cold cache share ONE fetch instead of each firing their own."""
    cached = _jf_users_cache
    if cached is not None and time.monotonic() - cached[0] < JELLYFIN_USERS_CACHE_SECONDS:
        return cached[1]
    async with _jf_users_lock:
        cached = _jf_users_cache
        if cached is not None and time.monotonic() - cached[0] < JELLYFIN_USERS_CACHE_SECONDS:
            return cached[1]
        return await _fetch_jellyfin_users()


def _invalidate_jellyfin_users_cache():
    global _jf_users_cache
    _jf_users_cache = None


async def _fetch_jellyfin_user(jellyfin_user_id: str) -> dict | None:
    try:
        session = await _get_jellyfin_session()
        async with session.get(f"{JELLYFIN_URL}/Users/{jellyfin_user_id}", headers=_JF_HEADERS, timeout=_JF_TIMEOUT) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def _fetch_jellyfin_user_strict(jellyfin_user_id: str) -> dict | None:
    """Returns None ONLY on a confirmed 404. Any other failure RAISES, so
    pending-signup reconciliation can tell 'confirmed gone' apart from
    'couldn't check'."""
    session = await _get_jellyfin_session()
    async with session.get(f"{JELLYFIN_URL}/Users/{jellyfin_user_id}", headers=_JF_HEADERS, timeout=_JF_TIMEOUT) as resp:
        if resp.status == 404:
            return None
        resp.raise_for_status()
        return await resp.json()


async def _create_jellyfin_user(username: str, password: str) -> dict:
    """POST /Users/New — creates the account and returns the new user object
    (includes its Id). Raises on any non-2xx via raise_for_status."""
    payload = {"Name": username, "Password": password}
    session = await _get_jellyfin_session()
    try:
        async with session.post(f"{JELLYFIN_URL}/Users/New", headers=_JF_HEADERS, json=payload, timeout=_JF_TIMEOUT) as resp:
            resp.raise_for_status()
            return await resp.json()
    finally:
        _invalidate_jellyfin_users_cache()  # even on error: the account may exist despite a failed response


async def _set_jellyfin_password(jellyfin_user_id: str, new_password: str):
    """POST /Users/{id}/Password — sets a NEW password directly. Safe with
    no old password because the admin API key bypasses Jellyfin's
    CurrentPw check. ResetPassword stays False (True would blank the
    password instead of setting NewPw)."""
    payload = {"CurrentPw": "", "NewPw": new_password, "ResetPassword": False}
    session = await _get_jellyfin_session()
    async with session.post(
        f"{JELLYFIN_URL}/Users/{jellyfin_user_id}/Password", headers=_JF_HEADERS, json=payload, timeout=_JF_TIMEOUT
    ) as resp:
        resp.raise_for_status()


async def _grant_full_library_access(jellyfin_user_id: str):
    """Fetches the account's current policy, flips EnableAllFolders on, and
    POSTs the whole policy back — the endpoint replaces the object
    wholesale rather than patching individual fields."""
    session = await _get_jellyfin_session()

    async with session.get(f"{JELLYFIN_URL}/Users/{jellyfin_user_id}", headers=_JF_HEADERS, timeout=_JF_TIMEOUT) as resp:
        resp.raise_for_status()
        user = await resp.json()

    policy = user.get("Policy", {})
    policy["EnableAllFolders"] = True
    policy["EnabledFolders"] = []

    async with session.post(
        f"{JELLYFIN_URL}/Users/{jellyfin_user_id}/Policy", headers=_JF_HEADERS, json=policy, timeout=_JF_TIMEOUT
    ) as resp:
        resp.raise_for_status()


async def _delete_jellyfin_user(jellyfin_user_id: str) -> str:
    """Returns 'deleted' (a real 200/204 — this call removed the account)
    or 'already_gone' (404 — nothing was there). Raises on any other
    failure. Callers that report counts MUST keep these two separate —
    collapsing them is what once reported 54 deletions when Jellyfin only
    lost 30 accounts."""
    session = await _get_jellyfin_session()
    try:
        async with session.delete(f"{JELLYFIN_URL}/Users/{jellyfin_user_id}", headers=_JF_HEADERS, timeout=_JF_TIMEOUT) as resp:
            if resp.status == 404:
                return "already_gone"
            resp.raise_for_status()
            return "deleted"
    finally:
        _invalidate_jellyfin_users_cache()


def _norm_guid(value) -> str:
    """Jellyfin writes GUIDs both with and without dashes — compare them
    in one form."""
    return str(value or "").replace("-", "").lower()


async def _get_user_devices(jellyfin_user_id: str) -> list[dict]:
    """Every device holding a login for THIS account only, most recently
    active first. Raises on failure.

    Jellyfin's ?userId= filter on /Devices does NOT mean "this user's
    devices" — it means "devices this user is allowed to access", and
    with the default 'Allow access from all devices' policy that is every
    device on the server. So the result is filtered here to devices whose
    LastUserId is this account. Anything without a LastUserId is
    excluded (fail closed): a user must never see, or be able to sign
    out, someone else's device."""
    session = await _get_jellyfin_session()
    async with session.get(
        f"{JELLYFIN_URL}/Devices", headers=_JF_HEADERS, params={"userId": jellyfin_user_id}, timeout=_JF_TIMEOUT,
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
    owner = _norm_guid(jellyfin_user_id)
    items = [d for d in (data.get("Items") or []) if d.get("Id")]
    devices = [d for d in items if owner and _norm_guid(d.get("LastUserId")) == owner]

    # Signing out works by device Id, and Jellyfin logs out EVERY account
    # signed in under that Id — so a device shared with another account
    # (same Id, different owner) must never be signed out from here, or
    # this user would kick the other person too. Flag those.
    other_owner_ids = {d["Id"] for d in items if _norm_guid(d.get("LastUserId")) != owner}
    for d in devices:
        d["_shared"] = d["Id"] in other_owner_ids
    devices.sort(key=lambda d: d.get("DateLastActivity") or "", reverse=True)
    return devices


async def _sign_out_device(device_id: str) -> bool:
    """DELETE /Devices?id= — removes the device and its login token, which
    signs it out immediately. True if Jellyfin removed it (or it was
    already gone). Raises on other failures."""
    session = await _get_jellyfin_session()
    async with session.delete(
        f"{JELLYFIN_URL}/Devices", headers=_JF_HEADERS, params={"id": device_id}, timeout=_JF_TIMEOUT,
    ) as resp:
        if resp.status == 404:
            return True
        resp.raise_for_status()
        return True


async def _rename_jellyfin_user(jellyfin_user_id: str, new_username: str):
    """POST /Users/{id} — replaces the whole user object, same
    fetch-modify-post pattern as the Policy update. Only Name changes;
    everything keyed off Id (watch history, links, Playback Reporting) is
    unaffected."""
    session = await _get_jellyfin_session()

    async with session.get(f"{JELLYFIN_URL}/Users/{jellyfin_user_id}", headers=_JF_HEADERS, timeout=_JF_TIMEOUT) as resp:
        resp.raise_for_status()
        user = await resp.json()

    user["Name"] = new_username

    try:
        async with session.post(
            f"{JELLYFIN_URL}/Users/{jellyfin_user_id}", headers=_JF_HEADERS, json=user, timeout=_JF_TIMEOUT
        ) as resp:
            resp.raise_for_status()
    finally:
        _invalidate_jellyfin_users_cache()


def _sql_escape(value: str) -> str:
    """Defense in depth for the raw-SQL custom-query endpoint below."""
    return value.replace("'", "''")


async def _get_all_time_watch_seconds(jellyfin_user_id: str) -> int | None:
    """All-time total watch seconds for one user, via Playback Reporting's
    custom-query endpoint, with the same >24h-row sanity bound stats_cog.py
    uses. Returns None on any query failure — 'couldn't compute', not
    'zero watched'."""
    session = await _get_jellyfin_session()
    cap = 24 * 3600
    uid = _sql_escape(jellyfin_user_id)
    sql = (
        "SELECT SUM(PlayDuration) AS total FROM PlaybackActivity "
        f"WHERE UserId = '{uid}' AND PlayDuration > 0 AND PlayDuration < {cap}"
    )
    try:
        async with session.post(
            f"{JELLYFIN_URL}/user_usage_stats/submit_custom_query", headers=_JF_HEADERS,
            json={"CustomQueryString": sql}, timeout=_JF_TIMEOUT,
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    columns = data.get("colums") or data.get("columns") or []
    rows = data.get("results") or []
    if not rows:
        return 0
    row = dict(zip(columns, rows[0]))
    return int(row.get("total") or 0)


def _format_duration(seconds: int) -> str:
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, _ = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _parse_jellyfin_date(raw: str | None) -> int | None:
    """The ONE parser for Jellyfin's ISO timestamps (LastLoginDate,
    LastActivityDate). Handles a trailing 'Z' or an explicit offset, and
    any number of fractional-second digits (Jellyfin sends 7, which some
    Python versions' fromisoformat rejects). Always timezone-aware, so the
    host's local timezone never skews the result. Jellyfin's 'never'
    sentinel (year 0001) and anything else before 1971 return None."""
    if not raw:
        return None
    try:
        text = raw.strip()
        offset = "+00:00"
        if text.endswith("Z"):
            text = text[:-1]
        elif len(text) > 6 and text[-6] in "+-" and text[-3] == ":":
            text, offset = text[:-6], text[-6:]
        base = text.split(".", 1)[0]
        dt = datetime.fromisoformat(base + offset)
        if dt.year < 1971:
            return None
        return int(dt.timestamp())
    except (ValueError, TypeError, OverflowError):
        return None


def _linked_at_ts(raw: str | None) -> int | None:
    """links.linked_at is SQLite's UTC CURRENT_TIMESTAMP — same parser as
    every other DB timestamp."""
    return _parse_utc_ts(raw)


def _format_ts(ts: int | None) -> str:
    if ts is None:
        return "Never"
    return f"<t:{ts}:R>"


def _is_owner(interaction: discord.Interaction) -> bool:
    return interaction.user.id == OWNER_DISCORD_ID


def _text_view(text: str) -> discord.ui.LayoutView:
    """Ephemeral one-off reply: a plain one-text container. Deliberately
    NOT TokenMessageView — errors and confirmations shouldn't carry the
    token card's welcome header and thumbnail."""
    view = discord.ui.LayoutView(timeout=None)
    container = discord.ui.Container()
    container.add_item(discord.ui.TextDisplay(text))
    view.add_item(container)
    return view


def _avatar_from(user) -> str:
    """256px avatar URL for a User/Member — display_avatar already falls
    back to Discord's default avatar for users without one."""
    try:
        return user.display_avatar.replace(size=256).url
    except Exception:
        return DEFAULT_AVATAR_URL


async def _avatar_url(bot, discord_id: int) -> str:
    """Token owner's avatar for the card thumbnail. Cache first (guild
    member -> server avatar if set; otherwise the global user), and only
    falls back to one fetch_user API call on a cache miss — e.g. someone
    who has already left. Never raises; DEFAULT_AVATAR_URL on failure."""
    guild = bot.get_guild(TARGET_GUILD_ID)
    user = (guild.get_member(discord_id) if guild is not None else None) or bot.get_user(discord_id)
    if user is None:
        try:
            user = await bot.fetch_user(discord_id)
        except discord.HTTPException:
            return DEFAULT_AVATAR_URL
    return _avatar_from(user)


async def _edit_token_message(bot, message_id: int | None, discord_id: int, text: str, include_button: bool = False):
    """Edits a token message in TOKEN_CHANNEL_ID to a new state (thumbnail
    re-resolved from discord_id, so it stays the owner's avatar), swallowing
    discord.HTTPException (message deleted, channel gone, etc. — never
    worth failing the calling action over)."""
    if not message_id:
        return
    channel = bot.get_channel(TOKEN_CHANNEL_ID)
    if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
        return
    try:
        msg = await channel.fetch_message(message_id)
        avatar = await _avatar_url(bot, discord_id)
        await msg.edit(view=TokenMessageView(text, include_button=include_button, avatar_url=avatar))
    except discord.HTTPException:
        pass


async def _send_dm(bot, discord_id: int, *, content: str | None = None, view: discord.ui.LayoutView | None = None) -> str:
    """Best-effort DM with a three-way outcome:
      'sent'        — delivered.
      'unreachable' — PERMANENT: the user doesn't exist (fetch_user 404),
                      or Discord refused with error 50007 (no shared
                      server / DMs closed / bot blocked). Retrying won't
                      help.
      'failed'      — anything else: rate limit, 5xx, other HTTP errors.
                      Transient — a later retry can succeed.
    Every failure is logged with its real cause."""
    kind = "DM (view)" if view is not None else "DM"
    try:
        user = bot.get_user(discord_id) or await bot.fetch_user(discord_id)
    except discord.NotFound:
        logger.warning(f"[JellyfinRegistryCommands] {kind} failed for {discord_id}: user not found (fetch_user 404 — invalid ID or deleted account).")
        return "unreachable"
    except discord.HTTPException as e:
        logger.warning(f"[JellyfinRegistryCommands] {kind} failed for {discord_id}: couldn't resolve user — {type(e).__name__} (status {getattr(e, 'status', '?')}, code {getattr(e, 'code', '?')}): {e.text if hasattr(e, 'text') else e}")
        return "failed"

    try:
        if view is not None:
            await user.send(view=view)
        else:
            await user.send(content)
        return "sent"
    except discord.Forbidden as e:
        logger.warning(f"[JellyfinRegistryCommands] {kind} failed for {discord_id}: Forbidden (status {e.status}, code {e.code}) — {e.text}.")
        return "unreachable" if e.code == DM_BLOCKED_ERROR_CODE else "failed"
    except discord.HTTPException as e:
        logger.warning(f"[JellyfinRegistryCommands] {kind} failed for {discord_id}: {type(e).__name__} (status {e.status}, code {e.code}) — {e.text}.")
        return "failed"


async def _dm_user(bot, discord_id: int, content: str) -> bool:
    """Bool wrapper over _send_dm for callers that only need sent/not-sent."""
    return (await _send_dm(bot, discord_id, content=content)) == "sent"


async def _dm_user_view(bot, discord_id: int, view: discord.ui.LayoutView) -> bool:
    """Same as _dm_user, but sends a LayoutView (used by /jellyfin)."""
    return (await _send_dm(bot, discord_id, view=view)) == "sent"


class _ActionLogBatcher:
    """Collects action-log lines and posts them as combined messages at
    most once per ACTION_LOG_FLUSH_SECONDS, packing as many lines as fit
    under Discord's 2000-character limit into each message.

    The log channel allows roughly 5 messages per 5 seconds. Posting one
    message per event made every batch operation (Delete-all, scheduled
    reminders, a prune) queue behind that limit; batching turns 50 events
    into one or two sends, and callers never wait on the network at all.

    Mentions render but never ping (allowed_mentions=none)."""

    def __init__(self):
        self._lines: list[str] = []
        self._bot = None
        self._task: asyncio.Task | None = None

    def add(self, bot, text: str):
        self._bot = bot
        self._lines.append(text)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def _run(self):
        # Keeps cycling while lines keep arriving, including lines added
        # while a flush was mid-send.
        while self._lines:
            await asyncio.sleep(ACTION_LOG_FLUSH_SECONDS)
            await self.flush()

    async def flush(self):
        if not self._lines or self._bot is None:
            return
        lines, self._lines = self._lines, []

        channel = self._bot.get_channel(LOG_CHANNEL_ID)
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel, discord.Thread)):
            return

        chunks: list[str] = []
        current = ""
        for line in lines:
            if len(line) > MESSAGE_CONTENT_LIMIT:
                line = line[: MESSAGE_CONTENT_LIMIT - 1] + "…"
            candidate = f"{current}\n{line}" if current else line
            if len(candidate) > MESSAGE_CONTENT_LIMIT:
                chunks.append(current)
                current = line
            else:
                current = candidate
        if current:
            chunks.append(current)

        for chunk in chunks:
            try:
                await channel.send(chunk, allowed_mentions=discord.AllowedMentions.none())
            except Exception as e:
                logger.warning(f"[JellyfinRegistryCommands] Action log failed: {type(e).__name__}: {e}")

    async def close(self):
        """Cancels the pending timer and posts whatever is queued."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        await self.flush()


_action_log = _ActionLogBatcher()


async def _log_action(bot, text: str):
    """Best-effort action log to LOG_CHANNEL_ID. Queues the line and
    returns immediately; never raises, never blocks the action it logs."""
    try:
        _action_log.add(bot, text)
    except Exception as e:
        logger.warning(f"[JellyfinRegistryCommands] Action log queue failed: {type(e).__name__}: {e}")


async def _member_presence(bot, discord_id: int) -> bool | None:
    """True = confirmed in TARGET_GUILD_ID, False = confirmed NOT in it
    (fetch_member 404), None = couldn't tell (guild unavailable, API
    error). Callers must never treat None as 'gone'."""
    guild = bot.get_guild(TARGET_GUILD_ID)
    if guild is None:
        return None
    if guild.get_member(discord_id) is not None:
        return True
    try:
        await guild.fetch_member(discord_id)
        return True
    except discord.NotFound:
        return False
    except Exception as e:
        logger.warning(f"[JellyfinRegistryCommands] Couldn't verify membership for {discord_id}: {type(e).__name__}: {e}")
        return None


async def _delete_left_user_account(bot, discord_id: int, jellyfin_user_id: str, actor_text: str, role_reason: str) -> str:
    """Shared core for deleting a leaver's account — used by instant
    auto-delete (on_member_remove) and by the Delete-all button. Returns
    'deleted', 'already_gone', or 'failed'.

    On 'failed' NOTHING local is touched: the left_users row stays queued
    so the button can retry it. On success, the link, the left_users row
    and the dashboard cache entry are cleared, the linked role is removed
    if they're somehow still a cached member, and the action is logged.
    Does NOT request a dashboard refresh — the caller does, once."""
    try:
        result = await _delete_jellyfin_user(jellyfin_user_id)
    except Exception as e:
        logger.error(
            f"[JellyfinRegistryCommands] Left-user deletion failed for <@{discord_id}> "
            f"(Jellyfin `{jellyfin_user_id}`): {type(e).__name__}: {e}",
            exc_info=True,
        )
        return "failed"

    if result == "already_gone":
        logger.warning(
            f"[JellyfinRegistryCommands] Left-user cleanup: <@{discord_id}>'s Jellyfin account "
            f"`{jellyfin_user_id}` was already gone — clearing stale tracking only."
        )

    await _purge_account_rows(jellyfin_user_id, discord_id)  # link + queue row, one commit

    guild = bot.get_guild(TARGET_GUILD_ID)
    if guild is not None:
        role = guild.get_role(LINKED_ROLE_ID)
        member = guild.get_member(discord_id)
        if role is not None and member is not None and role in member.roles:
            try:
                await member.remove_roles(role, reason=role_reason)
            except (discord.Forbidden, discord.HTTPException):
                pass

    dashboard_cog = bot.get_cog("JellyfinRegistryDashboard")
    if dashboard_cog is not None:
        dashboard_cog._member_cache.pop(discord_id, None)

    action_word = "Deleted" if result == "deleted" else "Cleaned up (already gone)"
    await _log_action(
        bot,
        f"🗑️ **{action_word} (left server)** — <@{discord_id}> — Jellyfin `{jellyfin_user_id}` — {actor_text}.",
    )
    return result


def _build_deletion_preview_file(bot, rows: list[dict], skipped_exempt: int) -> discord.File:
    """Full Preview Deletions list as a .txt attachment — used when the
    list is too long for a 2000-character message. Tab-separated so it
    pastes cleanly into a spreadsheet. Uses only cached Discord user data
    (no API calls), matching the preview's 'touches nothing' contract."""
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    out = [
        f"Deletion preview — generated {generated}",
        f"Would delete: {len(rows)} | Exempt (skipped): {skipped_exempt}",
        "Nothing has been deleted. This is a snapshot; the queue can change before Delete is pressed.",
        "",
        "discord_id\tdiscord_user\tjellyfin_user_id\tleft_at_utc",
    ]
    for row in rows:
        user = bot.get_user(row["discord_id"])
        user_str = str(user) if user is not None else "unknown (not cached)"
        out.append(f"{row['discord_id']}\t{user_str}\t{row['jellyfin_user_id']}\t{row.get('left_at') or 'unknown'}")
    data = "\n".join(out).encode("utf-8")
    return discord.File(io.BytesIO(data), filename="deletion_preview.txt")


async def _issue_token_and_post(bot, guild_id: int, target_id: int, issued_by: int) -> tuple[bool, str]:
    """Shared core of "grant a token": inserts the DB row, posts the token
    message to TOKEN_CHANNEL_ID, records the message_id, and DMs the
    target. Used by both the /manage Grant select and auto-token-on-join,
    so the two paths can never drift.

    Returns (success, note) — note is a warning string to surface to
    whoever triggered it ("" if everything went cleanly)."""
    issued = await _issue_token(target_id, issued_by)
    if not issued:
        return False, "already has a pending token or is already linked"

    channel = bot.get_channel(TOKEN_CHANNEL_ID)
    if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
        await _delete_token(target_id)
        return False, "couldn't find the signup channel"

    expires_ts = int(time.time()) + TOKEN_EXPIRY_HOURS * 3600
    token_view = TokenMessageView(_token_pending_text(target_id, expires_ts), avatar_url=await _avatar_url(bot, target_id))
    try:
        msg = await channel.send(view=token_view)
    except discord.HTTPException as e:
        # Without this, the row would sit with no message (a ghost token
        # the user can't use) until the next reboot's ghost recovery.
        await _delete_token(target_id)
        logger.error(f"[JellyfinRegistryCommands] Couldn't post token message for {target_id}: {type(e).__name__}: {e}")
        return False, "couldn't post the signup message"
    await _set_token_message_id(target_id, msg.id)

    message_link = f"https://discord.com/channels/{guild_id}/{channel.id}/{msg.id}"
    dm_sent = await _dm_user(
        bot, target_id,
        "You've been granted a token to sign up for the UsherEats Jellyfin media server!\n"
        f"Click here to sign up: {message_link}\n"
        "This token will expire in **24 hours**."
    )

    await _log_action(
        bot,
        f"🎫 **Token issued** — <@{target_id}> by <@{issued_by}>." + ("" if dm_sent else " ⚠️ DM failed (closed DMs).")
    )

    return True, "" if dm_sent else "couldn't DM them (their DMs may be closed)"


async def _reconcile_stale_tokens_on_startup(bot) -> None:
    """Runs once after startup. Catches a signup that fully succeeded
    (link exists) but whose token row survived because the bot restarted
    before the token deletion at the end of SignupModal.on_submit."""
    tokens = await _get_all_tokens()
    if not tokens:
        return

    fixed = 0

    for token in tokens:
        discord_id = token["discord_id"]
        link = await _get_link_by_discord_id(discord_id)
        if link is None:
            continue  # genuinely still pending — nothing to reconcile

        username = link["jellyfin_user_id"]
        jf_user = await _fetch_jellyfin_user(link["jellyfin_user_id"])
        if jf_user:
            username = jf_user.get("Name", username)

        await _edit_token_message(bot, token.get("message_id"), discord_id, _token_signed_up_text(discord_id, username, _linked_at_ts(link.get("linked_at"))))
        await _delete_token(discord_id)
        fixed += 1

    if fixed:
        logger.info(f"[JellyfinRegistryCommands] Startup reconciliation fixed {fixed} stale token message(s) from an interrupted signup.")
        await _log_action(bot, f"🔧 **Startup reconcile (stale tokens)** — fixed {fixed}.")


async def _finish_linking_recovered_account(bot, discord_id: int, jellyfin_user_id: str, username: str, channel) -> None:
    """Shared final steps once reconciliation has confirmed an account
    exists, belongs to this crashed signup, AND its owner is still in the
    server — grant access, link, assign role, fix the token message, DM
    the success message, clean up."""
    try:
        await _grant_full_library_access(jellyfin_user_id)
    except Exception as e:
        logger.warning(f"[JellyfinRegistryCommands] Pending-signup reconcile: couldn't grant full access to recovered account for {discord_id}: {type(e).__name__}: {e}")

    await _commit_signup_link(discord_id, jellyfin_user_id, discord_id)  # link + marker cleared together

    commands_cog = bot.get_cog("JellyfinRegistryCommands")
    if commands_cog is not None:
        await commands_cog._assign_linked_role(discord_id)

    token = await _get_token(discord_id)
    if token:
        await _edit_token_message(bot, token.get("message_id"), discord_id, _token_signed_up_text(discord_id, username))
        await _delete_token(discord_id)

    signup_message = _build_signup_success_message()
    await _dm_user(bot, discord_id, signup_message)


async def _discard_recovered_account_for_departed(bot, discord_id: int, jellyfin_user_id: str, username: str) -> bool:
    """Reconciliation found the orphaned account, but its owner has since
    LEFT the server. Linking it would hand an account to someone the
    leave rule says must not have one — so it's deleted instead, the same
    outcome instant auto-delete would have produced if the link had
    existed when they left. Returns False (marker kept for the next pass)
    if the deletion itself fails."""
    try:
        result = await _delete_jellyfin_user(jellyfin_user_id)
    except Exception as e:
        logger.error(
            f"[JellyfinRegistryCommands] Pending-signup reconcile: <@{discord_id}> left, but deleting their recovered "
            f"account `{jellyfin_user_id}` failed — keeping marker for next pass: {type(e).__name__}: {e}"
        )
        return False

    token = await _get_token(discord_id)
    if token:
        await _edit_token_message(bot, token.get("message_id"), discord_id, _token_left_text(discord_id))
        await _delete_token(discord_id)

    await _clear_pending_signup(discord_id)
    outcome = "deleted" if result == "deleted" else "was already gone"
    await _log_action(
        bot,
        f"🗑️ **Pending-signup reconcile** — <@{discord_id}> left before their interrupted signup was recovered; "
        f"Jellyfin account **{username}** (`{jellyfin_user_id}`) {outcome} instead of being linked.",
    )
    return True


async def _reconcile_pending_signups(bot) -> None:
    """Recovers from a crash between _create_jellyfin_user succeeding and
    _add_link being called. Runs at startup AND periodically
    (pending_signup_recheck_loop).

    Two lookup paths, in order of trust:

    1. PRECISE (jellyfin_user_id was recorded) — checks that exact
       account by ID with _fetch_jellyfin_user_strict: a confirmed 404 is
       safe to clear; any other failure leaves the marker for later.

    2. FALLBACK (no ID recorded) — name search. A miss is FLAGGED, not
       cleared; only a second consecutive miss clears it.

    When an account IS found, the owner's membership is checked before
    linking: still present → link as normal; confirmed gone → delete the
    account instead (_discard_recovered_account_for_departed); can't tell
    → leave the marker and retry next pass."""
    pending = await _get_all_pending_signups()
    if not pending:
        return

    channel = bot.get_channel(TOKEN_CHANNEL_ID)
    recovered = 0
    recovered_ids = []
    discarded = 0
    cleared = 0
    flagged_this_pass = 0

    jf_users_cache = None

    async def _get_jf_users_cached():
        nonlocal jf_users_cache
        if jf_users_cache is None:
            jf_users_cache = await _fetch_jellyfin_users()
        return jf_users_cache

    async def _resolve_found_account(discord_id: int, jellyfin_user_id: str, username: str, via: str) -> None:
        nonlocal recovered, discarded
        presence = await _member_presence(bot, discord_id)
        if presence is None:
            logger.warning(
                f"[JellyfinRegistryCommands] Pending-signup reconcile: found account `{jellyfin_user_id}` for "
                f"<@{discord_id}> but couldn't confirm their membership — leaving marker for next pass."
            )
            return
        if presence is False:
            if await _discard_recovered_account_for_departed(bot, discord_id, jellyfin_user_id, username):
                discarded += 1
            return
        await _finish_linking_recovered_account(bot, discord_id, jellyfin_user_id, username, channel)
        recovered += 1
        recovered_ids.append(discord_id)
        if via == "name":
            logger.warning(
                f"[JellyfinRegistryCommands] Pending-signup reconcile: recovered <@{discord_id}> via NAME FALLBACK "
                f"(no ID had been recorded for this marker — should be rare)."
            )
        else:
            logger.info(f"[JellyfinRegistryCommands] Pending-signup reconcile: recovered <@{discord_id}> via recorded ID `{jellyfin_user_id}` (precise path).")

    for row in pending:
        discord_id = row["discord_id"]
        username = row["jellyfin_username"]
        recorded_jellyfin_id = row.get("jellyfin_user_id")
        already_flagged = bool(row.get("flagged"))

        if await _get_link_by_discord_id(discord_id):
            await _clear_pending_signup(discord_id)
            cleared += 1
            continue

        if recorded_jellyfin_id:
            # --- PRECISE PATH ---
            try:
                jf_user = await _fetch_jellyfin_user_strict(recorded_jellyfin_id)
            except Exception as e:
                logger.error(
                    f"[JellyfinRegistryCommands] Pending-signup reconcile: couldn't verify recorded account "
                    f"`{recorded_jellyfin_id}` for <@{discord_id}> (username='{username}') — Jellyfin unreachable, "
                    f"leaving marker in place for next pass: {type(e).__name__}: {e}"
                )
                continue

            if jf_user is not None:
                await _resolve_found_account(discord_id, recorded_jellyfin_id, username, via="id")
            else:
                await _clear_pending_signup(discord_id)
                cleared += 1
            continue

        # --- FALLBACK PATH (no jellyfin_user_id was ever recorded) ---
        try:
            jf_users = await _get_jf_users_cached()
        except Exception as e:
            logger.error(
                f"[JellyfinRegistryCommands] Pending-signup reconcile: couldn't reach Jellyfin for fallback "
                f"name-search on <@{discord_id}> (username='{username}') — leaving marker in place for next pass: "
                f"{type(e).__name__}: {e}"
            )
            continue

        match = next((u for u in jf_users if u.get("Name", "").lower() == username.lower()), None)
        if match is not None:
            await _resolve_found_account(discord_id, match["Id"], username, via="name")
            continue

        if already_flagged:
            logger.info(
                f"[JellyfinRegistryCommands] Pending-signup reconcile: <@{discord_id}> (username='{username}') "
                f"confirmed empty after a repeat check — clearing."
            )
            await _clear_pending_signup(discord_id)
            cleared += 1
        else:
            await _flag_pending_signup(discord_id)
            flagged_this_pass += 1
            logger.warning(
                f"[JellyfinRegistryCommands] ⚠️ Pending signup for <@{discord_id}> (username='{username}') has no "
                f"recorded account ID and no name match found on Jellyfin — flagging for review, will re-check "
                f"next pass rather than assuming nothing was created."
            )

    commands_cog = bot.get_cog("JellyfinRegistryCommands")
    if recovered and commands_cog is not None:
        await commands_cog._notify_dashboard(member_ids=recovered_ids)

    if recovered or discarded or cleared or flagged_this_pass:
        summary = (
            f"[JellyfinRegistryCommands] Pending-signup reconcile: recovered {recovered}, discarded {discarded} "
            f"(owner left), cleared {cleared}, flagged {flagged_this_pass} ambiguous marker(s)."
        )
        if flagged_this_pass:
            logger.warning(summary)
        else:
            logger.info(summary)
        await _log_action(
            bot,
            f"🔧 **Pending-signup reconcile** — recovered {recovered}, discarded {discarded} (owner left), "
            f"cleared {cleared}, flagged {flagged_this_pass}.",
        )


async def _reconcile_ghost_tokens_on_startup(bot) -> None:
    """Recovers from a crash between _issue_token's DB insert and the
    channel.send()/_set_token_message_id that follows it — a token row
    that was never posted. Already linked or already expired → dropped.
    Otherwise the message is posted now with the original expiry and the
    user is DMed the link."""
    all_tokens = await _get_all_tokens()
    tokens = [t for t in all_tokens if t.get("message_id") is None]
    if not tokens:
        return

    guild = bot.get_guild(TARGET_GUILD_ID)
    channel = bot.get_channel(TOKEN_CHANNEL_ID)
    fixed = 0
    dropped = 0

    for token in tokens:
        discord_id = token["discord_id"]

        if await _get_link_by_discord_id(discord_id):
            await _delete_token(discord_id)
            dropped += 1
            continue

        if _is_token_expired(token):
            await _delete_token(discord_id)
            dropped += 1
            continue

        if guild is None or not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            continue  # can't fix yet — retry on next boot

        expires_ts = _token_expires_ts(token)
        token_view = TokenMessageView(_token_pending_text(discord_id, expires_ts), avatar_url=await _avatar_url(bot, discord_id))
        try:
            msg = await channel.send(view=token_view)
        except discord.HTTPException as e:
            logger.error(f"[JellyfinRegistryCommands] Ghost-token reconcile: couldn't post recovery message for {discord_id}: {type(e).__name__}: {e}")
            continue

        await _set_token_message_id(discord_id, msg.id)

        message_link = f"https://discord.com/channels/{guild.id}/{channel.id}/{msg.id}"
        await _dm_user(
            bot, discord_id,
            "You've been granted a token to sign up for the UsherEats Jellyfin media server!\n"
            f"Click here to sign up: {message_link}\n"
            "This token will expire in **24 hours**."
        )
        fixed += 1

    if fixed or dropped:
        logger.info(f"[JellyfinRegistryCommands] Ghost-token reconcile: recovered {fixed}, dropped {dropped}.")
        await _log_action(bot, f"🔧 **Startup reconcile (ghost tokens)** — recovered {fixed}, dropped {dropped}.")


async def _backfill_left_users(bot) -> None:
    """One-time (per BACKFILL_LEFT_USERS_ON_STARTUP=True boot) scan: every
    `links` row is checked against actual guild membership, and anyone
    no longer in TARGET_GUILD_ID is QUEUED in `left_users` — never
    auto-deleted; the Delete button handles them. Rows already present
    are left untouched, so this is safe to run repeatedly.

    Uses fetch_member as a fallback before concluding someone is gone —
    a cold member cache is not the same thing as having left."""
    guild = bot.get_guild(TARGET_GUILD_ID)
    if guild is None:
        logger.warning("[JellyfinRegistryCommands] Backfill skipped: bot is not in TARGET_GUILD_ID.")
        return

    links = await _get_all_links()
    added = 0

    for link in links:
        discord_id = link["discord_id"]

        if await _get_left_user(discord_id):
            continue

        member = guild.get_member(discord_id)
        if member is None:
            try:
                member = await guild.fetch_member(discord_id)
            except discord.NotFound:
                member = None
            except Exception as e:
                logger.warning(f"[JellyfinRegistryCommands] Backfill: couldn't verify membership for {discord_id}: {type(e).__name__}: {e}")
                continue

        if member is not None:
            continue

        await _add_left_user(discord_id, link["jellyfin_user_id"])
        added += 1

    if added:
        logger.info(f"[JellyfinRegistryCommands] Backfill: found {added} linked user(s) already gone from the server, queued in left_users.")
        await _log_action(bot, f"🔧 **Startup backfill** — found {added} linked user(s) already gone, queued in left_users.")
    else:
        logger.info("[JellyfinRegistryCommands] Backfill: no missing linked users found.")


# Discord IDs with an account creation currently running (signup or admin
# Register). Process-local, which is all that's needed: one bot process.
_signups_in_flight: set[int] = set()


class SignupModal(discord.ui.Modal, title="Create your Jellyfin account"):
    def __init__(self, discord_id: int, message_id: int, channel_id: int):
        super().__init__()
        self.discord_id = discord_id
        self.message_id = message_id
        self.channel_id = channel_id
        self.username_input = discord.ui.TextInput(label="Username", placeholder="Your desired Jellyfin username", required=True, max_length=64)
        self.password_input = discord.ui.TextInput(label="Password", placeholder="Your desired Jellyfin password", required=True, max_length=128)
        self.add_item(self.username_input)
        self.add_item(self.password_input)

    async def on_submit(self, interaction: discord.Interaction):
        # Claimed synchronously (no await between check and add), so two
        # modals submitted together can never both pass the token check
        # and create two Jellyfin accounts.
        if self.discord_id in _signups_in_flight:
            return await interaction.response.send_message(view=_text_view("⏳ Your signup is already being processed."), ephemeral=True)
        _signups_in_flight.add(self.discord_id)
        try:
            await self._process(interaction)
        finally:
            _signups_in_flight.discard(self.discord_id)

    async def _process(self, interaction: discord.Interaction):
        # Token must still exist — a submit after a completed signup finds it gone.
        token = await _get_token(self.discord_id)
        if token is None:
            return await interaction.response.send_message(view=_text_view("❌ Your token has already been used or was removed."), ephemeral=True)

        if _is_token_expired(token):
            await _delete_token(self.discord_id)
            await _edit_token_message(interaction.client, self.message_id, self.discord_id, _token_expired_text(self.discord_id))
            return await interaction.response.send_message(view=_text_view("❌ Your token expired while you were filling this out. Ask an admin for a new token."), ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        username = self.username_input.value.strip()
        password = self.password_input.value

        try:
            existing_jf_users = await _get_jellyfin_users_cached()
        except Exception as e:
            return await interaction.followup.send(view=_text_view(f"❌ Couldn't reach Jellyfin to check for a duplicate username: `{type(e).__name__}: {e}`"), ephemeral=True)
        if any(u.get("Name", "").lower() == username.lower() for u in existing_jf_users):
            return await interaction.followup.send(view=_text_view(f"❌ A Jellyfin account named **{username}** already exists — pick a different username."), ephemeral=True)

        # Marker written BEFORE account creation — lets reconciliation find
        # an account orphaned by a crash before _add_link below.
        await _set_pending_signup(self.discord_id, username)

        try:
            new_user = await _create_jellyfin_user(username, password)
        except Exception as e:
            await _clear_pending_signup(self.discord_id)  # nothing was created — safe to clear
            return await interaction.followup.send(view=_text_view(f"❌ Couldn't create the Jellyfin account: `{type(e).__name__}: {e}`"), ephemeral=True)

        jellyfin_user_id = new_user.get("Id")
        # Recorded IMMEDIATELY so reconciliation knows the exact account by ID.
        await _mark_pending_signup_created(self.discord_id, jellyfin_user_id)

        try:
            await _grant_full_library_access(jellyfin_user_id)
        except Exception as e:
            await interaction.followup.send(
                view=_text_view(
                    f"⚠️ Account **{username}** created but granting full library access failed: `{type(e).__name__}: {e}`. "
                    "It's still linked — the owner can fix access manually."
                ),
                ephemeral=True,
            )

        await _commit_signup_link(self.discord_id, jellyfin_user_id, self.discord_id)
        # Token deletion happens at the END, after the message edit —
        # deliberately, so _reconcile_stale_tokens_on_startup can detect
        # and fix a signup interrupted between here and there.

        try:
            await _edit_token_message(interaction.client, self.message_id, self.discord_id, _token_signed_up_text(self.discord_id, username))
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Post-signup token message edit failed for {self.discord_id}: {type(e).__name__}: {e}", exc_info=True)

        # Token deletion stops the button being pressable again — MUST run.
        await _delete_token(self.discord_id)

        signup_message = _build_signup_success_message()
        dm_sent = await _dm_user(interaction.client, self.discord_id, signup_message)
        try:
            await interaction.followup.send(view=_text_view(signup_message), ephemeral=True)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Final signup confirmation failed to send for {self.discord_id}: {type(e).__name__}: {e}", exc_info=True)

        # Best-effort housekeeping below — runs after the user has already
        # been told they're signed up; each step individually guarded.

        try:
            commands_cog = interaction.client.get_cog("JellyfinRegistryCommands")
            if commands_cog is not None:
                await commands_cog._assign_linked_role(self.discord_id)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Post-signup role assignment failed for {self.discord_id}: {type(e).__name__}: {e}", exc_info=True)

        commands_cog = interaction.client.get_cog("JellyfinRegistryCommands")
        if commands_cog is not None:
            await commands_cog._notify_dashboard(discord_id=self.discord_id)

        await _log_action(
            interaction.client,
            f"✅ **Signup completed** — <@{self.discord_id}> created Jellyfin account **{username}** "
            f"(`{jellyfin_user_id}`)." + ("" if dm_sent else " ⚠️ DM failed (closed DMs).")
        )


class SignupActionRow(discord.ui.ActionRow):
    """A single button with a fixed custom_id — what makes it a valid
    persistent-view target."""

    @discord.ui.button(label="Sign up", style=discord.ButtonStyle.success, custom_id="jellyfin_signup_button")
    async def signup_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        token = await _get_token_by_message_id(interaction.message.id)
        if token is None:
            return await interaction.response.send_message(view=_text_view("❌ This token is no longer valid."), ephemeral=True)
        if interaction.user.id != token["discord_id"]:
            return await interaction.response.send_message(view=_text_view("❌ This token isn't yours."), ephemeral=True)

        if _is_token_expired(token):
            await _delete_token(token["discord_id"])
            try:
                await interaction.message.edit(view=TokenMessageView(
                    _token_expired_text(token["discord_id"]),
                    include_button=False,
                    avatar_url=_avatar_from(interaction.user),  # clicker is verified as the owner above
                ))
            except discord.HTTPException:
                pass
            return await interaction.response.send_message(view=_text_view("❌ This token has expired."), ephemeral=True)

        await interaction.response.send_modal(
            SignupModal(token["discord_id"], interaction.message.id, interaction.channel.id)
        )


class TokenMessageView(discord.ui.LayoutView):
    """The token-channel card (Components V2):

        Container
          Section: TOKEN_CARD_HEADER  [owner's avatar]
          Separator (divider)
          TextDisplay: `text` — the per-state line
        ActionRow: Sign up            (only while pending)

    Every state (pending, signed up, expired, revoked, left, unreachable)
    is this same card with a different `text`; final states pass
    include_button=False so the button row disappears entirely.

    Registered ONCE as a persistent view in cog_load with placeholder
    text — only the button's custom_id needs to match on restart."""
    def __init__(self, text: str, include_button: bool = True, avatar_url: str | None = None):
        super().__init__(timeout=None)
        container = discord.ui.Container()
        container.add_item(discord.ui.Section(
            discord.ui.TextDisplay(TOKEN_CARD_HEADER),
            accessory=discord.ui.Thumbnail(media=avatar_url or DEFAULT_AVATAR_URL),
        ))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay(text or "\u200b"))
        self.add_item(container)
        if include_button:
            self.add_item(SignupActionRow())


# --- ACCOUNT SELF-SERVICE (/jellyfin) ---
# A linked user runs /jellyfin -> gets DMed an account summary with Reset
# Password and Change Username buttons. Both buttons use fixed custom_ids
# and are kept routable across restarts by registering a placeholder
# JellyfinAccountDMView in cog_load.

class ResetPasswordModal(discord.ui.Modal, title="Reset your Jellyfin password"):
    def __init__(self, jellyfin_user_id: str):
        super().__init__()
        self.jellyfin_user_id = jellyfin_user_id
        self.new_password_input = discord.ui.TextInput(
            label="New Password", placeholder="Your new Jellyfin password", required=True, max_length=128,
        )
        self.confirm_password_input = discord.ui.TextInput(
            label="Confirm New Password", placeholder="Type it again", required=True, max_length=128,
        )
        self.add_item(self.new_password_input)
        self.add_item(self.confirm_password_input)

    async def on_submit(self, interaction: discord.Interaction):
        new_pw = self.new_password_input.value
        confirm_pw = self.confirm_password_input.value

        if new_pw != confirm_pw:
            return await interaction.response.send_message("❌ Those passwords don't match — press **Reset Password** again and try once more.", ephemeral=True)

        # Re-check the link at submit time, not just at click time.
        link = await _get_link_by_discord_id(interaction.user.id)
        if link is None or link["jellyfin_user_id"] != self.jellyfin_user_id:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account anymore.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        try:
            await _set_jellyfin_password(self.jellyfin_user_id, new_pw)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Password reset failed for <@{interaction.user.id}>: {type(e).__name__}: {e}", exc_info=True)
            return await interaction.followup.send(f"❌ Couldn't reset your password: `{type(e).__name__}: {e}`", ephemeral=True)

        await interaction.followup.send("✅ Your Jellyfin password has been reset.", ephemeral=True)
        await _log_action(interaction.client, f"🔑 **Password reset** — <@{interaction.user.id}> reset their own Jellyfin password via `/jellyfin`.")


class ResetPasswordActionRow(discord.ui.ActionRow):
    """Fixed custom_id so the button on an already-sent DM keeps working
    after a restart. Re-checks the link fresh on click."""

    @discord.ui.button(label="Reset Password", style=discord.ButtonStyle.primary, custom_id="jellyfin_reset_password_button", emoji="🔑")
    async def reset_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        link = await _get_link_by_discord_id(interaction.user.id)
        if link is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)
        await interaction.response.send_modal(ResetPasswordModal(link["jellyfin_user_id"]))


class ResetPasswordDMView(discord.ui.LayoutView):
    """Legacy: the standalone DM the retired /reset command used to send.
    Not sent anymore — /jellyfin's JellyfinAccountDMView carries the same
    ResetPasswordActionRow. Kept so any old /reset DM still in someone's
    inbox has a matching class; routing itself is covered by the
    JellyfinAccountDMView placeholder registered in cog_load."""
    def __init__(self, text: str):
        super().__init__(timeout=None)
        container = discord.ui.Container()
        container.add_item(discord.ui.TextDisplay(text))
        self.add_item(container)
        self.add_item(ResetPasswordActionRow())


class ChangeUsernameModal(discord.ui.Modal, title="Change your Jellyfin username"):
    def __init__(self, jellyfin_user_id: str, current_username: str):
        super().__init__()
        self.jellyfin_user_id = jellyfin_user_id
        self.current_username = current_username
        self.new_username_input = discord.ui.TextInput(
            label="New Username", placeholder=f"Currently: {current_username}", required=True, max_length=64,
        )
        self.add_item(self.new_username_input)

    async def on_submit(self, interaction: discord.Interaction):
        new_username = self.new_username_input.value.strip()

        link = await _get_link_by_discord_id(interaction.user.id)
        if link is None or link["jellyfin_user_id"] != self.jellyfin_user_id:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account anymore.", ephemeral=True)

        if new_username.lower() == self.current_username.lower():
            return await interaction.response.send_message("❌ That's already your current username.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        try:
            existing_jf_users = await _get_jellyfin_users_cached()
        except Exception as e:
            return await interaction.followup.send(f"❌ Couldn't reach Jellyfin to check for a duplicate username: `{type(e).__name__}: {e}`", ephemeral=True)
        if any(u.get("Name", "").lower() == new_username.lower() for u in existing_jf_users):
            return await interaction.followup.send(f"❌ `{new_username}` is already taken — pick a different username.", ephemeral=True)

        try:
            await _rename_jellyfin_user(self.jellyfin_user_id, new_username)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] /jellyfin username change failed for <@{interaction.user.id}>: {type(e).__name__}: {e}", exc_info=True)
            return await interaction.followup.send(f"❌ Couldn't change your username: `{type(e).__name__}: {e}`", ephemeral=True)

        await interaction.followup.send(
            f"✅ Your Jellyfin username is now **{new_username}**. Your watch history, library access, and everything "
            "else stayed exactly the same — only the display name changed.",
            ephemeral=True,
        )
        await _log_action(
            interaction.client,
            f"✏️ **Username changed** — <@{interaction.user.id}> renamed their Jellyfin account from "
            f"**{self.current_username}** to **{new_username}** via `/jellyfin`.",
        )


class ChangeUsernameActionRow(discord.ui.ActionRow):
    """Fixed custom_id for restart persistence. Re-checks the link AND
    re-fetches the current username on every click."""

    @discord.ui.button(label="Change Username", style=discord.ButtonStyle.secondary, custom_id="jellyfin_change_username_button", emoji="✏️")
    async def change_username_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        link = await _get_link_by_discord_id(interaction.user.id)
        if link is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)

        jf_user = await _fetch_jellyfin_user(link["jellyfin_user_id"])
        current_username = jf_user.get("Name", "Unknown") if jf_user else "Unknown"

        await interaction.response.send_modal(ChangeUsernameModal(link["jellyfin_user_id"], current_username))


# --- MY DEVICES (button on the /jellyfin DM) ---

MAX_LISTED_DEVICES = 25  # Discord's select-menu option limit


def _device_label(device: dict) -> str:
    return (device.get("Name") or "Unknown device")[:100]


def _device_app(device: dict) -> str:
    return " ".join(filter(None, [device.get("AppName"), device.get("AppVersion")])) or "Unknown app"


class DevicesView(discord.ui.LayoutView):
    """A user's signed-in devices, sent into their DM. Built fresh on every
    open and after every action, so it always reflects Jellyfin's current
    list. Every action re-checks the Discord->Jellyfin link and that the
    device really belongs to this account before touching anything.

    confirm_all=True swaps the picker for a "sign out everything?"
    confirmation; status is a one-line result shown at the top."""

    def __init__(self, owner_id: int, jellyfin_user_id: str, devices: list[dict],
                 status: str | None = None, confirm_all: bool = False):
        super().__init__(timeout=600)
        self.owner_id = owner_id
        self.jellyfin_user_id = jellyfin_user_id
        self.devices = devices[:MAX_LISTED_DEVICES]
        self.message: discord.Message | None = None

        container = discord.ui.Container()
        header = "-# YOUR JELLYFIN\n# 📱 Signed-in devices"
        if status:
            header += f"\n{status}"
        container.add_item(discord.ui.TextDisplay(header))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

        if not self.devices:
            container.add_item(discord.ui.TextDisplay("No devices are signed into your account."))
            self.add_item(container)
            return

        lines = []
        for device in self.devices:
            ts = _parse_jellyfin_date(device.get("DateLastActivity"))
            shared = " · 👥 shared — sign out on the device itself" if device.get("_shared") else ""
            lines.append(f"**{_device_label(device)}** · {_device_app(device)}\n-# Last active {_format_ts(ts)}{shared}")
        container.add_item(discord.ui.TextDisplay("\n".join(lines)))
        self.add_item(container)

        if confirm_all:
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
            container.add_item(discord.ui.TextDisplay(
                f"⚠️ Sign out **all {len(self.devices)}** devices? Every app — including the one you're using — will need to log in again."
            ))
            row = discord.ui.ActionRow()
            confirm = discord.ui.Button(label="Yes, sign out all", style=discord.ButtonStyle.danger)
            confirm.callback = self._confirm_all
            cancel = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
            cancel.callback = self._cancel_all
            row.add_item(confirm)
            row.add_item(cancel)
            self.add_item(row)
            return

        select_row = discord.ui.ActionRow()
        select = discord.ui.Select(
            placeholder="Sign out a device…",
            options=[
                # Value is the device's POSITION in this view's list, not its
                # Jellyfin Id: some apps use device Ids longer than the 100
                # characters Discord allows for an option value, which made
                # Discord reject the whole menu. The callback maps the
                # position back to the real Id.
                discord.SelectOption(
                    label=_device_label(d),
                    description=_device_app(d)[:100],
                    value=str(index),
                )
                for index, d in enumerate(self.devices)
            ],
        )
        select.callback = self._sign_out_one
        select_row.add_item(select)
        self.add_item(select_row)

        button_row = discord.ui.ActionRow()
        all_button = discord.ui.Button(label="Sign out all devices", style=discord.ButtonStyle.danger)
        all_button.callback = self._ask_all
        button_row.add_item(all_button)
        self.add_item(button_row)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.owner_id

    async def _still_linked(self, interaction: discord.Interaction) -> bool:
        link = await _get_link_by_discord_id(self.owner_id)
        if link is None or link["jellyfin_user_id"] != self.jellyfin_user_id:
            self.stop()
            await interaction.edit_original_response(view=_text_view("❌ You don't have a linked Jellyfin account anymore."))
            return False
        return True

    async def _refresh(self, interaction: discord.Interaction, status: str | None = None):
        try:
            devices = await _get_user_devices(self.jellyfin_user_id)
        except Exception:
            devices = self.devices
            status = (status + " " if status else "") + "-# (couldn't refresh the list)"
        self.stop()
        new_view = DevicesView(self.owner_id, self.jellyfin_user_id, devices, status=status)
        new_view.message = self.message
        await interaction.edit_original_response(view=new_view)

    async def _sign_out_one(self, interaction: discord.Interaction):
        try:
            device_id = self.devices[int(interaction.data["values"][0])]["Id"]
        except (ValueError, IndexError, KeyError):
            return await interaction.response.send_message("❌ That option is out of date — press **My Devices** again.")
        await interaction.response.defer()
        if not await self._still_linked(interaction):
            return

        try:
            current = await _get_user_devices(self.jellyfin_user_id)
        except Exception:
            return await self._refresh(interaction, "❌ Couldn't reach Jellyfin — try again in a minute.")
        device = next((d for d in current if d.get("Id") == device_id), None)
        if device is None:
            return await self._refresh(interaction, "ℹ️ That device is already signed out.")
        if device.get("_shared"):
            return await self._refresh(
                interaction,
                f"🔒 **{_device_label(device)}** is also signed into another account, so it can't be signed out from here — sign out on the device itself.",
            )

        try:
            await _sign_out_device(device_id)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Device sign-out failed for <@{self.owner_id}>: {type(e).__name__}: {e}")
            return await self._refresh(interaction, "❌ Couldn't sign that device out — try again in a minute.")

        name = _device_label(device)
        await _log_action(interaction.client, f"📱 **Device signed out** — <@{self.owner_id}> signed out **{name}** ({_device_app(device)}).")
        await self._refresh(interaction, f"✅ Signed out **{name}**.")

    async def _ask_all(self, interaction: discord.Interaction):
        self.stop()
        new_view = DevicesView(self.owner_id, self.jellyfin_user_id, self.devices, confirm_all=True)
        new_view.message = self.message
        await interaction.response.edit_message(view=new_view)

    async def _cancel_all(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self._refresh(interaction, "Nothing was signed out.")

    async def _confirm_all(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._still_linked(interaction):
            return

        try:
            current = await _get_user_devices(self.jellyfin_user_id)
        except Exception:
            return await self._refresh(interaction, "❌ Couldn't reach Jellyfin — try again in a minute.")

        shared = [d for d in current if d.get("_shared")]
        current = [d for d in current if not d.get("_shared")]
        signed_out = 0
        for device in current:
            try:
                await _sign_out_device(device["Id"])
                signed_out += 1
            except Exception as e:
                logger.error(f"[JellyfinRegistryCommands] Sign-out-all failed on {device.get('Id')} for <@{self.owner_id}>: {type(e).__name__}: {e}")

        failed = len(current) - signed_out
        await _log_action(interaction.client, f"📱 **All devices signed out** — <@{self.owner_id}> signed out {signed_out} device(s).")
        status = f"✅ Signed out **{signed_out}** device(s)." + (f" ⚠️ {failed} failed — try again." if failed else "")
        if shared:
            status += f" 🔒 {len(shared)} shared device(s) skipped — sign out on those devices directly."
        await self._refresh(interaction, status)

    async def on_timeout(self):
        if self.message is not None:
            try:
                await self.message.edit(view=_text_view("-# This device list expired — press **My Devices** again for a fresh one."))
            except discord.HTTPException:
                pass


class DevicesActionRow(discord.ui.ActionRow):
    """Fixed custom_id so the button on an already-sent /jellyfin DM keeps
    working after a restart. Opens a fresh DevicesView every time."""

    @discord.ui.button(label="My Devices", style=discord.ButtonStyle.secondary, custom_id="jellyfin_my_devices_button", emoji="📱")
    async def my_devices(self, interaction: discord.Interaction, button: discord.ui.Button):
        link = await _get_link_by_discord_id(interaction.user.id)
        if link is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)

        await interaction.response.defer()
        try:
            devices = await _get_user_devices(link["jellyfin_user_id"])
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Device list failed for <@{interaction.user.id}>: {type(e).__name__}: {e}")
            return await interaction.followup.send(view=_text_view("❌ Couldn't reach Jellyfin — try again in a minute."))

        view = DevicesView(interaction.user.id, link["jellyfin_user_id"], devices)
        view.message = await interaction.followup.send(view=view, wait=True)


class JellyfinAccountDMView(discord.ui.LayoutView):
    """The DM sent by /jellyfin. Content is baked in at send time; both
    buttons re-fetch live data on click.

    Constructor args all default to placeholders so a bare
    JellyfinAccountDMView() can be registered in cog_load purely to keep
    both buttons' custom_ids routing after a restart."""
    def __init__(
        self,
        username: str = "placeholder",
        library_access: str = "—",
        linked_ts: int | None = None,
        last_login_ts: int | None = None,
        last_activity_ts: int | None = None,
        watch_seconds: int | None = None,
        avatar_url: str | None = None,
    ):
        super().__init__(timeout=None)
        container = discord.ui.Container()

        header = f"-# YOUR JELLYFIN ACCOUNT\n# {username}"
        if avatar_url:
            container.add_item(discord.ui.Section(
                discord.ui.TextDisplay(header),
                accessory=discord.ui.Thumbnail(media=avatar_url),
            ))
        else:
            container.add_item(discord.ui.TextDisplay(header))

        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay(f"🔑 **Username** — `{username}`\n🎬 **Library Access** — {library_access}"))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay(
            f"🔗 **Linked to Discord** — {_format_ts(linked_ts)}\n"
            f"🟢 **Last Login** — {_format_ts(last_login_ts)}\n"
            f"📺 **Last Active** — {_format_ts(last_activity_ts)}"
        ))

        if watch_seconds is not None:
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
            container.add_item(discord.ui.TextDisplay(f"📊 **Total Watch Time** — {_format_duration(watch_seconds)} all-time"))

        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay(f"-# Service: {JELLYFIN_URL}"))

        self.add_item(container)
        self.add_item(ResetPasswordActionRow())
        self.add_item(ChangeUsernameActionRow())
        self.add_item(DevicesActionRow())


# --- /manage PANEL COMPONENTS ---
# Accounts & Tokens section: GrantTokenSelect, RevokeTokenSelect,
#   AutoTokenToggleButton, plus the account stage buttons further down.
# Left Users section: AddExemptionSelect, RemoveExemptionSelect,
#   PreviewDeleteButton, DeleteLeftUsersButton, SyncButton.
# Every action re-renders through ManagePanelView.build_full_display(),
# which re-counts pages and clamps the current page first.

class GrantTokenSelect(discord.ui.UserSelect):
    def __init__(self, callback_view):
        super().__init__(placeholder="Grant a token to...", row=1)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]

        if await _get_link_by_discord_id(target.id):
            return await interaction.response.send_message(f"⚠️ {target.mention} already has a linked Jellyfin account.", ephemeral=True)
        if await _get_token(target.id):
            return await interaction.response.send_message(f"⚠️ {target.mention} already has a pending token.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        success, note = await _issue_token_and_post(
            interaction.client, interaction.guild.id, target.id, interaction.user.id,
        )
        if not success:
            return await interaction.followup.send(f"⚠️ {target.mention} {note}.", ephemeral=True)

        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)
        warn = f"\n⚠️ {note.capitalize()} — let them know manually." if note else ""
        await interaction.followup.send(f"✅ Token granted to {target.mention}.{warn}", ephemeral=True)


class RevokeTokenSelect(discord.ui.UserSelect):
    def __init__(self, callback_view):
        super().__init__(placeholder="Revoke a token from...", row=2)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]

        token = await _get_token(target.id)
        if token is None:
            return await interaction.response.send_message(f"⚠️ {target.mention} has no pending token.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        await _delete_token(target.id)
        await _edit_token_message(interaction.client, token.get("message_id"), target.id, _token_revoked_text(target.id))

        dm_sent = await _dm_user(
            interaction.client, target.id,
            "Your Jellyfin signup token has expired and is no longer valid. Ask an admin if you still want to sign up."
        )

        note = "" if dm_sent else "\n⚠️ Couldn't DM them (their DMs may be closed)."
        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)
        await interaction.followup.send(f"✅ Revoked {target.mention}'s token.{note}", ephemeral=True)
        await _log_action(interaction.client, f"🚫 **Token revoked** — <@{target.id}> by <@{interaction.user.id}>.")


class AutoTokenToggleButton(discord.ui.Button):
    """Row-3 toggle on Accounts & Tokens pages. Reads its on/off state from
    the panel's cached `auto_enabled` (Button.__init__ can't be async).
    Flips the setting on click; the re-render rebuilds this button from
    the updated cache."""
    def __init__(self, callback_view):
        enabled = callback_view.auto_enabled
        super().__init__(
            label=f"Auto-token on join: {'ON' if enabled else 'OFF'}",
            style=discord.ButtonStyle.success if enabled else discord.ButtonStyle.secondary,
            row=3,
        )
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        new_state = not self.callback_view.auto_enabled
        await _set_auto_token_enabled(TARGET_GUILD_ID, new_state)
        self.callback_view.auto_enabled = new_state

        content, embed = await self.callback_view.build_full_display()
        await interaction.response.edit_message(content=content, embed=embed, view=self.callback_view)


class AddExemptionSelect(discord.ui.UserSelect):
    """Left Users pages. An exempt user's account is NEVER deleted for
    leaving — neither by instant auto-delete nor by the Delete button.
    Takes effect immediately, including for someone already queued."""
    def __init__(self, callback_view):
        super().__init__(placeholder="Add user to exemption list...", row=0)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]

        if await _is_exempt(target.id):
            return await interaction.response.send_message(f"⚠️ {target.mention} is already exempt.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        await _add_exempt_user(target.id, interaction.user.id)

        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)
        await interaction.followup.send(f"🛡️ {target.mention} is now exempt — they will never be deleted for leaving.", ephemeral=True)
        await _log_action(interaction.client, f"🛡️ **Exemption added** — <@{target.id}> by <@{interaction.user.id}>.")


class RemoveExemptionSelect(discord.ui.UserSelect):
    """Left Users pages. Undoes an exemption. If they're already queued in
    left_users (left while exempt), the next Delete press deletes them;
    a future leave is auto-deleted as normal."""
    def __init__(self, callback_view):
        super().__init__(placeholder="Remove user from exemption list...", row=1)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]

        removed = await _remove_exempt_user(target.id)
        if not removed:
            return await interaction.response.send_message(f"⚠️ {target.mention} isn't exempt.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)
        await interaction.followup.send(f"✅ Removed {target.mention}'s exemption.", ephemeral=True)
        await _log_action(interaction.client, f"🛡️ **Exemption removed** — <@{target.id}> by <@{interaction.user.id}>.")


class PreviewDeleteButton(discord.ui.Button):
    """Left Users pages. Shows exactly who the next Delete press WOULD
    delete, respecting exemptions, without touching anything — no DB
    writes, no Jellyfin calls, no role changes.

    Short lists are shown inline. A list that won't fit Discord's
    2000-character message limit is attached in full as a .txt file
    instead, with only the counts in the message — the complete audit is
    always visible before an irreversible action, never truncated."""
    def __init__(self, callback_view):
        super().__init__(label="👁 Preview Deletions", style=discord.ButtonStyle.secondary, row=2)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        all_left = await _get_all_left_users()
        exempt_ids = {row["discord_id"] for row in await _get_all_exempt_users()}
        to_delete = [row for row in all_left if row["discord_id"] not in exempt_ids]
        skipped = len(all_left) - len(to_delete)

        cog = interaction.client.get_cog("JellyfinRegistryCommands")
        staleness_note = ""
        if cog is not None:
            if cog._last_sync_at is None:
                staleness_note = "⚠️ Never synced this session — this preview may be stale. Consider pressing **Sync Everything** first.\n"
            elif (time.time() - cog._last_sync_at) > STALE_SYNC_WARNING_HOURS * 3600:
                staleness_note = f"⚠️ Last synced over {STALE_SYNC_WARNING_HOURS}h ago — this preview may be stale.\n"

        if not to_delete:
            msg = "Nothing would be deleted right now."
            if skipped:
                msg += f" ({skipped} exempt user(s) would be skipped.)"
            return await interaction.followup.send(staleness_note + msg, ephemeral=True)

        header = f"{staleness_note}👁 **Preview — would delete {len(to_delete)} account(s)** (skipping {skipped} exempt)"
        footer = "\n\n-# Nothing was actually deleted. Press **Delete all left users now** to proceed for real."
        lines = [f"<@{row['discord_id']}> — Jellyfin `{row['jellyfin_user_id']}`" for row in to_delete]

        inline = f"{header}:\n" + "\n".join(lines) + footer
        if len(inline) <= MESSAGE_CONTENT_LIMIT:
            return await interaction.followup.send(inline, ephemeral=True)

        preview_file = _build_deletion_preview_file(interaction.client, to_delete, skipped)
        await interaction.followup.send(
            f"{header}.\n📎 Full list attached — too long to show inline.{footer}",
            file=preview_file,
            ephemeral=True,
        )


class DeleteLeftUsersButton(discord.ui.Button):
    """Left Users pages. Manual fallback for instant auto-delete: deletes
    every non-exempt row still queued in left_users — failed
    auto-deletions, leaves past the burst cap, and leaves found later by
    Sync or backfill. No DM is sent (Discord blocks DMing someone the bot
    no longer shares a server with).

    Exempt users are skipped and stay queued (visible, marked exempt)
    until their exemption is removed or they rejoin.

    Reports 'deleted' and 'already_gone' SEPARATELY (see
    _delete_jellyfin_user). Runs with bounded concurrency
    (BATCH_CONCURRENCY) and refreshes the dashboard ONCE at the end.

    Pressing this also RESUMES auto-delete if the burst cap had paused it
    — the press is the admin's confirmation that the queued removals
    were reviewed."""
    def __init__(self, callback_view):
        super().__init__(label="Delete all left users now", style=discord.ButtonStyle.danger, row=2)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        all_left = await _get_all_left_users()
        exempt_ids = {row["discord_id"] for row in await _get_all_exempt_users()}
        to_delete = [row for row in all_left if row["discord_id"] not in exempt_ids]
        skipped_exempt = len(all_left) - len(to_delete)

        deleted = 0
        already_gone = 0
        failed = 0
        completed = 0
        affected_discord_ids: list[int] = []

        progress_msg = None
        if len(to_delete) > 10:
            progress_msg = await interaction.followup.send(f"⏳ Processing {len(to_delete)} left user(s)...", ephemeral=True, wait=True)

        semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
        counters_lock = asyncio.Lock()

        async def _process(row: dict):
            nonlocal deleted, already_gone, failed, completed
            discord_id = row["discord_id"]

            async with semaphore:
                result = await _delete_left_user_account(
                    interaction.client, discord_id, row["jellyfin_user_id"],
                    actor_text=f"by <@{interaction.user.id}>",
                    role_reason="Left server — deleted via /manage",
                )

                async with counters_lock:
                    if result == "deleted":
                        deleted += 1
                    elif result == "already_gone":
                        already_gone += 1
                    else:
                        failed += 1  # left in left_users, retried on next press
                    if result != "failed":
                        affected_discord_ids.append(discord_id)
                    completed += 1
                    current = completed
                    snapshot = (deleted, already_gone, failed)

                if progress_msg is not None and current % 10 == 0:
                    try:
                        await progress_msg.edit(content=f"⏳ Processing... {current}/{len(to_delete)} (deleted {snapshot[0]}, already gone {snapshot[1]}, failed {snapshot[2]})")
                    except discord.HTTPException:
                        pass

        if to_delete:
            await asyncio.gather(*(_process(row) for row in to_delete))

        commands_cog = interaction.client.get_cog("JellyfinRegistryCommands")
        if commands_cog is not None and affected_discord_ids:
            await commands_cog._notify_dashboard()

        resumed = False
        if commands_cog is not None and commands_cog._auto_delete_paused:
            commands_cog.resume_auto_delete()
            resumed = True
            await _log_action(interaction.client, f"▶️ **Auto-delete on leave resumed** — by <@{interaction.user.id}>.")

        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)

        summary = f"✅ Deleted {deleted} account(s) from Jellyfin."
        if already_gone:
            summary += f" 🧹 {already_gone} were already gone (stale tracking cleared)."
        if skipped_exempt:
            summary += f" 🛡️ Skipped {skipped_exempt} exempt."
        if failed:
            summary += f" ⚠️ {failed} failed (will retry next press)."
        if resumed:
            summary += " ▶️ Auto-delete on leave resumed."
        if progress_msg is not None:
            try:
                await progress_msg.edit(content=summary)
            except discord.HTTPException:
                await interaction.followup.send(summary, ephemeral=True)
        else:
            await interaction.followup.send(summary, ephemeral=True)

        logger.info(
            f"[JellyfinRegistryCommands] Batch left-user deletion — deleted {deleted}, already_gone {already_gone}, "
            f"skipped {skipped_exempt} exempt, {failed} failed. By <@{interaction.user.id}>."
        )
        await _log_action(
            interaction.client,
            f"🗑️ **Batch left-user deletion** — deleted {deleted}, already gone {already_gone}, "
            f"skipped {skipped_exempt} exempt, {failed} failed. By <@{interaction.user.id}>.",
        )


class SyncButton(discord.ui.Button):
    """Left Users pages. Full audit — see run_full_sync. Never deletes a
    Jellyfin account; leaves it finds are only queued."""
    def __init__(self, callback_view):
        super().__init__(label="🔄 Sync Everything", style=discord.ButtonStyle.primary, row=3)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        cog = interaction.client.get_cog("JellyfinRegistryCommands")
        if cog is None:
            return await interaction.followup.send("❌ Registry cog not loaded.", ephemeral=True)

        summary = await cog.run_full_sync(interaction.user.id)

        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)

        if summary["errors"]:
            return await interaction.followup.send(f"⚠️ Sync hit an issue: {'; '.join(summary['errors'])}", ephemeral=True)

        await interaction.followup.send(
            f"✅ Sync complete — {summary['newly_tracked_left']} newly tracked as left, "
            f"{summary['rejoined_cleared']} rejoined (cleared), {summary['stale_left_cleaned']} stale entries cleaned, "
            f"{summary['orphaned_links_cleared']} orphaned link(s) cleared, "
            f"{len(summary['unlinked_accounts'])} unlinked Jellyfin account(s), "
            f"{len(summary['inactive_linked'])} inactive linked user(s) found.",
            ephemeral=True,
        )


class PreviewInactiveDeleteButton(discord.ui.Button):
    """Inactive Users pages. Shows exactly who the next Delete press WOULD
    delete, without touching anything."""
    def __init__(self, callback_view):
        super().__init__(label="👁 Preview Deletions", style=discord.ButtonStyle.secondary, row=2)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        cog = interaction.client.get_cog("JellyfinRegistryCommands")
        if cog is None or not cog._last_sync_inactive_linked:
            return await interaction.followup.send("No inactive users found — press **🔄 Refresh** to scan.", ephemeral=True)

        inactive = cog._last_sync_inactive_linked
        exempt_ids = {row["discord_id"] for row in await _get_all_exempt_users()}
        to_delete = [u for u in inactive if u["discord_id"] not in exempt_ids]
        skipped = len(inactive) - len(to_delete)

        staleness_note = ""
        if cog._last_sync_at is None:
            staleness_note = "⚠️ Never synced this session — press **🔄 Refresh** first.\n"
        elif (time.time() - cog._last_sync_at) > STALE_SYNC_WARNING_HOURS * 3600:
            staleness_note = f"⚠️ Last synced over {STALE_SYNC_WARNING_HOURS}h ago — data may be stale.\n"

        if not to_delete:
            msg = "Nothing would be deleted right now."
            if skipped:
                msg += f" ({skipped} exempt user(s) would be skipped.)"
            return await interaction.followup.send(staleness_note + msg, ephemeral=True)

        header = f"{staleness_note}👁 **Preview — would delete {len(to_delete)} inactive account(s)** (skipping {skipped} exempt)"
        footer = (
            f"\n\n-# Inactive = no Jellyfin activity in {INACTIVE_LINKED_DAYS}+ days."
            "\n-# Nothing was actually deleted. Press **Delete inactive users** to proceed."
        )
        lines = []
        for u in to_delete:
            last = f"<t:{int(u['last_activity_ts'])}:R>" if u.get("last_activity_ts") else "never"
            lines.append(f"<@{u['discord_id']}> — `{u['jellyfin_username']}` — last active {last}")

        inline = f"{header}:\n" + "\n".join(lines) + footer
        if len(inline) <= MESSAGE_CONTENT_LIMIT:
            return await interaction.followup.send(inline, ephemeral=True)

        txt = "\n".join(f"{u['jellyfin_username']} (Discord {u['discord_id']}) — last active {'never' if not u.get('last_activity_ts') else datetime.fromtimestamp(u['last_activity_ts'], tz=timezone.utc).isoformat()}" for u in to_delete)
        file = discord.File(io.BytesIO(txt.encode()), filename="inactive_preview.txt")
        await interaction.followup.send(
            f"{header}.\n📎 Full list attached — too long to show inline.{footer}",
            file=file,
            ephemeral=True,
        )


class DeleteInactiveUsersButton(discord.ui.Button):
    """Inactive Users pages. Deletes every non-exempt inactive user's
    Jellyfin account, removes their DB link and their linked role.

    Deletions are THROTTLED — one at a time with a
    INACTIVE_DELETE_DELAY_SECONDS pause between each — to avoid
    hammering the Jellyfin API or hitting Discord rate limits on role
    removal. Progress is reported every 5 deletions.

    Exempt users are silently skipped. The dashboard is refreshed ONCE
    at the end. The inactive list is cleared after processing, so
    pressing Refresh is required to see the updated state."""
    def __init__(self, callback_view):
        super().__init__(label="Delete inactive users", style=discord.ButtonStyle.danger, row=2)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        cog = interaction.client.get_cog("JellyfinRegistryCommands")
        if cog is None or not cog._last_sync_inactive_linked:
            return await interaction.followup.send("No inactive users found — press **🔄 Refresh** to scan.", ephemeral=True)

        inactive = list(cog._last_sync_inactive_linked)  # snapshot
        exempt_ids = {row["discord_id"] for row in await _get_all_exempt_users()}
        to_delete = [u for u in inactive if u["discord_id"] not in exempt_ids]
        skipped_exempt = len(inactive) - len(to_delete)

        if not to_delete:
            msg = "Nothing to delete."
            if skipped_exempt:
                msg += f" ({skipped_exempt} exempt user(s) skipped.)"
            return await interaction.followup.send(msg, ephemeral=True)

        progress_msg = await interaction.followup.send(
            f"⏳ Deleting {len(to_delete)} inactive account(s)… (throttled: ~{INACTIVE_DELETE_DELAY_SECONDS}s between each)",
            ephemeral=True,
            wait=True,
        )

        deleted = 0
        already_gone = 0
        failed = 0

        for i, entry in enumerate(to_delete):
            discord_id = entry["discord_id"]
            jf_username = entry.get("jellyfin_username", "?")

            # Resolve Jellyfin user ID from the link — the inactive list
            # only stores the username, so we need the link row.
            link = await _get_link_by_discord_id(discord_id)
            if link is None:
                already_gone += 1
                continue

            jellyfin_user_id = link["jellyfin_user_id"]

            result = "deleted"
            try:
                await _delete_jellyfin_user(jellyfin_user_id)
            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    result = "already_gone"
                else:
                    result = "failed"
                    logger.error(f"[DeleteInactive] Failed to delete {jf_username} ({jellyfin_user_id}): {type(e).__name__}: {e}")
            except Exception as e:
                result = "failed"
                logger.error(f"[DeleteInactive] Failed to delete {jf_username} ({jellyfin_user_id}): {type(e).__name__}: {e}")

            if result == "failed":
                failed += 1
            else:
                # Account gone (deleted or was already gone) — clean up DB + role.
                await _purge_account_rows(jellyfin_user_id, discord_id)

                guild = interaction.client.get_guild(TARGET_GUILD_ID)
                if guild is not None:
                    role = guild.get_role(LINKED_ROLE_ID)
                    member = guild.get_member(discord_id)
                    if role and member and role in member.roles:
                        try:
                            await member.remove_roles(role, reason="Inactive — deleted via /manage")
                        except (discord.Forbidden, discord.HTTPException):
                            pass

                dashboard_cog = interaction.client.get_cog("JellyfinRegistryDashboard")
                if dashboard_cog is not None:
                    dashboard_cog._member_cache.pop(discord_id, None)

                if result == "already_gone":
                    already_gone += 1
                else:
                    deleted += 1

                await _log_action(
                    interaction.client,
                    f"💤 **Inactive account deleted** — **{jf_username}** (`{jellyfin_user_id}`) "
                    f"<@{discord_id}> by <@{interaction.user.id}>.",
                )

            # Progress update every 5
            if (i + 1) % 5 == 0 or (i + 1) == len(to_delete):
                try:
                    await progress_msg.edit(
                        content=f"⏳ Processing… {i + 1}/{len(to_delete)} "
                        f"(deleted {deleted}, already gone {already_gone}, failed {failed})"
                    )
                except discord.HTTPException:
                    pass

            # Throttle — the whole point. Don't slam Jellyfin or Discord.
            if i < len(to_delete) - 1:
                await asyncio.sleep(INACTIVE_DELETE_DELAY_SECONDS)

        # Clear the cached list so the panel shows "press Refresh"
        # instead of a stale list that no longer matches reality.
        cog._last_sync_inactive_linked = []

        # One dashboard refresh at the end, not per-deletion.
        if cog is not None and (deleted or already_gone):
            await cog._notify_dashboard(full=True)

        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)

        summary = f"✅ Deleted {deleted} inactive account(s)."
        if already_gone:
            summary += f" 🧹 {already_gone} were already gone (stale tracking cleared)."
        if skipped_exempt:
            summary += f" 🛡️ Skipped {skipped_exempt} exempt."
        if failed:
            summary += f" ⚠️ {failed} failed — press **🔄 Refresh** then retry."
        summary += "\n-# Press **🔄 Refresh** to see the updated state."

        try:
            await progress_msg.edit(content=summary)
        except discord.HTTPException:
            await interaction.followup.send(summary, ephemeral=True)

        await _log_action(
            interaction.client,
            f"💤 **Batch inactive deletion** — deleted {deleted}, already gone {already_gone}, "
            f"skipped {skipped_exempt} exempt, {failed} failed. By <@{interaction.user.id}>.",
        )


class InactiveSyncButton(discord.ui.Button):
    """Inactive Users pages. Runs a full sync to refresh the inactive
    user list. Same as SyncButton but lands on the inactive section."""
    def __init__(self, callback_view):
        super().__init__(label="🔄 Refresh", style=discord.ButtonStyle.primary, row=3)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        cog = interaction.client.get_cog("JellyfinRegistryCommands")
        if cog is None:
            return await interaction.followup.send("❌ Registry cog not loaded.", ephemeral=True)

        summary = await cog.run_full_sync(interaction.user.id)

        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)

        if summary["errors"]:
            return await interaction.followup.send(f"⚠️ Sync hit an issue: {'; '.join(summary['errors'])}", ephemeral=True)

        inactive_count = len(summary["inactive_linked"])
        await interaction.followup.send(
            f"✅ Refreshed — found {inactive_count} inactive linked user(s) "
            f"(no activity in {INACTIVE_LINKED_DAYS}+ days).",
            ephemeral=True,
        )


class PrevPageButton(discord.ui.Button):
    def __init__(self, callback_view):
        super().__init__(label="◀ Prev", style=discord.ButtonStyle.secondary, row=4, disabled=(callback_view._flat_index() == 0))
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        view = self.callback_view
        view.step(-1)  # build_full_display below recounts and clamps, so a shrunken list can't strand the panel
        view.account_stage = "main"  # always land on a main screen, never mid-stage
        content, embed = await view.build_full_display()
        await interaction.response.edit_message(content=content, embed=embed, view=view)


class NextPageButton(discord.ui.Button):
    def __init__(self, callback_view):
        at_end = callback_view._flat_index() >= callback_view._total_pages() - 1
        super().__init__(label="Next ▶", style=discord.ButtonStyle.secondary, row=4, disabled=at_end)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        view = self.callback_view
        view.step(1)
        view.account_stage = "main"
        content, embed = await view.build_full_display()
        await interaction.response.edit_message(content=content, embed=embed, view=view)


def _join_lines_within_limit(lines: list[str], total_count: int, limit: int = EMBED_DESCRIPTION_LIMIT) -> str:
    """Joins lines with newlines, stopping before the joined text would
    exceed `limit` and appending a '…and N more' note instead of
    overflowing. The suffix reserve is sized off the real suffix text, so
    the final string never exceeds `limit`.

    Pagination keeps pages far below the limits in normal use — this is
    the safety net for a single unexpectedly long page."""
    def _suffix(remaining: int) -> str:
        return f"\n…and {remaining} more (not shown — Discord size limit)."

    suffix_reserve = len(_suffix(total_count))
    budget = limit - suffix_reserve

    included = []
    used = 0
    for line in lines:
        added = len(line) + (1 if included else 0)
        if used + added > budget:
            break
        included.append(line)
        used += added

    if len(included) < total_count:
        remaining = total_count - len(included)
        result = "\n".join(included) + _suffix(remaining)
        if len(result) > limit:  # belt-and-braces, shouldn't trigger
            result = result[: limit - 1].rstrip() + "…"
        return result
    return "\n".join(included)


def _add_budgeted_fields(embed: discord.Embed, specs: list[tuple[str, list[str], str | None]]) -> None:
    """Adds fields to an embed whose title, description and footer are
    already set, keeping the WHOLE embed under Discord's 6000-character
    total. Each spec is (name, lines, fallback): `fallback`, when not
    None, is used verbatim instead of joining `lines`. The remaining
    budget is split evenly across the fields still to be added, capped at
    1024 per field, so one long list can't starve the others."""
    used = len(embed.title or "") + len(embed.description or "") + len(embed.footer.text or "")
    remaining = EMBED_TOTAL_LIMIT - EMBED_TOTAL_SAFETY_MARGIN - used - sum(len(name) for name, _, _ in specs)

    for i, (name, lines, fallback) in enumerate(specs):
        fields_left = len(specs) - i
        budget = min(EMBED_FIELD_VALUE_LIMIT, max(0, remaining // fields_left))
        if fallback is not None:
            value = fallback
        elif budget >= MIN_FIELD_BUDGET:
            value = _join_lines_within_limit(lines, len(lines), limit=budget)
        else:
            value = f"{len(lines)} entr{'y' if len(lines) == 1 else 'ies'} — not shown (embed size limit)."
        value = value[:EMBED_FIELD_VALUE_LIMIT]
        embed.add_field(name=name, value=value, inline=False)
        remaining -= len(value)


class ManagePanelView(discord.ui.View):
    """The /manage admin panel. One FLAT page sequence, walked by
    Prev/Next:

        [Accounts & Tokens 1..T] [Left Users 1..L] [Inactive Users 1..I]

    T = ceil(visible tokens / TOKENS_PER_PAGE), L = ceil(queued left
    users / LEFT_USERS_PER_PAGE), I = ceil(inactive linked users /
    INACTIVE_PER_PAGE), each at least 1. Every page in a
    section shows that section's action components, so Grant/Revoke/
    Auto-toggle work from any token page and the delete/sync tools from
    any left-users page. The position is (section, sub_page); counts are
    recomputed and sub_page clamped on EVERY render, since revokes,
    deletions and syncs shrink the lists underneath an open panel.

    Accounts & Tokens pages also carry a nested stage machine
    (account_stage: main/link/unlink/register/unregister). Only "main"
    shows the token list and token widgets; the other stages swap in that
    sub-flow's components. Prev/Next always return to "main".

    __init__ stays synchronous (required by discord.ui.View) and does NOT
    build items — call `await refresh_items()` once after construction.
    Stays open 5 minutes."""
    def __init__(self, bot: commands.Bot, guild_id: int):
        super().__init__(timeout=300.0)
        self.bot = bot
        self.guild_id = guild_id
        self.section = "tokens"  # "tokens" | "left" | "inactive"
        self.sub_page = 0
        self.token_pages = 1
        self.left_pages = 1
        self.inactive_pages = 1
        self.account_stage = "main"
        self.auto_enabled = False  # placeholder until refresh_items() runs
        self.token_count = 0
        self.expired_hidden = 0
        self.left_count = 0
        self.inactive_count = 0
        self.message: discord.WebhookMessage | None = None  # set by /manage so on_timeout can edit it

    async def refresh_items(self):
        """Fetches the auto-token state and page counts, then builds items.
        Call once after construction."""
        self.auto_enabled = await _get_auto_token_enabled(TARGET_GUILD_ID)
        await self._refresh_page_counts()
        self._build_items()

    async def _refresh_page_counts(self):
        """COUNT queries + inactive list length — no row data loaded.
        Cached on the view so the embed builders don't count again."""
        self.token_count, self.expired_hidden = await _count_tokens()
        self.left_count = await _count_left_users()
        cog = self.bot.get_cog("JellyfinRegistryCommands") if self.bot is not None else None
        self.inactive_count = len(cog._last_sync_inactive_linked) if (cog and cog._last_sync_inactive_linked) else 0
        self.token_pages = max(1, math.ceil(self.token_count / TOKENS_PER_PAGE))
        self.left_pages = max(1, math.ceil(self.left_count / LEFT_USERS_PER_PAGE))
        self.inactive_pages = max(1, math.ceil(self.inactive_count / INACTIVE_PER_PAGE)) if self.inactive_count else 1
        if self.section == "tokens":
            section_pages = self.token_pages
        elif self.section == "left":
            section_pages = self.left_pages
        else:
            section_pages = self.inactive_pages
        self.sub_page = max(0, min(self.sub_page, section_pages - 1))

    def _flat_index(self) -> int:
        if self.section == "tokens":
            return self.sub_page
        elif self.section == "left":
            return self.token_pages + self.sub_page
        else:  # inactive
            return self.token_pages + self.left_pages + self.sub_page

    def _total_pages(self) -> int:
        return self.token_pages + self.left_pages + self.inactive_pages

    def step(self, delta: int):
        position = max(0, min(self._total_pages() - 1, self._flat_index() + delta))
        if position < self.token_pages:
            self.section = "tokens"
            self.sub_page = position
        elif position < self.token_pages + self.left_pages:
            self.section = "left"
            self.sub_page = position - self.token_pages
        else:
            self.section = "inactive"
            self.sub_page = position - self.token_pages - self.left_pages

    def _build_items(self):
        self.clear_items()
        if self.section == "tokens":
            if self.account_stage == "main":
                self.add_item(_AccountStageButton(self, "link", "🔗 Link", discord.ButtonStyle.success))
                self.add_item(_AccountStageButton(self, "unlink", "🔓 Unlink", discord.ButtonStyle.secondary))
                self.add_item(_AccountStageButton(self, "register", "📝 Register", discord.ButtonStyle.primary))
                self.add_item(_AccountStageButton(self, "unregister", "🗑️ Unregister", discord.ButtonStyle.danger))
                self.add_item(GrantTokenSelect(self))
                self.add_item(RevokeTokenSelect(self))
                self.add_item(AutoTokenToggleButton(self))
            elif self.account_stage == "link":
                self.add_item(LinkUserSelect(self))
                self.add_item(_AccountBackButton(self))
            elif self.account_stage == "unlink":
                self.add_item(UnlinkUserSelect(self))
                self.add_item(_AccountBackButton(self))
            elif self.account_stage == "register":
                self.add_item(RegisterUserSelect(self))
                self.add_item(_AccountBackButton(self))
            elif self.account_stage == "unregister":
                self.add_item(UnregisterUserSelect(self))
                self.add_item(UnregisterByUsernameButton(self))
                self.add_item(_AccountBackButton(self))
        elif self.section == "left":
            self.add_item(AddExemptionSelect(self))
            self.add_item(RemoveExemptionSelect(self))
            self.add_item(PreviewDeleteButton(self))
            self.add_item(DeleteLeftUsersButton(self))
            self.add_item(SyncButton(self))
        else:  # inactive
            self.add_item(PreviewInactiveDeleteButton(self))
            self.add_item(DeleteInactiveUsersButton(self))
            self.add_item(InactiveSyncButton(self))
        self.add_item(PrevPageButton(self))
        self.add_item(NextPageButton(self))

    async def build_full_display(self):
        """Single render path: recount pages, clamp, rebuild components,
        build the embed for the current position."""
        await self._refresh_page_counts()
        self._build_items()
        if self.section == "tokens":
            if self.account_stage == "main":
                embed = await self._build_page_accounts_tokens_embed()
            else:
                embed = self._build_page_accounts_embed()
        elif self.section == "left":
            embed = await self._build_page_leftusers_embed()
        else:  # inactive
            embed = self._build_page_inactive_embed()
        return None, embed

    def _build_page_accounts_embed(self) -> discord.Embed:
        """Only shown for a sub-stage (link/unlink/register/unregister)."""
        stages = {
            "link": ("🔗 Link", "Select the Discord user to link to an existing Jellyfin account.", discord.Color.green()),
            "unlink": ("🔓 Unlink", "Select the Discord user to remove their Jellyfin link.", discord.Color.orange()),
            "register": ("📝 Register", "Select the Discord user to create and link a new Jellyfin account for.", discord.Color.blurple()),
            "unregister": ("🗑️ Unregister", "Select a linked Discord member to delete their account, or delete an orphaned Jellyfin account by username below.", discord.Color.red()),
        }
        title, desc, color = stages[self.account_stage]
        return discord.Embed(title=title, description=desc, color=color)

    async def _build_page_accounts_tokens_embed(self) -> discord.Embed:
        start = self.sub_page * TOKENS_PER_PAGE
        page_tokens = await _get_visible_tokens_page(start, TOKENS_PER_PAGE)
        visible_count = self.token_count
        hidden_expired = self.expired_hidden

        before = "Use the buttons above to link, unlink, register, or unregister an account.\n\n"
        after = ""
        if hidden_expired:
            after = f"\n\n-# {hidden_expired} expired token(s) hidden — cleared automatically within {TOKEN_EXPIRY_CHECK_MINUTES} min."

        if not page_tokens:
            list_text = "No pending signup tokens."
        else:
            before += f"**Pending tokens {start + 1}–{start + len(page_tokens)} of {visible_count}**\n"
            lines = []
            for t in page_tokens:
                expires_ts = _token_expires_ts(t)
                status = f"expires <t:{expires_ts}:R>" if expires_ts is not None else "unknown expiry"
                remind = "🔔 reminded" if t.get("reminded") else "⏳ not reminded"
                lines.append(f"<@{t['discord_id']}> — {status} — {remind}")
            list_text = _join_lines_within_limit(lines, len(lines), limit=EMBED_DESCRIPTION_LIMIT - len(before) - len(after))

        embed = discord.Embed(
            title=f"👤 Accounts & Tokens — Page {self._flat_index() + 1}/{self._total_pages()}",
            description=before + list_text + after,
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Auto-token on join",
            value="🟢 ON — new members are issued a token automatically" if self.auto_enabled
                  else "🔴 OFF — new members are not issued a token automatically",
            inline=False,
        )
        embed.set_footer(text=(
            f"Grant/Revoke DM the user. Pending users are reminded every {REMIND_INTERVAL_HOURS}h until they sign up "
            "or the token expires. Users Discord permanently refuses DMs to have their token expired early."
        ))
        return embed

    async def _build_page_leftusers_embed(self) -> discord.Embed:
        start = self.sub_page * LEFT_USERS_PER_PAGE
        page_rows = await _get_left_users_page(start, LEFT_USERS_PER_PAGE)
        left_count = self.left_count
        exempt_rows = await _get_all_exempt_users()
        exempt_ids = {r["discord_id"] for r in exempt_rows}
        cog = self.bot.get_cog("JellyfinRegistryCommands") if self.bot is not None else None

        # Sync freshness first — a Delete/Preview press against stale data
        # is exactly what produced the deleted-count discrepancy.
        if cog is not None and cog._last_sync_at is not None:
            age_seconds = time.time() - cog._last_sync_at
            sync_line = f"🔄 Last synced <t:{int(cog._last_sync_at)}:R>"
            if age_seconds > STALE_SYNC_WARNING_HOURS * 3600:
                sync_line += f" — ⚠️ over {STALE_SYNC_WARNING_HOURS}h ago, consider syncing before deleting."
        else:
            sync_line = "🔄 Never synced this session — ⚠️ press **Sync Everything** before deleting."

        if not AUTO_DELETE_ON_LEAVE:
            auto_line = "⚡ Auto-delete on leave: **OFF** — leavers are only deleted by the button below."
        elif cog is not None and cog._auto_delete_paused:
            auto_line = (
                f"⏸️ Auto-delete on leave: **PAUSED** — more than {AUTO_DELETE_BURST_LIMIT} removals within "
                f"{AUTO_DELETE_BURST_WINDOW_SECONDS // 60} min. Review the queue, then press "
                "**Delete all left users now** to process it and resume."
            )
        else:
            auto_line = "⚡ Auto-delete on leave: **ON** — linked users are deleted the moment they leave."

        before = f"{sync_line}\n{auto_line}\n\n"
        if not page_rows:
            list_text = "No linked users are queued — nothing left for the button to do."
        else:
            before += f"**Queued left users {start + 1}–{start + len(page_rows)} of {left_count}**\n"
            lines = []
            for row in page_rows:
                left_ts = _left_user_left_ts(row)
                left_str = f"<t:{left_ts}:R>" if left_ts is not None else "unknown time"
                status = "🛡️ exempt — never deleted" if row["discord_id"] in exempt_ids else "⏳ queued — deleted on next press"
                lines.append(f"<@{row['discord_id']}> — left {left_str} — {status}")
            list_text = _join_lines_within_limit(lines, len(lines), limit=EMBED_DESCRIPTION_LIMIT - len(before))

        embed = discord.Embed(
            title=f"🚪 Left Users — Page {self._flat_index() + 1}/{self._total_pages()}",
            description=before + list_text,
            color=discord.Color.red(),
        )
        embed.set_footer(text=(
            "Leavers are deleted instantly; this queue holds exempt users, failed or paused auto-deletions, "
            "and leaves found by Sync. Sync never deletes Jellyfin accounts."
        ))

        specs: list[tuple[str, list[str], str | None]] = []

        if exempt_rows:
            exempt_lines = [f"<@{r['discord_id']}>" + (f" — {r['reason']}" if r.get("reason") else "") for r in exempt_rows]
            specs.append((f"🛡️ Exempt Users ({len(exempt_rows)})", exempt_lines, None))

        if cog is not None and cog._last_sync_unlinked_accounts is not None:
            unlinked = cog._last_sync_unlinked_accounts
            sync_age = f"<t:{int(cog._last_sync_at)}:R>" if cog._last_sync_at else "unknown"
            if unlinked:
                specs.append((
                    f"🔎 Unlinked Jellyfin Accounts ({len(unlinked)}) — as of {sync_age}",
                    [f"`{u.get('Name', '?')}`" for u in unlinked],
                    None,
                ))
            else:
                specs.append(("🔎 Unlinked Jellyfin Accounts", [], f"None found (as of {sync_age})."))
        else:
            specs.append(("🔎 Unlinked Jellyfin Accounts", [], "Press **Sync Everything** to check."))

        _add_budgeted_fields(embed, specs)
        return embed

    def _build_page_inactive_embed(self) -> discord.Embed:
        """Inactive Users section — paginated list of linked users with
        no Jellyfin activity in INACTIVE_LINKED_DAYS+ days. Data comes
        from the last Sync; press Refresh to re-scan."""
        cog = self.bot.get_cog("JellyfinRegistryCommands") if self.bot is not None else None
        inactive = cog._last_sync_inactive_linked if (cog and cog._last_sync_inactive_linked) else []

        sync_line = ""
        if cog is not None and cog._last_sync_at is not None:
            age_seconds = time.time() - cog._last_sync_at
            sync_line = f"🔄 Last synced <t:{int(cog._last_sync_at)}:R>"
            if age_seconds > STALE_SYNC_WARNING_HOURS * 3600:
                sync_line += f" — ⚠️ over {STALE_SYNC_WARNING_HOURS}h ago, press **🔄 Refresh** before deleting."
        else:
            sync_line = "🔄 Never synced this session — press **🔄 Refresh** to populate this list."

        before = f"{sync_line}\n-# Inactive = no Jellyfin activity in {INACTIVE_LINKED_DAYS}+ days. Still in the server.\n\n"

        if not inactive:
            list_text = "No inactive users found. Press **🔄 Refresh** to scan."
        else:
            start = self.sub_page * INACTIVE_PER_PAGE
            page_items = inactive[start:start + INACTIVE_PER_PAGE]
            before += f"**Inactive users {start + 1}–{start + len(page_items)} of {len(inactive)}**\n"
            lines = []
            for entry in page_items:
                last_seen = f"<t:{int(entry['last_activity_ts'])}:R>" if entry.get("last_activity_ts") else "never"
                lines.append(f"<@{entry['discord_id']}> — `{entry['jellyfin_username']}` — last active {last_seen}")
            list_text = _join_lines_within_limit(lines, len(lines), limit=EMBED_DESCRIPTION_LIMIT - len(before))

        embed = discord.Embed(
            title=f"💤 Inactive Users — Page {self._flat_index() + 1}/{self._total_pages()}",
            description=before + list_text,
            color=discord.Color.dark_grey(),
        )
        embed.set_footer(text=(
            "Preview shows exactly who would be deleted. Delete removes their Jellyfin account, "
            "DB link, and linked role — throttled to avoid rate limits. Exempt users are always skipped."
        ))
        return embed

    async def on_timeout(self):
        """Disabling items alone changes nothing on screen — the message has
        to be edited, or the buttons keep looking live and just fail with
        'interaction failed'."""
        for item in self.children:
            item.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


# --- ACCOUNT MANAGEMENT (Accounts & Tokens section of /manage) ---
#   Four stage buttons, each swapping the page into a stage-specific
#   UserSelect (+ a modal for the actions that need text input). Driven
#   by ManagePanelView.account_stage.
#
#   Unregister keeps TWO paths: UnregisterUserSelect deletes a linked
#   Discord member's account; UnregisterByUsernameButton deletes an
#   ORPHANED Jellyfin account with no Discord link — a target a
#   UserSelect structurally can't reach.

async def _unregister_by_jellyfin_id(interaction: discord.Interaction, target_id: str, target_name: str, strip_discord_id: int | None) -> str:
    """Shared deletion core for both Unregister paths. Returns the
    confirmation string to send — callers send it via followup."""
    try:
        await _delete_jellyfin_user(target_id)
    except Exception as e:
        return f"❌ Couldn't delete the Jellyfin account: `{type(e).__name__}: {e}`"

    removed_link = await _purge_account_rows(target_id, strip_discord_id)  # link + any queue row, one commit

    role_note = ""
    if strip_discord_id:
        guild = interaction.client.get_guild(TARGET_GUILD_ID)
        if guild is not None:
            role = guild.get_role(LINKED_ROLE_ID)
            member = guild.get_member(strip_discord_id)
            if role and member and role in member.roles:
                try:
                    await member.remove_roles(role, reason="Jellyfin account deleted via /manage")
                except (discord.Forbidden, discord.HTTPException):
                    role_note = "\n⚠️ Couldn't remove the linked role."
        dashboard_cog = interaction.client.get_cog("JellyfinRegistryDashboard")
        if dashboard_cog is not None:
            dashboard_cog._member_cache.pop(strip_discord_id, None)

    commands_cog = interaction.client.get_cog("JellyfinRegistryCommands")
    if commands_cog is not None:
        await commands_cog._notify_dashboard()

    await _log_action(interaction.client, f"🗑️ **Unregister (via /manage)** — <@{interaction.user.id}> deleted **{target_name}** (`{target_id}`).")

    return (
        f"✅ Deleted Jellyfin account **{target_name}** (ID: `{target_id}`)."
        + (" Link removed from the database." if removed_link else " No matching link existed in the database.")
        + role_note
    )


class LinkModal(discord.ui.Modal, title="Link Jellyfin account"):
    def __init__(self, target_user: discord.User):
        super().__init__()
        self.target_user = target_user
        self.jellyfin_username_input = discord.ui.TextInput(
            label="Jellyfin username", placeholder="Their exact Jellyfin username", required=True, max_length=64,
        )
        self.add_item(self.jellyfin_username_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        jellyfin_username = self.jellyfin_username_input.value.strip()

        try:
            jf_users = await _get_jellyfin_users_cached()
        except Exception as e:
            return await interaction.followup.send(f"❌ Couldn't reach Jellyfin: `{type(e).__name__}: {e}`", ephemeral=True)

        match = next((u for u in jf_users if u.get("Name", "").lower() == jellyfin_username.lower()), None)
        if not match:
            available = ", ".join(sorted(u.get("Name", "?") for u in jf_users))
            msg = f"❌ No Jellyfin user named `{jellyfin_username}` found.\nAvailable accounts: {available}"
            if len(msg) > MESSAGE_CONTENT_LIMIT:
                msg = msg[: MESSAGE_CONTENT_LIMIT - 1] + "…"
            return await interaction.followup.send(msg, ephemeral=True)

        jellyfin_user_id = match["Id"]
        await _add_link(self.target_user.id, jellyfin_user_id, interaction.user.id)

        commands_cog = interaction.client.get_cog("JellyfinRegistryCommands")
        if commands_cog is not None:
            await commands_cog._assign_linked_role(self.target_user.id)
            await commands_cog._notify_dashboard(discord_id=self.target_user.id)

        await interaction.followup.send(
            f"✅ Linked **{self.target_user.mention}** ({self.target_user.id}) → Jellyfin user **{match['Name']}** "
            f"(ID: `{jellyfin_user_id}`)",
            ephemeral=True,
        )
        await _log_action(interaction.client, f"🔗 **Link (via /manage)** — <@{interaction.user.id}> linked <@{self.target_user.id}> → **{match['Name']}** (`{jellyfin_user_id}`).")


class LinkUserSelect(discord.ui.UserSelect):
    def __init__(self, callback_view):
        super().__init__(placeholder="Pick the Discord user to link...", row=0)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]
        await interaction.response.send_modal(LinkModal(target))


class UnlinkUserSelect(discord.ui.UserSelect):
    def __init__(self, callback_view):
        super().__init__(placeholder="Pick the Discord user to unlink...", row=0)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]
        await interaction.response.defer(ephemeral=True)

        removed = await _remove_link(target.id)

        self.callback_view.account_stage = "main"
        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)

        if removed:
            commands_cog = interaction.client.get_cog("JellyfinRegistryCommands")
            if commands_cog is not None:
                await commands_cog._notify_dashboard()
            await interaction.followup.send(f"✅ Unlinked **{target.mention}** ({target.id})", ephemeral=True)
            await _log_action(interaction.client, f"🔓 **Unlink (via /manage)** — <@{interaction.user.id}> unlinked <@{target.id}>.")
        else:
            await interaction.followup.send(f"⚠️ **{target.mention}** wasn't linked to anything.", ephemeral=True)


class RegisterModal(discord.ui.Modal, title="Register new Jellyfin account"):
    def __init__(self, target_user: discord.User):
        super().__init__()
        self.target_user = target_user
        self.jellyfin_username_input = discord.ui.TextInput(
            label="Jellyfin username", placeholder="Username for the new account", required=True, max_length=64,
        )
        self.password_input = discord.ui.TextInput(
            label="Password", placeholder="Password for the new account", required=True, max_length=128,
        )
        self.add_item(self.jellyfin_username_input)
        self.add_item(self.password_input)

    async def on_submit(self, interaction: discord.Interaction):
        # Same in-flight claim as SignupModal, shared set: an admin Register
        # and the user's own signup can't race into two accounts either.
        target_id = self.target_user.id
        if target_id in _signups_in_flight:
            return await interaction.response.send_message("⏳ An account creation for this user is already in progress.", ephemeral=True)
        _signups_in_flight.add(target_id)
        try:
            await self._process(interaction)
        finally:
            _signups_in_flight.discard(target_id)

    async def _process(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        username = self.jellyfin_username_input.value.strip()
        password = self.password_input.value

        try:
            existing_jf_users = await _get_jellyfin_users_cached()
        except Exception as e:
            return await interaction.followup.send(f"❌ Couldn't reach Jellyfin to check for a duplicate username: `{type(e).__name__}: {e}`", ephemeral=True)
        if any(u.get("Name", "").lower() == username.lower() for u in existing_jf_users):
            return await interaction.followup.send(f"❌ A Jellyfin account named `{username}` already exists — pick a different username.", ephemeral=True)

        await _set_pending_signup(self.target_user.id, username)

        try:
            new_user = await _create_jellyfin_user(username, password)
        except Exception as e:
            await _clear_pending_signup(self.target_user.id)
            return await interaction.followup.send(f"❌ Couldn't create the Jellyfin account: `{type(e).__name__}: {e}`", ephemeral=True)

        jellyfin_user_id = new_user.get("Id")
        await _mark_pending_signup_created(self.target_user.id, jellyfin_user_id)

        commands_cog = interaction.client.get_cog("JellyfinRegistryCommands")

        try:
            await _grant_full_library_access(jellyfin_user_id)
        except Exception as e:
            await _commit_signup_link(self.target_user.id, jellyfin_user_id, interaction.user.id)
            if commands_cog is not None:
                await commands_cog._assign_linked_role(self.target_user.id)
                await commands_cog._notify_dashboard(discord_id=self.target_user.id)
            return await interaction.followup.send(
                f"⚠️ Account **{username}** created but granting full library access failed: `{type(e).__name__}: {e}`\n"
                "It still got linked — fix access manually in Jellyfin.",
                ephemeral=True,
            )

        await _commit_signup_link(self.target_user.id, jellyfin_user_id, interaction.user.id)
        role_assigned = False
        if commands_cog is not None:
            role_assigned = await commands_cog._assign_linked_role(self.target_user.id)
            await commands_cog._notify_dashboard(discord_id=self.target_user.id)

        note = "" if role_assigned else "\n⚠️ Couldn't assign the linked role (member not in server or missing permissions)."
        await interaction.followup.send(
            f"✅ Created **{username}** with full library access → linked to **{self.target_user.mention}** "
            f"(ID: `{jellyfin_user_id}`){note}",
            ephemeral=True,
        )
        await _log_action(interaction.client, f"📝 **Register (via /manage)** — <@{interaction.user.id}> created **{username}** (`{jellyfin_user_id}`) → linked to <@{self.target_user.id}>.")


class RegisterUserSelect(discord.ui.UserSelect):
    def __init__(self, callback_view):
        super().__init__(placeholder="Pick the Discord user to register...", row=0)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]
        await interaction.response.send_modal(RegisterModal(target))


class UnregisterUserSelect(discord.ui.UserSelect):
    """Deletes a linked Discord member's Jellyfin account outright."""
    def __init__(self, callback_view):
        super().__init__(placeholder="Unregister by Discord member...", row=0)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        target = self.values[0]
        await interaction.response.defer(ephemeral=True)

        link = await _get_link_by_discord_id(target.id)
        if link is None:
            self.callback_view.account_stage = "main"
            content, embed = await self.callback_view.build_full_display()
            await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)
            return await interaction.followup.send(f"❌ {target.mention} isn't linked to a Jellyfin account.", ephemeral=True)

        jellyfin_user_id = link["jellyfin_user_id"]
        try:
            jf_users = await _get_jellyfin_users_cached()
            target_name = next((u["Name"] for u in jf_users if u["Id"] == jellyfin_user_id), jellyfin_user_id)
        except Exception:
            target_name = jellyfin_user_id

        result = await _unregister_by_jellyfin_id(interaction, jellyfin_user_id, target_name, strip_discord_id=target.id)

        self.callback_view.account_stage = "main"
        content, embed = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embed=embed, view=self.callback_view)
        await interaction.followup.send(result, ephemeral=True)


class UnregisterUsernameModal(discord.ui.Modal, title="Delete Jellyfin account by username"):
    """For an orphaned Jellyfin account with no Discord link. Role-stripping
    is skipped — there's no member reference without a reverse lookup."""
    def __init__(self):
        super().__init__()
        self.jellyfin_username_input = discord.ui.TextInput(
            label="Jellyfin username", placeholder="Exact Jellyfin username to delete", required=True, max_length=64,
        )
        self.add_item(self.jellyfin_username_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        username = self.jellyfin_username_input.value.strip()

        try:
            jf_users = await _get_jellyfin_users_cached()
        except Exception as e:
            return await interaction.followup.send(f"❌ Couldn't reach Jellyfin: `{type(e).__name__}: {e}`", ephemeral=True)

        match = next((u for u in jf_users if u.get("Name", "").lower() == username.lower()), None)
        if not match:
            return await interaction.followup.send(f"❌ No Jellyfin user named `{username}` found.", ephemeral=True)

        result = await _unregister_by_jellyfin_id(interaction, match["Id"], match["Name"], strip_discord_id=None)
        await interaction.followup.send(result, ephemeral=True)


class UnregisterByUsernameButton(discord.ui.Button):
    def __init__(self, callback_view):
        super().__init__(label="Delete by Jellyfin username", style=discord.ButtonStyle.danger, row=1)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(UnregisterUsernameModal())


class _AccountStageButton(discord.ui.Button):
    def __init__(self, callback_view: "ManagePanelView", stage: str, label: str, style: discord.ButtonStyle):
        super().__init__(label=label, style=style, row=0)
        self.callback_view = callback_view
        self.stage = stage

    async def callback(self, interaction: discord.Interaction):
        self.callback_view.account_stage = self.stage
        content, embed = await self.callback_view.build_full_display()
        await interaction.response.edit_message(content=content, embed=embed, view=self.callback_view)


class _AccountBackButton(discord.ui.Button):
    def __init__(self, callback_view: "ManagePanelView"):
        super().__init__(label="← Back", style=discord.ButtonStyle.secondary, row=4)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        self.callback_view.account_stage = "main"
        content, embed = await self.callback_view.build_full_display()
        await interaction.response.edit_message(content=content, embed=embed, view=self.callback_view)


class JellyfinRegistryCommands(commands.Cog):
    def __init__(self, bot: commands.AutoShardedBot):
        self.bot = bot
        self.category = "Admin"
        # Cached result of the last Sync — shown on the /manage Left Users
        # pages. The lists are None until Sync runs in this process;
        # _last_sync_at is restored from registry_meta in cog_load.
        self._last_sync_unlinked_accounts: list[dict] | None = None
        self._last_sync_inactive_linked: list[dict] | None = None
        self._last_sync_at: float | None = None
        # Instant auto-delete burst protection. In-memory: a restart
        # resets the window and un-pauses. _auto_delete_recent holds
        # monotonic timestamps of auto-deletions inside the current window.
        self._auto_delete_paused = False
        self._auto_delete_recent: deque[float] = deque()
        self._auto_delete_lock = asyncio.Lock()
        # Debounced dashboard refresh state — see _notify_dashboard.
        self._dash_requested = False
        self._dash_member_ids: set[int] = set()
        self._dash_full = False
        self._dash_task: asyncio.Task | None = None
        # Strong references to fire-and-forget tasks. asyncio only keeps
        # weak references, so an unreferenced task can be garbage-collected
        # mid-run.
        self._background_tasks: set[asyncio.Task] = set()

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _loops(self) -> tuple:
        return (self.expire_tokens_loop, self.remind_pending_loop, self.pending_signup_recheck_loop, self.weekly_sync_loop)

    async def cog_load(self):
        global _bot_ref
        _bot_ref = self.bot
        if _db_conn is None:
            await _open_db()
        await _ensure_db()
        stored_sync = await _get_meta("last_full_sync_at")
        if stored_sync is not None:
            try:
                self._last_sync_at = float(stored_sync)
            except ValueError:
                self._last_sync_at = None
        self.bot.add_view(TokenMessageView(""))
        self.bot.add_view(JellyfinAccountDMView())
        for loop in self._loops():
            if not loop.is_running():
                loop.start()
        self._spawn(self._run_startup_reconcile())
        if BACKFILL_LEFT_USERS_ON_STARTUP:
            self._spawn(self._run_startup_backfill())

    async def _run_startup_reconcile(self):
        await self.bot.wait_until_ready()
        try:
            await _reconcile_stale_tokens_on_startup(self.bot)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Startup token reconciliation failed: {type(e).__name__}: {e}", exc_info=True)
        try:
            await _reconcile_pending_signups(self.bot)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Startup pending-signup reconciliation failed: {type(e).__name__}: {e}", exc_info=True)
        try:
            await _reconcile_ghost_tokens_on_startup(self.bot)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Startup ghost-token reconciliation failed: {type(e).__name__}: {e}", exc_info=True)

    async def _run_startup_backfill(self):
        await self.bot.wait_until_ready()
        # Extra delay so the member cache can populate on larger guilds.
        await asyncio.sleep(10)
        try:
            await _backfill_left_users(self.bot)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Startup left-user backfill failed: {type(e).__name__}: {e}", exc_info=True)

    async def cog_unload(self):
        """Stops everything that could touch the DB, WAITS for it to stop,
        flushes the queued log lines, and only then closes the connection —
        closing first used to let an in-flight query hit a None connection."""
        for loop in self._loops():
            loop.cancel()
        pending = [loop.get_task() for loop in self._loops()]
        pending += list(self._background_tasks)
        if self._dash_task is not None:
            pending.append(self._dash_task)
        pending = [t for t in pending if t is not None and not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await _action_log.close()
        await _close_db()
        # No aiohttp session close here — main.py owns bot.http_session.

    # --- Auto-delete burst protection ---

    async def _claim_auto_delete_slot(self) -> str:
        """Returns 'ok' (go ahead and delete), 'paused_now' (this call hit
        the burst cap and just paused auto-delete), or 'paused' (already
        paused). Lock-protected: member-remove events run concurrently, and
        a prune fires many at once."""
        async with self._auto_delete_lock:
            if self._auto_delete_paused:
                return "paused"
            now = time.monotonic()
            while self._auto_delete_recent and now - self._auto_delete_recent[0] > AUTO_DELETE_BURST_WINDOW_SECONDS:
                self._auto_delete_recent.popleft()
            if len(self._auto_delete_recent) >= AUTO_DELETE_BURST_LIMIT:
                self._auto_delete_paused = True
                return "paused_now"
            self._auto_delete_recent.append(now)
            return "ok"

    def resume_auto_delete(self):
        """Called by the Delete-all button after it processes the queue."""
        self._auto_delete_paused = False
        self._auto_delete_recent.clear()

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Auto-token entry point. Guarded by TARGET_GUILD_ID, bot accounts,
        exemption, the toggle itself, and any existing link/token so this
        can never double-issue.

        A rejoining member who still has a queued `left_users` row (their
        account survived: exempt, auto-delete paused, or deletion failed)
        has that row cleared and is NOT an auto-token candidate.

        An EXEMPT member is never auto-issued a token on join."""
        if member.guild.id != TARGET_GUILD_ID:
            return
        if member.bot:
            return

        # Queue row cleared FIRST — an exempt rejoiner used to return on the
        # exemption check below and stay listed as "left" until a Sync.
        if await _remove_left_user(member.id):
            await _log_action(self.bot, f"🔄 **Rejoined** — <@{member.id}> rejoined while still queued, pending deletion cancelled.")
            return

        if await _is_exempt(member.id):
            logger.info(f"[JellyfinRegistryCommands] Auto-token skipped for {member.id}: exempt user.")
            return

        if not await _get_auto_token_enabled(TARGET_GUILD_ID):
            return
        if await _get_link_by_discord_id(member.id) or await _get_token(member.id):
            return

        try:
            success, note = await _issue_token_and_post(
                self.bot, member.guild.id, member.id, self.bot.user.id,
            )
            if not success:
                logger.info(f"[JellyfinRegistryCommands] Auto-token skipped for {member.id}: {note}")
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] Auto-token failed for {member.id}: {type(e).__name__}: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Fires on leave, kick, ban and prune alike.

        1. Any pending signup token is revoked and its message edited.
        2. A linked member is queued in left_users FIRST, so nothing is
           lost if anything below fails.
        3. With AUTO_DELETE_ON_LEAVE on, their Jellyfin account is deleted
           immediately — unless they're exempt, or the burst cap pauses
           auto-delete. On success the queue row is cleared; on failure it
           stays for the Delete button."""
        if member.guild.id != TARGET_GUILD_ID:
            return
        if member.bot:
            return

        token = await _get_token(member.id)
        if token is not None:
            await _delete_token(member.id)
            await _edit_token_message(self.bot, token.get("message_id"), member.id, _token_left_text(member.id))
            await _log_action(self.bot, f"🚫 **Token revoked (left server)** — <@{member.id}>.")

        link = await _get_link_by_discord_id(member.id)
        if link is None:
            return  # not a linked account — nothing to delete

        jellyfin_user_id = link["jellyfin_user_id"]
        await _add_left_user(member.id, jellyfin_user_id)

        if not AUTO_DELETE_ON_LEAVE:
            await _log_action(self.bot, f"🚪 **Left server** — <@{member.id}> (Jellyfin `{jellyfin_user_id}`) queued for deletion (auto-delete is off).")
            return

        if await _is_exempt(member.id):
            await _log_action(self.bot, f"🛡️ **Left server (exempt)** — <@{member.id}> (Jellyfin `{jellyfin_user_id}`) — account kept.")
            return

        slot = await self._claim_auto_delete_slot()
        if slot == "paused_now":
            logger.warning(
                f"[JellyfinRegistryCommands] Auto-delete PAUSED — more than {AUTO_DELETE_BURST_LIMIT} removals within "
                f"{AUTO_DELETE_BURST_WINDOW_SECONDS}s. <@{member.id}> and any further leavers are queued."
            )
            await _log_action(
                self.bot,
                f"⏸️ **Auto-delete paused** — more than {AUTO_DELETE_BURST_LIMIT} removals within "
                f"{AUTO_DELETE_BURST_WINDOW_SECONDS // 60} min (prune/mass kick?). <@{member.id}> and any further leavers "
                "are queued. Review the Left Users pages of `/manage`, then press **Delete all left users now** to "
                "process the queue and resume.",
            )
            return
        if slot == "paused":
            await _log_action(self.bot, f"🚪 **Left server** — <@{member.id}> (Jellyfin `{jellyfin_user_id}`) queued — auto-delete is paused.")
            return

        result = await _delete_left_user_account(
            self.bot, member.id, jellyfin_user_id,
            actor_text="auto-deleted on leave",
            role_reason="Left server — auto-deleted",
        )
        if result == "failed":
            await _log_action(
                self.bot,
                f"⚠️ **Auto-delete failed** — <@{member.id}> (Jellyfin `{jellyfin_user_id}`) stays queued; "
                "retry with **Delete all left users now**.",
            )
            return

        await self._notify_dashboard()

    # --- Background loops ---
    # Every loop body is wrapped in try/except inside the loop itself.
    # Relying on @loop.error to restart a crashed loop does NOT work: the
    # error handler runs inside the loop's own task, so is_running() is
    # still True there and the "restart if stopped" check never fires —
    # one unexpected exception used to kill token expiry or reminders
    # until the next reboot. Caught here, a failed pass is logged and the
    # loop simply runs again next interval.

    @tasks.loop(minutes=TOKEN_EXPIRY_CHECK_MINUTES)
    async def expire_tokens_loop(self):
        try:
            await self._expire_tokens_pass()
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] expire_tokens_loop pass failed: {type(e).__name__}: {e}", exc_info=True)

    async def _expire_tokens_pass(self):
        """Expires every token past TOKEN_EXPIRY_HOURS, even if nobody
        clicks it, and DMs the user. Only expired rows are loaded (SQL
        filter), and they're processed with bounded concurrency. Whoever
        actually deletes the row (this sweep, a revoke, a leave) is the
        only one that edits the message and DMs."""
        expired = await _get_expired_tokens()
        if not expired:
            return

        semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)

        async def _expire(token: dict):
            discord_id = token["discord_id"]
            async with semaphore:
                if not await _delete_token(discord_id):
                    return  # already handled elsewhere since the query ran
                await _edit_token_message(self.bot, token.get("message_id"), discord_id, _token_expired_text(discord_id))
                await _dm_user(
                    self.bot, discord_id,
                    "Your Jellyfin signup token has expired and is no longer valid. Ask an admin if you still want to sign up."
                )
                await _log_action(self.bot, f"⌛ **Token auto-expired** — <@{discord_id}> (24h elapsed, unused).")

        await asyncio.gather(*(_expire(t) for t in expired))

    @expire_tokens_loop.before_loop
    async def before_expire_tokens_loop(self):
        await self.bot.wait_until_ready()

    @tasks.loop(hours=REMIND_INTERVAL_HOURS)
    async def remind_pending_loop(self):
        try:
            await self._remind_pending_pass()
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] remind_pending_loop pass failed: {type(e).__name__}: {e}", exc_info=True)

    async def _remind_pending_pass(self):
        """DMs every pending user whose token is at least
        REMIND_MIN_AGE_HOURS old and not expired — every cycle, until they
        sign up or the token expires (3 reminders over a 24h token at the
        default settings).

        DM outcome handling:
          sent        → marked reminded.
          unreachable → Discord PERMANENTLY refused (user gone, or error
                        50007): the token is expired now.
          failed      → transient (rate limit, 5xx, other error): the
                        token is left alone and retried next cycle."""
        pending = await _get_remindable_tokens(REMIND_MIN_AGE_HOURS)
        if not pending:
            return

        reminded_count = 0
        expired_count = 0
        failed_count = 0
        counters_lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)

        async def _process(token: dict):
            nonlocal reminded_count, expired_count, failed_count
            discord_id = token["discord_id"]

            async with semaphore:
                expires_ts = _token_expires_ts(token)
                message_link = f"https://discord.com/channels/{TARGET_GUILD_ID}/{TOKEN_CHANNEL_ID}/{token['message_id']}"
                status = await _send_dm(
                    self.bot, discord_id,
                    content=_build_pending_reminder_message(message_link, expires_ts),
                )

                if status == "sent":
                    await _set_token_reminded(discord_id)
                    async with counters_lock:
                        reminded_count += 1
                elif status == "unreachable":
                    if not await _delete_token(discord_id):
                        return  # signed up / revoked / expired meanwhile
                    await _edit_token_message(self.bot, token.get("message_id"), discord_id, _token_unreachable_text(discord_id))
                    async with counters_lock:
                        expired_count += 1
                    await _log_action(
                        self.bot,
                        f"⌛ **Token auto-expired (unreachable)** — <@{discord_id}>: Discord permanently refused the "
                        "reminder DM, token cleared instead of waiting out the normal expiry.",
                    )
                else:
                    async with counters_lock:
                        failed_count += 1

        await asyncio.gather(*(_process(token) for token in pending))

        if reminded_count or expired_count or failed_count:
            await _log_action(
                self.bot,
                f"📨 **Scheduled token-remind** — {reminded_count}/{len(pending)} reminded, "
                f"{expired_count} auto-expired (unreachable), {failed_count} transient failure(s) (kept, retried next cycle).",
            )

    @remind_pending_loop.before_loop
    async def before_remind_pending_loop(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=PENDING_SIGNUP_RECHECK_MINUTES)
    async def pending_signup_recheck_loop(self):
        """Runs the same reconciliation as startup on a recurring interval,
        so a flagged/unverifiable marker gets resolved on a later, healthier
        pass instead of sitting forever."""
        try:
            await _reconcile_pending_signups(self.bot)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] pending_signup_recheck_loop failed: {type(e).__name__}: {e}", exc_info=True)

    @pending_signup_recheck_loop.before_loop
    async def before_pending_signup_recheck_loop(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=SYNC_DUE_CHECK_MINUTES)
    async def weekly_sync_loop(self):
        """Checks hourly whether a full sync is due; runs one only when the
        last sync (button or automatic, persisted in registry_meta) is
        older than WEEKLY_SYNC_INTERVAL_HOURS. A restart no longer triggers
        a full sync, and frequent restarts no longer postpone it forever —
        the schedule follows the stored timestamp, not the process."""
        try:
            last = self._last_sync_at
            if last is not None and time.time() - last < WEEKLY_SYNC_INTERVAL_HOURS * 3600:
                return
            await self.run_full_sync(actor_discord_id=self.bot.user.id)
        except Exception as e:
            logger.error(f"[JellyfinRegistryCommands] weekly_sync_loop failed: {type(e).__name__}: {e}", exc_info=True)

    @weekly_sync_loop.before_loop
    async def before_weekly_sync_loop(self):
        await self.bot.wait_until_ready()
        # Don't compete with the startup reconcile/backfill tasks.
        await asyncio.sleep(30)

    async def run_full_sync(self, actor_discord_id: int) -> dict:
        """Full audit (Sync button + weekly loop). Cross-checks everything
        against live Jellyfin + live Discord membership and repairs drift:
          1. Linked accounts whose leave was never seen (bot offline) are
             QUEUED in left_users — never deleted here.
          2. left_users rows for people who've since rejoined are cleared.
          3. left_users rows whose Jellyfin account no longer exists are
             cleared, so they can't pad future 'deleted' counts.
          4. Links whose Jellyfin account no longer exists are removed.
          5. Computes unlinked Jellyfin accounts and inactive linked users
             (no activity in INACTIVE_LINKED_DAYS) for the panel.

        Deliberately NEVER deletes a Jellyfin account."""
        summary = {
            "newly_tracked_left": 0,
            "rejoined_cleared": 0,
            "stale_left_cleaned": 0,
            "orphaned_links_cleared": 0,
            "unlinked_accounts": [],
            "inactive_linked": [],
            "errors": [],
        }

        try:
            jf_users = await _fetch_jellyfin_users()
        except Exception as e:
            summary["errors"].append(f"Couldn't reach Jellyfin: {type(e).__name__}: {e}")
            logger.error(f"[JellyfinRegistryCommands] Full sync: couldn't fetch Jellyfin users: {type(e).__name__}: {e}")
            return summary

        jf_users_by_id = {u["Id"]: u for u in jf_users if u.get("Id")}
        links = await _get_all_links()
        linked_jf_ids = {link["jellyfin_user_id"] for link in links}

        unlinked = [u for u in jf_users if u.get("Id") not in linked_jf_ids]
        summary["unlinked_accounts"] = unlinked

        guild = self.bot.get_guild(TARGET_GUILD_ID)
        if guild is not None and not guild.chunked:
            try:
                await guild.chunk(cache=True)
            except Exception as e:
                logger.warning(f"[JellyfinRegistryCommands] Full sync: guild chunk failed, falling back to individual lookups: {type(e).__name__}: {e}")

        inactive_cutoff = time.time() - (INACTIVE_LINKED_DAYS * 86400)

        # First pass: drop orphaned links (Jellyfin account gone entirely).
        active_links = []
        for link in links:
            jf_user = jf_users_by_id.get(link["jellyfin_user_id"])
            if jf_user is None:
                await _remove_link(link["discord_id"])
                summary["orphaned_links_cleared"] += 1
                logger.warning(
                    f"[JellyfinRegistryCommands] Full sync: cleared orphaned link — <@{link['discord_id']}> was linked to "
                    f"Jellyfin `{link['jellyfin_user_id']}`, which no longer exists."
                )
                continue
            active_links.append((link, jf_user))

        # Second pass: resolve membership; cache misses fan out with
        # bounded concurrency.
        semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)

        async def _resolve_member(discord_id: int):
            if guild is None:
                return None, False
            member = guild.get_member(discord_id)
            if member is not None:
                return member, True
            async with semaphore:
                try:
                    member = await guild.fetch_member(discord_id)
                    return member, True
                except discord.NotFound:
                    return None, True
                except Exception as e:
                    logger.warning(f"[JellyfinRegistryCommands] Full sync: couldn't verify membership for {discord_id}: {type(e).__name__}: {e}")
                    return None, False  # ambiguous — do NOT conclude they've left

        resolved = await asyncio.gather(*(_resolve_member(link["discord_id"]) for link, _ in active_links))

        for (link, jf_user), (member, membership_checked) in zip(active_links, resolved):
            discord_id = link["discord_id"]
            jellyfin_user_id = link["jellyfin_user_id"]

            existing_left_row = await _get_left_user(discord_id)

            if membership_checked and member is None and existing_left_row is None:
                await _add_left_user(discord_id, jellyfin_user_id)
                summary["newly_tracked_left"] += 1
            elif member is not None and existing_left_row is not None:
                await _remove_left_user(discord_id)
                summary["rejoined_cleared"] += 1

            if member is not None and existing_left_row is None:
                # Same parser as everywhere else — no activity recorded
                # (None) counts as inactive; a real date is compared.
                last_activity_ts = _parse_jellyfin_date(jf_user.get("LastActivityDate"))
                is_inactive = last_activity_ts is None or last_activity_ts < inactive_cutoff
                if is_inactive:
                    summary["inactive_linked"].append({
                        "discord_id": discord_id,
                        "jellyfin_username": jf_user.get("Name", "?"),
                        "last_activity_ts": last_activity_ts,
                    })

        left_rows = await _get_all_left_users()
        for row in left_rows:
            if row["jellyfin_user_id"] not in jf_users_by_id:
                await _remove_left_user(row["discord_id"])
                summary["stale_left_cleaned"] += 1

        self._last_sync_unlinked_accounts = unlinked
        self._last_sync_inactive_linked = summary["inactive_linked"]
        self._last_sync_at = time.time()
        await _set_meta("last_full_sync_at", str(self._last_sync_at))  # survives restarts — see weekly_sync_loop

        logger.info(
            f"[JellyfinRegistryCommands] Full sync complete — newly tracked left: {summary['newly_tracked_left']}, "
            f"rejoined cleared: {summary['rejoined_cleared']}, stale left cleaned: {summary['stale_left_cleaned']}, "
            f"orphaned links cleared: {summary['orphaned_links_cleared']}, unlinked accounts: {len(unlinked)}, "
            f"inactive linked: {len(summary['inactive_linked'])}."
        )
        await _log_action(
            self.bot,
            f"🔄 **Full sync** — newly tracked left: {summary['newly_tracked_left']}, "
            f"rejoined cleared: {summary['rejoined_cleared']}, stale cleaned: {summary['stale_left_cleaned']}, "
            f"orphaned links cleared: {summary['orphaned_links_cleared']}, unlinked accounts: {len(unlinked)}, "
            f"inactive linked: {len(summary['inactive_linked'])}. By <@{actor_discord_id}>.",
        )

        return summary

    async def _assign_linked_role(self, discord_id: int) -> bool:
        """Best-effort — adds LINKED_ROLE_ID if the member is in the target
        guild. Returns whether it succeeded."""
        guild = self.bot.get_guild(TARGET_GUILD_ID)
        if guild is None:
            return False
        role = guild.get_role(LINKED_ROLE_ID)
        if role is None:
            return False
        member = guild.get_member(discord_id)
        if member is None:
            try:
                member = await guild.fetch_member(discord_id)
            except Exception:
                return False
        try:
            await member.add_roles(role, reason="Linked to a Jellyfin account")
            return True
        except (discord.Forbidden, discord.HTTPException):
            return False

    async def _notify_dashboard(self, discord_id: int | None = None, full: bool = False, member_ids=None):
        """Requests a dashboard refresh. Requests arriving within
        DASHBOARD_REFRESH_DEBOUNCE_SECONDS collapse into ONE refresh —
        a burst of leaves, signups or a Delete-all batch costs one redraw,
        not one per event. Targeted member IDs from every merged request
        are combined; full=True from any of them escalates to a complete
        re-resolution. Returns immediately; never raises."""
        if discord_id is not None:
            self._dash_member_ids.add(discord_id)
        if member_ids:
            self._dash_member_ids.update(member_ids)
        if full:
            self._dash_full = True
        self._dash_requested = True
        if self._dash_task is None or self._dash_task.done():
            self._dash_task = asyncio.create_task(self._run_dashboard_refresh())

    async def _run_dashboard_refresh(self):
        # Loops while requests keep arriving, including ones made while a
        # refresh was in progress.
        while self._dash_requested:
            await asyncio.sleep(DASHBOARD_REFRESH_DEBOUNCE_SECONDS)
            self._dash_requested = False
            member_ids, self._dash_member_ids = self._dash_member_ids, set()
            full, self._dash_full = self._dash_full, False

            cog = self.bot.get_cog("JellyfinRegistryDashboard")
            if cog is None:
                continue
            try:
                if full:
                    await cog.refresh_dashboard(force=True, resolve_members=True)
                elif member_ids:
                    await cog.refresh_dashboard(force=True, resolve_member_ids=list(member_ids))
                else:
                    await cog.refresh_dashboard(force=True)
            except Exception as e:
                logger.error(f"[JellyfinRegistryCommands] Dashboard refresh failed: {type(e).__name__}: {e}", exc_info=True)

    @app_commands.command(name="jellyfin", description="View your Jellyfin account details, and reset your password or change your username")
    async def jellyfin_command(self, interaction: discord.Interaction):
        """DMs the caller a full account summary with Reset Password and
        Change Username buttons."""
        link = await _get_link_by_discord_id(interaction.user.id)
        if link is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account. Ask an admin to link or register one for you.", ephemeral=True)

        # Deferred PUBLICLY — once the initial response is ephemeral,
        # Discord forces every followup ephemeral too. Only the failure
        # followups below are marked ephemeral.
        await interaction.response.defer(ephemeral=False)

        jellyfin_user_id = link["jellyfin_user_id"]
        jf_user, watch_seconds = await asyncio.gather(
            _fetch_jellyfin_user(jellyfin_user_id),
            _get_all_time_watch_seconds(jellyfin_user_id),
        )

        if jf_user is None:
            return await interaction.followup.send("❌ Couldn't find your Jellyfin account — it may have been deleted. Contact an admin.", ephemeral=True)

        username = jf_user.get("Name", "Unknown")
        policy = jf_user.get("Policy", {})
        if policy.get("EnableAllFolders"):
            library_access = "Full Library"
        else:
            folder_count = len(policy.get("EnabledFolders") or [])
            library_access = f"{folder_count} folder(s)" if folder_count else "None configured"

        avatar_url = f"{JELLYFIN_URL}/Users/{jellyfin_user_id}/Images/Primary" if jf_user.get("PrimaryImageTag") else None

        dm_view = JellyfinAccountDMView(
            username=username,
            library_access=library_access,
            linked_ts=_linked_at_ts(link.get("linked_at")),
            last_login_ts=_parse_jellyfin_date(jf_user.get("LastLoginDate")),
            last_activity_ts=_parse_jellyfin_date(jf_user.get("LastActivityDate")),
            watch_seconds=watch_seconds,
            avatar_url=avatar_url,
        )
        dm_sent = await _dm_user_view(interaction.client, interaction.user.id, dm_view)

        if dm_sent:
            confirm_container = discord.ui.Container()
            confirm_container.add_item(discord.ui.TextDisplay(
                f"✅ {interaction.user.mention}, I've DMed you your Jellyfin account details — check your private messages!"
            ))
            confirm_view = discord.ui.LayoutView(timeout=None)
            confirm_view.add_item(confirm_container)
            await interaction.followup.send(view=confirm_view, ephemeral=False)
        else:
            await interaction.followup.send("❌ Couldn't DM you — check that your DMs are open and try again.", ephemeral=True)

    @jellyfin_command.error
    async def jellyfin_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="leaveserver", description="Voluntarily delete your own Jellyfin account and unlink it")
    async def leaveserver(self, interaction: discord.Interaction):
        """Self-serve opt-out without leaving the Discord server. Requires
        an explicit confirmation click (irreversible)."""
        link = await _get_link_by_discord_id(interaction.user.id)
        if link is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)

        view = discord.ui.View(timeout=60.0)

        async def _confirm(confirm_interaction: discord.Interaction):
            if confirm_interaction.user.id != interaction.user.id:
                return await confirm_interaction.response.send_message("❌ This isn't your confirmation to press.", ephemeral=True)
            await confirm_interaction.response.defer(ephemeral=True)

            try:
                await _delete_jellyfin_user(link["jellyfin_user_id"])
            except Exception as e:
                logger.error(f"[JellyfinRegistryCommands] /leaveserver deletion failed for <@{interaction.user.id}>: {type(e).__name__}: {e}", exc_info=True)
                return await confirm_interaction.followup.send(f"❌ Couldn't delete the account: `{type(e).__name__}: {e}`", ephemeral=True)

            await _purge_account_rows(link["jellyfin_user_id"], interaction.user.id)  # link + any queue row, one commit
            dashboard_cog = self.bot.get_cog("JellyfinRegistryDashboard")
            if dashboard_cog is not None:
                dashboard_cog._member_cache.pop(interaction.user.id, None)

            guild = self.bot.get_guild(TARGET_GUILD_ID)
            if guild is not None:
                role = guild.get_role(LINKED_ROLE_ID)
                member = guild.get_member(interaction.user.id)
                if role is not None and member is not None and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="Self-serve /leaveserver")
                    except (discord.Forbidden, discord.HTTPException):
                        pass

            await self._notify_dashboard()
            await confirm_interaction.followup.send("✅ Your Jellyfin account has been deleted and unlinked.", ephemeral=True)
            logger.info(f"[JellyfinRegistryCommands] Self-serve deletion — <@{interaction.user.id}> deleted their own account via /leaveserver.")
            await _log_action(self.bot, f"🚪 **/leaveserver** — <@{interaction.user.id}> voluntarily deleted their own Jellyfin account (`{link['jellyfin_user_id']}`).")

        async def _cancel(cancel_interaction: discord.Interaction):
            if cancel_interaction.user.id != interaction.user.id:
                return await cancel_interaction.response.send_message("❌ This isn't your confirmation to press.", ephemeral=True)
            await cancel_interaction.response.edit_message(content="Cancelled — nothing was deleted.", view=None)

        confirm_button = discord.ui.Button(label="Yes, delete my account", style=discord.ButtonStyle.danger)
        confirm_button.callback = _confirm
        cancel_button = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary)
        cancel_button.callback = _cancel
        view.add_item(confirm_button)
        view.add_item(cancel_button)

        await interaction.response.send_message(
            "⚠️ This will **permanently delete** your Jellyfin account and unlink it. This cannot be undone. Are you sure?",
            view=view,
            ephemeral=True,
        )

    @app_commands.command(name="manage", description="[Admin] Manage Jellyfin accounts, signup tokens, and left-user cleanup")
    async def manage(self, interaction: discord.Interaction):
        if not _is_owner(interaction):
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        view = ManagePanelView(interaction.client, interaction.guild.id)
        await view.refresh_items()
        content, embed = await view.build_full_display()
        view.message = await interaction.followup.send(content=content, embed=embed, view=view, ephemeral=True, wait=True)

    @manage.error
    async def manage_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.command(name="exempt", description="[Owner] Add or remove a user from the leave-deletion exemption list, by Discord ID")
    @app_commands.describe(
        action="Add or remove the exemption",
        discord_id="The Discord user ID (works even if they aren't in the server)",
        reason="Optional — only used when adding, shown later on the exemption list",
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="Add", value="add"),
        app_commands.Choice(name="Remove", value="remove"),
    ])
    async def exempt(self, interaction: discord.Interaction, action: app_commands.Choice[str], discord_id: str, reason: str = None):
        """ID-based, separate from the panel's UserSelect pickers — a
        UserSelect can't reliably target someone who isn't in the server,
        which is exactly the case this command exists for."""
        if not _is_owner(interaction):
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        try:
            parsed_id = int(discord_id.strip())
        except ValueError:
            return await interaction.response.send_message(f"❌ `{discord_id}` isn't a valid Discord user ID (should be all digits).", ephemeral=True)

        if action.value == "add":
            if await _is_exempt(parsed_id):
                return await interaction.response.send_message(f"⚠️ <@{parsed_id}> is already exempt.", ephemeral=True)

            await interaction.response.defer(ephemeral=True)

            # A confirmed 404 refuses (catches typos); a transient failure
            # doesn't block adding a valid-looking ID.
            try:
                user_obj = interaction.client.get_user(parsed_id) or await interaction.client.fetch_user(parsed_id)
            except discord.NotFound:
                return await interaction.followup.send(f"❌ No Discord account exists with ID `{parsed_id}` — check the ID and try again.", ephemeral=True)
            except discord.HTTPException as e:
                logger.warning(f"[JellyfinRegistryCommands] /exempt add: couldn't verify ID {parsed_id} via Discord (proceeding anyway): {type(e).__name__}: {e}")
                user_obj = None

            await _add_exempt_user(parsed_id, interaction.user.id, reason=reason)

            display = f"**{user_obj}** (<@{parsed_id}>)" if user_obj is not None else f"<@{parsed_id}> (`{parsed_id}`)"
            await interaction.followup.send(
                f"🛡️ {display} is now exempt — they will never be deleted for leaving, even if they aren't currently in the server.",
                ephemeral=True,
            )
            logger.info(f"[JellyfinRegistryCommands] Exemption added by ID — {parsed_id} — by <@{interaction.user.id}>.")
            await _log_action(interaction.client, f"🛡️ **Exemption added (by ID)** — <@{parsed_id}> (`{parsed_id}`) by <@{interaction.user.id}>.")

        else:  # remove
            removed = await _remove_exempt_user(parsed_id)
            if not removed:
                return await interaction.response.send_message(f"⚠️ <@{parsed_id}> isn't exempt.", ephemeral=True)

            await interaction.response.send_message(f"✅ Removed <@{parsed_id}>'s exemption.", ephemeral=True)
            logger.info(f"[JellyfinRegistryCommands] Exemption removed by ID — {parsed_id} — by <@{interaction.user.id}>.")
            await _log_action(interaction.client, f"🛡️ **Exemption removed (by ID)** — <@{parsed_id}> (`{parsed_id}`) by <@{interaction.user.id}>.")

    @exempt.error
    async def exempt_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(JellyfinRegistryCommands(bot))