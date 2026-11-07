"""
Jellyfin Dashboard Cog (Tracearr SSE transport, original architecture)
  - /dashboard: configure a live, auto-updating "Now Playing" dashboard —
    driven by the Tracearr SSE plugin's event stream. The moment someone
    starts/stops/pauses playback, Tracearr pushes the update to the bot,
    which edits the one dashboard message live.

    The dashboard message itself is built entirely with Components V2
    (LayoutView / Container / Section / TextDisplay / Separator /
    Thumbnail) — no discord.Embed objects are sent for it.

    UPDATE PIPELINE — same architecture as the original native-WebSocket
    version (fingerprint diffing, tick_loop, stats_loop, debounce), only
    the connection underneath is now Tracearr's SSE stream instead of
    Jellyfin's native WebSocket:
      - Tracearr sends discrete per-event data (sessionId/itemId/
        positionTicks), not a full session snapshot like the native WS
        did. A new session's full details (poster/title/device/client)
        get hydrated with ONE REST call the first time it's seen —
        concurrent new sessions share that call via a coalesced fetch.
        A known session's paused state / position update in place, no
        REST call needed.
      - After every update to the in-memory session set, the exact same
        _sessions_fingerprint comparison from the original design decides
        whether to broadcast — position-agnostic, only reacts to a real
        session/pause-state change. This is the mechanism that was
        already proven not to rate-limit in production.
      - Now Playing refreshes on: an SSE-driven fingerprint change, the
        30s tick_loop (while something is playing, position-aware), and
        Refresh Now / dashboard setup.
      - Library Statistics refresh on: an SSE library.item.added/removed
        event (forced re-fetch), AND independently every 60s via
        stats_loop as a safety net. library.item.added additionally
        forwards the item id to new_media.py's queue_item(), same as the
        earlier Tracearr integration.
      - Rapid-fire events inside the 2s debounce window are coalesced —
        one broadcast fires ~2s after the window opened.

    LOGGING — single source of truth: every real broadcast eventually
    calls refresh_message(), and that is the ONLY place that logs at
    INFO level for a broadcast — one line per actual Discord edit,
    regardless of which of the several trigger paths caused it (SSE
    push, tick_loop, stats_loop, manual refresh, etc.). Every trigger
    site that decides WHETHER to broadcast (fingerprint diffing, the
    debounce timer, tick/stats loops noticing no real change) logs at
    DEBUG instead — that mechanism-level detail is still fully available
    by lowering the log level, it just doesn't spam the console at
    normal verbosity. Connection health (SSE connect/disconnect) and
    real errors are untouched, at their original levels.

    REQUIRES the Tracearr SSE plugin installed on the Jellyfin server:
    Dashboard -> Plugins -> Repositories -> add repo "Tracearr" with URL
    https://raw.githubusercontent.com/Tracearr/Media-Server-SSE/main/manifest.json
    -> Catalog -> install "Tracearr SSE" -> restart Jellyfin.
    Requires Jellyfin 10.11+. See https://docs.tracearr.com/sse-plugin
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiohttp
import asyncio
import json
import logging
import aiosqlite
import os
import shutil
import time

from .utils.config import JELLYFIN_URL, DASHBOARD_API_KEY, DASHBOARD_DB_PATH, DB_DIR
from .utils.jellyfin_api import JellyfinClient

logger = logging.getLogger("jellyfin_dashboard_cog")

# --- Timing / tuning constants ---
SSE_RECONNECT_DELAY = 5       # base seconds between reconnect attempts if the stream drops
SSE_MAX_RECONNECT_DELAY = 60  # cap for exponential backoff during a real outage
SSE_READ_TIMEOUT_SECONDS = 60 # plugin pings every 30s — 60s of silence means the connection is actually dead
DEBOUNCE_SECONDS = 2.0        # minimum gap between dashboard broadcasts, coalesces rapid-fire events
STATS_LOOP_SECONDS = 60       # independent stats freshness check, runs regardless of playback state

# Independent safety net for session tracking (self._sessions_by_id) —
# separate from STATS_LOOP_SECONDS, which only ever touches stats. Without
# this, a session's removal depends ENTIRELY on either an explicit
# 'stopped' SSE event arriving, or the connection happening to reconnect
# (which also depends on that reconnect's own REST /Sessions call
# succeeding). If a single 'stopped' event is ever lost while the
# connection otherwise stays healthy, nothing else corrects it — a
# stopped stream can sit showing as 'still playing' indefinitely. This
# loop periodically re-verifies against Jellyfin's live REST /Sessions
# regardless of connection state, so drift self-heals on a fixed cadence
# instead of depending entirely on the event stream never dropping one.
SESSION_RESYNC_INTERVAL_SECONDS = 120

# Hard, self-imposed floor on how often THIS SPECIFIC MESSAGE can actually
# be edited — separate from, and layered ON TOP OF, the 2s debounce above.
# Debounce only collapses truly near-simultaneous events; it does nothing
# for events spaced further apart than its own window (e.g. every 3-9s
# during real session flapping — confirmed in production logs). Discord
# imposes an undocumented hard cap on edits to a message once it's over
# an hour old (error code 30046, "Maximum number of edits to messages
# older than 1 hour reached") — no official threshold number exists
# anywhere, including Discord's own community asking for years with no
# answer. Rather than guess that number and edge close to it, this
# enforces a conservative, deliberately-chosen minimum gap ourselves,
# BEFORE any edit call ever reaches Discord — a structural ceiling that
# holds regardless of how bursty upstream events get, instead of hoping
# debounce + luck keeps us under an unknown limit. A broadcast that wants
# to fire sooner than this is never dropped — it's deferred to fire
# exactly when the cooldown clears, still rendering whatever the CURRENT
# state is at that moment, not stale data from when it was queued.
MIN_EDIT_INTERVAL_SECONDS = 10

# Static thumbnail used on the homelab "quick access" Section. Swap for
# per-service icons if you have them; falls back to the bot's own avatar.
HOMELAB_THUMBNAIL_URL = None  # Set a permanent (non-expiring) image URL here if you have one; otherwise falls back to the bot's avatar.

# One-time migration: if the old relative-path DB exists and the absolute
# one doesn't yet, carry it over so the existing config isn't lost.
os.makedirs(DB_DIR, exist_ok=True)
_LEGACY_DB = os.path.abspath(os.path.join("DATABASES", "jellyfin.db"))
if _LEGACY_DB != os.path.abspath(DASHBOARD_DB_PATH) and os.path.exists(_LEGACY_DB) and not os.path.exists(DASHBOARD_DB_PATH):
    shutil.copy2(_LEGACY_DB, DASHBOARD_DB_PATH)

# /Users returns every full user object (policies, config) — the single
# heaviest call this cog makes, used only for a count. Cached for this long.
USER_COUNT_TTL_SECONDS = 600

# /Sessions filter — skip sessions idle longer than this (Jellyfin's own
# dashboard uses 960s).
ACTIVE_WITHIN_SECONDS = 960


# --- DATABASE LAYER (aiosqlite) ---
# Dashboard uses a custom error-tuple pattern for its config DB.
# These stay local; the shared utils.db is better suited to cogs
# that don't need per-call error differentiation.
async def _run_query(query, params=(), fetch=False, fetchall=False):
    try:
        async with aiosqlite.connect(DASHBOARD_DB_PATH) as conn:
            cursor = await conn.execute(query, params)
            if fetchall:
                result = await cursor.fetchall()
            elif fetch:
                result = await cursor.fetchone()
            else:
                result = None
            await cursor.close()
            await conn.commit()
            return result, None
    except aiosqlite.Error as e:
        logger.error(f"DB error on query [{query}]: {e}")
        return None, str(e)


async def init_db():
    await _run_query("PRAGMA journal_mode=WAL")
    await _run_query('''
        CREATE TABLE IF NOT EXISTS dashboard_config (
            guild_id INTEGER PRIMARY KEY,
            dashboard_channel_id INTEGER,
            dashboard_message_id INTEGER
        )
    ''')


async def get_guild_config(guild_id: int):
    row, err = await _run_query(
        "SELECT guild_id, dashboard_channel_id, dashboard_message_id FROM dashboard_config WHERE guild_id=?",
        (guild_id,), fetch=True
    )
    if err:
        return None, err
    if row is None:
        return {"guild_id": guild_id, "dashboard_channel_id": None, "dashboard_message_id": None}, None
    return {"guild_id": row[0], "dashboard_channel_id": row[1], "dashboard_message_id": row[2]}, None


async def upsert_guild_config(guild_id: int, **fields):
    existing, err = await _run_query("SELECT guild_id FROM dashboard_config WHERE guild_id=?", (guild_id,), fetch=True)
    if err:
        return err
    if existing is None:
        columns = ["guild_id"] + list(fields.keys())
        placeholders = ", ".join(["?"] * len(columns))
        values = [guild_id] + list(fields.values())
        _, err = await _run_query(f"INSERT INTO dashboard_config ({', '.join(columns)}) VALUES ({placeholders})", tuple(values))
        return err
    set_clause = ", ".join(f"{k}=?" for k in fields.keys())
    values = list(fields.values()) + [guild_id]
    _, err = await _run_query(f"UPDATE dashboard_config SET {set_clause} WHERE guild_id=?", tuple(values))
    return err


async def get_all_dashboard_configs():
    rows, err = await _run_query(
        "SELECT guild_id, dashboard_channel_id, dashboard_message_id FROM dashboard_config "
        "WHERE dashboard_channel_id IS NOT NULL AND dashboard_message_id IS NOT NULL",
        fetchall=True
    )
    if err or not rows:
        return []
    return [{"guild_id": r[0], "dashboard_channel_id": r[1], "dashboard_message_id": r[2]} for r in rows]


# --- HELPER: search current guild for a message by ID (used for /dashboard's Set Message ID) ---
async def search_guild_for_message(guild: discord.Guild, msg_id_int: int):
    for ch in guild.text_channels:
        perms = ch.permissions_for(guild.me)
        if not perms.view_channel or not perms.read_message_history:
            continue
        try:
            return await ch.fetch_message(msg_id_int), None
        except discord.NotFound:
            continue
        except discord.Forbidden:
            continue
        except discord.HTTPException:
            continue
    return None, "Message not found anywhere in this server."


def ticks_to_seconds(ticks) -> float:
    return (ticks or 0) / 10_000_000  # Jellyfin uses 100ns ticks


def format_progress_bar(position_ticks, runtime_ticks, length=20) -> str:
    if not runtime_ticks:
        return ""
    pct = max(0.0, min(1.0, ticks_to_seconds(position_ticks) / ticks_to_seconds(runtime_ticks)))
    filled = round(pct * length)
    return "▓" * filled + "░" * (length - filled)


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def get_poster_url(item: dict) -> str | None:
    """Live poster straight from Jellyfin. Episodes use the series poster
    (SeriesId), not the episode stillframe. Requested at 300px tall —
    Discord renders thumbnails small anyway, so this loads far faster than
    the full-size original. The image tag is appended so a changed poster
    gets a new URL instead of Discord serving its stale cached copy."""
    if item.get("Type") == "Episode" and item.get("SeriesId"):
        item_id = item["SeriesId"]
        tag = item.get("SeriesPrimaryImageTag")
    else:
        item_id = item.get("Id")
        tag = (item.get("ImageTags") or {}).get("Primary")

    if not item_id or not JELLYFIN_URL:
        return None
    url = f"{JELLYFIN_URL}/Items/{item_id}/Images/Primary?fillHeight=300&quality=80"
    if tag:
        url += f"&tag={tag}"
    return url


def _norm_id(value) -> str:
    """Jellyfin REST returns ids without dashes; event payloads can carry
    the dashed GUID form. Normalise before comparing."""
    return str(value or "").replace("-", "").lower()


def _effective_position_ticks(s: dict) -> int:
    """Stored position only moves when an event arrives, which can be
    minutes apart. While playing, add the wall-clock time since that
    position was recorded so the bar tracks real progress. Capped at the
    runtime; paused streams don't move."""
    play_state = s.get("PlayState", {})
    ticks = play_state.get("PositionTicks") or 0
    if not play_state.get("IsPaused", False):
        recorded_at = s.get("_PosAt")
        if recorded_at:
            ticks += int((time.time() - recorded_at) * 10_000_000)
    runtime = (s.get("NowPlayingItem") or {}).get("RunTimeTicks")
    if runtime:
        ticks = min(ticks, runtime)
    return max(0, ticks)


def _progress(s: dict) -> dict | None:
    """Everything the progress line shows, computed once. None if the item
    has no runtime (live TV, some music)."""
    runtime_ticks = (s.get("NowPlayingItem") or {}).get("RunTimeTicks")
    if not runtime_ticks:
        return None
    position_ticks = _effective_position_ticks(s)
    pos_s = ticks_to_seconds(position_ticks)
    run_s = ticks_to_seconds(runtime_ticks)
    return {
        "pct": round(max(0.0, min(1.0, pos_s / run_s)) * 100),
        "left": format_duration(max(0.0, run_s - pos_s)),
        "bar": format_progress_bar(position_ticks, runtime_ticks, length=15),
    }


def _sessions_fingerprint(sessions: list) -> frozenset:
    """A snapshot of only the parts of session state that should trigger a
    dashboard update: which session, which item, paused or not. Position
    ticks are deliberately excluded — those change on almost every push
    during normal playback and would defeat the entire point of this check.
    Two fingerprints being equal means 'nothing worth broadcasting changed'.

    This is the exact mechanism from the original native-WebSocket design
    — unchanged. Only what feeds it (SSE-reconstructed sessions instead of
    a native WS Sessions push) is different."""
    parts = []
    for s in sessions:
        item = s.get("NowPlayingItem")
        if not item:
            continue
        play_state = s.get("PlayState", {})
        parts.append((
            s.get("Id"),
            item.get("Id"),
            play_state.get("IsPaused", False),
        ))
    return frozenset(parts)


def _now_playing_signature(sessions: list) -> tuple:
    """Snapshot of everything the Now Playing card renders, at the
    precision it renders it: percent and minutes-left, not raw ticks. So
    tick_loop only edits when the visible text would actually change."""
    parts = []
    for s in sessions:
        item = s.get("NowPlayingItem")
        if not item:
            continue
        prog = _progress(s)
        parts.append((
            s.get("Id") or "",
            item.get("Id") or "",
            s.get("PlayState", {}).get("IsPaused", False),
            prog["pct"] if prog else None,
            prog["left"] if prog else None,
        ))
    return tuple(sorted(parts, key=lambda p: (p[0], p[1])))


# Fields that represent an actual library change. latency_ms fluctuates on
# every single fetch and should never by itself count as "the library
# changed" — comparing on this field is what caused stats_loop to
# broadcast on every single tick even when nothing real changed.
STATS_COMPARABLE_KEYS = ("movie_count", "series_count", "episode_count", "user_count", "version")


def _stats_comparable(stats: dict | None) -> dict | None:
    if stats is None:
        return None
    return {k: stats.get(k) for k in STATS_COMPARABLE_KEYS}


_user_count_cache = {"count": None, "ts": 0.0}


async def _get_user_count(jf: JellyfinClient, force: bool = False):
    """Returns (count, err). /Users is only actually fetched every
    USER_COUNT_TTL_SECONDS (or when forced); otherwise the cached count is
    reused. A failed fetch keeps serving the last good count."""
    now = time.time()
    if not force and _user_count_cache["count"] is not None and now - _user_count_cache["ts"] < USER_COUNT_TTL_SECONDS:
        return _user_count_cache["count"], None
    try:
        users = await jf.get_users()
        _user_count_cache["count"] = len(users)
        _user_count_cache["ts"] = now
        return _user_count_cache["count"], None
    except Exception as e:
        return _user_count_cache["count"], str(e)


async def get_system_stats(jf: JellyfinClient, retry_on_counts_failure: bool = True, force_users: bool = False):
    """Returns (stats_dict, errors_list). Each of the three underlying calls
    is independent — a failure in one no longer blanks out data from the
    other two. Missing individual values fall back to 'N/A'. stats_dict is
    only None if the integration isn't configured at all.

    /Items/Counts gets one automatic retry after a short delay before
    falling back to 'N/A' — this is the call most likely to matter right
    after a library-changed event, so a single transient failure shouldn't
    silently blank the numbers out."""
    errors = []

    async def _fetch_sys_info_timed():
        start = time.perf_counter()
        try:
            result = await jf.get_json("/System/Info")
            elapsed_ms = round((time.perf_counter() - start) * 1000)
            return result, None, elapsed_ms
        except Exception as e:
            elapsed_ms = round((time.perf_counter() - start) * 1000)
            return None, str(e), elapsed_ms

    async def _fetch_counts_with_retry():
        try:
            return await jf.get_json("/Items/Counts"), None
        except Exception as e:
            if retry_on_counts_failure:
                logger.warning(f"[Jellyfin] /Items/Counts failed once, retrying in 2s: {e}")
                await asyncio.sleep(2)
                try:
                    return await jf.get_json("/Items/Counts"), None
                except Exception as e2:
                    return None, str(e2)
            return None, str(e)

    (sys_info, sys_err, latency_ms), (counts, counts_err), (user_count, users_err) = await asyncio.gather(
        _fetch_sys_info_timed(),
        _fetch_counts_with_retry(),
        _get_user_count(jf, force=force_users),
    )

    if sys_err:
        errors.append(f"/System/Info failed: {sys_err}")
        sys_info = {}
        latency_ms = "N/A"

    if counts_err:
        errors.append(f"/Items/Counts failed: {counts_err}")
        counts = {}

    if users_err:
        errors.append(f"/Users failed: {users_err}")

    stats = {
        "movie_count": counts.get("MovieCount", "N/A"),
        "series_count": counts.get("SeriesCount", "N/A"),
        "episode_count": counts.get("EpisodeCount", "N/A"),
        "user_count": user_count if user_count is not None else "N/A",
        "version": sys_info.get("Version", "N/A"),
        "latency_ms": latency_ms,
    }

    return stats, errors


# --- COMPONENTS V2 BUILDERS ---

def _fmt(value) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


_NBSP = " "  # Discord collapses runs of normal spaces; non-breaking spaces survive, so use these for column padding.

WATCH_URL = "https://jellyfin.ushereats.com/"
APP_URL = "https://github.com/Moonfin-Client"

# ANSI colours for the stats block. 0;3x = colour only, no bold.
_ESC = "\u001b"
_CYAN = f"{_ESC}[0;36m"
_GREEN = f"{_ESC}[0;32m"
_YELLOW = f"{_ESC}[0;33m"
_PINK = f"{_ESC}[0;35m"
_GREY = f"{_ESC}[0;30m"
_RESET = f"{_ESC}[0m"

# Every component counts toward Discord's 40-per-message cap, nested ones
# included. Homelab card = 11, stats card = 3, Now Playing header = 3,
# each poster row = 3 (Section + TextDisplay + Thumbnail), overflow block = 1.
# 11 + 3 + 3 + 3*7 + 1 = 39. Streams past 7 go into the overflow block as
# compact text lines, limited only by the shared 4000-character cap.
POSTER_LIMIT = 7
MESSAGE_TEXT_LIMIT = 3900  # Discord's cap is 4000; 100 kept as headroom


def build_homelab_container(bot) -> discord.ui.Container:
    container = discord.ui.Container()
    thumbnail_url = HOMELAB_THUMBNAIL_URL or bot.user.display_avatar.url
    container.add_item(discord.ui.Section(
        discord.ui.TextDisplay("# Media Server Access\n> Quick access to the UsherEats Jellyfin server."),
        accessory=discord.ui.Thumbnail(media=thumbnail_url),
    ))
    container.add_item(discord.ui.Separator(visible=True, spacing=discord.SeparatorSpacing.small))
    container.add_item(discord.ui.Section(
        discord.ui.TextDisplay(
            "**🌐 Watch in browser**\n"
            "-# jellyfin.ushereats.com · works on any device, nothing to install\n"
            "-# Can buffer or lag when the server has to transcode, especially 4K, HDR or surround audio."
        ),
        accessory=discord.ui.Button(style=discord.ButtonStyle.link, label="Open", url=WATCH_URL),
    ))
    container.add_item(discord.ui.Section(
        discord.ui.TextDisplay(
            "**📱 Moonfin app**\n"
            "-# Recommended · plays more formats natively, so less transcoding\n"
            "-# Smoother playback, faster loading and better quality. Sign in with the same username and password."
        ),
        accessory=discord.ui.Button(style=discord.ButtonStyle.link, label="Get", url=APP_URL),
    ))
    return container


def build_stats_container(bot, stats: dict | None) -> discord.ui.Container:
    container = discord.ui.Container()
    container.add_item(discord.ui.TextDisplay(
        "## Library Statistics\n> Jellyfin overview — refreshed automatically"
    ))

    if stats is None:
        container.add_item(discord.ui.TextDisplay(f"{bot.cross_emoji}  Unable to reach Jellyfin."))
        return container

    active_sessions = getattr(bot.get_cog("JellyfinDashboard"), "active_session_count", 0)
    latency = stats["latency_ms"]
    latency_text = f"{latency}ms" if isinstance(latency, int) else "N/A"

    table = (
        "```ansi\n"
        f"Movies            : {_CYAN}{_fmt(stats['movie_count']):>6}{_RESET}\n"
        f"Series            : {_CYAN}{_fmt(stats['series_count']):>6}{_RESET}\n"
        f"Episodes          : {_CYAN}{_fmt(stats['episode_count']):>6}{_RESET}\n"
        f"{_GREY}────────────────────────────{_RESET}\n"
        f"Registered Users  : {_GREEN}{_fmt(stats['user_count']):>6}{_RESET}\n"
        f"Active Sessions   : {_GREEN}{_fmt(active_sessions):>6}{_RESET}\n"
        f"API Latency       : {_YELLOW}{latency_text:>6}{_RESET}\n"
        f"Jellyfin Version  : {_PINK}{stats['version']}{_RESET}\n"
        "```"
    )
    container.add_item(discord.ui.TextDisplay(table))
    return container


def _md(value) -> str:
    """Escape Discord markdown in user-controlled text (usernames, titles,
    device/client names) so a stray * _ ` ~ | can't break the layout."""
    return discord.utils.escape_markdown(str(value))


def _describe(s: dict) -> dict:
    """Shared pieces for both the poster row and the compact overflow row."""
    item = s["NowPlayingItem"]
    play_state = s.get("PlayState", {})
    paused = play_state.get("IsPaused", False)
    play_method = play_state.get("PlayMethod", "Unknown")

    if item.get("Type") == "Episode" and item.get("SeriesName"):
        headline = item["SeriesName"]
        sub_parts = []
        season, episode = item.get("ParentIndexNumber"), item.get("IndexNumber")
        if season is not None and episode is not None:
            sub_parts.append(f"S{season}E{episode}")
        if item.get("Name"):
            sub_parts.append(_md(item["Name"]))
    else:
        headline = item.get("Name", "Unknown title")
        sub_parts = []
        if item.get("ProductionYear"):
            sub_parts.append(str(item["ProductionYear"]))
        if item.get("RunTimeTicks"):
            sub_parts.append(format_duration(ticks_to_seconds(item["RunTimeTicks"])))

    method = "Transcode" if play_method == "Transcode" else ("Direct Stream" if play_method == "DirectStream" else "Direct Play")
    return {
        "user": _md(s.get("UserName", "Unknown")),
        "verb": "paused" if paused else "is watching",
        "headline": _md(headline),
        "sub": " · ".join(sub_parts),
        "progress": _progress(s),
        "status": "⏸ Paused" if paused else "▶ Playing",
        "icon": "⏸" if paused else "▶",
        "method": method,
        "client": _md(s.get("Client", "Unknown app")),
        "device": _md(s.get("DeviceName", "Unknown device")),
    }


def _stream_text(s: dict) -> str:
    d = _describe(s)
    lines = [f"**{d['user']}** {d['verb']} **{d['headline']}**"]
    if d["sub"]:
        lines.append(f"-# {d['sub']}")
    if d["progress"]:
        p = d["progress"]
        lines.append(f"`{p['bar']}` {p['pct']}% · {p['left']} left")
    lines.append(f"-# {d['status']} · {d['method']} · {d['client']} · {d['device']}")
    return "\n".join(lines)


def _stream_compact(s: dict) -> str:
    """Two-line version for streams past the poster limit — no thumbnail,
    so it costs no components beyond the one shared TextDisplay."""
    d = _describe(s)
    first = f"**{d['user']}** {d['verb']} **{d['headline']}**"
    if d["sub"]:
        first += f" · {d['sub']}"
    second = [d["icon"]]
    if d["progress"]:
        second.append(f"{d['progress']['pct']}% · {d['progress']['left']} left ·")
    second.append(f"{d['method']} · {d['client']} · {d['device']}")
    return first + "\n-# " + " ".join(second)


def build_now_playing_container(bot, sessions: list, text_budget: int = 3000) -> discord.ui.Container:
    """text_budget = characters still free in the whole message (Discord's
    4000 cap is shared across every TextDisplay in all three cards)."""
    playing = [s for s in sessions if s.get("NowPlayingItem")]
    container = discord.ui.Container()
    now_ts = int(time.time())

    if not playing:
        container.add_item(discord.ui.TextDisplay(
            f"### ▶ Now Playing\n-# nobody's watching right now · updated <t:{now_ts}:R>"
        ))
        return container

    paused_count = sum(1 for s in playing if s.get("PlayState", {}).get("IsPaused", False))
    playing_count = len(playing) - paused_count
    header = (
        f"### ▶ Now Playing\n"
        f"-# {len(playing)} stream{'s' if len(playing) != 1 else ''} · {playing_count} playing · "
        f"{paused_count} paused · updated <t:{now_ts}:R>"
    )
    container.add_item(discord.ui.TextDisplay(header))
    container.add_item(discord.ui.Separator(visible=True, spacing=discord.SeparatorSpacing.small))
    budget = text_budget - len(header) - 60  # 60 reserved for a "+N more" line

    # Playing first, then paused; alphabetical by user within each, so rows
    # don't shuffle around between edits.
    ordered = sorted(playing, key=lambda s: (s.get("PlayState", {}).get("IsPaused", False), (s.get("UserName") or "").lower()))

    shown = 0
    for s in ordered[:POSTER_LIMIT]:
        text = _stream_text(s)
        if len(text) > budget:
            break
        budget -= len(text)
        text_display = discord.ui.TextDisplay(text)
        poster_url = get_poster_url(s["NowPlayingItem"])
        if poster_url:
            container.add_item(discord.ui.Section(text_display, accessory=discord.ui.Thumbnail(media=poster_url)))
        else:
            container.add_item(text_display)
        shown += 1

    # Everything past the poster limit goes into ONE compact text block.
    overflow_lines = []
    if shown == POSTER_LIMIT:
        for s in ordered[POSTER_LIMIT:]:
            line = _stream_compact(s)
            if len(line) + 2 > budget:
                break
            budget -= len(line) + 2
            overflow_lines.append(line)
            shown += 1

    remaining = len(ordered) - shown
    if remaining > 0:
        overflow_lines.append(f"-# + {remaining} more stream{'s' if remaining != 1 else ''} not shown")
    if overflow_lines:
        container.add_item(discord.ui.TextDisplay("\n".join(overflow_lines)))

    return container


def _container_text_len(container: discord.ui.Container) -> int:
    return sum(len(c.content) for c in container.walk_children() if isinstance(c, discord.ui.TextDisplay))


class DashboardLayoutView(discord.ui.LayoutView):
    def __init__(self, bot, stats: dict | None, sessions: list):
        super().__init__(timeout=None)
        homelab = build_homelab_container(bot)
        stats_card = build_stats_container(bot, stats)
        used = _container_text_len(homelab) + _container_text_len(stats_card)
        self.add_item(homelab)
        self.add_item(stats_card)
        self.add_item(build_now_playing_container(bot, sessions, text_budget=MESSAGE_TEXT_LIMIT - used))


# --- CONFIG PANEL (classic embed-based ephemeral panel) ---
class SetMessageIdModal(discord.ui.Modal, title="Set Dashboard Message"):
    def __init__(self, callback_view):
        super().__init__()
        self.callback_view = callback_view
        self.message_id_input = discord.ui.TextInput(
            label="Existing message ID",
            placeholder="Paste the ID of a message already sent in this server",
            required=True,
            max_length=25
        )
        self.add_item(self.message_id_input)

    async def on_submit(self, interaction: discord.Interaction):
        bot = interaction.client
        try:
            msg_id_int = int(self.message_id_input.value.strip())
        except ValueError:
            embed = discord.Embed(description=f"{bot.cross_emoji}  Invalid message ID format.", color=bot.red_color)
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        msg, err = await search_guild_for_message(interaction.guild, msg_id_int)
        if err:
            embed = discord.Embed(description=f"{bot.cross_emoji}  {err}", color=bot.red_color)
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        db_err = await upsert_guild_config(
            interaction.guild.id,
            dashboard_channel_id=msg.channel.id,
            dashboard_message_id=msg.id
        )
        if db_err:
            embed = discord.Embed(description=f"{bot.cross_emoji}  Database error: `{db_err}`", color=bot.red_color)
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        # Immediately refresh that message with current live session state.
        cog = bot.get_cog("JellyfinDashboard")
        if cog is not None:
            cog._dashboard_configs_cache = None
            await cog.refresh_message(interaction.guild.id, msg.channel.id, msg.id, source="set_message_id")

        content, embeds = await self.callback_view.build_full_display()
        await interaction.response.edit_message(content=content, embeds=embeds, view=self.callback_view)


class DashboardChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, callback_view, current_channel_id: int = None):
        kwargs = dict(placeholder="Select dashboard channel...", channel_types=[discord.ChannelType.text], row=1)
        if current_channel_id:
            kwargs["default_values"] = [discord.Object(id=current_channel_id)]
        super().__init__(**kwargs)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        bot = interaction.client
        try:
            channel = interaction.guild.get_channel(self.values[0].id)
            if channel is None:
                embed = discord.Embed(description=f"{bot.cross_emoji}  Couldn't resolve that channel — try again.", color=bot.red_color)
                return await interaction.response.send_message(embed=embed, ephemeral=True)

            perms = channel.permissions_for(interaction.guild.me)
            if not perms.send_messages or not perms.embed_links:
                embed = discord.Embed(
                    description=f"{bot.cross_emoji}  I lack `Send Messages` or `Embed Links` permission in {channel.mention}.",
                    color=bot.red_color
                )
                return await interaction.response.send_message(embed=embed, ephemeral=True)

            await interaction.response.defer()

            old_config, _ = await get_guild_config(interaction.guild.id)
            if old_config and old_config.get("dashboard_channel_id") and old_config.get("dashboard_message_id"):
                old_channel = interaction.guild.get_channel(old_config["dashboard_channel_id"])
                if old_channel is not None:
                    try:
                        old_msg = await old_channel.fetch_message(old_config["dashboard_message_id"])
                        await old_msg.delete()
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        pass

            cog = bot.get_cog("JellyfinDashboard")
            sessions = cog.latest_sessions if cog else []
            stats = await cog.get_stats() if cog else None
            dashboard_view = DashboardLayoutView(bot, stats, sessions)

            try:
                new_msg = await channel.send(view=dashboard_view)
            except discord.HTTPException as e:
                embed = discord.Embed(description=f"{bot.cross_emoji}  Failed to post dashboard message: `{e}`", color=bot.red_color)
                return await interaction.followup.send(embed=embed, ephemeral=True)

            await upsert_guild_config(interaction.guild.id, dashboard_channel_id=channel.id, dashboard_message_id=new_msg.id)
            if cog:
                cog._dashboard_configs_cache = None

            content, embeds = await self.callback_view.build_full_display()
            await interaction.edit_original_response(content=content, embeds=embeds, view=self.callback_view)

        except Exception as e:
            logger.error(f"[JellyfinDashboard] Unhandled exception: {e}", exc_info=True)
            embed = discord.Embed(description=f"{bot.cross_emoji}  Something broke: `{type(e).__name__}: {e}`", color=bot.red_color)
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(embed=embed, ephemeral=True)
                else:
                    await interaction.response.send_message(embed=embed, ephemeral=True)
            except discord.HTTPException:
                pass


class DashboardConfigView(discord.ui.View):
    def __init__(self, bot, guild_id: int, config: dict):
        super().__init__(timeout=300.0)
        self.bot = bot
        self.guild_id = guild_id
        self.config = config
        self.add_item(DashboardChannelSelect(self, config.get("dashboard_channel_id")))

    async def refresh_config(self):
        config, err = await get_guild_config(self.guild_id)
        if err:
            return None, err
        self.config = config
        return config, None

    async def build_full_display(self):
        config, err = await self.refresh_config()
        if err:
            embed = discord.Embed(description=f"{self.bot.cross_emoji}  Database error: `{err}`", color=self.bot.red_color)
            return None, [embed]

        cog = self.bot.get_cog("JellyfinDashboard")
        ws_status = f"{self.bot.check_emoji} **CONNECTED**" if cog and cog.ws_connected else f"{self.bot.cross_emoji} **DISCONNECTED**"
        channel_text = f"<#{config['dashboard_channel_id']}>" if config["dashboard_channel_id"] else "`Not Set`"
        message_text = f"`{config['dashboard_message_id']}`" if config["dashboard_message_id"] else "`Not Set`"

        embed = discord.Embed(
            title="⚙️ Now Playing Dashboard — Module Configuration",
            description=(
                "Select a channel below to post a fresh dashboard there, or use **Set Message ID** "
                "to have an existing message adopted as the dashboard instead of creating a new one."
            ),
            color=self.bot.embed_color
        )
        embed.add_field(name="Jellyfin Connection", value=ws_status, inline=True)
        embed.add_field(name="Dashboard Channel", value=channel_text, inline=True)
        embed.add_field(name="Dashboard Message ID", value=message_text, inline=True)
        embed.set_footer(text=f"Guild ID Node: {self.guild_id}")

        return None, [embed]

    @discord.ui.button(label="Set Message ID", style=discord.ButtonStyle.secondary, row=0)
    async def set_message_id_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(SetMessageIdModal(self))
        except discord.HTTPException as e:
            embed = discord.Embed(description=f"{self.bot.cross_emoji}  Failed to open editor: `{e}`", color=self.bot.red_color)
            await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="Refresh Now", style=discord.ButtonStyle.success, row=0)
    async def refresh_now_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        config, err = await get_guild_config(self.guild_id)
        if err:
            embed = discord.Embed(description=f"{bot.cross_emoji}  Database error: `{err}`", color=bot.red_color)
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        if not config.get("dashboard_channel_id") or not config.get("dashboard_message_id"):
            embed = discord.Embed(
                description=f"{bot.error_emoji}  No dashboard message configured yet — pick a channel first.",
                color=bot.yellow_color
            )
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        # Ack immediately — the refresh below can exceed the 3s window an
        # unacknowledged interaction gets before its token expires.
        await interaction.response.defer()

        cog = bot.get_cog("JellyfinDashboard")
        if cog is not None:
            await cog.get_stats(force=True, force_users=True)
            await cog.refresh_message(config["guild_id"], config["dashboard_channel_id"], config["dashboard_message_id"], source="manual_refresh_button")

        content, embeds = await self.build_full_display()
        await interaction.edit_original_response(content=content, embeds=embeds, view=self)
        await interaction.followup.send("✅ Dashboard refreshed.", ephemeral=True)


# --- COG ---
class JellyfinDashboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.jf = JellyfinClient(bot, api_key=DASHBOARD_API_KEY)
        self.category = "Utility"
        self._sessions_by_id: dict[str, dict] = {}  # rebuilt from discrete SSE events into the same shape the native WS used to push wholesale
        self.ws_connected = False
        self._ws_task = None
        self._last_sessions_fingerprint = frozenset()
        self._stats_cache: dict | None = None
        self._dashboard_configs_cache: list | None = None
        self._pending_broadcast_task: asyncio.Task | None = None  # trailing-edge debounce handle
        self._last_now_playing_signature: tuple | None = None  # what tick_loop compares against
        self._pending_sessions_fetch: asyncio.Task | None = None  # coalesces concurrent new-session hydration into one shared REST call
        self._last_edit_ts: dict[int, float] = {}  # message_id -> unix ts of the last SUCCESSFUL edit — the actual gate enforcing MIN_EDIT_INTERVAL_SECONDS
        self._pending_edit_tasks: dict[int, asyncio.Task] = {}  # message_id -> a deferred edit already queued for when its cooldown clears
        self._edit_locks: dict[int, asyncio.Lock] = {}  # message_id -> lock, so two edits to one message can never overlap

    @property
    def latest_sessions(self) -> list:
        return list(self._sessions_by_id.values())

    @property
    def active_session_count(self) -> int:
        return sum(1 for s in self.latest_sessions if s.get("NowPlayingItem"))

    async def get_stats(self, force: bool = False, force_users: bool = False) -> dict | None:
        """Cached Jellyfin stats — only re-fetched when forced (a
        library-changed event, a manual refresh, or stats_loop) instead of
        on every broadcast. force_users also bypasses the 10-minute user
        count cache (manual Refresh Now only)."""
        if force or self._stats_cache is None:
            stats, _ = await get_system_stats(self.jf, force_users=force_users)
            self._stats_cache = stats
        return self._stats_cache

    async def get_dashboard_configs(self, force: bool = False) -> list:
        """Cached list of configured dashboards — only re-read from SQLite
        when forced (i.e. right after /dashboard actually changes a config)
        instead of on every single broadcast."""
        if force or self._dashboard_configs_cache is None:
            self._dashboard_configs_cache = await get_all_dashboard_configs()
        return self._dashboard_configs_cache

    async def cog_load(self):
        await init_db()
        self._ws_task = asyncio.create_task(self._sse_loop())
        if not self.tick_loop.is_running():
            self.tick_loop.start()
        if not self.stats_loop.is_running():
            self.stats_loop.start()
        if not self.session_resync_loop.is_running():
            self.session_resync_loop.start()

    async def cog_unload(self):
        for task in (self._ws_task, self._pending_broadcast_task, self._pending_sessions_fetch):
            if task is not None and not task.done():
                task.cancel()
        for task in self._pending_edit_tasks.values():
            if not task.done():
                task.cancel()
        self.session_resync_loop.cancel()
        self.tick_loop.cancel()
        self.stats_loop.cancel()
        # No session close here — main.py owns bot.http_session.

    # --- LOOPS ---
    # Each loop body catches Exception itself. That is the real protection:
    # tasks.loop's .error handler runs while the loop is still marked as
    # running, so calling start() from it never worked — the old "restart
    # on crash" handlers were dead code. The handlers below only log
    # whatever BaseException could still escape.

    @tasks.loop(seconds=30)
    async def tick_loop(self):
        """Keeps progress bars moving while something plays. Only edits if
        the rendered Now Playing content (percent / minutes left / pause
        state / which streams) actually changed since the last broadcast
        from ANY source. Also runs once after the last stream disappears,
        so the card can't get stuck showing a stream that already ended."""
        try:
            sessions = self.latest_sessions
            any_playing = any(s.get("NowPlayingItem") for s in sessions)
            if not any_playing and not self._last_now_playing_signature:
                return
            if _now_playing_signature(sessions) == self._last_now_playing_signature:
                logger.debug("[JellyfinDashboard] tick_loop: Now Playing content unchanged, skipping broadcast.")
                return
            await self._broadcast_dashboard_update(source="tick_loop_30s")
        except Exception as e:
            logger.error(f"[JellyfinDashboard] tick_loop failed to broadcast update: {e}", exc_info=True)

    @tick_loop.before_loop
    async def before_tick_loop(self):
        await self.bot.wait_until_ready()

    @tick_loop.error
    async def tick_loop_error(self, error: BaseException):
        logger.error(f"[JellyfinDashboard] tick_loop crashed: {type(error).__name__}: {error}", exc_info=error)

    @tasks.loop(seconds=STATS_LOOP_SECONDS)
    async def stats_loop(self):
        """Independent stats freshness check. Always refreshes the cache,
        only edits the message when a meaningful field changed (counts or
        version — latency deliberately excluded)."""
        try:
            fresh_stats, _ = await get_system_stats(self.jf)
            if fresh_stats is None:
                return
            old_comparable = _stats_comparable(self._stats_cache)
            new_comparable = _stats_comparable(fresh_stats)
            self._stats_cache = fresh_stats
            if new_comparable != old_comparable:
                await self._broadcast_dashboard_update(source="stats_loop_60s")
            else:
                logger.debug("[JellyfinDashboard] stats_loop: no meaningful change, skipping broadcast.")
        except Exception as e:
            logger.error(f"[JellyfinDashboard] stats_loop failed: {e}", exc_info=True)

    @stats_loop.before_loop
    async def before_stats_loop(self):
        await self.bot.wait_until_ready()

    @stats_loop.error
    async def stats_loop_error(self, error: BaseException):
        logger.error(f"[JellyfinDashboard] stats_loop crashed: {type(error).__name__}: {error}", exc_info=error)

    @tasks.loop(seconds=SESSION_RESYNC_INTERVAL_SECONDS)
    async def session_resync_loop(self):
        """Safety net against lost 'stopped' events: re-reads the live
        session list from REST. If that changes anything, the resync itself
        now triggers a broadcast (previously it updated state silently and
        relied on tick_loop, which never ran once nothing was playing)."""
        try:
            await self._resync_sessions_from_rest(source="session_resync_loop")
        except Exception as e:
            logger.error(f"[JellyfinDashboard] session_resync_loop failed: {e}", exc_info=True)

    @session_resync_loop.before_loop
    async def before_session_resync_loop(self):
        await self.bot.wait_until_ready()

    @session_resync_loop.error
    async def session_resync_loop_error(self, error: BaseException):
        logger.error(f"[JellyfinDashboard] session_resync_loop crashed: {type(error).__name__}: {error}", exc_info=error)

    # --- SSE CONNECTION (Tracearr) ---
    async def _sse_loop(self):
        """Persistent connection to the Tracearr SSE plugin's event stream.
        Reconnects automatically on any drop, with exponential backoff
        during a real outage. Requires the plugin installed on the
        Jellyfin server — see module docstring.

        Parsing follows the SSE spec: an event can carry several `data:`
        lines (joined with newlines) and is only dispatched at the blank
        line that ends it. The stream is read in raw chunks and split
        manually, so one unusually long line can't raise aiohttp's
        "line too long" error and drop the connection."""
        await self.bot.wait_until_ready()

        url = f"{JELLYFIN_URL}/api/sse/events"
        # SSE uses raw streaming — manual headers with the dashboard API key.
        headers = {"Authorization": f'MediaBrowser Token="{DASHBOARD_API_KEY}"', "Accept": "text/event-stream"}

        consecutive_failures = 0

        while True:
            connected_at = None
            try:
                session = self.bot.http_session
                timeout = aiohttp.ClientTimeout(total=None, sock_read=SSE_READ_TIMEOUT_SECONDS)
                async with session.get(url, headers=headers, timeout=timeout) as resp:
                    if resp.status == 404:
                        logger.error(
                            "[JellyfinDashboard] SSE endpoint returned 404 — is the Tracearr SSE plugin "
                            "installed and has Jellyfin been restarted since? See module docstring."
                        )
                    resp.raise_for_status()

                    self.ws_connected = True
                    connected_at = time.time()
                    logger.info("[JellyfinDashboard] SSE connected.")

                    # A disconnect may have missed events — rebuild session
                    # state from a fresh REST snapshot (broadcasts if it
                    # differs from what's on screen).
                    await self._resync_sessions_from_rest(source="sse_reconnect")

                    buffer = b""
                    event_type = None
                    data_lines: list[str] = []
                    async for chunk in resp.content.iter_any():
                        buffer += chunk
                        while b"\n" in buffer:
                            raw_line, buffer = buffer.split(b"\n", 1)
                            line = raw_line.decode("utf-8", errors="replace").rstrip("\r")

                            if line == "":
                                # Blank line = end of one event. Dispatch it.
                                if event_type is not None or data_lines:
                                    etype = event_type or "message"
                                    try:
                                        await self._handle_sse_event(etype, "\n".join(data_lines))
                                    except Exception as e:
                                        logger.error(f"[JellyfinDashboard] Failed handling SSE event '{etype}': {type(e).__name__}: {e}", exc_info=True)
                                event_type = None
                                data_lines = []
                            elif line.startswith(":"):
                                continue  # SSE comment / keepalive
                            elif line.startswith("event:"):
                                event_type = line[len("event:"):].strip()
                            elif line.startswith("data:"):
                                data_lines.append(line[len("data:"):].lstrip(" "))
                            # id:/retry: fields aren't used.

            except asyncio.CancelledError:
                self.ws_connected = False
                raise
            except Exception as e:
                logger.warning(f"[JellyfinDashboard] SSE disconnected — {type(e).__name__}: {e}")

            self.ws_connected = False

            if connected_at is not None and (time.time() - connected_at) > 10:
                consecutive_failures = 0
            else:
                consecutive_failures += 1

            delay = min(SSE_RECONNECT_DELAY * (2 ** consecutive_failures), SSE_MAX_RECONNECT_DELAY)
            logger.warning(f"[JellyfinDashboard] SSE reconnecting in {delay}s.")
            await asyncio.sleep(delay)

    async def _handle_sse_event(self, event_type: str, data_str: str):
        try:
            data = json.loads(data_str) if data_str else {}
        except json.JSONDecodeError:
            return
        if not isinstance(data, dict):
            return
        if event_type == "hello":
            logger.debug(f"[JellyfinDashboard] SSE hello — plugin v{data.get('version')} on {data.get('server')}.")
            return
        if event_type == "ping":
            return  # keepalive only
        if event_type in ("playing", "progress", "paused", "stopped"):
            await self._handle_playback_event(event_type, data)
        elif event_type in ("session.start", "session.end"):
            watching_role_cog = self.bot.get_cog("JellyfinWatchingRole")
            if watching_role_cog is not None:
                watching_role_cog.handle_session_event(event_type, data)
        elif event_type in ("library.item.added", "library.item.removed"):
            logger.debug(f"[JellyfinDashboard] {event_type}: itemId={data.get('itemId')} itemType={data.get('itemType')}")
            self._stats_cache = None  # library counts may have changed, force a re-fetch
            try:
                await self._debounced_broadcast(source="library_changed")
            except Exception as e:
                logger.error(f"[JellyfinDashboard] Broadcast failed after {event_type} push: {e}", exc_info=True)
            # Forward new items to the new-media announcer cog.
            if event_type == "library.item.added":
                new_media_cog = self.bot.get_cog("NewMediaAnnouncer")
                if new_media_cog is not None:
                    item_id = data.get("itemId")
                    if item_id:
                        new_media_cog.queue_item(item_id)
        # task.* and anything else isn't rendered on this dashboard — ignored.

    async def _hydrate_session(self, session_id: str) -> dict | None:
        """Full session details (title, poster, client, device) from REST.
        Concurrent callers share one in-flight request. If the session
        isn't in that snapshot — the shared request may have started just
        before this session existed — fetch once more after a short delay
        instead of dropping it until the next 2-minute resync."""
        for attempt in range(2):
            sessions = await self._get_sessions_snapshot_coalesced()
            if sessions is not None:
                session_obj = next((s for s in sessions if s.get("Id") == session_id), None)
                if session_obj is not None:
                    session_obj["_PosAt"] = time.time()
                    return session_obj
            if attempt == 0:
                await asyncio.sleep(1.5)
        return None

    async def _handle_playback_event(self, event_type: str, data: dict):
        watching_role_cog = self.bot.get_cog("JellyfinWatchingRole")
        if watching_role_cog is not None:
            watching_role_cog.handle_playback_event(event_type, data)

        session_id = data.get("sessionId")
        if not session_id:
            return

        if event_type == "stopped":
            self._sessions_by_id.pop(session_id, None)
        else:
            existing = self._sessions_by_id.get(session_id)
            existing_item = (existing or {}).get("NowPlayingItem")
            event_item_id = data.get("itemId")

            # Unknown session, or a known one that moved to a different item
            # (autoplay next episode) — fetch full details again, otherwise
            # the old title/poster stays up with the new episode's position.
            needs_hydration = (
                existing_item is None
                or (event_item_id and _norm_id(event_item_id) != _norm_id(existing_item.get("Id")))
            )

            if needs_hydration:
                session_obj = await self._hydrate_session(session_id)
                if session_obj is None:
                    return
                self._sessions_by_id[session_id] = session_obj
            else:
                play_state = existing.setdefault("PlayState", {})
                old_ticks = play_state.get("PositionTicks")
                new_ticks = data.get("positionTicks")
                if new_ticks is not None:
                    play_state["PositionTicks"] = new_ticks
                    existing["_PosAt"] = time.time()

                # Pause state. Tracearr has no separate resume event — a
                # resume arrives as 'progress' events. Every playback event
                # carries a 'state' field ("playing" / "paused"), so that is
                # the source of truth. Fallbacks if it's ever missing: the
                # event type itself, then "position moved forward = playing".
                state = str(data.get("state") or "").lower()
                if state == "paused":
                    play_state["IsPaused"] = True
                elif state == "playing":
                    play_state["IsPaused"] = False
                elif event_type == "paused":
                    play_state["IsPaused"] = True
                elif event_type == "playing":
                    play_state["IsPaused"] = False
                elif new_ticks is not None and old_ticks is not None and new_ticks > old_ticks:
                    play_state["IsPaused"] = False

        new_fingerprint = _sessions_fingerprint(self.latest_sessions)
        if new_fingerprint == self._last_sessions_fingerprint:
            return

        added = new_fingerprint - self._last_sessions_fingerprint
        removed = self._last_sessions_fingerprint - new_fingerprint
        logger.debug(f"[JellyfinDashboard] Sessions fingerprint changed — added={added} removed={removed}")
        self._last_sessions_fingerprint = new_fingerprint
        try:
            await self._debounced_broadcast(source="sessions_push")
        except Exception as e:
            logger.error(f"[JellyfinDashboard] Broadcast failed after sessions_push: {e}", exc_info=True)

    async def _fetch_all_sessions(self) -> list | None:
        try:
            return await self.jf.get_sessions(active_within=ACTIVE_WITHIN_SECONDS)
        except Exception as e:
            logger.error(f"[JellyfinDashboard] REST /Sessions fetch failed: {e}")
            return None

    async def _get_sessions_snapshot_coalesced(self) -> list | None:
        """If a fetch is already in flight, every concurrent caller awaits
        that SAME task instead of starting a redundant one."""
        if self._pending_sessions_fetch is None or self._pending_sessions_fetch.done():
            self._pending_sessions_fetch = asyncio.create_task(self._fetch_all_sessions())
        return await asyncio.shield(self._pending_sessions_fetch)

    async def _resync_sessions_from_rest(self, source: str = "resync"):
        """Replaces in-memory sessions with REST's live list. If that
        changes what's on screen (a stream that ended without a 'stopped'
        event, one that started while disconnected), broadcasts — this was
        the gap that let a finished stream sit on the dashboard forever."""
        sessions = await self._fetch_all_sessions()
        if sessions is None:
            return
        now = time.time()
        for s in sessions:
            s["_PosAt"] = now
        self._sessions_by_id = {s["Id"]: s for s in sessions if s.get("Id")}
        new_fingerprint = _sessions_fingerprint(self.latest_sessions)
        if new_fingerprint != self._last_sessions_fingerprint:
            logger.debug(f"[JellyfinDashboard] {source}: REST state differs from last known — broadcasting.")
            self._last_sessions_fingerprint = new_fingerprint
            await self._debounced_broadcast(source=source)

    async def _debounced_broadcast(self, source: str = "unknown"):
        """Trailing-edge debounce: every call resets the timer. Only once
        DEBOUNCE_SECONDS pass with no further event does one broadcast fire,
        showing the final state."""
        if self._pending_broadcast_task is not None and not self._pending_broadcast_task.done():
            self._pending_broadcast_task.cancel()
            logger.debug(f"[JellyfinDashboard] Debounce timer reset by '{source}'.")
        else:
            logger.debug(f"[JellyfinDashboard] Debounce timer started by '{source}'.")
        self._pending_broadcast_task = asyncio.create_task(self._delayed_broadcast(source))

    async def _delayed_broadcast(self, source: str):
        try:
            await asyncio.sleep(DEBOUNCE_SECONDS)
        except asyncio.CancelledError:
            return
        try:
            await asyncio.shield(self._broadcast_dashboard_update(source=f"{source}_debounced"))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[JellyfinDashboard] Debounced broadcast failed: {e}", exc_info=True)

    async def _broadcast_dashboard_update(self, source: str = "unknown"):
        """Refreshes every guild's dashboard with the current state. Each
        guild is isolated: one failing never stops the others."""
        self._last_now_playing_signature = _now_playing_signature(self.latest_sessions)
        try:
            configs = await self.get_dashboard_configs()
        except Exception as e:
            logger.error(f"[JellyfinDashboard] Failed to load dashboard configs: {e}", exc_info=True)
            return

        for config in configs:
            try:
                await self.refresh_message(config["guild_id"], config["dashboard_channel_id"], config["dashboard_message_id"], source=source)
            except Exception as e:
                logger.error(f"[JellyfinDashboard] Unexpected error refreshing dashboard for guild {config.get('guild_id')}: {e}", exc_info=True)

    async def refresh_message(self, guild_id: int, channel_id: int, message_id: int, source: str = "unknown"):
        """PUBLIC entry point and throttling gate for every edit. Edits now
        if MIN_EDIT_INTERVAL_SECONDS have passed since the last successful
        edit; otherwise schedules ONE deferred edit that renders whatever
        the state is when it fires. If a deferred edit is already queued,
        this call is covered by it — including after the cooldown clears,
        so an immediate edit can never race the queued one."""
        existing = self._pending_edit_tasks.get(message_id)
        if existing is not None and not existing.done():
            logger.debug(f"[JellyfinDashboard] Edit for message {message_id} already queued (trigger: {source}).")
            return

        elapsed = time.time() - self._last_edit_ts.get(message_id, 0.0)
        if elapsed >= MIN_EDIT_INTERVAL_SECONDS:
            await self._do_edit(guild_id, channel_id, message_id, source)
            return

        wait_for = MIN_EDIT_INTERVAL_SECONDS - elapsed
        logger.debug(f"[JellyfinDashboard] Edit for message {message_id} deferred {wait_for:.1f}s (cooldown active, trigger: {source}).")
        self._pending_edit_tasks[message_id] = asyncio.create_task(
            self._deferred_edit(guild_id, channel_id, message_id, wait_for, source)
        )

    async def _deferred_edit(self, guild_id: int, channel_id: int, message_id: int, wait_for: float, source: str):
        try:
            await asyncio.sleep(wait_for)
        except asyncio.CancelledError:
            return
        await self._do_edit(guild_id, channel_id, message_id, source=f"{source}_cooldown_deferred")

    async def _do_edit(self, guild_id: int, channel_id: int, message_id: int, source: str = "unknown"):
        """The actual Discord edit — only ever reached through
        refresh_message. One lock per message so two edits can't overlap.
        Edits through a PartialMessage: no fetch_message round-trip first,
        so each update is one Discord API call instead of two. Single INFO
        log line per real edit."""
        lock = self._edit_locks.setdefault(message_id, asyncio.Lock())
        async with lock:
            # Re-check inside the lock: another edit may have just landed.
            if time.time() - self._last_edit_ts.get(message_id, 0.0) < MIN_EDIT_INTERVAL_SECONDS and not source.endswith("_cooldown_deferred"):
                logger.debug(f"[JellyfinDashboard] Edit for message {message_id} skipped — another edit just landed (trigger: {source}).")
                return

            channel = self.bot.get_channel(channel_id)
            if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                return
            try:
                stats = await self.get_stats()
                dashboard_view = DashboardLayoutView(self.bot, stats, self.latest_sessions)
                await channel.get_partial_message(message_id).edit(content=None, embeds=[], view=dashboard_view)
                self._last_edit_ts[message_id] = time.time()
                logger.info(f"[JellyfinDashboard] Dashboard updated — guild {guild_id} (trigger: {source}).")
            except discord.NotFound:
                logger.warning(f"[JellyfinDashboard] Dashboard message missing in guild {guild_id} — clearing stored reference.")
                await upsert_guild_config(guild_id, dashboard_channel_id=None, dashboard_message_id=None)
                self._dashboard_configs_cache = None
            except discord.HTTPException as e:
                logger.error(f"[JellyfinDashboard] Failed to refresh dashboard in guild {guild_id} (triggered by: {source}): {e}")

    # --- /dashboard ---
    @app_commands.command(name="dashboard", description="Configure the live Now Playing dashboard.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def dashboard_command(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        if interaction.guild is None:
            embed = discord.Embed(description=f"{self.bot.cross_emoji}  This command only works inside a server.", color=self.bot.red_color)
            return await interaction.followup.send(embed=embed, ephemeral=True)

        config, err = await get_guild_config(interaction.guild.id)
        if err:
            embed = discord.Embed(description=f"{self.bot.cross_emoji}  Database error: `{err}`", color=self.bot.red_color)
            return await interaction.followup.send(embed=embed, ephemeral=True)

        view = DashboardConfigView(self.bot, interaction.guild.id, config)
        content, embeds = await view.build_full_display()
        await interaction.followup.send(content=content, embeds=embeds, view=view, ephemeral=True)

    @dashboard_command.error
    async def dashboard_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            msg = f"{self.bot.cross_emoji}  You need `Manage Server` permission to use this command."
        else:
            msg = f"{self.bot.cross_emoji}  Unexpected error: `{type(error).__name__}: {error}`"
        embed = discord.Embed(description=msg, color=self.bot.red_color)
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(JellyfinDashboard(bot))
