"""
Stats Cog (Playback Reporting-backed)
  /stats — shows a linked user their own Jellyfin watch time (this
  week / this month / all-time), most-watched title, and titles
  completed.

  /leaderboard was retired from this cog — daily.py's auto-updating
  Daily/Weekly/Lifetime containers cover the same ranked-watchers-with-
  Rising-callout functionality, always visible in its configured
  channel rather than pulled on demand. Two maintained copies of the
  same query logic (this cog's now-removed _get_ranked_watch_totals vs.
  daily.py's) meant a fix in one could silently stay broken in the
  other — not worth carrying forward.

  DATA SOURCE: the official jellyfin-plugin-playbackreporting, already
  installed on the Jellyfin server. All watch-time numbers come from
  querying its PlaybackActivity table directly via its
  /user_usage_stats/submit_custom_query REST endpoint — NOT from
  tracking playback events ourselves.

  This replaces an earlier version of this cog that maintained its own
  watch_sessions table by listening to the shared Tracearr SSE stream.
  That approach was retired because it inherently couldn't be more
  reliable than the SSE connection itself — any missed event (most
  commonly from the connection reconnecting, which happens routinely)
  risked corrupting tracked state. Playback Reporting hooks Jellyfin's
  playback lifecycle server-side, inside Jellyfin itself, so it has
  complete, gap-free data regardless of whether this bot was even
  running at the time someone watched something. This cog is now a pure
  query layer — nothing is tracked locally except a small permanent
  cache of item runtimes (used only to compute "completed" status,
  since Playback Reporting's own schema has no completion flag).

  PlaybackActivity rows are INCREMENTAL, not cumulative snapshots — the
  plugin periodically flushes chunks of watched duration during playback
  (confirmed directly against a real server: consecutive rows for the
  same in-progress watch show smaller, not larger, PlayDuration values
  over time). SUM(PlayDuration) over a user/time-window is therefore the
  correct way to total watch time — no double-counting risk.

  Reads jellyfin_links.db (read-only) to map a Jellyfin user id to a
  Discord id, same DB the registry/whoami cogs already use.
"""

import os
import time
import asyncio
import logging
import discord
from datetime import datetime, timedelta, timezone
from discord import app_commands
from discord.ext import commands

from utils.config import (
    JELLYFIN_URL, DB_DIR,
    STATS_DB_PATH, LINKS_DB_PATH,
)
from utils.jellyfin_api import JellyfinClient
from utils.db import run_query, run_write, run_script

logger = logging.getLogger("stats_cog")

COMPLETED_THRESHOLD = 0.9  # fraction of an item's runtime that counts as "completed"

# Sanity bound applied to every PlayDuration used in a SUM(). Playback
# Reporting has no guard of its own against a corrupted row — confirmed
# in production: a session that never got a clean stop event (a Jellyfin
# restart, client crash, etc.) can get flushed with a wildly wrong
# duration, and SUM() has no way to know one row is garbage. A single
# corrupted row like that produced a genuinely negative "-596428h" total
# for one real user. Rows outside this range are excluded from every
# aggregate query entirely — the plugin flushes duration in periodic
# small chunks during normal playback (confirmed directly against real
# data: 73s, 49s, 122s...), so any single row above 24h is obviously not
# a real watch, corrupted rather than merely unusual.
MAX_PLAUSIBLE_PLAY_DURATION_SECONDS = 24 * 3600


async def _ensure_db():
    """Only table left: a permanent cache of item_id -> runtime_seconds,
    used solely to compute completed-title counts (Playback Reporting's
    schema has no completion/percentage field of its own, so runtime has
    to come from Jellyfin's normal item endpoint and gets cached here
    since it never changes once an item exists)."""
    os.makedirs(DB_DIR, exist_ok=True)
    await run_script(STATS_DB_PATH, """
        CREATE TABLE IF NOT EXISTS item_runtime_cache (
            item_id TEXT PRIMARY KEY,
            runtime_seconds INTEGER NOT NULL,
            cached_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)


async def _get_cached_runtimes_batch(item_ids: list[str]) -> dict[str, int]:
    """One query for however many item_ids are given, instead of one
    query per item — the whole point of batching."""
    if not item_ids:
        return {}
    placeholders = ",".join("?" for _ in item_ids)
    rows = await run_query(STATS_DB_PATH,
        f"SELECT item_id, runtime_seconds FROM item_runtime_cache WHERE item_id IN ({placeholders})",
        item_ids)
    return {r["item_id"]: r["runtime_seconds"] for r in rows}


async def _cache_runtimes_batch(pairs: list[tuple[str, int]]):
    """One executemany for however many (item_id, runtime_seconds) pairs
    are given, instead of one INSERT per item."""
    if not pairs:
        return
    for item_id, runtime in pairs:
        await run_write(STATS_DB_PATH,
            "INSERT INTO item_runtime_cache (item_id, runtime_seconds) VALUES (?, ?) "
            "ON CONFLICT(item_id) DO UPDATE SET runtime_seconds=excluded.runtime_seconds",
            (item_id, runtime))


async def _get_link_by_discord_id(discord_id: int) -> dict | None:
    rows = await run_query(LINKS_DB_PATH, "SELECT * FROM links WHERE discord_id = ?", (discord_id,))
    return rows[0] if rows else None


async def _get_link_by_jellyfin_user_id(jellyfin_user_id: str) -> dict | None:
    rows = await run_query(LINKS_DB_PATH, "SELECT * FROM links WHERE jellyfin_user_id = ?", (jellyfin_user_id,))
    return rows[0] if rows else None


# --- SHARED JELLYFIN CLIENT ---
# Set once in cog_load; module-level so all helpers can use it.
_jf: JellyfinClient | None = None


async def _fetch_all_jellyfin_users() -> list[dict]:
    try:
        return await _jf.get_users()
    except Exception:
        return []


async def _fetch_jellyfin_user(jellyfin_user_id: str) -> dict | None:
    return await _jf.get_user(jellyfin_user_id)


def _ticks_to_seconds(ticks) -> float:
    return (ticks or 0) / 10_000_000  # Jellyfin uses 100ns ticks


def _sql_escape(value: str) -> str:
    """Defense in depth for the raw-SQL custom-query endpoint below —
    jellyfin_user_id values only ever come from our own links DB (plain
    hex GUIDs) so this shouldn't matter in practice, but never build a
    SQL string from user-influenced data without escaping quotes."""
    return value.replace("'", "''")


async def _query_playback_reporting(sql: str) -> list[dict] | None:
    """POSTs a raw SQL query to Playback Reporting's custom-query
    endpoint and returns the results as a list of dicts. Returns None on
    any failure (unreachable, non-200, bad JSON) — callers treat that as
    'couldn't get an answer', not as 'the answer is empty/zero'."""
    return await _jf.playback_query(sql)


async def _get_watch_totals(jellyfin_user_id: str) -> dict:
    """Week/month/all-time totals in one query — three CASE-guarded sums
    over the same table scan rather than three separate round-trips.
    Rolling windows (last 7/30 days), not calendar week/month, to avoid
    timezone/calendar-boundary edge cases.

    Every row is bounded to a plausible PlayDuration before being
    summed — see MAX_PLAUSIBLE_PLAY_DURATION_SECONDS above."""
    now = datetime.now(timezone.utc)
    week_cutoff = (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    month_cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    uid = _sql_escape(jellyfin_user_id)
    cap = MAX_PLAUSIBLE_PLAY_DURATION_SECONDS

    sql = (
        "SELECT "
        f"SUM(CASE WHEN DateCreated >= '{week_cutoff}' THEN PlayDuration ELSE 0 END) AS week_seconds, "
        f"SUM(CASE WHEN DateCreated >= '{month_cutoff}' THEN PlayDuration ELSE 0 END) AS month_seconds, "
        "SUM(PlayDuration) AS all_time_seconds "
        f"FROM PlaybackActivity WHERE UserId = '{uid}' "
        f"AND PlayDuration > 0 AND PlayDuration < {cap}"
    )
    rows = await _query_playback_reporting(sql)
    if not rows:
        return {"week_seconds": 0, "month_seconds": 0, "all_time_seconds": 0}
    row = rows[0]
    return {
        "week_seconds": int(row.get("week_seconds") or 0),
        "month_seconds": int(row.get("month_seconds") or 0),
        "all_time_seconds": int(row.get("all_time_seconds") or 0),
    }


async def _get_most_watched(jellyfin_user_id: str) -> tuple[str | None, int]:
    """Groups episodes under their show — Playback Reporting's ItemName
    already comes pre-formatted as 'Show - s01e01 - Title' for episodes,
    so the show name is extracted with a plain SUBSTR/INSTR split done
    server-side in the query itself, same as the old in-memory grouping
    used to do via series_name. Same PlayDuration sanity bound as
    _get_watch_totals."""
    uid = _sql_escape(jellyfin_user_id)
    cap = MAX_PLAUSIBLE_PLAY_DURATION_SECONDS
    sql = (
        "SELECT CASE WHEN ItemType='Episode' AND ItemName LIKE '% - s%' "
        "THEN SUBSTR(ItemName, 1, INSTR(ItemName, ' - s') - 1) ELSE ItemName END AS display_name, "
        "SUM(PlayDuration) AS total "
        f"FROM PlaybackActivity WHERE UserId = '{uid}' "
        f"AND PlayDuration > 0 AND PlayDuration < {cap} "
        "GROUP BY display_name ORDER BY total DESC LIMIT 1"
    )
    rows = await _query_playback_reporting(sql)
    if not rows:
        return None, 0
    row = rows[0]
    return row.get("display_name"), int(row.get("total") or 0)


async def _get_per_item_totals(jellyfin_user_id: str) -> list[dict]:
    """Same PlayDuration sanity bound as the two queries above — without
    it, a single corrupted row for one item could push that item's
    summed total past its real runtime and either wrongly mark it
    completed or throw off the ratio comparison entirely."""
    uid = _sql_escape(jellyfin_user_id)
    cap = MAX_PLAUSIBLE_PLAY_DURATION_SECONDS
    sql = (
        "SELECT ItemId, SUM(PlayDuration) AS total "
        f"FROM PlaybackActivity WHERE UserId = '{uid}' AND ItemId IS NOT NULL AND ItemId != '' "
        f"AND PlayDuration > 0 AND PlayDuration < {cap} "
        "GROUP BY ItemId"
    )
    return await _query_playback_reporting(sql) or []


async def _fetch_items_runtime_batch(item_ids: list[str]) -> dict[str, int]:
    """ONE Jellyfin call for however many item_ids are missing from the
    cache, via /Items?Ids=a,b,c,... — instead of one round-trip per item.
    This is the actual structural fix for /mystats being slow: a cold
    cache for a user with 20+ distinct watched titles used to mean 20+
    sequential (then, briefly, 20+ concurrent) HTTP calls just to compute
    one number. Now it's one call, chunked only as a defensive measure
    against pathologically long URLs (unlikely in practice, but cheap to
    guard against)."""
    if not item_ids:
        return {}
    result: dict[str, int] = {}
    CHUNK_SIZE = 200

    for i in range(0, len(item_ids), CHUNK_SIZE):
        chunk = item_ids[i:i + CHUNK_SIZE]
        try:
            data = await _jf.get_json("/Items", params={"Ids": ",".join(chunk)})
            if data is None:
                continue
        except Exception as e:
            logger.warning(f"[MyStats] Batch runtime fetch failed for a chunk of {len(chunk)} item(s): {type(e).__name__}: {e}")
            continue

        for item in data.get("Items", []):
            item_id = item.get("Id")
            ticks = item.get("RunTimeTicks")
            if item_id and ticks:
                result[item_id] = int(_ticks_to_seconds(ticks))

    return result


async def _get_completed_count(jellyfin_user_id: str) -> int:
    """Batched end to end: one query for per-item watch totals, one query
    for whatever's already cached, one Jellyfin call for whatever's still
    missing, one write to persist it — regardless of how many distinct
    titles this user has watched."""
    per_item = await _get_per_item_totals(jellyfin_user_id)
    totals_by_id = {
        row["ItemId"]: int(row.get("total") or 0)
        for row in per_item
        if row.get("ItemId") and int(row.get("total") or 0) > 0
    }
    if not totals_by_id:
        return 0

    item_ids = list(totals_by_id.keys())
    cached = await _get_cached_runtimes_batch(item_ids)

    missing_ids = [iid for iid in item_ids if iid not in cached]
    if missing_ids:
        fetched = await _fetch_items_runtime_batch(missing_ids)
        if fetched:
            await _cache_runtimes_batch(list(fetched.items()))
        cached.update(fetched)

    count = 0
    for item_id, watched_seconds in totals_by_id.items():
        runtime = cached.get(item_id)
        if runtime and runtime > 0 and (watched_seconds / runtime) >= COMPLETED_THRESHOLD:
            count += 1
    return count


async def _get_watch_stats_parallel(jellyfin_user_id: str) -> dict:
    """Runs every independent query concurrently instead of one after
    another — totals, most-watched, and completed-count (which itself
    does its own internal batched work) all fire at once via
    asyncio.gather. None of the underlying functions raise on failure
    (they catch their own errors and return safe defaults), so gather
    here doesn't need return_exceptions=True."""
    totals, (most_watched_name, most_watched_seconds), completed_count = await asyncio.gather(
        _get_watch_totals(jellyfin_user_id),
        _get_most_watched(jellyfin_user_id),
        _get_completed_count(jellyfin_user_id),
    )
    return {
        "week_seconds": totals["week_seconds"],
        "month_seconds": totals["month_seconds"],
        "all_time_seconds": totals["all_time_seconds"],
        "most_watched_name": most_watched_name,
        "most_watched_seconds": most_watched_seconds,
        "completed_count": completed_count,
    }


def _format_duration(seconds: int) -> str:
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, _ = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _parse_jellyfin_date(raw: str | None) -> int | None:
    """Parses Jellyfin's own ISO timestamp fields (LastLoginDate,
    LastActivityDate, UserData.LastPlayedDate) — a different format from
    the links DB's own linked_at field, handled separately below."""
    if not raw:
        return None
    try:
        raw = raw.split(".")[0] + "Z" if "." in raw else raw
        dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ")
        return int(dt.timestamp())
    except Exception:
        return None


def _format_ts(ts: int | None) -> str:
    if ts is None:
        return "Never"
    return f"<t:{ts}:R>"


def _linked_at_ts(raw: str | None) -> int | None:
    """jellyfin_links.db's linked_at is SQLite's own CURRENT_TIMESTAMP
    format ('YYYY-MM-DD HH:MM:SS'), not Jellyfin's ISO format — different
    parser from _parse_jellyfin_date above."""
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    except Exception:
        return None


async def _fetch_recent_watches(jellyfin_user_id: str, limit: int = 3) -> list[dict]:
    """Deliberately NOT filtered to Filters=IsPlayed — that only returns
    fully-completed items, which would hide anything still in progress.
    UserData.LastPlayedDate updates on any playback progress, completed
    or not, so sorting/filtering on that directly (client-side, after a
    slightly larger fetch) surfaces in-progress watches too. Unrelated to
    the Playback Reporting rewrite above — this always came from a normal
    Jellyfin Items query, not the old tracking table."""
    params = {
        "SortBy": "DatePlayed",
        "SortOrder": "Descending",
        "Recursive": "true",
        "Limit": "50",
        "Fields": "UserData,RunTimeTicks,ProductionYear,SeriesName,ParentIndexNumber,IndexNumber",
    }
    try:
        data = await _jf.get_json(f"/Users/{jellyfin_user_id}/Items", params=params)
        if data is None:
            return []
        items = data.get("Items", [])
    except Exception:
        return []

    watched = [i for i in items if i.get("UserData", {}).get("LastPlayedDate")]
    watched.sort(key=lambda i: i["UserData"]["LastPlayedDate"], reverse=True)
    return watched[:limit]


def _format_recent_watch_line_compact(item: dict) -> str:
    if item.get("Type") == "Episode" and item.get("SeriesName"):
        title = item["SeriesName"]
        season = item.get("ParentIndexNumber")
        episode = item.get("IndexNumber")
        if season is not None and episode is not None:
            title += f" `S{season}E{episode}`"
    else:
        title = item.get("Name", "Unknown title")

    user_data = item.get("UserData", {})
    runtime_ticks = item.get("RunTimeTicks")
    position_ticks = user_data.get("PlaybackPositionTicks")

    pct_text = ""
    if runtime_ticks:
        total = _ticks_to_seconds(runtime_ticks)
        watched = _ticks_to_seconds(position_ticks) if position_ticks else total
        pct = min(100, round((watched / total) * 100)) if total else 0
        pct_text = f"{pct}%"

    date_ts = _parse_jellyfin_date(user_data.get("LastPlayedDate"))
    date_str = _format_ts(date_ts) if date_ts else ""

    details = " · ".join(filter(None, [pct_text, date_str]))
    return f"{title} — {details}" if details else title


def _build_stats_box(week_str: str, month_str: str, all_time_str: str) -> str:
    """Box-drawing ASCII table with a solid yellow background across the
    whole box (black text on it). Value column uses SINGLE-space padding
    on each side, matching the label column — using double-space here
    previously made every row width value_width+4, while the top/bottom
    border only reserved value_width+2 for that column, so every row's
    right-hand │ sat two characters right of where the corners actually
    were."""
    BG = "\u001b[1;30;47m"  # black text, yellow background
    RESET = "\u001b[0m"

    label_width = max(len("This Week"), len("This Month"), len("All Time"))
    value_width = max(len(week_str), len(month_str), len(all_time_str), 4)

    def _row(label: str, value: str) -> str:
        return f"{BG}│ {label.ljust(label_width)} │ {value.center(value_width)} │{RESET}"

    top = f"{BG}╭" + "─" * (label_width + 2) + "┬" + "─" * (value_width + 2) + f"╮{RESET}"
    bottom = f"{BG}╰" + "─" * (label_width + 2) + "┴" + "─" * (value_width + 2) + f"╯{RESET}"

    return "\n".join([
        "```ansi",
        top,
        _row("This Week", week_str),
        _row("This Month", month_str),
        _row("All Time", all_time_str),
        bottom,
        "```",
    ])


class JellyfinStats(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Utility"
        # No in-memory tracking state anymore — this cog is a pure query
        # layer over Playback Reporting now. It no longer registers for
        # or handles any playback events from dashboard.py's SSE stream.

    async def cog_load(self):
        global _jf
        _jf = JellyfinClient(self.bot)
        await _ensure_db()

    async def cog_unload(self):
        pass  # no session close here — main.py owns bot.http_session

    # --- /stats ---
    @app_commands.command(name="stats", description="View a linked user's Jellyfin watch stats.")
    @app_commands.describe(
        discord_user="Look up someone else by their Discord account (defaults to you)",
        jellyfin_username="Look up someone by their exact Jellyfin username instead",
    )
    async def stats(self, interaction: discord.Interaction, discord_user: discord.User = None, jellyfin_username: str = None):
        await interaction.response.defer()

        # Resolve which account we're actually showing. discord_user takes
        # priority if both are given, rather than guessing which one the
        # caller actually meant if they conflict.
        link = None
        target_discord_id = None

        if discord_user is not None:
            target_discord_id = discord_user.id
            link = await _get_link_by_discord_id(target_discord_id)
            if link is None:
                return await interaction.followup.send(f"❌ {discord_user.mention} doesn't have a linked Jellyfin account.", ephemeral=True)

        elif jellyfin_username:
            jf_users = await _fetch_all_jellyfin_users()
            match = next((u for u in jf_users if u.get("Name", "").lower() == jellyfin_username.lower()), None)
            if not match:
                return await interaction.followup.send(f"❌ No Jellyfin user named `{jellyfin_username}` found.", ephemeral=True)
            link = await _get_link_by_jellyfin_user_id(match["Id"])
            if link is None:
                return await interaction.followup.send(f"❌ `{jellyfin_username}` isn't linked to a Discord account.", ephemeral=True)
            target_discord_id = link["discord_id"]

        else:
            target_discord_id = interaction.user.id
            link = await _get_link_by_discord_id(target_discord_id)
            if link is None:
                return await interaction.followup.send("❌ You don't have a linked Jellyfin account. Ask an admin for a token.", ephemeral=True)

        # Resolve a real Discord user object for the mention + avatar
        # fallback — only need an extra fetch if it wasn't handed to us
        # directly via the discord_user parameter.
        if discord_user is not None:
            target_user = discord_user
        else:
            target_user = self.bot.get_user(target_discord_id)
            if target_user is None:
                try:
                    target_user = await self.bot.fetch_user(target_discord_id)
                except discord.HTTPException:
                    target_user = None

        target_mention = target_user.mention if target_user else f"<@{target_discord_id}>"
        fallback_avatar_url = target_user.display_avatar.url if target_user else None

        # These three are fully independent of each other — fetched
        # concurrently instead of one after another. jf_user (1 call),
        # stats (internally 3 more concurrent calls, one of which does
        # its own internal batched work), and recent_items (1 call) used
        # to run sequentially; this is the other half of the actual fix
        # for /mystats being slow.
        jf_user, stats, recent_items = await asyncio.gather(
            _fetch_jellyfin_user(link["jellyfin_user_id"]),
            _get_watch_stats_parallel(link["jellyfin_user_id"]),
            _fetch_recent_watches(link["jellyfin_user_id"], limit=3),
        )
        jf_username = jf_user.get("Name", "Unknown") if jf_user else "Unknown"

        # Jellyfin PFP if they've set one, falling back to their Discord
        # avatar otherwise — same convention whoami.py already uses.
        if jf_user and jf_user.get("PrimaryImageTag"):
            avatar_url = f"{JELLYFIN_URL}/Users/{link['jellyfin_user_id']}/Images/Primary"
        elif fallback_avatar_url:
            avatar_url = fallback_avatar_url
        else:
            avatar_url = interaction.client.user.display_avatar.url

        header_lines = [
            f"# 📊 {jf_username}",
            f"-# JELLYFIN STATISTICS · {target_mention}",
        ]

        stats_box = _build_stats_box(
            _format_duration(stats["week_seconds"]),
            _format_duration(stats["month_seconds"]),
            _format_duration(stats["all_time_seconds"]),
        )

        if stats["most_watched_name"]:
            most_watched_line = f"🏆 Most Watched — **{stats['most_watched_name']}**\n-# {_format_duration(stats['most_watched_seconds'])} total · {stats['completed_count']} completed"
        else:
            most_watched_line = f"🏆 Most Watched — *Nothing tracked yet*\n-# {stats['completed_count']} completed"

        # --- Account section ---
        last_login_ts = _parse_jellyfin_date(jf_user.get("LastLoginDate")) if jf_user else None
        last_activity_ts = _parse_jellyfin_date(jf_user.get("LastActivityDate")) if jf_user else None
        linked_ts = _linked_at_ts(link.get("linked_at"))

        account_line = (
            f"-# ACCOUNT\n"
            f"🔑 Login **{_format_ts(last_login_ts)}** · "
            f"🟢 Active **{_format_ts(last_activity_ts)}** · "
            f"🔗 Linked **{_format_ts(linked_ts)}**"
        )

        # --- Recent Watches section ---
        recent_lines = ["-# RECENT WATCHES"]
        if recent_items:
            for i, item in enumerate(recent_items, start=1):
                recent_lines.append(f"**{i}.** {_format_recent_watch_line_compact(item)}")
        else:
            recent_lines.append("Nothing watched yet.")

        view = discord.ui.LayoutView(timeout=None)
        container = discord.ui.Container()
        container.add_item(discord.ui.Section(
            discord.ui.TextDisplay("\n".join(header_lines)),
            accessory=discord.ui.Thumbnail(media=avatar_url),
        ))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(discord.ui.TextDisplay(stats_box))
        container.add_item(discord.ui.TextDisplay(most_watched_line))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(discord.ui.TextDisplay(account_line))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        container.add_item(discord.ui.TextDisplay("\n".join(recent_lines)))
        view.add_item(container)

        await interaction.followup.send(view=view)



async def setup(bot) -> None:
    await bot.add_cog(JellyfinStats(bot))