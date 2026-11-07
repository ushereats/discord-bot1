"""
Daily Stats Cog (Playback Reporting-backed)
  /daily — admin command (Manage Server). Sends a Components V2
  ChannelSelect. Picking a channel posts ONE message into that channel:
  a single "Watch Report" container with three blocks — Daily (rolling
  last 24h), Weekly (rolling last 7 days) and Lifetime (all-time). The
  message is edited in place every 5 minutes with freshly-queried
  numbers — never reposted unless it was deleted. Its message_id is
  persisted (daily_stats_messages, period = "combined") so the loop
  always knows which message to edit, across restarts.

  Layout per period block (condensed so all three fit one message):
    - a Section: eyebrow, server total, most-watched title, and the #1
      watcher, with #1's avatar as the thumbnail;
    - one TextDisplay listing ranks 2-10.
  Only #1 gets a thumbnail: each Section+Thumbnail costs 3 components,
  and Discord caps a message at 40. Three full-detail containers (top 3
  with thumbnails + a Rising section each) came to ~66 and was rejected
  ('maximum number of children exceeded (40)'). This layout is ~19
  components and ~2,000 characters at TOP_N = 10, inside both the
  40-component and 4,000-character limits.

  Rising is shown as a 📈 marker on the rising user's own row, not as a
  separate section (zero extra components). It's computed the same way
  as before: the current window's top 10 against the immediately
  preceding equal-length window — a brand-new entrant beats any rank
  climb. Lifetime has no prior window, so it never shows a marker.

  Migration: the first refresh after this update finds no "combined"
  message, posts one, then deletes the three old per-period messages
  (daily/weekly/lifetime ids) and their DB rows — no manual cleanup.

  Data source, sanity bound, and linked-user filtering follow the exact
  same conventions as stats_cog.py: raw SQL against Playback Reporting's
  PlaybackActivity table via its /user_usage_stats/submit_custom_query
  endpoint, every SUM() bounded by MAX_PLAUSIBLE_PLAY_DURATION_SECONDS
  to exclude corrupted rows, and ranking done strictly among linked
  (displayable) accounts.
"""

import os
import asyncio
import logging
import discord
from datetime import datetime, timezone, timedelta
from discord import app_commands
from discord.ext import commands, tasks

from utils.config import (
    JELLYFIN_URL, DB_DIR,
    DAILY_DB_PATH, LINKS_DB_PATH,
)
from utils.jellyfin_api import JellyfinClient
from utils.db import run_query, run_write, run_script

logger = logging.getLogger("daily_stats_cog")

TOP_N = 10
MAX_PLAUSIBLE_PLAY_DURATION_SECONDS = 24 * 3600
REFRESH_INTERVAL_MINUTES = 5
MAX_TITLE_LENGTH = 80  # most-watched title is trimmed past this to protect the 4,000-char message budget

COMBINED_KEY = "combined"  # daily_stats_messages.period value for the single report message

# Every <@discord_id> mention renders as a normal clickable mention but
# never pings — otherwise every top-10 user is notified on every
# 5-minute refresh. Passed to every send()/edit() call in this cog.
NO_PING_MENTIONS = discord.AllowedMentions(users=False, roles=False, everyone=False)

# Each period's own window (days back from now) and the equal-length
# PRIOR window it's compared against for Rising. Lifetime's
# prev_start_days is None -> never a Rising marker. 36500 days (~100
# years) is a practical "everything" bound, same convention as
# stats_cog.py.
PERIODS = {
    "daily":    {"eyebrow": "DAILY · LAST 24 HOURS", "start_days": 1,     "prev_start_days": 2},
    "weekly":   {"eyebrow": "WEEKLY · LAST 7 DAYS",   "start_days": 7,     "prev_start_days": 14},
    "lifetime": {"eyebrow": "LIFETIME",               "start_days": 36500, "prev_start_days": None},
}
PERIOD_ORDER = ["daily", "weekly", "lifetime"]


async def _ensure_db():
    os.makedirs(DB_DIR, exist_ok=True)
    await run_script(DAILY_DB_PATH, """
        CREATE TABLE IF NOT EXISTS daily_stats_config (
            guild_id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            set_by INTEGER,
            set_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS daily_stats_messages (
            guild_id INTEGER NOT NULL,
            period TEXT NOT NULL,
            message_id INTEGER,
            PRIMARY KEY (guild_id, period)
        );
    """)


async def _set_channel(guild_id: int, channel_id: int, set_by: int):
    await run_write(DAILY_DB_PATH,
        "INSERT INTO daily_stats_config (guild_id, channel_id, set_by) VALUES (?, ?, ?) "
        "ON CONFLICT(guild_id) DO UPDATE SET channel_id=excluded.channel_id, "
        "set_by=excluded.set_by, set_at=CURRENT_TIMESTAMP",
        (guild_id, channel_id, set_by))
    await run_write(DAILY_DB_PATH, "DELETE FROM daily_stats_messages WHERE guild_id = ?", (guild_id,))


async def _set_message_id(guild_id: int, period: str, message_id: int):
    await run_write(DAILY_DB_PATH,
        "INSERT INTO daily_stats_messages (guild_id, period, message_id) VALUES (?, ?, ?) "
        "ON CONFLICT(guild_id, period) DO UPDATE SET message_id=excluded.message_id",
        (guild_id, period, message_id))


async def _delete_message_rows(guild_id: int, periods: list[str]):
    if not periods:
        return
    placeholders = ",".join("?" for _ in periods)
    await run_write(DAILY_DB_PATH,
        f"DELETE FROM daily_stats_messages WHERE guild_id = ? AND period IN ({placeholders})",
        (guild_id, *periods))


async def _get_message_ids(guild_id: int) -> dict[str, int]:
    rows = await run_query(DAILY_DB_PATH,
        "SELECT period, message_id FROM daily_stats_messages WHERE guild_id = ? AND message_id IS NOT NULL",
        (guild_id,))
    return {r["period"]: r["message_id"] for r in rows}


async def _get_all_configs() -> list[dict]:
    return await run_query(DAILY_DB_PATH, "SELECT * FROM daily_stats_config")


async def _get_links_by_jellyfin_ids_batch(jellyfin_user_ids: list[str]) -> dict[str, int]:
    if not jellyfin_user_ids:
        return {}
    placeholders = ",".join("?" for _ in jellyfin_user_ids)
    rows = await run_query(LINKS_DB_PATH,
        f"SELECT jellyfin_user_id, discord_id FROM links WHERE jellyfin_user_id IN ({placeholders})",
        jellyfin_user_ids)
    return {r["jellyfin_user_id"]: r["discord_id"] for r in rows}


_jf: JellyfinClient | None = None


async def _fetch_all_jellyfin_users() -> list[dict]:
    try:
        return await _jf.get_users()
    except Exception:
        return []


async def _query_playback_reporting(sql: str) -> list[dict] | None:
    return await _jf.playback_query(sql)


async def _get_ranked_totals(start_days_ago: float, end_days_ago: float = 0) -> list[tuple[str, int]]:
    """Every user's total watch seconds within [now - start_days_ago,
    now - end_days_ago), descending. Used for both the current window
    and the comparison window for Rising."""
    now = datetime.now(timezone.utc)
    start_cutoff = (now - timedelta(days=start_days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    cap = MAX_PLAUSIBLE_PLAY_DURATION_SECONDS

    if end_days_ago > 0:
        end_cutoff = (now - timedelta(days=end_days_ago)).strftime("%Y-%m-%d %H:%M:%S")
        window_clause = f"DateCreated >= '{start_cutoff}' AND DateCreated < '{end_cutoff}'"
    else:
        window_clause = f"DateCreated >= '{start_cutoff}'"

    sql = (
        "SELECT UserId, SUM(PlayDuration) AS total FROM PlaybackActivity "
        f"WHERE {window_clause} AND PlayDuration > 0 AND PlayDuration < {cap} "
        "AND UserId IS NOT NULL AND UserId != '' "
        "GROUP BY UserId ORDER BY total DESC"
    )
    rows = await _query_playback_reporting(sql) or []
    return [(row["UserId"], int(row.get("total") or 0)) for row in rows if row.get("UserId")]


async def _get_most_watched(start_days_ago: float) -> tuple[str | None, int]:
    """Server-wide most-watched title within the window, episodes
    grouped under their show."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=start_days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    cap = MAX_PLAUSIBLE_PLAY_DURATION_SECONDS
    sql = (
        "SELECT CASE WHEN ItemType='Episode' AND ItemName LIKE '% - s%' "
        "THEN SUBSTR(ItemName, 1, INSTR(ItemName, ' - s') - 1) ELSE ItemName END AS display_name, "
        "SUM(PlayDuration) AS total "
        f"FROM PlaybackActivity WHERE DateCreated >= '{cutoff}' "
        f"AND PlayDuration > 0 AND PlayDuration < {cap} "
        "GROUP BY display_name ORDER BY total DESC LIMIT 1"
    )
    rows = await _query_playback_reporting(sql)
    if not rows:
        return None, 0
    row = rows[0]
    return row.get("display_name"), int(row.get("total") or 0)


def _format_duration(seconds: int) -> str:
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, _ = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _rising_rank(top_entries: list[tuple], previous_linked: list[tuple]) -> int | None:
    """Rank (1-based, within top_entries) of the Rising user, or None.
    Same priority as before: the highest-ranked brand-new entrant to the
    current top 10 wins; otherwise the biggest climb versus the previous
    window."""
    previous_rank_by_uid = {uid: rank for rank, (uid, _, _) in enumerate(previous_linked, start=1)}

    for rank, (uid, _, _) in enumerate(top_entries, start=1):
        if uid not in previous_rank_by_uid:
            return rank  # first (= highest-ranked) new entrant

    best_rise, best_rank = 0, None
    for rank, (uid, _, _) in enumerate(top_entries, start=1):
        rise = previous_rank_by_uid[uid] - rank
        if rise > best_rise:
            best_rise, best_rank = rise, rank
    return best_rank


async def _build_period_block(period_key: str, jf_users_by_id: dict) -> list[discord.ui.Item]:
    """One period's block for the combined container: a Section (eyebrow,
    totals, most-watched, #1 with avatar thumbnail) followed by a
    TextDisplay of ranks 2..TOP_N. jf_users_by_id is one bulk /Users
    fetch shared across all periods by the caller."""
    config = PERIODS[period_key]

    if config["prev_start_days"] is not None:
        current_raw, previous_raw, (most_watched_name, most_watched_seconds) = await asyncio.gather(
            _get_ranked_totals(start_days_ago=config["start_days"], end_days_ago=0),
            _get_ranked_totals(start_days_ago=config["prev_start_days"], end_days_ago=config["start_days"]),
            _get_most_watched(config["start_days"]),
        )
    else:
        current_raw, (most_watched_name, most_watched_seconds) = await asyncio.gather(
            _get_ranked_totals(start_days_ago=config["start_days"], end_days_ago=0),
            _get_most_watched(config["start_days"]),
        )
        previous_raw = []

    eyebrow = f"-# {config['eyebrow']}"

    all_ids = list({uid for uid, _ in current_raw} | {uid for uid, _ in previous_raw})
    links = await _get_links_by_jellyfin_ids_batch(all_ids)
    current_linked = [(uid, links[uid], total) for uid, total in current_raw if uid in links]
    previous_linked = [(uid, links[uid], total) for uid, total in previous_raw if uid in links]

    if not current_linked:
        return [discord.ui.TextDisplay(f"{eyebrow}\nNothing watched in this period.")]

    total_seconds = sum(total for _, _, total in current_linked)
    top_entries = current_linked[:TOP_N]
    rising_rank = _rising_rank(top_entries, previous_linked) if config["prev_start_days"] is not None else None

    def _marker(rank: int) -> str:
        return "  📈" if rank == rising_rank else ""

    viewers = len(current_linked)
    lines = [eyebrow, f"⏱️ **{_format_duration(total_seconds)}** across {viewers} viewer{'s' if viewers != 1 else ''}"]
    if most_watched_name:
        title = most_watched_name if len(most_watched_name) <= MAX_TITLE_LENGTH else most_watched_name[: MAX_TITLE_LENGTH - 1] + "…"
        lines.append(f"🏆 **{title}** · {_format_duration(most_watched_seconds)}")

    first_uid, first_discord_id, first_total = top_entries[0]
    lines.append(f"🥇 <@{first_discord_id}> · **{_format_duration(first_total)}**{_marker(1)}")

    jf_user = jf_users_by_id.get(first_uid)
    if jf_user and jf_user.get("PrimaryImageTag"):
        avatar_url = f"{JELLYFIN_URL}/Users/{first_uid}/Images/Primary"
    else:
        avatar_url = f"https://cdn.discordapp.com/embed/avatars/{first_discord_id % 5}.png"

    items: list[discord.ui.Item] = [discord.ui.Section(
        discord.ui.TextDisplay("\n".join(lines)),
        accessory=discord.ui.Thumbnail(media=avatar_url),
    )]

    remainder = top_entries[1:]
    if remainder:
        rows = [
            f"`{rank:>2}`  <@{discord_id}> — `{_format_duration(total)}`{_marker(rank)}"
            for rank, (_, discord_id, total) in enumerate(remainder, start=2)
        ]
        items.append(discord.ui.TextDisplay("\n".join(rows)))

    return items


async def _build_report_view() -> discord.ui.LayoutView:
    """The single combined Watch Report: title, then Daily / Weekly /
    Lifetime blocks separated by large dividers, then the footer. All
    three periods' queries run concurrently; /Users is fetched once and
    shared."""
    jf_users = await _fetch_all_jellyfin_users()
    jf_users_by_id = {u["Id"]: u for u in jf_users if u.get("Id")}

    blocks = await asyncio.gather(*[
        _build_period_block(period_key, jf_users_by_id) for period_key in PERIOD_ORDER
    ])

    container = discord.ui.Container()
    container.add_item(discord.ui.TextDisplay("# 📊 Watch Report"))
    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

    for index, block in enumerate(blocks):
        if index > 0:
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        for item in block:
            container.add_item(item)

    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
    container.add_item(discord.ui.TextDisplay(
        f"-# Refreshes every {REFRESH_INTERVAL_MINUTES}m · Last updated <t:{int(datetime.now(timezone.utc).timestamp())}:R>"
    ))

    view = discord.ui.LayoutView(timeout=None)
    view.add_item(container)
    return view


class DailyStatsChannelSelectView(discord.ui.LayoutView):
    """Sent by /daily. Selecting a channel posts the live report there
    immediately — this view itself is single-use (timeout=180) and only
    exists to pick the channel."""

    def __init__(self, cog: "DailyStats", guild_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id

        container = discord.ui.Container()
        container.add_item(discord.ui.TextDisplay(
            "-# STATS SETUP\n# 📊 Pick a channel\nThe Daily / Weekly / Lifetime watch report will post here and refresh every "
            f"{REFRESH_INTERVAL_MINUTES} minutes."
        ))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

        select_row = discord.ui.ActionRow()
        channel_select = discord.ui.ChannelSelect(
            placeholder="Select a channel",
            channel_types=[discord.ChannelType.text, discord.ChannelType.news],
            custom_id="daily_stats_channel_select",
        )
        channel_select.callback = self._on_select
        select_row.add_item(channel_select)
        container.add_item(select_row)

        self.add_item(container)

    async def _on_select(self, interaction: discord.Interaction):
        channel_id = int(interaction.data["values"][0])
        await interaction.response.defer(ephemeral=True)

        await _set_channel(self.guild_id, channel_id, interaction.user.id)

        channel = self.cog.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.cog.bot.fetch_channel(channel_id)
            except discord.HTTPException:
                await interaction.followup.send("❌ Couldn't access that channel — check my permissions there.", ephemeral=True)
                return

        try:
            view = await _build_report_view()
            message = await channel.send(view=view, allowed_mentions=NO_PING_MENTIONS)
            await _set_message_id(self.guild_id, COMBINED_KEY, message.id)
        except discord.HTTPException as e:
            logger.error(f"[DailyStats] Failed to send initial report: {type(e).__name__}: {e}")
            await interaction.followup.send(f"❌ Set the channel but failed to send the report: {e}", ephemeral=True)
            return

        await interaction.followup.send(f"✅ Live watch report posted in <#{channel_id}> — refreshing every {REFRESH_INTERVAL_MINUTES}m.", ephemeral=True)


class DailyStats(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Utility"

    async def cog_load(self):
        global _jf
        _jf = JellyfinClient(self.bot)
        await _ensure_db()
        self.refresh_stats.start()

    async def cog_unload(self):
        self.refresh_stats.cancel()

    @app_commands.command(name="daily", description="Set the channel for the live Daily/Weekly/Lifetime Jellyfin watch report.")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.guild_only()
    async def daily(self, interaction: discord.Interaction):
        view = DailyStatsChannelSelectView(self, interaction.guild_id)
        await interaction.response.send_message(view=view, ephemeral=True)

    @daily.error
    async def daily_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message(
                "❌ You need the **Manage Server** permission to set the stats channel.", ephemeral=True
            )
        else:
            raise error

    @tasks.loop(minutes=REFRESH_INTERVAL_MINUTES)
    async def refresh_stats(self):
        # Caught here rather than relying on @refresh_stats.error: an
        # uncaught exception stops a tasks.loop for good, which would
        # freeze the report until the next restart.
        try:
            await self._refresh_once()
        except Exception as e:
            logger.error(f"[DailyStats] Refresh failed: {type(e).__name__}: {e}", exc_info=True)

    async def _refresh_once(self):
        configs = await _get_all_configs()
        if not configs:
            return

        # Data is server-wide, not per-guild — built once per tick and
        # reused for every configured guild.
        view = await _build_report_view()

        for config in configs:
            guild_id = config["guild_id"]
            channel = self.bot.get_channel(config["channel_id"])
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(config["channel_id"])
                except discord.HTTPException:
                    logger.warning(f"[DailyStats] Couldn't resolve channel {config['channel_id']} for guild {guild_id}")
                    continue

            stored = await _get_message_ids(guild_id)
            message_id = stored.get(COMBINED_KEY)

            if message_id:
                # Edit by ID directly — no fetch_message round-trip first.
                try:
                    await channel.get_partial_message(message_id).edit(view=view, allowed_mentions=NO_PING_MENTIONS)
                    continue
                except discord.NotFound:
                    pass  # deleted — fall through and send a fresh one
                except discord.HTTPException as e:
                    logger.warning(f"[DailyStats] Edit failed for report {message_id} in channel {config['channel_id']}: {type(e).__name__}: {e}")
                    continue  # transient — try again next tick rather than posting a duplicate

            try:
                new_message = await channel.send(view=view, allowed_mentions=NO_PING_MENTIONS)
                await _set_message_id(guild_id, COMBINED_KEY, new_message.id)
            except discord.HTTPException as e:
                logger.warning(f"[DailyStats] Failed to send report to channel {config['channel_id']}: {type(e).__name__}: {e}")
                continue

            # One-time migration: remove the old three per-period messages.
            legacy = [period for period in PERIOD_ORDER if period in stored]
            for period in legacy:
                try:
                    await channel.get_partial_message(stored[period]).delete()
                except discord.HTTPException:
                    pass
            await _delete_message_rows(guild_id, legacy)

    @refresh_stats.before_loop
    async def before_refresh_stats(self):
        await self.bot.wait_until_ready()


async def setup(bot) -> None:
    await bot.add_cog(DailyStats(bot))