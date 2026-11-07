"""
Peak Streams Cog (live recording)
  Every SAMPLE_INTERVAL_SECONDS the bot asks Jellyfin for its live
  sessions (GET /Sessions — the same list the Jellyfin dashboard shows)
  and counts every session that has something OPEN: a video, a song,
  anything — playing or paused, it doesn't matter. Each day's highest
  count (and the time it happened) is stored in its own small database.

  /peak — [Owner] shows the last 7 days' peaks, plus how many streams
  are open right now.

  Why live and not Playback Reporting: that plugin only records time
  actually PLAYED, so a paused-but-open stream is invisible in its
  history. Jellyfin's live session list sees every open stream.

  Trade-offs:
    - No history before this cog started recording; days with no data
      show "—" and fill in as the days pass.
    - One sample per minute: a stream opened AND closed entirely between
      two samples can be missed. Anything open for a minute or more is
      caught.

  Days follow TIMEZONE (not the host's clock), so "Sat" is Saturday in
  Sydney regardless of where the bot runs.
"""

import time
import logging
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands, tasks

from .utils.config import OWNER_DISCORD_ID, PEAK_DB_PATH as DB_PATH, TIMEZONE
from .utils.jellyfin_api import JellyfinClient
from .utils.db import Database

logger = logging.getLogger("peak_streams_cog")
DAYS = 7
SAMPLE_INTERVAL_SECONDS = 60
# Sessions whose client hasn't checked in for this long are treated as
# gone. Open players (paused included) keep reporting to Jellyfin, so
# this only drops dead sessions left behind by a crashed app.
ACTIVE_WITHIN_SECONDS = 600


def _today() -> str:
    return datetime.now(TIMEZONE).strftime("%Y-%m-%d")


class PeakStreams(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Admin"
        self.jf = JellyfinClient(bot)
        self._db = Database(DB_PATH, row_factory=False)
        # Today's recorded peak, cached so the database is only written
        # when a NEW high is reached, not every minute.
        self._cached_day: str | None = None
        self._cached_peak: int = 0

    async def cog_load(self):
        await self._db.open()
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS daily_peaks (
                day TEXT PRIMARY KEY,
                peak INTEGER NOT NULL,
                peak_at INTEGER NOT NULL
            )
        """)
        if not self.sample_loop.is_running():
            self.sample_loop.start()

    async def cog_unload(self):
        self.sample_loop.cancel()
        task = self.sample_loop.get_task()
        if task is not None:
            try:
                await task
            except BaseException:
                pass
        await self._db.close()

    # --- live count ---

    async def _count_open_streams(self) -> int | None:
        """Number of sessions with something open (playing or paused).
        None if Jellyfin couldn't be reached."""
        try:
            sessions = await self.jf.get_sessions(active_within=ACTIVE_WITHIN_SECONDS)
        except Exception as e:
            logger.warning(f"[PeakStreams] /Sessions failed: {type(e).__name__}: {e}")
            return None
        return sum(1 for s in sessions if s.get("NowPlayingItem") is not None)

    async def _record(self, count: int):
        day = _today()
        if day != self._cached_day:
            self._cached_day = day
            async with self._db.conn.execute("SELECT peak FROM daily_peaks WHERE day = ?", (day,)) as cursor:
                row = await cursor.fetchone()
            self._cached_peak = row[0] if row else -1  # -1: nothing stored yet, so even 0 gets recorded

        if count <= self._cached_peak:
            return
        await self._db.execute(
            "INSERT INTO daily_peaks (day, peak, peak_at) VALUES (?, ?, ?) "
            "ON CONFLICT(day) DO UPDATE SET peak = excluded.peak, peak_at = excluded.peak_at "
            "WHERE excluded.peak > daily_peaks.peak",
            (day, count, int(time.time())),
        )
        self._cached_peak = count

    @tasks.loop(seconds=SAMPLE_INTERVAL_SECONDS)
    async def sample_loop(self):
        # Errors are caught here so one bad sample never stops recording.
        try:
            count = await self._count_open_streams()
            if count is not None:
                await self._record(count)
        except Exception as e:
            logger.error(f"[PeakStreams] Sample failed: {type(e).__name__}: {e}", exc_info=True)

    @sample_loop.before_loop
    async def before_sample_loop(self):
        await self.bot.wait_until_ready()

    # --- /peak ---

    @app_commands.command(name="peak", description="[Owner] Most streams open at the same time, per day, last 7 days")
    async def peak(self, interaction: discord.Interaction):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        await interaction.response.defer()

        today = datetime.now(TIMEZONE).date()
        days = [today - timedelta(days=i) for i in range(DAYS)]  # newest first
        keys = [d.strftime("%Y-%m-%d") for d in days]

        placeholders = ",".join("?" for _ in keys)
        async with self._db.conn.execute(
            f"SELECT day, peak, peak_at FROM daily_peaks WHERE day IN ({placeholders})", keys,
        ) as cursor:
            stored = {day: (peak, peak_at) for day, peak, peak_at in await cursor.fetchall()}

        best = max((peak for peak, _ in stored.values()), default=0)

        lines = []
        for day, key in zip(days, keys):
            label = day.strftime("%a %d %b")
            if key not in stored:
                lines.append(f"`{label}`  —")
                continue
            peak, peak_at = stored[key]
            suffix = " · so far" if day == today else ""
            trophy = "  🏆" if peak == best and best > 0 else ""
            lines.append(f"`{label}`  **{peak}** at <t:{peak_at}:t>{suffix}{trophy}")

        now_count = await self._count_open_streams()
        now_line = f"🟢 Open right now — **{now_count}**" if now_count is not None else "🟢 Open right now — couldn't reach Jellyfin"

        container = discord.ui.Container()
        container.add_item(discord.ui.TextDisplay("-# LAST 7 DAYS\n# 📈 Peak Active Streams"))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay("\n".join(lines)))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay(now_line))
        container.add_item(discord.ui.TextDisplay(
            f"-# Most streams open at the same time each day, playing or paused · checked every {SAMPLE_INTERVAL_SECONDS}s"
        ))
        view = discord.ui.LayoutView(timeout=None)
        view.add_item(container)
        await interaction.followup.send(view=view)

    @peak.error
    async def peak_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(PeakStreams(bot))