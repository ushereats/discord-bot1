"""
Trending Cog — /trending (anyone can use it)
  A Netflix-style Top 10 for UsherEats.

  HOW IT RANKS (same method Netflix has used since June 2023)
    views = total hours watched ÷ runtime
      - Movies: runtime of the movie.
      - Shows:  each SEASON is its own entry ("Silo: Season 2"), and its
                runtime is all of that season's episodes added together.
    Partial watches count for their share — half a movie is half a view —
    so long shows no longer win just for being long. Ties go to hours.

  LISTS & PERIODS (Netflix keeps films and TV separate)
    Buttons switch between 🎬 Movies / 📺 Shows and This week / Last week /
    30 days. Weeks run Monday–Sunday, Sydney time. Weekly lists also show:
      - rank change vs the week before (▲2 / ▼1 / new)
      - weeks in the Top 10 (over the last LOOKBACK_WEEKS weeks)
    Finished weeks never change, so their Top 10s are saved to
    trending.db and only ever computed once.

  EXTRAS ON EACH LINE
    hours watched, and how many people watched at least half of a movie or
    of at least one episode of the season.

  DATA
    Playback Reporting (PlaybackActivity, via submit_custom_query) for watch
    time — same source and 24h-per-row sanity cap as daily.py — and
    Jellyfin /Items + /Shows/{id}/Episodes for runtimes and posters. Items
    since deleted from Jellyfin fall back to 2h per movie / 45m per episode.
"""

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands

logger = logging.getLogger("trending_cog")

JELLYFIN_URL = "https://jellyfin.ushereats.com"
JELLYFIN_API_KEY = "9cb945d0d38d45cb84fcc8a2e1541bd8"
QUERY_URL = f"{JELLYFIN_URL}/user_usage_stats/submit_custom_query"

DB_DIR = "/home/container/ushereats/DATABASES"
DB_PATH = os.path.join(DB_DIR, "trending.db")

TZ = ZoneInfo("Australia/Sydney")
TOP_N = 10
SHORTLIST = 25                      # entries ranked by hours before views are worked out
LOOKBACK_WEEKS = 8                  # how far back "weeks in Top 10" looks
HALF = 0.5                          # "watched it" for the people count
MAX_PLAUSIBLE_PLAY_DURATION_SECONDS = 24 * 3600
FALLBACK_MOVIE_SECONDS = 2 * 3600
FALLBACK_EPISODE_SECONDS = 45 * 60
LIVE_CACHE_SECONDS = 600            # this week / 30 days
DONE_CACHE_SECONDS = 6 * 3600       # last week (finished, won't change)
META_CACHE_SECONDS = 6 * 3600       # runtimes / posters

DEFAULT_KIND = "shows"
DEFAULT_PERIOD = "week"
KINDS = {"movies": ("Movie", "🎬", "Movies"), "shows": ("Episode", "📺", "Shows")}
PERIODS = {"week": "This week", "lastweek": "Last week", "30d": "30 days"}

EPISODE_RE = re.compile(r"^(?P<series>.*?) - s(?P<season>\d+)e\d+", re.IGNORECASE)


# ------------------------------------------------------------------ small helpers

def _norm(item_id) -> str:
    return str(item_id or "").replace("-", "").lower()


def _sql_str(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _week_start(offset: int) -> datetime:
    """Monday 00:00 Sydney time of the week `offset` weeks from this one
    (0 = this week, -1 = last week, ...)."""
    now = datetime.now(TZ)
    monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return monday + timedelta(weeks=offset)


def _window(period: str) -> tuple[datetime, datetime]:
    now = datetime.now(TZ)
    if period == "week":
        return _week_start(0), now
    if period == "lastweek":
        return _week_start(-1), _week_start(0)
    return now - timedelta(days=30), now


def _fmt_views(v: float) -> str:
    if v >= 100:
        text = f"{v:,.0f}"
    elif v >= 10:
        text = f"{v:.0f}"
    else:
        text = f"{v:.1f}".rstrip("0").rstrip(".")
    return f"{text} view{'' if text == '1' else 's'}"


def _fmt_hours(seconds: float) -> str:
    hours = seconds / 3600
    if hours >= 100:
        return f"{hours:,.0f}h"
    if hours >= 1:
        return f"{hours:.1f}".rstrip("0").rstrip(".") + "h"
    return f"{max(1, round(seconds / 60))}m"


def _ordinal(n: int) -> str:
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _range_label(period: str) -> str:
    start, end = _window(period)
    if period == "week":
        return f"this week · since Mon {start.day} {start:%b}"
    if period == "lastweek":
        last_day = end - timedelta(days=1)
        return f"last week · Mon {start.day} – Sun {last_day.day} {last_day:%b}"
    return "last 30 days"


# ------------------------------------------------------------------ data

class TrendingData:
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._lists: dict[tuple, tuple[float, list[dict]]] = {}
        self._items: dict[str, tuple[float, dict | None]] = {}
        self._seasons: dict[str, tuple[float, float]] = {}

    @property
    def _headers(self):
        return {"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"', "accept": "application/json"}

    async def init_db(self):
        os.makedirs(DB_DIR, exist_ok=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS weekly (kind TEXT, week TEXT, rank INTEGER, key TEXT, "
                "PRIMARY KEY (kind, week, rank))"
            )
            await db.execute("CREATE TABLE IF NOT EXISTS weeks_done (kind TEXT, week TEXT, PRIMARY KEY (kind, week))")
            await db.commit()

    # -- raw sources --

    async def _sql(self, sql: str) -> list[dict] | None:
        try:
            async with self.bot.http_session.post(
                QUERY_URL, headers=self._headers, json={"CustomQueryString": sql},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    logger.error(f"[Trending] Playback Reporting query failed: HTTP {resp.status}")
                    return None
                data = await resp.json(content_type=None)
        except Exception as e:
            logger.error(f"[Trending] Playback Reporting query failed: {type(e).__name__}: {e}")
            return None
        columns = data.get("colums") or data.get("columns") or []  # the plugin spells it "colums"
        return [dict(zip(columns, row)) for row in data.get("results") or []]

    async def _get_json(self, path: str, params: dict):
        try:
            async with self.bot.http_session.get(
                f"{JELLYFIN_URL}{path}", headers=self._headers, params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
        except Exception as e:
            logger.warning(f"[Trending] Jellyfin {path} failed: {type(e).__name__}: {e}")
            return None

    async def items(self, ids: list[str]) -> dict[str, dict]:
        """Jellyfin item info by (normalised) id, cached. Missing = deleted."""
        now = time.monotonic()
        wanted = [i for i in {_norm(x) for x in ids} if i and (i not in self._items or now - self._items[i][0] > META_CACHE_SECONDS)]
        for start in range(0, len(wanted), 100):
            batch = wanted[start:start + 100]
            data = await self._get_json("/Items", {
                "ids": ",".join(batch), "Fields": "SeriesPrimaryImageTag,ProductionYear", "enableUserData": "false",
            })
            if data is None:
                continue  # leave uncached so the next call retries
            got = {_norm(it.get("Id")): it for it in data.get("Items") or []}
            for i in batch:
                self._items[i] = (now, got.get(i))
        return {i: self._items[i][1] for i in {_norm(x) for x in ids} if i in self._items and self._items[i][1]}

    async def season_runtime(self, series_id: str, season_id: str) -> float | None:
        cached = self._seasons.get(season_id)
        if cached and time.monotonic() - cached[0] < META_CACHE_SECONDS:
            return cached[1]
        data = await self._get_json(f"/Shows/{series_id}/Episodes", {"seasonId": season_id})
        if data is None:
            return None
        seconds = sum((ep.get("RunTimeTicks") or 0) for ep in data.get("Items") or []) / 10_000_000
        if seconds > 0:
            self._seasons[season_id] = (time.monotonic(), seconds)
        return seconds or None

    # -- ranking --

    async def compute(self, kind: str, start: datetime, end: datetime, with_people: bool) -> list[dict] | None:
        item_type = KINDS[kind][0]
        where = (
            f"DateCreated >= '{_utc(start)}' AND DateCreated < '{_utc(end)}' "
            f"AND ItemType = '{item_type}' AND PlayDuration > 0 "
            f"AND PlayDuration < {MAX_PLAUSIBLE_PLAY_DURATION_SECONDS} "
            "AND ItemName IS NOT NULL AND ItemName != ''"
        )
        rows = await self._sql(
            f"SELECT ItemId, ItemName, SUM(PlayDuration) AS secs FROM PlaybackActivity "
            f"WHERE {where} GROUP BY ItemId, ItemName"
        )
        if rows is None:
            return None

        # Group items into entries: a movie, or one season of a show.
        entries: dict[str, dict] = {}
        for r in rows:
            name = str(r.get("ItemName") or "")
            if item_type == "Movie":
                key, label, season = f"movie:{name.lower()}", name, None
            else:
                m = EPISODE_RE.match(name)
                if m:
                    series, season = m["series"].strip(), int(m["season"])
                    label = f"{series}: {'Specials' if season == 0 else f'Season {season}'}"
                else:
                    series, season, label = name, None, name
                key = f"show:{series.lower()}:{season}"
            e = entries.setdefault(key, {"key": key, "label": label, "seconds": 0.0, "item_ids": set(), "season": season})
            e["seconds"] += float(r.get("secs") or 0)
            if r.get("ItemId"):
                e["item_ids"].add(str(r["ItemId"]))

        if not entries:
            return []

        shortlist = sorted(entries.values(), key=lambda e: -e["seconds"])[:SHORTLIST]
        info = await self.items([i for e in shortlist for i in e["item_ids"]])

        for e in shortlist:
            known = [info[_norm(i)] for i in e["item_ids"] if _norm(i) in info]
            e["poster"] = None
            if item_type == "Movie":
                runtimes = [it.get("RunTimeTicks") or 0 for it in known]
                runtime = max(runtimes) / 10_000_000 if runtimes and max(runtimes) else FALLBACK_MOVIE_SECONDS
                if known and (known[0].get("ImageTags") or {}).get("Primary"):
                    e["poster"] = (known[0]["Id"], known[0]["ImageTags"]["Primary"])
            else:
                runtime = None
                if known and known[0].get("SeriesId") and known[0].get("SeasonId"):
                    runtime = await self.season_runtime(known[0]["SeriesId"], known[0]["SeasonId"])
                    e["poster"] = (known[0]["SeriesId"], known[0].get("SeriesPrimaryImageTag"))
                if not runtime:
                    runtime = max(1, len(e["item_ids"])) * FALLBACK_EPISODE_SECONDS
            e["runtime"] = runtime
            e["views"] = e["seconds"] / runtime if runtime else 0.0

        ranked = sorted(shortlist, key=lambda e: (-e["views"], -e["seconds"], e["label"].lower()))[:TOP_N]

        if with_people:
            await self._add_people(ranked, info, where)
        return ranked

    async def _add_people(self, ranked: list[dict], info: dict, where: str):
        ids = [i for e in ranked for i in e["item_ids"]]
        for e in ranked:
            e["people"] = None
        if not ids:
            return
        rows = await self._sql(
            f"SELECT UserId, ItemId, SUM(PlayDuration) AS secs FROM PlaybackActivity WHERE {where} "
            f"AND ItemId IN ({', '.join(_sql_str(i) for i in ids)}) GROUP BY UserId, ItemId"
        )
        if rows is None:
            return
        owner = {i: e for e in ranked for i in e["item_ids"]}
        people: dict[str, set] = {e["key"]: set() for e in ranked}
        for r in rows:
            e = owner.get(str(r.get("ItemId")))
            if e is None:
                continue
            item = info.get(_norm(r.get("ItemId")))
            runtime = (item or {}).get("RunTimeTicks")
            fallback = FALLBACK_MOVIE_SECONDS if e["key"].startswith("movie:") else FALLBACK_EPISODE_SECONDS
            needed = (runtime / 10_000_000 if runtime else fallback) * HALF
            if float(r.get("secs") or 0) >= needed:
                people[e["key"]].add(r.get("UserId"))
        for e in ranked:
            e["people"] = len(people[e["key"]])

    # -- finished weeks: computed once, stored --

    async def week_keys(self, kind: str, offset: int) -> list[str] | None:
        """Top 10 keys (in order) for a FINISHED week (offset <= -1)."""
        week = _week_start(offset).date().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT 1 FROM weeks_done WHERE kind=? AND week=?", (kind, week)) as cur:
                done = await cur.fetchone()
            if done:
                async with db.execute("SELECT key FROM weekly WHERE kind=? AND week=? ORDER BY rank", (kind, week)) as cur:
                    return [row[0] for row in await cur.fetchall()]
        ranked = await self.compute(kind, _week_start(offset), _week_start(offset + 1), with_people=False)
        if ranked is None:
            return None
        async with aiosqlite.connect(DB_PATH) as db:
            await db.executemany(
                "INSERT OR REPLACE INTO weekly (kind, week, rank, key) VALUES (?, ?, ?, ?)",
                [(kind, week, n, e["key"]) for n, e in enumerate(ranked, start=1)],
            )
            await db.execute("INSERT OR REPLACE INTO weeks_done (kind, week) VALUES (?, ?)", (kind, week))
            await db.commit()
        return [e["key"] for e in ranked]

    async def top(self, kind: str, period: str) -> list[dict] | None:
        cache_key = (kind, period)
        cached = self._lists.get(cache_key)
        ttl = DONE_CACHE_SECONDS if period == "lastweek" else LIVE_CACHE_SECONDS
        if cached and time.monotonic() - cached[0] < ttl:
            return cached[1]

        start, end = _window(period)
        ranked = await self.compute(kind, start, end, with_people=True)
        if ranked is None:
            return None

        if period in ("week", "lastweek"):
            this_offset = 0 if period == "week" else -1
            history: dict[int, list[str]] = {}
            for offset in range(-1, -LOOKBACK_WEEKS - 1, -1):
                keys = await self.week_keys(kind, offset)
                if keys is not None:
                    history[offset] = keys
            previous = history.get(this_offset - 1)
            for rank, e in enumerate(ranked, start=1):
                if previous is None:
                    e["move"] = None
                elif e["key"] in previous:
                    e["move"] = previous.index(e["key"]) + 1 - rank   # + = climbed
                else:
                    e["move"] = "new"
                weeks = sum(1 for off, keys in history.items() if off < this_offset and e["key"] in keys) + 1
                e["weeks"] = weeks
        self._lists[cache_key] = (time.monotonic(), ranked)
        return ranked


# ------------------------------------------------------------------ rendering

def _poster_url(poster) -> str | None:
    if not poster:
        return None
    item_id, tag = poster
    return f"{JELLYFIN_URL}/Items/{item_id}/Images/Primary?fillHeight=300&quality=80" + (f"&tag={tag}" if tag else "")


def _move_text(e: dict) -> str:
    move = e.get("move")
    if move is None:
        return ""
    if move == "new":
        return " · 🆕"
    if move > 0:
        return f" · ▲{move}"
    if move < 0:
        return f" · ▼{-move}"
    return " · ="


def _detail_line(e: dict) -> str:
    bits = [_fmt_hours(e["seconds"]) + " watched"]
    if e.get("people") is not None:
        bits.append(f"{e['people']} {'person' if e['people'] == 1 else 'people'}")
    if e.get("weeks"):
        bits.append("1st week in Top 10" if e["weeks"] == 1 else f"{e['weeks']} weeks in Top 10")
    return " · ".join(bits)


def build_view(rows: list[dict], kind: str, period: str) -> discord.ui.LayoutView:
    _, icon, word = KINDS[kind]
    c = discord.ui.Container()
    c.add_item(discord.ui.TextDisplay(f"# {icon} Top 10 {word}\n-# UsherEats · {_range_label(period)}"))
    c.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

    if not rows:
        c.add_item(discord.ui.TextDisplay("-# Nothing watched in this period yet — try **Last week**."))
    else:
        for rank, e in enumerate(rows[:3], start=1):
            text = discord.ui.TextDisplay(
                f"## {rank}  {discord.utils.escape_markdown(e['label'])}\n"
                f"**{_fmt_views(e['views'])}**{_move_text(e)}\n"
                f"-# {_detail_line(e)}"
            )
            poster = _poster_url(e.get("poster"))
            c.add_item(discord.ui.Section(text, accessory=discord.ui.Thumbnail(media=poster)) if poster else text)
        if len(rows) > 3:
            c.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
            c.add_item(discord.ui.TextDisplay("\n".join(
                f"`{rank:>2}` **{discord.utils.escape_markdown(e['label'])}** · {_fmt_views(e['views'])}{_move_text(e)}"
                for rank, e in enumerate(rows[3:], start=4)
            )))

    c.add_item(discord.ui.TextDisplay(
        f"-# views = hours watched ÷ runtime, like Netflix · updated <t:{int(time.time())}:R>"
    ))
    # Top row switches list (keeps the period); bottom row switches period (keeps the list).
    c.add_item(discord.ui.ActionRow(*(
        TrendingButton(k, period, active=(k == kind), label=f"{KINDS[k][1]} {KINDS[k][2]}", row="l") for k in KINDS
    )))
    c.add_item(discord.ui.ActionRow(*(
        TrendingButton(kind, p, active=(p == period), label=PERIODS[p], row="p") for p in PERIODS
    )))
    view = discord.ui.LayoutView(timeout=None)
    view.add_item(c)
    return view


class TrendingButton(discord.ui.DynamicItem[discord.ui.Button],
                     template=r"trending:(?P<row>[lp]):(?P<kind>movies|shows):(?P<period>week|lastweek|30d)"):
    """Restart-proof list/period switch. Anyone can press it; the message
    is redrawn in place for everyone. The same class draws both rows: on
    the top row it switches list (keeping the period), on the bottom row it
    switches period (keeping the list)."""

    def __init__(self, kind: str, period: str, active: bool = False, label: str | None = None, row: str = "p"):
        super().__init__(discord.ui.Button(
            label=label or "",
            style=discord.ButtonStyle.primary if active else discord.ButtonStyle.secondary,
            custom_id=f"trending:{row}:{kind}:{period}",  # row (l=list, p=period) keeps ids unique
        ))
        self.kind = kind
        self.period = period

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match):
        return cls(match["kind"], match["period"], row=match["row"])

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Trending")
        if cog is None:
            return await interaction.response.send_message("❌ Trending is unavailable right now.", ephemeral=True)
        await interaction.response.defer()
        rows = await cog.data.top(self.kind, self.period)
        if rows is None:
            return await interaction.followup.send("❌ Couldn't reach Jellyfin right now — try again in a minute.", ephemeral=True)
        await interaction.edit_original_response(view=build_view(rows, self.kind, self.period))


# ------------------------------------------------------------------ cog

class Trending(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Stats"
        self.data = TrendingData(bot)

    async def cog_load(self):
        await self.data.init_db()
        self.bot.add_dynamic_items(TrendingButton)

    async def cog_unload(self):
        self.bot.remove_dynamic_items(TrendingButton)

    @app_commands.command(name="trending", description="Top 10 movies and shows on UsherEats, ranked like Netflix.")
    @app_commands.describe(list="Movies or shows (default: shows)", period="Which week (default: this week)")
    @app_commands.choices(
        list=[app_commands.Choice(name="Shows", value="shows"), app_commands.Choice(name="Movies", value="movies")],
        period=[
            app_commands.Choice(name="This week", value="week"),
            app_commands.Choice(name="Last week", value="lastweek"),
            app_commands.Choice(name="Last 30 days", value="30d"),
        ],
    )
    async def trending(self, interaction: discord.Interaction,
                       list: app_commands.Choice[str] | None = None,
                       period: app_commands.Choice[str] | None = None):
        kind = list.value if list else DEFAULT_KIND
        period_key = period.value if period else DEFAULT_PERIOD
        await interaction.response.defer()
        rows = await self.data.top(kind, period_key)
        if rows is None:
            return await interaction.followup.send(
                "❌ Couldn't reach Jellyfin's Playback Reporting right now — try again in a minute.", ephemeral=True
            )
        await interaction.followup.send(view=build_view(rows, kind, period_key))


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(Trending(bot))