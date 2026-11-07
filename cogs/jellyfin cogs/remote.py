"""
Remote Cog — shared Jellyfin panel (Remote + Quick Connect) + /remote

SHARED PANEL (REMOTE_CHANNEL_ID): one permanent public message with two
cards — "Jellyfin Remote" and "Jellyfin Quick Connect". Its buttons act
on the PRESSER'S OWN account; everything personal comes back privately,
so the channel stays a single clean message.
  - ⏯️ ⏪ ⏩ ⏹️ quick buttons: act on whatever the presser is playing
    (asks which device if several are playing).
  - 🔍 Search & Play: pop-up search -> private results -> plays on their TV.
  - 🎮 Open my remote: their full personal panel, privately.
  - 📺 Sign in a device: Quick Connect — enter the 6-digit code from the
    device's login screen; approved for their OWN linked account.
  - Live "🟢 N streams playing" (count only) or "🔴 Jellyfin unreachable".
  The panel's message id is stored in remote.db, so restarts reuse it.

/remote: the same personal panel posted publicly (only its owner can
press it).

HOW IT STAYS FAST AND SOLID
  Live session feed — one websocket to Jellyfin (SessionsStart) pushes
    session changes as they happen; every panel reads that one shared
    snapshot. Polling is only a fallback (fast while the socket is down
    and panels are open, slow otherwise), and any press triggers an
    immediate fresh read.
  Instant feedback — a press updates the panel immediately (optimistic
    play/pause/stop), then the live feed confirms it.
  No flick-back — each panel renders one update at a time from the
    latest snapshot, so an older update can never overwrite a newer one.
    Panels are only edited when what they show actually changed.
  Restart-proof buttons — every panel button/select is a DynamicItem whose
    custom_id carries the owner (and device), so panels opened before a
    restart keep working: the panel is rebuilt from the press itself.
  Native skip — ⏪/⏩ use Jellyfin's Rewind/FastForward, so the device skips
    from its own exact position (skip length = the app's own setting).
  Smart ⏮️/⏭️ — for episodes the bot looks up the adjacent episode and
    plays it (works with no queue); other media uses Previous/NextTrack.
  Mashed presses — one user's commands run strictly in order (max
    MAX_QUEUED_COMMANDS waiting), so taps never race each other.
  Circuit breaker — after BREAKER_FAILURES consecutive Jellyfin failures,
    calls fail fast for BREAKER_COOLDOWN_SECONDS with a clear message,
    then recover automatically.
  Per-call timeouts — controls fail fast, search gets longer.
  Caches — Discord->Jellyfin links (in memory, short TTL), search results
    (short TTL); poster URLs carry Jellyfin's image tag so Discord reuses
    cached images.
  Concurrency cap — at most JELLYFIN_CONCURRENCY Jellyfin calls at once.
  Latency logging — slow Jellyfin calls / actions are logged.

SECURITY — a user can only ever see or control their own playback:
  account comes ONLY from the presser's Discord ID -> links table;
  only that account's sessions are listed or targeted and every target is
  re-checked against the live list; search and item lookups run AS that
  user (library access respected); only a panel's owner can press it.
"""

import os
import json
import time
import asyncio
import logging
from datetime import datetime

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands

logger = logging.getLogger("remote_cog")

JELLYFIN_URL = "https://jellyfin.ushereats.com"
JELLYFIN_WS_URL = "wss://jellyfin.ushereats.com/socket"
JELLYFIN_API_KEY = "9cb945d0d38d45cb84fcc8a2e1541bd8"
LOG_CHANNEL_ID = 1539141762600402994
REMOTE_CHANNEL_ID = 1556565053166190662  # where the shared panel lives

DB_DIR = "/home/container/ushereats/DATABASES"
LINKS_DB_PATH = os.path.join(DB_DIR, "jellyfin_links.db")  # read-only here, owned by the registry cog
REMOTE_DB_PATH = os.path.join(DB_DIR, "remote.db")  # this cog's own state (panel message id)

# --- tuning ---
ACTIVE_WINDOW_SECONDS = 120  # a session counts as open if it checked in this recently
WS_SESSIONS_INTERVAL_MS = 1500  # how often Jellyfin pushes session updates over the socket
POLL_FAST_SECONDS = 4  # fallback polling while the socket is down and panels are open
POLL_SLOW_SECONDS = 30  # fallback polling otherwise
FEED_FRESH_SECONDS = 3  # a press re-reads sessions if the snapshot is older than this
PANEL_MIN_EDIT_SECONDS = 2  # min gap between feed-driven edits of one panel
IDLE_CLOSE_SECONDS = 600  # a panel closes after this long without a press (< Discord's 15-min token)
OVERRIDE_SECONDS = 5  # optimistic state is trusted this long before the feed wins
MAX_QUEUED_COMMANDS = 5
SEARCH_LIMIT = 25  # Discord select-menu option limit
SHARED_PANEL_CHECK_SECONDS = 30
SEEK_SECONDS = 30                    # ⏪ / ⏩ jump size
CONFIRM_DELAYS = (1.2, 1.5, 2.5)     # re-check the device after a command (≈5s total)
PANEL_EXISTS_CHECK_SECONDS = 300  # backstop fetch in case a delete event was missed
PANEL_SCAN_LIMIT = 50

TIMEOUT_CONTROL = 4
TIMEOUT_SESSIONS = 5
TIMEOUT_SEARCH = 8
TIMEOUT_DEFAULT = 8
JELLYFIN_CONCURRENCY = 8
BREAKER_FAILURES = 3
BREAKER_COOLDOWN_SECONDS = 20
SLOW_CALL_SECONDS = 2.5

LINK_CACHE_SECONDS = 60
LINK_MISS_CACHE_SECONDS = 15
SEARCH_CACHE_SECONDS = 60

QC_COOLDOWN_SECONDS = 10
QC_DEVICE_DETECT_SECONDS = 20
QC_DEVICE_DETECT_POLL_SECONDS = 2

TICKS_PER_SECOND = 10_000_000
_JF_HEADERS = {"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"', "accept": "application/json"}
_NO_PINGS = discord.AllowedMentions.none()

SHARED_IDS = {
    "playpause": "remote_shared_playpause",
    "back30": "remote_shared_back30",
    "fwd30": "remote_shared_fwd30",
    "stop": "remote_shared_stop",
    "search": "remote_shared_search",
    "open": "remote_shared_open",
    "signin": "qc_panel_open",
}

# Action codes used in dynamic custom_ids (kept short: custom_id max 100 chars).
ACTIONS = {"pp": "playpause", "rw": "rewind", "ff": "forward", "pv": "prev", "nx": "next", "st": "stop"}
SHARED_TO_CODE = {"playpause": "pp", "back30": "rw", "fwd30": "ff", "stop": "st"}


class JellyfinDown(Exception):
    """Jellyfin isn't responding (or the circuit breaker is open)."""


# --- formatting helpers ---

def _norm_guid(value) -> str:
    return str(value or "").replace("-", "").lower()


def _esc(text) -> str:
    return discord.utils.escape_markdown(str(text or ""))


def _parse_iso(raw) -> float | None:
    """Jellyfin ISO timestamp (Z/offset, any fraction digits) -> epoch."""
    if not raw:
        return None
    try:
        text = str(raw).strip()
        offset = "+00:00"
        if text.endswith("Z"):
            text = text[:-1]
        elif len(text) > 6 and text[-6] in "+-" and text[-3] == ":":
            text, offset = text[:-6], text[-6:]
        dt = datetime.fromisoformat(text.split(".", 1)[0] + offset)
        return dt.timestamp() if dt.year > 1970 else None
    except (ValueError, TypeError, OverflowError):
        return None


def _fmt_time(ticks) -> str:
    seconds = int((ticks or 0) / TICKS_PER_SECOND)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def _fmt_runtime(ticks) -> str:
    minutes = int((ticks or 0) / TICKS_PER_SECOND / 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m" if hours else f"{minutes}m"


def _progress_bar(position, runtime, slots: int = 14) -> str:
    if not runtime:
        return ""
    filled = max(0, min(slots, round(slots * (position or 0) / runtime)))
    return "▰" * filled + "▱" * (slots - filled)


def _poster_url(item: dict) -> str | None:
    """Poster with Jellyfin's image tag, so Discord can reuse its cache."""
    if item.get("Type") == "Episode" and item.get("SeriesId") and item.get("SeriesPrimaryImageTag"):
        image_id, tag = item["SeriesId"], item["SeriesPrimaryImageTag"]
    elif item.get("Type") == "Audio" and item.get("AlbumId") and item.get("AlbumPrimaryImageTag"):
        image_id, tag = item["AlbumId"], item["AlbumPrimaryImageTag"]
    elif (item.get("ImageTags") or {}).get("Primary"):
        image_id, tag = item["Id"], item["ImageTags"]["Primary"]
    else:
        return None
    return f"{JELLYFIN_URL}/Items/{image_id}/Images/Primary?fillHeight=360&quality=90&tag={tag}"


def _title_lines(item: dict) -> str:
    kind = item.get("Type")
    if kind == "Episode":
        season, episode = item.get("ParentIndexNumber"), item.get("IndexNumber")
        code = f"**S{season}E{episode}** · " if season is not None and episode is not None else ""
        return f"### {_esc(item.get('SeriesName') or item.get('Name'))}\n{code}{_esc(item.get('Name'))}"
    if kind == "Audio":
        artists = ", ".join(item.get("Artists") or [])
        return f"### {_esc(item.get('Name'))}" + (f"\n{_esc(artists)}" if artists else "")
    year = f" ({item['ProductionYear']})" if item.get("ProductionYear") else ""
    return f"### {_esc(item.get('Name'))}{year}"


def _short_title(item: dict) -> str:
    if item.get("Type") == "Episode" and item.get("SeriesName"):
        return f"{item['SeriesName']} S{item.get('ParentIndexNumber')}E{item.get('IndexNumber')}"
    return item.get("Name") or "that"


def _private_text(text: str) -> discord.ui.LayoutView:
    view = discord.ui.LayoutView(timeout=None)
    view.add_item(discord.ui.Container(discord.ui.TextDisplay(text)))
    return view


DOWN_TEXT = "🔴 Jellyfin isn't responding right now — try again in a moment."


# --- Jellyfin client: concurrency cap, timeouts, circuit breaker ---

class JellyfinClient:
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._sem = asyncio.Semaphore(JELLYFIN_CONCURRENCY)
        self._failures = 0
        self._open_until = 0.0

    @property
    def down(self) -> bool:
        return time.monotonic() < self._open_until

    def _failed(self, why: str):
        self._failures += 1
        if self._failures >= BREAKER_FAILURES and not self.down:
            self._open_until = time.monotonic() + BREAKER_COOLDOWN_SECONDS
            logger.warning(f"[Remote] Jellyfin breaker OPEN for {BREAKER_COOLDOWN_SECONDS}s after {self._failures} failures ({why})")

    def _succeeded(self):
        if self._failures >= BREAKER_FAILURES:
            logger.info("[Remote] Jellyfin breaker closed — Jellyfin is responding again")
        self._failures = 0
        self._open_until = 0.0

    async def call(self, method: str, path: str, *, params: dict | None = None,
                   timeout: float = TIMEOUT_DEFAULT) -> tuple[int, object]:
        """Returns (status, parsed JSON or None). Raises JellyfinDown on
        network errors, timeouts, 5xx, or while the breaker is open."""
        if self.down:
            raise JellyfinDown()
        started = time.monotonic()
        async with self._sem:
            try:
                async with self.bot.http_session.request(
                    method, f"{JELLYFIN_URL}{path}", headers=_JF_HEADERS, params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    status = resp.status
                    raw = await resp.read()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self._failed(type(e).__name__)
                raise JellyfinDown() from e
        elapsed = time.monotonic() - started
        if elapsed > SLOW_CALL_SECONDS:
            logger.warning(f"[Remote] Slow Jellyfin call {method} {path}: {elapsed:.1f}s")
        if status >= 500:
            self._failed(f"HTTP {status}")
            raise JellyfinDown()
        self._succeeded()
        data = None
        if raw:
            try:
                data = json.loads(raw)
            except ValueError:
                data = None
        return status, data

    # -- endpoints --

    async def playstate(self, session_id: str, command: str, params: dict | None = None) -> bool:
        status, _ = await self.call("POST", f"/Sessions/{session_id}/Playing/{command}", params=params, timeout=TIMEOUT_CONTROL)
        return status < 300

    async def play_now(self, session_id: str, item_id: str, start_ticks: int = 0) -> bool:
        params = {"playCommand": "PlayNow", "itemIds": item_id}
        if start_ticks:
            params["startPositionTicks"] = str(int(start_ticks))
        status, _ = await self.call("POST", f"/Sessions/{session_id}/Playing", params=params, timeout=TIMEOUT_CONTROL)
        return status < 300

    async def item_for_user(self, jellyfin_user_id: str, item_id: str) -> dict | None:
        """The item as the user sees it (library access + their resume
        position). None if they can't access it."""
        status, data = await self.call("GET", "/Items", params={
            "ids": item_id, "userId": jellyfin_user_id, "Fields": "ProductionYear,RunTimeTicks", "EnableUserData": "true",
        })
        items = (data or {}).get("Items") or [] if status == 200 else []
        return items[0] if items else None

    async def search(self, jellyfin_user_id: str, query: str) -> list[dict] | None:
        """Search/Hints first (built for type-ahead: faster, better
        ranking), plain item search as a fallback. Runs AS the user."""
        status, data = await self.call("GET", "/Search/Hints", params={
            "searchTerm": query, "userId": jellyfin_user_id, "includeItemTypes": "Movie,Series,Episode",
            "limit": str(SEARCH_LIMIT),
        }, timeout=TIMEOUT_SEARCH)
        if status == 200 and isinstance(data, dict) and "SearchHints" in data:
            items = []
            for hint in data["SearchHints"]:
                item_id = hint.get("Id") or hint.get("ItemId")
                if not item_id:
                    continue
                items.append({
                    "Id": item_id, "Name": hint.get("Name"), "Type": hint.get("Type"),
                    "ProductionYear": hint.get("ProductionYear"), "SeriesName": hint.get("Series"),
                    "IndexNumber": hint.get("IndexNumber"), "ParentIndexNumber": hint.get("ParentIndexNumber"),
                    "RunTimeTicks": hint.get("RunTimeTicks"),
                })
            return items
        status, data = await self.call("GET", "/Items", params={
            "searchTerm": query, "userId": jellyfin_user_id, "IncludeItemTypes": "Movie,Series,Episode",
            "Recursive": "true", "Limit": str(SEARCH_LIMIT), "Fields": "ProductionYear,RunTimeTicks",
            "EnableImages": "false",
        }, timeout=TIMEOUT_SEARCH)
        if status != 200:
            return None
        return [i for i in (data or {}).get("Items") or [] if i.get("Id")]

    async def next_up(self, jellyfin_user_id: str, series_id: str) -> dict | None:
        """The user's Next Up episode of a show, else its first episode."""
        common = {"userId": jellyfin_user_id, "Fields": "ProductionYear,RunTimeTicks", "EnableUserData": "true"}
        for path, params in (
            ("/Shows/NextUp", {**common, "seriesId": series_id, "Limit": "1"}),
            (f"/Shows/{series_id}/Episodes", {**common, "Limit": "1"}),
        ):
            status, data = await self.call("GET", path, params=params)
            items = (data or {}).get("Items") or [] if status == 200 else []
            if items:
                return items[0]
        return None

    async def adjacent_episode(self, jellyfin_user_id: str, item: dict, direction: int) -> dict | None:
        """The episode before (-1) or after (+1) `item` in its show."""
        series_id = item.get("SeriesId")
        if not series_id:
            return None
        status, data = await self.call("GET", f"/Shows/{series_id}/Episodes", params={
            "userId": jellyfin_user_id, "adjacentTo": item["Id"], "Fields": "ProductionYear,RunTimeTicks",
        })
        items = (data or {}).get("Items") or [] if status == 200 else []
        ids = [i.get("Id") for i in items]
        if item["Id"] not in ids:
            return None
        index = ids.index(item["Id"]) + direction
        return items[index] if 0 <= index < len(items) else None

    async def authorize_quick_connect(self, jellyfin_user_id: str, code: str) -> str:
        """'ok' | 'invalid' (no pending request — typo or expired) |
        'forbidden' (Quick Connect disabled / server too old for an admin
        to approve on a user's behalf)."""
        status, data = await self.call("POST", "/QuickConnect/Authorize",
                                       params={"code": code, "userId": jellyfin_user_id}, timeout=TIMEOUT_CONTROL)
        if status in (400, 404):
            return "invalid"
        if status in (401, 403):
            return "forbidden"
        return "invalid" if data is False else "ok"

    async def username(self, jellyfin_user_id: str) -> str:
        try:
            status, data = await self.call("GET", f"/Users/{jellyfin_user_id}")
        except JellyfinDown:
            return "your account"
        return ((data or {}).get("Name") if status == 200 else None) or "your account"


# --- live session feed ---

class SessionFeed:
    """One shared, live view of every open Jellyfin session. A websocket
    push keeps it current; polling is the fallback."""

    def __init__(self, cog: "Remote"):
        self.cog = cog
        self.client: JellyfinClient = cog.client
        self.sessions: list[dict] | None = None
        self.updated = 0.0
        self.ws_connected = False
        self._sig = None
        self._tasks: list[asyncio.Task] = []
        self._poll_lock = asyncio.Lock()
        self._soon: set[asyncio.Task] = set()

    def start(self):
        self._tasks = [asyncio.create_task(self._ws_loop()), asyncio.create_task(self._poll_loop())]

    async def stop(self):
        tasks = [*self._tasks, *self._soon]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _active(sessions: list) -> list[dict]:
        cutoff = time.time() - ACTIVE_WINDOW_SECONDS
        out = []
        for s in sessions or []:
            if not isinstance(s, dict) or not s.get("Id"):
                continue
            last = _parse_iso(s.get("LastActivityDate"))
            if last is None or last >= cutoff:
                out.append(s)
        return out

    def _set(self, sessions: list):
        active = self._active(sessions)
        self.sessions = active
        self.updated = time.monotonic()
        sig = tuple(sorted(
            (s["Id"], _norm_guid(s.get("UserId")), (s.get("NowPlayingItem") or {}).get("Id"),
             bool((s.get("PlayState") or {}).get("IsPaused")),
             int(((s.get("PlayState") or {}).get("PositionTicks") or 0) // (5 * TICKS_PER_SECOND)))
            for s in active
        ))
        if sig != self._sig:
            self._sig = sig
            self.cog.on_feed_change()

    async def poll(self) -> bool:
        async with self._poll_lock:
            try:
                status, data = await self.client.call(
                    "GET", "/Sessions", params={"activeWithinSeconds": str(ACTIVE_WINDOW_SECONDS)}, timeout=TIMEOUT_SESSIONS,
                )
            except JellyfinDown:
                return False
            if status == 200 and isinstance(data, list):
                self._set(data)
                return True
            return False

    async def fresh(self, max_age: float = FEED_FRESH_SECONDS) -> list[dict] | None:
        if self.sessions is None or time.monotonic() - self.updated > max_age:
            await self.poll()
        return self.sessions

    def refresh_soon(self, delay: float = 0.7):
        async def _later():
            await asyncio.sleep(delay)
            await self.poll()
        task = asyncio.create_task(_later())
        self._soon.add(task)
        task.add_done_callback(self._soon.discard)

    def user_sessions(self, jellyfin_user_id: str, controllable: bool = True) -> list[dict]:
        owner = _norm_guid(jellyfin_user_id)
        mine = [
            s for s in self.sessions or []
            if owner and _norm_guid(s.get("UserId")) == owner and (s.get("SupportsRemoteControl") or not controllable)
        ]
        mine.sort(key=lambda s: (s.get("NowPlayingItem") is None, s.get("DeviceName") or "", s["Id"]))
        return mine

    def stream_count(self) -> int | None:
        if self.sessions is None:
            return None
        return sum(1 for s in self.sessions if s.get("NowPlayingItem") is not None)

    async def _poll_loop(self):
        while True:
            try:
                fresh_enough = self.ws_connected and time.monotonic() - self.updated < 10
                if not fresh_enough:
                    await self.poll()
                fast = (not self.ws_connected) and bool(self.cog.controllers)
                await asyncio.sleep(POLL_FAST_SECONDS if fast else POLL_SLOW_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[Remote] Poll loop error: {type(e).__name__}: {e}")
                await asyncio.sleep(POLL_SLOW_SECONDS)

    async def _ws_loop(self):
        backoff = 2
        while True:
            keepalive: asyncio.Task | None = None
            try:
                ws_timeout = aiohttp.ClientWSTimeout(ws_close=10) if hasattr(aiohttp, "ClientWSTimeout") else 10.0
                async with self.cog.bot.http_session.ws_connect(
                    f"{JELLYFIN_WS_URL}?api_key={JELLYFIN_API_KEY}&deviceId=ushereats-remote-bot",
                    headers=_JF_HEADERS, heartbeat=30, timeout=ws_timeout,
                ) as ws:
                    await ws.send_json({"MessageType": "SessionsStart", "Data": f"0,{WS_SESSIONS_INTERVAL_MS}"})
                    self.ws_connected = True
                    backoff = 2
                    logger.info("[Remote] Live session feed connected")
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        try:
                            payload = json.loads(msg.data)
                        except ValueError:
                            continue
                        kind = payload.get("MessageType")
                        if kind == "Sessions" and isinstance(payload.get("Data"), list):
                            self._set(payload["Data"])
                        elif kind == "ForceKeepAlive" and keepalive is None:
                            interval = max(5, int(payload.get("Data") or 60) / 2)
                            keepalive = asyncio.create_task(self._keepalive(ws, interval))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[Remote] Live session feed error: {type(e).__name__}: {e}")
            finally:
                self.ws_connected = False
                if keepalive is not None:
                    keepalive.cancel()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    @staticmethod
    async def _keepalive(ws, interval: float):
        try:
            while not ws.closed:
                await asyncio.sleep(interval)
                await ws.send_json({"MessageType": "KeepAlive"})
        except asyncio.CancelledError:
            pass
        except Exception:
            pass


# --- actions ---

ACTION_DONE = {
    "playpause": "⏯️ Toggled play/pause on",
    "rewind": "⏪ Skipped back on",
    "forward": "⏩ Skipped ahead on",
    "stop": "⏹️ Stopped",
}


async def _perform(client: JellyfinClient, jellyfin_user_id: str, session: dict, action: str, feed=None) -> tuple[bool, str]:
    """Runs one action on a session ALREADY verified to be the user's.
    Returns (ok, message for the user)."""
    device = _esc(session.get("DeviceName") or "your device")
    item = session.get("NowPlayingItem") or {}
    sid = session["Id"]

    if action in ("next", "prev"):
        direction = 1 if action == "next" else -1
        if item.get("Type") == "Episode" and item.get("SeriesId"):
            target = await client.adjacent_episode(jellyfin_user_id, item, direction)
            if target is None:
                return False, "ℹ️ That's the " + ("last" if direction > 0 else "first") + " episode."
            ok = await client.play_now(sid, target["Id"])
            label = f"S{target.get('ParentIndexNumber')}E{target.get('IndexNumber')}"
            return ok, (f"{'⏭️' if direction > 0 else '⏮️'} Playing **{_esc(label)}** on **{device}**." if ok
                        else "❌ The device didn't accept that.")
        ok = await client.playstate(sid, "NextTrack" if direction > 0 else "PreviousTrack")
        return ok, (f"{'⏭️ Next' if direction > 0 else '⏮️ Previous'} on **{device}**." if ok else "❌ The device didn't accept that.")

    # Jellyfin answers 204 "OK" the moment it FORWARDS a command — it never
    # waits to see whether the app on the device obeyed. Plenty of apps
    # report "supports remote control" and then ignore playback commands, so
    # an OK from Jellyfin means nothing on its own. Each command below is
    # therefore checked against the device's real state afterwards.
    play_state = session.get("PlayState") or {}
    was_paused = bool(play_state.get("IsPaused"))
    position = int(play_state.get("PositionTicks") or 0)
    runtime = int(item.get("RunTimeTicks") or 0)

    if action == "playpause":
        # Explicit Pause / Unpause rather than the PlayPause toggle — more
        # apps implement these, and the check below knows what to expect.
        ok = await client.playstate(sid, "Unpause" if was_paused else "Pause")
        target = None
    elif action in ("rewind", "forward"):
        # Seek to an exact position instead of Rewind/FastForward, which
        # many apps ignore and which skip by an app-defined amount.
        delta = SEEK_SECONDS * 10_000_000 * (1 if action == "forward" else -1)
        target = max(0, position + delta)
        if runtime:
            target = min(target, max(0, runtime - 10_000_000))
        ok = await client.playstate(sid, "Seek", params={"seekPositionTicks": str(target)})
    else:
        ok = await client.playstate(sid, "Stop")
        target = None

    if not ok:
        return False, "❌ Jellyfin couldn't reach that device."

    if feed is not None:
        obeyed = await _confirm(feed, sid, action, was_paused, target)
        if not obeyed:
            return False, (
                f"⚠️ Sent to **{device}**, but it didn't react. The app on that device ignores remote "
                f"commands — Jellyfin's own apps (web, Android TV, Android, iOS) support them; "
                f"some third-party apps don't."
            )

    title = _esc(_short_title(item))
    if action == "stop":
        return True, f"⏹️ Stopped **{title}** on **{device}**."
    if action == "playpause":
        return True, f"{'▶️ Resumed' if was_paused else '⏸️ Paused'} **{title}** on **{device}**."
    return True, f"{'⏩ Skipped ahead' if action == 'forward' else '⏪ Skipped back'} {SEEK_SECONDS}s on **{device}** — {title}."


async def _confirm(feed, session_id: str, action: str, was_paused: bool, target_ticks: int | None) -> bool:
    """Re-reads the session from Jellyfin a few times over ~5s and checks
    the device actually did what was asked."""
    for delay in CONFIRM_DELAYS:
        await asyncio.sleep(delay)
        if not await feed.poll():
            continue
        current = next((s for s in feed.sessions or [] if s.get("Id") == session_id), None)
        if action == "stop":
            if current is None or not current.get("NowPlayingItem"):
                return True
            continue
        if current is None or not current.get("NowPlayingItem"):
            return action != "playpause" or not was_paused  # gone: can't confirm a resume
        state = current.get("PlayState") or {}
        if action == "playpause":
            if bool(state.get("IsPaused")) != was_paused:
                return True
        else:
            now = int(state.get("PositionTicks") or 0)
            # Within 15s of where we asked it to go (playback keeps moving).
            if target_ticks is not None and abs(now - target_ticks) <= 15 * 10_000_000:
                return True
    return False


def _result_option(item: dict) -> discord.SelectOption:
    kind = item.get("Type")
    year = f" ({item['ProductionYear']})" if item.get("ProductionYear") else ""
    if kind == "Episode":
        season, episode = item.get("ParentIndexNumber"), item.get("IndexNumber")
        code = f"S{season}E{episode} " if season is not None and episode is not None else ""
        label = f"{item.get('SeriesName') or ''} — {code}{item.get('Name') or ''}"
        description = "Episode"
    elif kind == "Series":
        label, description = f"{item.get('Name')}{year}", "Show · plays your next episode"
    else:
        label = f"{item.get('Name')}{year}"
        description = "Movie" + (f" · {_fmt_runtime(item.get('RunTimeTicks'))}" if item.get("RunTimeTicks") else "")
    # Value = Jellyfin item id (32 hex chars), so a pick works even after a restart.
    return discord.SelectOption(label=label[:100] or "Untitled", description=description[:100], value=str(item["Id"])[:100])


# --- restart-proof panel components (DynamicItems) ---

class RemoteButton(discord.ui.DynamicItem[discord.ui.Button],
                   template=r"rmt:(?P<action>[a-z]{2}):(?P<owner>\d+):(?P<sid>[0-9A-Za-z-]*)"):
    """Panel button. custom_id = rmt:<action>:<owner>:<session id>."""

    def __init__(self, action: str, owner: int, sid: str = "", *, label: str | None = None, emoji: str | None = None,
                 style: discord.ButtonStyle = discord.ButtonStyle.secondary, disabled: bool = False):
        super().__init__(discord.ui.Button(
            label=label, emoji=emoji, style=style, disabled=disabled, custom_id=f"rmt:{action}:{owner}:{sid or ''}",
        ))
        self.action, self.owner, self.sid = action, owner, sid or ""

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match):
        return cls(match["action"], int(match["owner"]), match["sid"])

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Remote")
        if cog is None:
            return await interaction.response.send_message("❌ The remote is unavailable right now.", ephemeral=True)
        await cog.on_panel_button(interaction, self.action, self.owner, self.sid)


class RemoteSelect(discord.ui.DynamicItem[discord.ui.Select],
                   template=r"rmt:(?P<kind>dv|pk|qp):(?P<owner>\d+):(?P<extra>[0-9A-Za-z-]*)"):
    """dv = device picker, pk = search result (extra = target session),
    qp = shared quick-button device picker (extra = action code)."""

    def __init__(self, kind: str, owner: int, extra: str = "", *, options=None, placeholder: str | None = None):
        super().__init__(discord.ui.Select(
            custom_id=f"rmt:{kind}:{owner}:{extra or ''}",
            options=options or [discord.SelectOption(label="…", value="0")],
            placeholder=placeholder,
        ))
        self.kind, self.owner, self.extra = kind, owner, extra or ""

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Select, match):
        return cls(match["kind"], int(match["owner"]), match["extra"])

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Remote")
        if cog is None:
            return await interaction.response.send_message("❌ The remote is unavailable right now.", ephemeral=True)
        values = (interaction.data or {}).get("values") or []
        await cog.on_panel_select(interaction, self.kind, self.owner, self.extra, values[0] if values else "")


class SearchModal(discord.ui.Modal, title="Search & Play"):
    query = discord.ui.TextInput(label="Movie or show", placeholder="e.g. Dune, Breaking Bad", min_length=1, max_length=80)

    def __init__(self, owner: int, session_id: str = ""):
        super().__init__()
        self.owner, self.session_id = owner, session_id

    async def on_submit(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Remote")
        if cog is None:
            return await interaction.response.send_message("❌ The remote is unavailable right now.", ephemeral=True)
        await cog.on_panel_search(interaction, self.owner, self.session_id, self.query.value.strip())


# --- the personal panel ---

class RemoteController:
    """One user's panel. Renders from the shared live feed; only one render
    runs at a time and an edit only happens when the content changed."""

    def __init__(self, cog: "Remote", owner_id: int, jellyfin_user_id: str, *, private: bool,
                 message_id: int | None = None, channel=None, interaction: discord.Interaction | None = None):
        self.cog = cog
        self.owner_id = owner_id
        self.jellyfin_user_id = jellyfin_user_id
        self.private = private
        self.message_id = message_id
        self.channel = channel  # public panels: edited through the channel (no 15-min token limit)
        self.interaction = interaction  # private panels: edited through the owner's latest interaction
        self.session_id: str | None = None
        self.mode = "control"  # "control" | "results"
        self.results: list[dict] = []
        self.query = ""
        self.status: str | None = None
        self.status_expires = 0.0
        self.override: dict | None = None  # optimistic state {"sid", "paused"/"stopped", "expires"}
        self.last_used = time.monotonic()
        self.closed = False
        self._render_lock = asyncio.Lock()
        self._last_sig: str | None = None
        self._dirty = asyncio.Event()
        self._task: asyncio.Task | None = None

    # -- state --

    def sessions(self) -> list[dict]:
        return self.cog.feed.user_sessions(self.jellyfin_user_id)

    def current(self) -> dict | None:
        sessions = self.sessions()
        chosen = next((s for s in sessions if s["Id"] == self.session_id), None)
        if chosen is None:
            chosen = next((s for s in sessions if s.get("NowPlayingItem")), None) or (sessions[0] if sessions else None)
            self.session_id = chosen["Id"] if chosen else None
        if chosen is not None and self.override and self.override["sid"] == chosen["Id"]:
            if time.monotonic() < self.override["expires"]:
                chosen = json.loads(json.dumps(chosen))
                if self.override.get("stopped"):
                    chosen.pop("NowPlayingItem", None)
                elif "paused" in self.override:
                    chosen.setdefault("PlayState", {})["IsPaused"] = self.override["paused"]
            else:
                self.override = None
        return chosen

    def set_status(self, text: str, seconds: float = 20):
        self.status, self.status_expires = text, time.monotonic() + seconds

    def touch(self, interaction: discord.Interaction | None = None):
        self.last_used = time.monotonic()
        if self.private and interaction is not None:
            self.interaction = interaction  # newest token -> edits keep working

    # -- rendering --

    def _btn(self, code: str, *, label=None, emoji=None, style=discord.ButtonStyle.secondary, disabled=False, sid=None):
        return RemoteButton(code, self.owner_id, sid if sid is not None else (self.session_id or ""),
                            label=label, emoji=emoji, style=style, disabled=disabled)

    def build(self) -> discord.ui.LayoutView:
        view = discord.ui.LayoutView(timeout=None)
        container = discord.ui.Container()
        if self.status and time.monotonic() > self.status_expires:
            self.status = None
        session = self.current()
        sessions = self.sessions()

        if self.mode == "results":
            device = (session or {}).get("DeviceName") or "your device"
            if self.results:
                head = f"-# SEARCH · “{_esc(self.query)}”\n### Pick something to play on **{_esc(device)}**"
            else:
                head = f"-# SEARCH · “{_esc(self.query)}”\n### No results\nTry a different spelling, or a shorter title."
            container.add_item(discord.ui.TextDisplay(head))
            if self.status:
                container.add_item(discord.ui.TextDisplay(self.status))
            view.add_item(container)
            if self.results and len(sessions) > 1:
                view.add_item(discord.ui.ActionRow(self._device_select(sessions)))
            if self.results and session is not None:
                view.add_item(discord.ui.ActionRow(RemoteSelect(
                    "pk", self.owner_id, session["Id"], placeholder="Choose a title…",
                    options=[_result_option(i) for i in self.results[:SEARCH_LIMIT]],
                )))
            view.add_item(discord.ui.ActionRow(
                self._btn("bk", label="Back", emoji="↩️"),
                self._btn("se", label="Search again", emoji="🔍"),
            ))
            return view

        playing = (session or {}).get("NowPlayingItem")
        if session is None:
            container.add_item(discord.ui.TextDisplay(
                "-# REMOTE\n### No devices open\n"
                "Open Jellyfin on your TV, phone or browser — or sign one in with **📺 Sign in a device** on the remote panel — then press 🔄."
            ))
        else:
            eyebrow = f"-# REMOTE · {_esc(session.get('DeviceName') or 'Device')} · {_esc(session.get('Client') or '')}"
            if playing:
                state = session.get("PlayState") or {}
                position, runtime = state.get("PositionTicks") or 0, playing.get("RunTimeTicks") or 0
                word = "⏸️ Paused" if state.get("IsPaused") else "▶️ Playing"
                bar = _progress_bar(position, runtime)
                timing = f"{_fmt_time(position)} / {_fmt_time(runtime)}" if runtime else _fmt_time(position)
                body = f"{eyebrow}\n{_title_lines(playing)}\n" + (f"{bar}\n" if bar else "") + f"-# {word} · {timing}"
            else:
                body = f"{eyebrow}\n### Nothing playing\nPress **Search & Play** to send something to this device."
            poster = _poster_url(playing) if playing else None
            if poster:
                container.add_item(discord.ui.Section(discord.ui.TextDisplay(body), accessory=discord.ui.Thumbnail(media=poster)))
            else:
                container.add_item(discord.ui.TextDisplay(body))

        if self.cog.client.down:
            self.set_status(DOWN_TEXT, 5)
        if self.status:
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
            container.add_item(discord.ui.TextDisplay(self.status))
        live = "live" if self.cog.feed.ws_connected else "updating"
        container.add_item(discord.ui.TextDisplay(f"-# {live} · closes after {IDLE_CLOSE_SECONDS // 60} min idle"))
        view.add_item(container)

        if len(sessions) > 1:
            view.add_item(discord.ui.ActionRow(self._device_select(sessions)))

        no_media = playing is None
        paused = bool(((session or {}).get("PlayState") or {}).get("IsPaused"))
        view.add_item(discord.ui.ActionRow(
            self._btn("pv", emoji="⏮️", disabled=no_media),
            self._btn("rw", emoji="⏪", disabled=no_media),
            self._btn("pp", label="Resume" if paused else "Pause", emoji="▶️" if paused else "⏸️",
                      style=discord.ButtonStyle.primary, disabled=no_media),
            self._btn("ff", emoji="⏩", disabled=no_media),
            self._btn("nx", emoji="⏭️", disabled=no_media),
        ))
        view.add_item(discord.ui.ActionRow(
            self._btn("se", label="Search & Play", emoji="🔍", style=discord.ButtonStyle.success, disabled=session is None),
            self._btn("st", label="Stop", emoji="⏹️", style=discord.ButtonStyle.danger, disabled=no_media),
            self._btn("rf", emoji="🔄"),
            self._btn("cl", label="Close", emoji="✖️"),
        ))
        return view

    def _device_select(self, sessions: list[dict]) -> RemoteSelect:
        return RemoteSelect("dv", self.owner_id, "", placeholder="Choose a device…", options=[
            discord.SelectOption(
                label=(s.get("DeviceName") or "Device")[:100],
                description=(f"{s.get('Client') or ''} · " + ("playing" if s.get("NowPlayingItem") else "idle"))[:100],
                value=s["Id"][:100],
                default=s["Id"] == self.session_id,
            )
            for s in sessions[:25]
        ])

    async def _edit(self, view: discord.ui.LayoutView):
        if self.private:
            await self.interaction.edit_original_response(view=view)
        else:
            await self.channel.get_partial_message(self.message_id).edit(view=view, allowed_mentions=_NO_PINGS)

    async def render(self, force: bool = False):
        async with self._render_lock:
            if self.closed:
                return
            view = self.build()
            sig = repr(view.to_components())
            if not force and sig == self._last_sig:
                return
            try:
                await self._edit(view)
                self._last_sig = sig
            except discord.NotFound:
                await self.close(silent=True)  # deleted / dismissed
            except discord.HTTPException as e:
                logger.warning(f"[Remote] Panel edit failed: {type(e).__name__}: {e}")

    def notify(self):
        self._dirty.set()

    def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        try:
            while not self.closed:
                try:
                    await asyncio.wait_for(self._dirty.wait(), timeout=30)
                except asyncio.TimeoutError:
                    pass
                if self.closed:
                    return
                if time.monotonic() - self.last_used > IDLE_CLOSE_SECONDS:
                    await self.close("-# REMOTE\nClosed after inactivity — open it again any time.")
                    return
                if self._dirty.is_set():
                    self._dirty.clear()
                    if self.mode == "control":
                        await self.render()
                    await asyncio.sleep(PANEL_MIN_EDIT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[Remote] Panel loop stopped: {type(e).__name__}: {e}")

    async def close(self, text: str = "-# REMOTE\nClosed — open it again any time.", silent: bool = False):
        if self.closed:
            return
        self.closed = True
        if not silent:
            try:
                await self._edit(_private_text(text))
            except discord.HTTPException:
                pass
        if self._task is not None and self._task is not asyncio.current_task():
            self._task.cancel()
        if self.cog.controllers.get(self.owner_id) is self:
            del self.cog.controllers[self.owner_id]


# --- shared panel ---

def _remote_header(count: int | None, down: bool) -> str:
    if down:
        live = "🔴 Jellyfin unreachable · "
    elif count is None:
        live = ""
    elif count == 0:
        live = "⚪ nobody watching · "
    else:
        live = f"🟢 {count} stream{'s' if count != 1 else ''} playing · "
    return f"# 🎮 Jellyfin Remote\n-# {live}controls only ever touch your own devices"


FULL_REMOTE_TEXT = "**Full remote**\n-# Seek, next episode, pick which device — everything in one place."
SEARCH_TEXT = "**Search & Play**\n-# Find a movie or show and start it on your TV."
QUICK_CONNECT_TEXT = (
    "**📺 Quick Connect**\n"
    "-# Choose Quick Connect on your device's login screen, press Sign in, enter the 6-digit code. "
    "Only use codes from your own screen."
)
PANEL_FOOTER = "-# Everything you press is visible only to you."


class _SharedControlsRow(discord.ui.ActionRow):
    @discord.ui.button(label="Pause", emoji="⏯️", style=discord.ButtonStyle.secondary, custom_id=SHARED_IDS["playpause"])
    async def playpause(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _shared_quick(interaction, "playpause")

    @discord.ui.button(label="30s", emoji="⏪", style=discord.ButtonStyle.secondary, custom_id=SHARED_IDS["back30"])
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _shared_quick(interaction, "back30")

    @discord.ui.button(label="30s", emoji="⏩", style=discord.ButtonStyle.secondary, custom_id=SHARED_IDS["fwd30"])
    async def fwd(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _shared_quick(interaction, "fwd30")

    @discord.ui.button(label="Stop", emoji="⏹️", style=discord.ButtonStyle.danger, custom_id=SHARED_IDS["stop"])
    async def stop_(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _shared_quick(interaction, "stop")


# Section accessories can't use the @discord.ui.button decorator, so the
# three side buttons are small Button subclasses. Same fixed custom_ids as
# before, so panels already posted keep working through a restart.

class _OpenRemoteButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Open", emoji="🎮", style=discord.ButtonStyle.primary, custom_id=SHARED_IDS["open"])

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Remote")
        if cog is None:
            return await interaction.response.send_message("❌ The remote is unavailable right now.", ephemeral=True)
        await cog.open_private_panel(interaction)


class _SearchButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Search", emoji="🔍", style=discord.ButtonStyle.success, custom_id=SHARED_IDS["search"])

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Remote")
        if cog is None or await cog.linked_jellyfin_id(interaction.user.id) is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)
        await interaction.response.send_modal(SharedSearchModal())


class _SignInButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Sign in", emoji="📺", style=discord.ButtonStyle.success, custom_id=SHARED_IDS["signin"])

    async def callback(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Remote")
        if cog is None:
            return await interaction.response.send_message("❌ Quick Connect is unavailable right now.", ephemeral=True)
        await cog.open_signin(interaction)


class SharedPanelView(discord.ui.LayoutView):
    """One card: title + live count, quick controls, then Full remote,
    Search & Play and Quick Connect as rows with their button on the side."""

    def __init__(self, stream_count: int | None = None, down: bool = False):
        super().__init__(timeout=None)
        panel = discord.ui.Container()
        panel.add_item(discord.ui.TextDisplay(_remote_header(stream_count, down)))
        panel.add_item(_SharedControlsRow())
        panel.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        panel.add_item(discord.ui.Section(discord.ui.TextDisplay(FULL_REMOTE_TEXT), accessory=_OpenRemoteButton()))
        panel.add_item(discord.ui.Section(discord.ui.TextDisplay(SEARCH_TEXT), accessory=_SearchButton()))
        panel.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))
        panel.add_item(discord.ui.Section(discord.ui.TextDisplay(QUICK_CONNECT_TEXT), accessory=_SignInButton()))
        panel.add_item(discord.ui.TextDisplay(PANEL_FOOTER))
        self.add_item(panel)


class SharedSearchModal(discord.ui.Modal, title="Search & Play"):
    query = discord.ui.TextInput(label="Movie or show", placeholder="e.g. Dune, Breaking Bad", min_length=1, max_length=80)

    async def on_submit(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Remote")
        if cog is None:
            return await interaction.response.send_message("❌ The remote is unavailable right now.", ephemeral=True)
        await cog.open_private_panel(interaction, search_query=self.query.value.strip())


async def _shared_quick(interaction: discord.Interaction, shared_action: str):
    cog = interaction.client.get_cog("Remote")
    if cog is None:
        return await interaction.response.send_message("❌ The remote is unavailable right now.", ephemeral=True)
    await cog.quick_action(interaction, SHARED_TO_CODE[shared_action])


class QuickConnectModal(discord.ui.Modal, title="Jellyfin Quick Connect"):
    code = discord.ui.TextInput(label="6-digit code", placeholder="Shown on your device's login screen", min_length=6, max_length=6)

    async def on_submit(self, interaction: discord.Interaction):
        cog = interaction.client.get_cog("Remote")
        if cog is None:
            return await interaction.response.send_message("❌ Quick Connect is unavailable right now.", ephemeral=True)
        await cog.handle_signin(interaction, self.code.value)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        logger.error(f"[Remote] Quick Connect error for {interaction.user.id}: {type(error).__name__}: {error}", exc_info=error)
        try:
            if interaction.response.is_done():
                await interaction.edit_original_response(view=_private_text("❌ Something went wrong — try again in a moment."))
            else:
                await interaction.response.send_message("❌ Something went wrong — try again in a moment.", ephemeral=True)
        except discord.HTTPException:
            pass


# --- cog ---

class Remote(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Jellyfin"
        self.client = JellyfinClient(bot)
        self.feed = SessionFeed(self)
        self.controllers: dict[int, RemoteController] = {}
        self._links_db: aiosqlite.Connection | None = None
        self._db: aiosqlite.Connection | None = None
        self._link_cache: dict[int, tuple[str | None, float]] = {}
        self._search_cache: dict[tuple[str, str], tuple[list[dict], float]] = {}
        self._command_locks: dict[int, asyncio.Lock] = {}
        self._queued: dict[int, int] = {}
        self._panel_message_id: int | None = None
        self._panel_shown: tuple | None = None
        self._panel_task: asyncio.Task | None = None
        self._panel_lock = asyncio.Lock()
        self._panel_wakeup = asyncio.Event()
        self._qc_last_submit: dict[int, float] = {}
        self._qc_tasks: set[asyncio.Task] = set()

    async def cog_load(self):
        if not os.path.exists(LINKS_DB_PATH):
            raise RuntimeError(f"[Remote] Database not found at {LINKS_DB_PATH}")
        self._links_db = await aiosqlite.connect(LINKS_DB_PATH)
        self._db = await aiosqlite.connect(REMOTE_DB_PATH)
        await self._db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        await self._db.commit()
        self._shared_view = SharedPanelView()
        self.bot.add_view(self._shared_view)
        logger.info("[Remote] Loaded — single-card panel version.")
        self.bot.add_dynamic_items(RemoteButton, RemoteSelect)
        self.feed.start()
        self._panel_task = asyncio.create_task(self._panel_loop())

    async def cog_unload(self):
        self.bot.remove_dynamic_items(RemoteButton, RemoteSelect)
        if getattr(self, "_shared_view", None) is not None:
            self._shared_view.stop()  # unregister the old handlers so a reload can't run stale code
        if self._panel_task is not None:
            self._panel_task.cancel()
            await asyncio.gather(self._panel_task, return_exceptions=True)
        await self.feed.stop()
        for task in list(self._qc_tasks):
            task.cancel()
        await asyncio.gather(*self._qc_tasks, return_exceptions=True)
        for controller in list(self.controllers.values()):
            await controller.close(silent=True)  # buttons keep working after reload (DynamicItems)
        for conn in (self._links_db, self._db):
            if conn is not None:
                await conn.close()
        self._links_db = self._db = None

    # --- shared state helpers ---

    def on_feed_change(self):
        for controller in list(self.controllers.values()):
            controller.notify()

    async def linked_jellyfin_id(self, discord_id: int) -> str | None:
        cached = self._link_cache.get(discord_id)
        now = time.monotonic()
        if cached is not None:
            value, at = cached
            if now - at < (LINK_CACHE_SECONDS if value else LINK_MISS_CACHE_SECONDS):
                return value
        async with self._links_db.execute("SELECT jellyfin_user_id FROM links WHERE discord_id = ?", (discord_id,)) as cursor:
            row = await cursor.fetchone()
        value = row[0] if row else None
        self._link_cache[discord_id] = (value, now)
        return value

    async def _search(self, jellyfin_user_id: str, query: str) -> list[dict] | None:
        key = (jellyfin_user_id, query.lower())
        cached = self._search_cache.get(key)
        if cached and time.monotonic() - cached[1] < SEARCH_CACHE_SECONDS:
            return cached[0]
        results = await self.client.search(jellyfin_user_id, query)
        if results is not None:
            if len(self._search_cache) > 200:
                self._search_cache.clear()
            self._search_cache[key] = (results, time.monotonic())
        return results

    async def _log(self, text: str):
        try:
            channel = self.bot.get_channel(LOG_CHANNEL_ID)
            if isinstance(channel, (discord.TextChannel, discord.VoiceChannel, discord.Thread)):
                await channel.send(text, allowed_mentions=_NO_PINGS)
        except Exception as e:
            logger.warning(f"[Remote] Action log failed: {type(e).__name__}: {e}")

    async def _run_command(self, owner: int, coro_factory):
        """Runs one user's commands strictly in order; extra mashed taps
        beyond MAX_QUEUED_COMMANDS are dropped. Returns the result, or None
        if dropped."""
        lock = self._command_locks.setdefault(owner, asyncio.Lock())
        if self._queued.get(owner, 0) >= MAX_QUEUED_COMMANDS:
            return None
        self._queued[owner] = self._queued.get(owner, 0) + 1
        try:
            async with lock:
                started = time.monotonic()
                result = await coro_factory()
                if time.monotonic() - started > SLOW_CALL_SECONDS:
                    logger.warning(f"[Remote] Slow command for {owner}: {time.monotonic() - started:.1f}s")
                return result
        finally:
            self._queued[owner] -= 1

    # --- controllers ---

    async def _controller_for(self, interaction: discord.Interaction, owner: int) -> RemoteController | None:
        """The controller for the panel this interaction came from —
        rebuilt from the interaction itself if the bot restarted."""
        message = interaction.message
        controller = self.controllers.get(owner)
        if controller is not None and message is not None and controller.message_id == message.id and not controller.closed:
            controller.touch(interaction)
            return controller
        jellyfin_user_id = await self.linked_jellyfin_id(owner)
        if jellyfin_user_id is None:
            return None
        if controller is not None:
            await controller.close("-# REMOTE\nReplaced by a newer remote.")
        private = bool(message is not None and message.flags.ephemeral)
        controller = RemoteController(
            self, owner, jellyfin_user_id, private=private, message_id=message.id if message else None,
            channel=None if private else interaction.channel, interaction=interaction if private else None,
        )
        self.controllers[owner] = controller
        controller.start()
        return controller

    async def _guard(self, interaction: discord.Interaction, owner: int) -> bool:
        if interaction.user.id != owner:
            await interaction.response.send_message("❌ This isn't your remote — open your own from the remote panel.", ephemeral=True)
            return False
        return True

    async def on_panel_button(self, interaction: discord.Interaction, code: str, owner: int, sid: str):
        if not await self._guard(interaction, owner):
            return
        if code == "se":
            return await interaction.response.send_modal(SearchModal(owner, sid))
        await interaction.response.defer()
        controller = await self._controller_for(interaction, owner)
        if controller is None:
            return await interaction.followup.send("❌ You don't have a linked Jellyfin account.", ephemeral=True)
        if sid:
            controller.session_id = sid

        if code == "cl":
            return await controller.close()
        if code == "bk":
            controller.mode = "control"
            return await controller.render(force=True)
        if code == "rf":
            await self.feed.poll()
            return await controller.render(force=True)

        action = ACTIONS.get(code)
        if action is None:
            return
        await self._run_command(owner, lambda: self._panel_action(controller, action))

    async def _panel_action(self, controller: RemoteController, action: str):
        try:
            await self.feed.fresh()
        except JellyfinDown:
            pass
        session = next((s for s in controller.sessions() if s["Id"] == controller.session_id), None)
        if session is None or not session.get("NowPlayingItem"):
            controller.set_status("ℹ️ Nothing is playing on that device anymore.")
            return await controller.render(force=True)
        try:
            ok, text = await _perform(self.client, controller.jellyfin_user_id, session, action, feed=self.feed)
        except JellyfinDown:
            controller.set_status(DOWN_TEXT)
            return await controller.render(force=True)
        if ok and action == "playpause":
            paused = bool((session.get("PlayState") or {}).get("IsPaused"))
            controller.override = {"sid": session["Id"], "paused": not paused, "expires": time.monotonic() + OVERRIDE_SECONDS}
        elif ok and action == "stop":
            controller.override = {"sid": session["Id"], "stopped": True, "expires": time.monotonic() + OVERRIDE_SECONDS}
        controller.set_status(text, 8)
        await controller.render(force=True)  # instant feedback
        self.feed.refresh_soon()  # the live feed confirms within a second

    async def on_panel_select(self, interaction: discord.Interaction, kind: str, owner: int, extra: str, value: str):
        if not await self._guard(interaction, owner):
            return
        await interaction.response.defer()

        if kind == "qp":
            return await self._quick_pick(interaction, owner, extra, value)

        controller = await self._controller_for(interaction, owner)
        if controller is None:
            return await interaction.followup.send("❌ You don't have a linked Jellyfin account.", ephemeral=True)

        if kind == "dv":
            controller.session_id = value
            return await controller.render(force=True)

        if kind == "pk":
            controller.session_id = extra or controller.session_id
            await self._run_command(owner, lambda: self._play_pick(controller, value))

    async def _play_pick(self, controller: RemoteController, item_id: str):
        controller.mode = "control"
        try:
            await self.feed.fresh()
            session = next((s for s in controller.sessions() if s["Id"] == controller.session_id), None)
            if session is None:
                controller.set_status("ℹ️ That device closed — open Jellyfin on it and try again.")
                return await controller.render(force=True)
            item = await self.client.item_for_user(controller.jellyfin_user_id, item_id)
            if item is None:
                controller.set_status("❌ That title isn't available to your account.")
                return await controller.render(force=True)
            if item.get("Type") == "Series":
                episode = await self.client.next_up(controller.jellyfin_user_id, item["Id"])
                if episode is None:
                    controller.set_status(f"❌ Couldn't find an episode of **{_esc(item.get('Name'))}** to play.")
                    return await controller.render(force=True)
                item = episode
            user_data = item.get("UserData") or {}
            start = 0 if user_data.get("Played") else (user_data.get("PlaybackPositionTicks") or 0)
            ok = await self.client.play_now(session["Id"], item["Id"], start)
        except JellyfinDown:
            controller.set_status(DOWN_TEXT)
            return await controller.render(force=True)
        device = session.get("DeviceName") or "your device"
        if ok:
            resume = f" from {_fmt_time(start)}" if start else ""
            controller.set_status(f"✅ Sent **{_esc(_short_title(item))}** to **{_esc(device)}**{resume}.", 10)
        else:
            controller.set_status(f"❌ **{_esc(device)}** didn't accept it — it may not support remote play.")
        await controller.render(force=True)
        self.feed.refresh_soon(1.5)

    async def on_panel_search(self, interaction: discord.Interaction, owner: int, sid: str, query: str):
        if not await self._guard(interaction, owner):
            return
        await interaction.response.defer()
        controller = await self._controller_for(interaction, owner)
        if controller is None:
            return await interaction.followup.send("❌ You don't have a linked Jellyfin account.", ephemeral=True)
        if sid:
            controller.session_id = sid
        try:
            results = await self._search(controller.jellyfin_user_id, query)
        except JellyfinDown:
            results = None
        if results is None:
            controller.mode = "control"
            controller.set_status("⚠️ Couldn't search right now — try again in a moment.")
        else:
            controller.mode, controller.results, controller.query = "results", results, query
        await controller.render(force=True)

    async def open_private_panel(self, interaction: discord.Interaction, search_query: str | None = None):
        jellyfin_user_id = await self.linked_jellyfin_id(interaction.user.id)
        if jellyfin_user_id is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)

        previous = self.controllers.get(interaction.user.id)
        if previous is not None:
            await previous.close("-# REMOTE\nReplaced by a newer remote.")
        controller = RemoteController(self, interaction.user.id, jellyfin_user_id, private=True, interaction=interaction)
        try:
            await self.feed.fresh()
        except JellyfinDown:
            pass
        if search_query:
            try:
                results = await self._search(jellyfin_user_id, search_query)
            except JellyfinDown:
                results = None
            if results is None:
                controller.set_status("⚠️ Couldn't search right now — try again in a moment.")
            elif not controller.sessions():
                controller.set_status("ℹ️ Open Jellyfin on a device first — then your results can play there.")
            else:
                controller.mode, controller.results, controller.query = "results", results, search_query

        view = controller.build()
        message = await interaction.edit_original_response(view=view)
        controller.message_id = message.id
        controller._last_sig = repr(view.to_components())
        self.controllers[interaction.user.id] = controller
        controller.start()

    # --- shared quick buttons ---

    async def quick_action(self, interaction: discord.Interaction, code: str):
        jellyfin_user_id = await self.linked_jellyfin_id(interaction.user.id)
        if jellyfin_user_id is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await self.feed.fresh()
        except JellyfinDown:
            pass
        if self.client.down:
            return await interaction.edit_original_response(view=_private_text(DOWN_TEXT))
        playing = [s for s in self.feed.user_sessions(jellyfin_user_id) if s.get("NowPlayingItem")]
        if not playing:
            return await interaction.edit_original_response(view=_private_text(
                "ℹ️ Nothing is playing on your devices. Use **🔍 Search & Play** to start something."
            ))
        if len(playing) > 1:
            view = discord.ui.LayoutView(timeout=None)
            view.add_item(discord.ui.Container(discord.ui.TextDisplay("You've got more than one device playing — which one?")))
            view.add_item(discord.ui.ActionRow(RemoteSelect(
                "qp", interaction.user.id, code, placeholder="Choose a device…",
                options=[
                    discord.SelectOption(label=(s.get("DeviceName") or "Device")[:100],
                                         description=_short_title(s.get("NowPlayingItem") or {})[:100], value=s["Id"][:100])
                    for s in playing[:25]
                ],
            )))
            return await interaction.edit_original_response(view=view)
        text = await self._run_command(interaction.user.id, lambda: self._quick_run(jellyfin_user_id, playing[0]["Id"], code))
        await interaction.edit_original_response(view=_private_text(text or "⏳ Slow down — still working through your last few presses."))

    async def _quick_pick(self, interaction: discord.Interaction, owner: int, code: str, session_id: str):
        jellyfin_user_id = await self.linked_jellyfin_id(owner)
        if jellyfin_user_id is None:
            return await interaction.edit_original_response(view=_private_text("❌ You don't have a linked Jellyfin account."))
        text = await self._run_command(owner, lambda: self._quick_run(jellyfin_user_id, session_id, code))
        await interaction.edit_original_response(view=_private_text(text or "⏳ Slow down — still working through your last few presses."))

    async def _quick_run(self, jellyfin_user_id: str, session_id: str, code: str) -> str:
        try:
            await self.feed.fresh()
            session = next((s for s in self.feed.user_sessions(jellyfin_user_id)
                            if s["Id"] == session_id and s.get("NowPlayingItem")), None)
            if session is None:
                return "ℹ️ That device isn't playing anymore."
            _, text = await _perform(self.client, jellyfin_user_id, session, ACTIONS[code], feed=self.feed)
        except JellyfinDown:
            return DOWN_TEXT
        self.feed.refresh_soon()
        return text

    # --- shared panel upkeep ---

    @staticmethod
    def _is_shared_panel(message: discord.Message) -> bool:
        stack = list(message.components)
        while stack:
            component = stack.pop()
            if getattr(component, "custom_id", None) == SHARED_IDS["open"]:
                return True
            stack.extend(getattr(component, "children", None) or [])
            # The Open button now sits on a Section as its accessory, which
            # isn't in .children — check it too, or the panel isn't recognised.
            accessory = getattr(component, "accessory", None)
            if accessory is not None:
                stack.append(accessory)
            stack.extend(getattr(component, "components", None) or [])
        return False

    async def _meta_get(self, key: str) -> str | None:
        async with self._db.execute("SELECT value FROM meta WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
        return row[0] if row else None

    async def _meta_set(self, key: str, value: str):
        await self._db.execute("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))
        await self._db.commit()

    # --- shared panel upkeep ---
    # One rule: the remote channel always has exactly ONE panel message from
    # the bot, showing the current layout.
    #   - Startup: scan the channel. Panel found -> keep the newest, delete
    #     any extra copies, redraw it in the current layout. None found ->
    #     send a fresh one.
    #   - Deleted (by anyone, any time): the delete event wakes the loop
    #     straight away and a new panel is sent.
    #   - Every PANEL_EXISTS_CHECK_SECONDS the message is also fetched, as a
    #     backstop for a delete event that was missed.
    # The current message ID is saved in remote.db each time it changes.

    async def _find_panels(self, channel) -> list[discord.Message]:
        found: list[discord.Message] = []
        async for message in channel.history(limit=PANEL_SCAN_LIMIT):
            if message.author.id == self.bot.user.id and self._is_shared_panel(message):
                found.append(message)
        return found  # newest first

    async def _send_new_panel(self, channel):
        shown = (self.feed.stream_count(), self.client.down)
        message = await channel.send(view=SharedPanelView(*shown))
        self._panel_message_id = message.id
        self._panel_shown = shown
        await self._meta_set(f"panel:{channel.id}", str(message.id))
        logger.info(f"[Remote] Posted a new shared panel → {message.jump_url}")

    async def _ensure_panel(self, channel, check_exists: bool = False):
        shown = (self.feed.stream_count(), self.client.down)

        if self._panel_message_id is None:
            panels = await self._find_panels(channel)
            if not panels:
                return await self._send_new_panel(channel)
            keep, extras = panels[0], panels[1:]
            for extra in extras:
                try:
                    await extra.delete()
                except discord.HTTPException:
                    pass
            self._panel_message_id = keep.id
            self._panel_shown = None  # force a redraw into the current layout
            await self._meta_set(f"panel:{channel.id}", str(keep.id))
            logger.info(f"[Remote] Using existing shared panel → {keep.jump_url}"
                        + (f" (deleted {len(extras)} extra)" if extras else ""))

        try:
            if shown != self._panel_shown:
                await channel.get_partial_message(self._panel_message_id).edit(view=SharedPanelView(*shown))
                self._panel_shown = shown
            elif check_exists:
                await channel.fetch_message(self._panel_message_id)
        except discord.NotFound:
            logger.info("[Remote] Shared panel was deleted — posting a new one.")
            self._panel_message_id = None
            self._panel_shown = None
            await self._send_new_panel(channel)

    async def _remove_panels_in_other_channels(self, current_channel_id: int):
        """If REMOTE_CHANNEL_ID changed, the old channel still has a panel.
        Every panel's message ID is saved per channel, so delete any saved
        panel that isn't in the current channel."""
        try:
            async with self._db.execute("SELECT key, value FROM meta WHERE key LIKE 'panel:%'") as cursor:
                rows = await cursor.fetchall()
        except Exception as e:
            logger.warning(f"[Remote] Couldn't read saved panels: {type(e).__name__}: {e}")
            return
        for key, value in rows:
            channel_id = int(key.split(":", 1)[1])
            if channel_id == current_channel_id:
                continue
            old_channel = self.bot.get_channel(channel_id)
            if old_channel is not None:
                try:
                    await old_channel.get_partial_message(int(value)).delete()
                    logger.info(f"[Remote] Removed the old panel from #{old_channel.name}.")
                except discord.HTTPException:
                    pass
            await self._db.execute("DELETE FROM meta WHERE key = ?", (key,))
        await self._db.commit()

    async def _panel_loop(self):
        await self.bot.wait_until_ready()
        channel = self.bot.get_channel(REMOTE_CHANNEL_ID)
        if channel is None:
            logger.warning(f"[Remote] Channel {REMOTE_CHANNEL_ID} not found — shared panel not posted.")
            return
        await asyncio.sleep(3)  # let the live feed fill in first
        await self._remove_panels_in_other_channels(channel.id)
        last_exists_check = 0.0
        while True:
            self._panel_wakeup.clear()  # a delete arriving during the work below re-wakes the loop
            try:
                check_exists = time.monotonic() - last_exists_check >= PANEL_EXISTS_CHECK_SECONDS
                async with self._panel_lock:
                    await self._ensure_panel(channel, check_exists=check_exists)
                if check_exists:
                    last_exists_check = time.monotonic()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[Remote] Shared panel upkeep failed: {type(e).__name__}: {e}")
            try:
                await asyncio.wait_for(self._panel_wakeup.wait(), timeout=SHARED_PANEL_CHECK_SECONDS)
            except asyncio.TimeoutError:
                pass

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        if payload.message_id == self._panel_message_id:
            self._panel_message_id = None
            self._panel_shown = None
            self._panel_wakeup.set()

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent):
        if self._panel_message_id in payload.message_ids:
            self._panel_message_id = None
            self._panel_shown = None
            self._panel_wakeup.set()

    # --- Quick Connect ---

    def qc_cooldown_remaining(self, discord_id: int) -> int:
        last = self._qc_last_submit.get(discord_id)
        if last is None:
            return 0
        return max(0, int(QC_COOLDOWN_SECONDS - (time.monotonic() - last) + 0.999))

    async def open_signin(self, interaction: discord.Interaction):
        remaining = self.qc_cooldown_remaining(interaction.user.id)
        if remaining > 0:
            return await interaction.response.send_message(f"⏳ Give it {remaining}s before trying again.", ephemeral=True)
        await interaction.response.send_modal(QuickConnectModal())

    async def _name_signed_in_device(self, interaction: discord.Interaction, discord_id: int, username: str,
                                     jellyfin_user_id: str, known_session_ids: set[str] | None):
        try:
            deadline = time.monotonic() + QC_DEVICE_DETECT_SECONDS
            session = None
            while time.monotonic() < deadline and session is None:
                await asyncio.sleep(QC_DEVICE_DETECT_POLL_SECONDS)
                try:
                    await self.feed.fresh(max_age=QC_DEVICE_DETECT_POLL_SECONDS)
                except JellyfinDown:
                    continue
                sessions = self.feed.user_sessions(jellyfin_user_id, controllable=False)
                new = [s for s in sessions if s["Id"] not in known_session_ids] if known_session_ids is not None else sessions
                if new:
                    new.sort(key=lambda s: s.get("LastActivityDate") or "", reverse=True)
                    session = new[0]
            if session is None or not session.get("DeviceName"):
                return
            device = session["DeviceName"]
            app = " ".join(filter(None, [session.get("Client"), session.get("ApplicationVersion")])) or None
            footer = " · ".join(filter(None, [_esc(app) if app else None, f"Account: **{_esc(username)}**"]))
            await interaction.edit_original_response(view=_private_text(f"✅ Signed in on **{_esc(device)}**\n-# {footer}"))
            await self._log(f"📺 **Quick Connect device** — <@{discord_id}> → **{device}**" + (f" ({app})" if app else "") + ".")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[Remote] Device naming failed: {type(e).__name__}: {e}")

    async def handle_signin(self, interaction: discord.Interaction, raw_code: str):
        user = interaction.user
        code = raw_code.replace(" ", "").strip()
        if not (len(code) == 6 and code.isdigit()):
            return await interaction.response.send_message("❌ Quick Connect codes are 6 digits — check the code on your device.", ephemeral=True)
        remaining = self.qc_cooldown_remaining(user.id)
        if remaining > 0:
            return await interaction.response.send_message(f"⏳ Give it {remaining}s before trying again.", ephemeral=True)
        self._qc_last_submit[user.id] = time.monotonic()

        jellyfin_user_id = await self.linked_jellyfin_id(user.id)  # account ONLY from the presser's Discord ID
        if jellyfin_user_id is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await self.feed.fresh(max_age=1)
            known = {s["Id"] for s in self.feed.user_sessions(jellyfin_user_id, controllable=False)} if self.feed.sessions is not None else None
            result = await self.client.authorize_quick_connect(jellyfin_user_id, code)
        except JellyfinDown:
            return await interaction.edit_original_response(view=_private_text(DOWN_TEXT))

        if result == "invalid":
            return await interaction.edit_original_response(view=_private_text(
                "❌ That code isn't valid or has expired — get a fresh one on your device and try again."
            ))
        if result == "forbidden":
            return await interaction.edit_original_response(view=_private_text(
                "❌ Quick Connect isn't available on the server right now. Ask an admin."
            ))

        username = await self.client.username(jellyfin_user_id)
        await interaction.edit_original_response(view=_private_text(
            f"✅ Signed in as **{_esc(username)}** — your device will open in a moment."
        ))
        await self._log(f"📺 **Quick Connect** — <@{user.id}> signed a device into **{username}**.")
        task = asyncio.create_task(self._name_signed_in_device(interaction, user.id, username, jellyfin_user_id, known))
        self._qc_tasks.add(task)
        task.add_done_callback(self._qc_tasks.discard)

    # --- /remote ---

    @app_commands.command(name="remote", description="Control playback on your Jellyfin devices, or send something to your TV")
    async def remote(self, interaction: discord.Interaction):
        jellyfin_user_id = await self.linked_jellyfin_id(interaction.user.id)
        if jellyfin_user_id is None:
            return await interaction.response.send_message("❌ You don't have a linked Jellyfin account.", ephemeral=True)
        await interaction.response.defer()

        previous = self.controllers.get(interaction.user.id)
        if previous is not None:
            await previous.close("-# REMOTE\nReplaced by a newer remote.")
        try:
            await self.feed.fresh()
        except JellyfinDown:
            pass
        controller = RemoteController(self, interaction.user.id, jellyfin_user_id, private=False, channel=interaction.channel)
        view = controller.build()
        sent = await interaction.followup.send(view=view, allowed_mentions=_NO_PINGS, wait=True)
        controller.message_id = sent.id
        controller._last_sig = repr(view.to_components())
        self.controllers[interaction.user.id] = controller
        controller.start()

    @remote.error
    async def remote_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        logger.error(f"[Remote] /remote failed: {type(error).__name__}: {error}", exc_info=error)
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(Remote(bot))