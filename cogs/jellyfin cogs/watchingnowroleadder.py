"""
Watching / Browsing Role Sync Cog
  WATCHING_ROLE_ID  -> linked users currently playing something (paused
                       still counts, see COUNT_PAUSED_AS_WATCHING).
  BROWSING_ROLE_ID  -> linked users active on Jellyfin in the last
                       ACTIVE_WITHIN_SECONDS who are NOT watching.
  The two roles are exclusive: a watcher never holds Browsing.

  WHEN ROLES UPDATE
    1. Live playback events (playing / progress / paused / stopped),
       forwarded by dashboard.py's shared Tracearr SSE connection. Watching
       starts and stops apply within about a second.
    2. Every POLL_SECONDS: one REST /Sessions call filtered to sessions
       active in the last ACTIVE_WITHIN_SECONDS (the same window Jellyfin's
       own dashboard uses). This is the only reliable source for Browsing —
       nothing fires when someone simply stops using the app, their session
       just goes quiet — and it also corrects anything a live event missed.
    3. Once at startup (the first poll), which also cleans up everyone
       holding a role they shouldn't.

  ONE SOURCE OF TRUTH
    The previous revision kept two separate session lists (one built from
    live events, one from the REST resync) and diffed one against the
    other, which removed and re-added ~130 roles every few minutes. Now
    there is a single in-memory state that both live events and the poll
    update, and every sync is a RECONCILE: work out who should hold each
    role, compare against who actually holds it (from the member cache,
    zero API calls), and only edit the members that differ. Members who
    hold a role but are no longer linked are covered too, and a role
    removed by hand is put back on the next pass.

  session.start / session.end are ignored. Jellyfin keeps a session open
  long after someone stops using the app (until logout or a restart), so
  those events never meant "browsing right now".
"""

import asyncio
import logging
import time

import aiohttp
import aiosqlite
import discord
from discord.ext import commands, tasks

logger = logging.getLogger("watching_role_cog")

JELLYFIN_URL = "https://jellyfin.ushereats.com"
JELLYFIN_API_KEY = "9cb945d0d38d45cb84fcc8a2e1541bd8"
TARGET_GUILD_ID = 1537023658323869757
WATCHING_ROLE_ID = 1539199204017315911
BROWSING_ROLE_ID = 1540324328996872203

DB_PATH = "/home/container/ushereats/DATABASES/jellyfin_links.db"

# Paused still counts as watching — only stopping playback drops it.
COUNT_PAUSED_AS_WATCHING = True

POLL_SECONDS = 60
# A session counts as active (browsing) if Jellyfin saw it within this
# many seconds. 960 matches Jellyfin's own dashboard, so the bot's count
# lines up with what you see there. Lower it to drop Browsing sooner after
# someone closes the app.
ACTIVE_WITHIN_SECONDS = 960

REQUEST_TIMEOUT_SECONDS = 10
LINKS_CACHE_SECONDS = 60
# Gap between individual role edits. The first run after this update may
# need to clean up ~130 members; 0.5s keeps that well clear of Discord's
# rate limit (about a minute for the whole cleanup).
ROLE_EDIT_GAP_SECONDS = 0.5
# Short coalescing window: a burst of live events becomes one reconcile.
EVENT_COALESCE_SECONDS = 1.0


class JellyfinWatchingRole(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # --- single shared state, updated by BOTH live events and the poll ---
        self._watch_sessions: dict[str, str] = {}   # sessionId -> jellyfin userId, sessions currently playing
        self._active_users: set[str] = set()        # jellyfin userIds active within the window
        self._event_seq = 0                         # bumps on every live event, lets the poll detect a race
        self._polled_once = False                   # no reconciling until the first poll has a real baseline

        self._links: dict[str, list[int]] = {}      # jellyfin userId -> [discord ids]
        self._links_loaded_at = 0.0

        self._reconcile_lock = asyncio.Lock()
        self._reconcile_task: asyncio.Task | None = None
        self._dirty = False

    # ------------------------------------------------------------------ lifecycle
    async def cog_load(self):
        if not self.poll_loop.is_running():
            self.poll_loop.start()

    async def cog_unload(self):
        self.poll_loop.cancel()
        if self._reconcile_task is not None and not self._reconcile_task.done():
            self._reconcile_task.cancel()

    # ------------------------------------------------------------------ entry points (called by dashboard.py)
    def handle_playback_event(self, event_type: str, data: dict):
        """Updates the shared state from a live Tracearr playback event and
        schedules a reconcile. Non-blocking."""
        session_id = data.get("sessionId")
        user_id = data.get("userId")
        if not session_id or not user_id:
            return
        user_id = _norm_id(user_id)
        self._event_seq += 1

        state = str(data.get("state") or "").lower()
        stopped = event_type == "stopped" or state == "stopped"
        paused = event_type == "paused" or state == "paused"

        if stopped or (paused and not COUNT_PAUSED_AS_WATCHING):
            self._watch_sessions.pop(session_id, None)
        else:
            self._watch_sessions[session_id] = user_id

        # Any playback activity means they're active right now. After a
        # 'stopped' they're still in the app, so they fall to Browsing
        # until the poll sees their session go quiet.
        self._active_users.add(user_id)
        self._schedule_reconcile()

    def handle_session_event(self, event_type: str, data: dict):
        """Deliberately ignored — see module docstring. Kept so
        dashboard.py's forwarding call doesn't need to change."""
        return

    # ------------------------------------------------------------------ poll
    @tasks.loop(seconds=POLL_SECONDS)
    async def poll_loop(self):
        try:
            seq_before = self._event_seq
            sessions = await self._fetch_active_sessions()
            if sessions is None:
                return  # failed fetch: never guess, leave roles as they are

            active, watching = _parse_sessions(sessions)

            if self._event_seq == seq_before:
                # No live event landed while the request was in flight —
                # the REST snapshot is the newest truth, take it whole.
                self._watch_sessions = watching
                self._active_users = active
            else:
                # A live event arrived mid-request and is newer than this
                # snapshot for whatever it touched. Keep event-driven
                # watching state; only refresh who is active.
                self._active_users = active | set(self._watch_sessions.values())

            self._polled_once = True
            await self._reload_links_if_stale()
            self._schedule_reconcile(immediate=True)
        except Exception as e:
            logger.error(f"[WatchingRole] poll failed: {type(e).__name__}: {e}", exc_info=True)

    @poll_loop.before_loop
    async def before_poll_loop(self):
        await self.bot.wait_until_ready()
        guild = self.bot.get_guild(TARGET_GUILD_ID)
        if guild is not None and not guild.chunked:
            try:
                await guild.chunk(cache=True)
                logger.info(f"[WatchingRole] Guild chunked — {len(guild.members)} members cached.")
            except Exception as e:
                logger.warning(f"[WatchingRole] Guild chunk failed: {type(e).__name__}: {e}")

    @poll_loop.error
    async def poll_loop_error(self, error: BaseException):
        # The body catches Exception itself; this only logs anything else.
        logger.error(f"[WatchingRole] poll_loop crashed: {type(error).__name__}: {error}", exc_info=error)

    async def _fetch_active_sessions(self) -> list | None:
        headers = {"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"', "accept": "application/json"}
        try:
            async with self.bot.http_session.get(
                f"{JELLYFIN_URL}/Sessions",
                headers=headers,
                params={"activeWithinSeconds": ACTIVE_WITHIN_SECONDS},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS),
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.error(f"[WatchingRole] REST /Sessions fetch failed: {type(e).__name__}: {e}")
            return None

    async def _reload_links_if_stale(self, force: bool = False):
        if not force and self._links and time.time() - self._links_loaded_at < LINKS_CACHE_SECONDS:
            return
        try:
            async with aiosqlite.connect(DB_PATH, timeout=5) as conn:
                async with conn.execute("SELECT discord_id, jellyfin_user_id FROM links") as cursor:
                    rows = await cursor.fetchall()
        except Exception as e:
            logger.error(f"[WatchingRole] DB read failed, keeping previous links: {type(e).__name__}: {e}")
            return
        links: dict[str, list[int]] = {}
        for discord_id, jf_id in rows:
            links.setdefault(_norm_id(jf_id), []).append(int(discord_id))
        self._links = links
        self._links_loaded_at = time.time()

    # ------------------------------------------------------------------ reconcile
    def _schedule_reconcile(self, immediate: bool = False):
        """Coalesces: if a reconcile is already queued or running, it just
        marks state dirty so one more pass runs after the current one."""
        if not self._polled_once:
            return  # no baseline yet — the first poll triggers the first pass
        self._dirty = True
        if self._reconcile_task is None or self._reconcile_task.done():
            self._reconcile_task = asyncio.create_task(self._reconcile_worker(0 if immediate else EVENT_COALESCE_SECONDS))

    async def _reconcile_worker(self, delay: float):
        try:
            if delay:
                await asyncio.sleep(delay)
            while self._dirty:
                self._dirty = False
                await self._reconcile()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[WatchingRole] reconcile failed: {type(e).__name__}: {e}", exc_info=True)

    async def _reconcile(self):
        guild = self.bot.get_guild(TARGET_GUILD_ID)
        if guild is None:
            logger.error(f"[WatchingRole] Guild {TARGET_GUILD_ID} not found — skipping.")
            return
        watching_role = guild.get_role(WATCHING_ROLE_ID)
        browsing_role = guild.get_role(BROWSING_ROLE_ID)
        if watching_role is None or browsing_role is None:
            logger.error("[WatchingRole] Watching or Browsing role not found in guild — skipping.")
            return

        await self._reload_links_if_stale()

        # Who SHOULD hold each role (Discord ids).
        watching_jf = set(self._watch_sessions.values())
        browsing_jf = self._active_users - watching_jf
        want_watch = {d for jf in watching_jf for d in self._links.get(jf, [])}
        want_browse = {d for jf in browsing_jf for d in self._links.get(jf, [])} - want_watch

        # Who ACTUALLY holds each role right now (member cache, no API calls).
        have_watch = {m.id for m in watching_role.members}
        have_browse = {m.id for m in browsing_role.members}

        to_check = (want_watch ^ have_watch) | (want_browse ^ have_browse)
        if not to_check:
            return

        async with self._reconcile_lock:
            edits = 0
            for discord_id in to_check:
                member = guild.get_member(discord_id)
                if member is None:
                    continue  # not in the server (or not cached) — nothing to edit
                if await self._apply_member(member, watching_role, browsing_role,
                                            discord_id in want_watch, discord_id in want_browse):
                    edits += 1
                    await asyncio.sleep(ROLE_EDIT_GAP_SECONDS)
            if edits:
                logger.info(f"[WatchingRole] Reconciled {edits} member(s) — {len(want_watch)} watching, {len(want_browse)} browsing.")

    async def _apply_member(self, member: discord.Member, watching_role: discord.Role, browsing_role: discord.Role,
                            should_watch: bool, should_browse: bool) -> bool:
        """One role edit per member covering both roles. Returns True if an
        edit was sent. A failed edit is simply retried on the next pass,
        because every pass compares against the member's real roles."""
        roles = set(member.roles)
        before = set(roles)
        (roles.add if should_watch else roles.discard)(watching_role)
        (roles.add if should_browse else roles.discard)(browsing_role)
        if roles == before:
            return False

        changes = []
        for role, label in ((watching_role, "watching"), (browsing_role, "browsing")):
            if role in roles and role not in before:
                changes.append(f"+{label}")
            elif role in before and role not in roles:
                changes.append(f"-{label}")
        try:
            await member.edit(roles=list(roles), reason="Jellyfin status sync")
            logger.debug(f"[WatchingRole] {' '.join(changes)} {member} ({member.id})")
        except (discord.Forbidden, discord.HTTPException) as e:
            logger.error(f"[WatchingRole] Failed to update roles ({' '.join(changes)}) for {member.id}: {e}")
        return True


def _norm_id(value) -> str:
    """Event payloads can carry dashed GUIDs; REST and the links DB use
    the undashed form. Normalise everywhere before comparing."""
    return str(value or "").replace("-", "").lower()


def _parse_sessions(sessions: list) -> tuple[set[str], dict[str, str]]:
    """REST /Sessions (already filtered to active ones) ->
    (active user ids, {sessionId: userId} for sessions playing now)."""
    active: set[str] = set()
    watching: dict[str, str] = {}
    for s in sessions or []:
        user_id = s.get("UserId")
        if not user_id:
            continue
        user_id = _norm_id(user_id)
        active.add(user_id)
        if not s.get("NowPlayingItem"):
            continue
        if not COUNT_PAUSED_AS_WATCHING and s.get("PlayState", {}).get("IsPaused"):
            continue
        if s.get("Id"):
            watching[s["Id"]] = user_id
    return active, watching


async def setup(bot) -> None:
    await bot.add_cog(JellyfinWatchingRole(bot))