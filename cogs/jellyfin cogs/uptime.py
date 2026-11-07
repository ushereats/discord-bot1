"""
Jellyfin Uptime Cog
  Polls HEALTH_URL every CHECK_INTERVAL_SECONDS (every DOWN_CHECK_INTERVAL_SECONDS
  while Jellyfin is down, so recovery is noticed within seconds). A failed
  check is confirmed with one re-check (CONFIRM_DELAY_SECONDS later) before
  being trusted, and confirmed failures are further checked against
  CANARY_URL to rule out this container's own network being down rather
  than Jellyfin itself.

  HEALTHY means: /health answered 200 with Jellyfin's own health text
  ("Healthy" or "Degraded"). Anything else counts as a failure — including
  a 404 / 401 / reverse-proxy error page, which used to be treated as up.

  INCIDENT GROUPING: the first DOWN of a fresh incident posts a message —
  that message is never touched again for the rest of the incident. Its
  matching UP posts its OWN separate message. If a FURTHER outage lands
  within FLAP_WINDOW_SECONDS of that UP, the UP message is DELETED (it was
  a false recovery) and nothing new is posted for that down. Whenever it
  next recovers, a new UP message posts showing the COMBINED downtime
  across every drop-out so far. At most 2 messages are ever visible for an
  entire flapping incident. Once FLAP_WINDOW_SECONDS passes with no further
  transition, the incident closes quietly.

  DOWNTIME is measured from the FIRST failed check (not from when the
  failure was confirmed) to the check that saw it healthy again.

  PERSISTENCE: incident state lives in STATE_FILE, written atomically (temp
  file + rename), so a crash mid-write can never leave a corrupt file that
  makes the cog forget an open incident and post a duplicate DOWN.
"""

import aiohttp
import asyncio
import time
import json
import os
import logging
import discord
from discord.ext import commands, tasks

from .utils.config import JELLYFIN_URL, UPTIME_CHANNEL_ID as CHANNEL_ID

logger = logging.getLogger("jellyfin_uptime_cog")

CHECK_URL = JELLYFIN_URL
# /health checks HTTP AND database connectivity — catches "up but broken"
# (locked/corrupt DB, etc.) that a plain root-URL check would miss.
HEALTH_URL = f"{CHECK_URL}/health"
HEALTHY_BODIES = {"healthy", "degraded"}
# Used only to tell "Jellyfin is down" apart from "this container's own
# network is down".
CANARY_URL = "https://discord.com"

CHECK_INTERVAL_SECONDS = 60
DOWN_CHECK_INTERVAL_SECONDS = 10  # faster while down, so recovery time is accurate
STATE_FILE = "/home/container/ushereats/DATABASES/uptime_state.json"

CONFIRM_DELAY_SECONDS = 5
CHECK_TIMEOUT_SECONDS = 15
CANARY_TIMEOUT_SECONDS = 10

# How long after the LAST transition a further transition still counts as
# the same flapping incident. Resets on every transition.
FLAP_WINDOW_SECONDS = 600


def _load_state() -> dict:
    """Defaults cover a missing, corrupt, or older state file — every field
    this cog reads is guaranteed present after this call."""
    defaults = {
        "status": "unknown",
        "down_since": None,
        "last_reason": None,
        "incident_started_at": None,
        "incident_down_message_id": None,
        "incident_pending_up_message_id": None,
        "incident_flap_count": 0,
        "incident_total_downtime_seconds": 0.0,
        "incident_last_transition_at": None,
    }
    if not os.path.exists(STATE_FILE):
        return defaults
    try:
        with open(STATE_FILE, "r") as f:
            defaults.update(json.load(f))
    except Exception as e:
        logger.error(f"[JellyfinUptime] State file unreadable, using defaults: {type(e).__name__}: {e}")
    return defaults


def _save_state(state: dict):
    """Atomic: write a temp file, then rename it over the real one. A crash
    at any point leaves either the old file or the new file, never half of
    one."""
    tmp = f"{STATE_FILE}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m {secs}s" if secs else f"{minutes}m"


def _describe_error(e: Exception) -> str:
    if isinstance(e, asyncio.TimeoutError):
        return f"Timed out after {CHECK_TIMEOUT_SECONDS}s"
    if isinstance(e, aiohttp.ClientConnectorError):
        return "Can't connect"
    if isinstance(e, aiohttp.ClientError):
        return f"Connection error ({type(e).__name__})"
    return type(e).__name__


class JellyfinUptime(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.session: aiohttp.ClientSession | None = None

    async def cog_load(self):
        # Shared across the whole bot process — created in main.py's
        # setup_hook. Set BEFORE the loop starts, so the first check never
        # runs without a session.
        self.session = self.bot.http_session
        if not self.check_loop.is_running():
            self.check_loop.start()

    async def cog_unload(self):
        self.check_loop.cancel()

    # --- checks ---

    async def _check_reachable(self) -> tuple[bool, str | None]:
        try:
            async with self.session.get(HEALTH_URL, timeout=aiohttp.ClientTimeout(total=CHECK_TIMEOUT_SECONDS)) as resp:
                body = (await resp.text())[:200].strip()
                if resp.status == 200 and body.lower() in HEALTHY_BODIES:
                    return True, None
                reason = f"HTTP {resp.status} {resp.reason or ''}".strip()
                if resp.status == 200:
                    reason = f"Health check reported '{body[:40]}'" if body else "Unexpected health response"
                return False, reason
        except Exception as e:
            return False, _describe_error(e)

    async def _check_canary(self) -> bool:
        try:
            async with self.session.get(CANARY_URL, timeout=aiohttp.ClientTimeout(total=CANARY_TIMEOUT_SECONDS)) as resp:
                return resp.status < 500
        except Exception:
            return False

    # --- cards ---

    @staticmethod
    def _card(header: str, body: str) -> discord.ui.LayoutView:
        container = discord.ui.Container()
        container.add_item(discord.ui.TextDisplay(header))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay(body))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay(f"-# Service: {CHECK_URL}"))
        view = discord.ui.LayoutView(timeout=None)
        view.add_item(container)
        return view

    def _down_card(self, detected_at: float, down_since: float, reason: str | None) -> discord.ui.LayoutView:
        detected, since = int(detected_at), int(down_since)
        return self._card(
            f"### 🔴 Jellyfin is down\n-# Detected <t:{detected}:R> · <t:{detected}:f>",
            f"**Reason** · `{reason or 'Unknown'}`\n"
            f"**Down since** · <t:{since}:t>\n"
            f"-# Checking every {DOWN_CHECK_INTERVAL_SECONDS}s · a message posts here the moment it's back",
        )

    def _up_card(self, recovered_at: float, started_at: float, flap_count: int, total_downtime: float) -> discord.ui.LayoutView:
        recovered, started = int(recovered_at), int(started_at)
        header = f"### 🟢 Jellyfin is back up\n-# Recovered <t:{recovered}:R> · <t:{recovered}:f>"
        if flap_count > 1:
            body = (
                f"**Downtime** · {_format_duration(total_downtime)} total\n"
                f"**Dropped out** · {flap_count} times since <t:{started}:t>\n"
                "-# It was unstable for a while, so this one card covers the whole incident"
            )
        else:
            body = (
                f"**Downtime** · {_format_duration(total_downtime)}\n"
                f"**Outage** · <t:{started}:t> → <t:{recovered}:t>"
            )
        return self._card(header, body)

    async def _post(self, view: discord.ui.LayoutView) -> int | None:
        channel = self.bot.get_channel(CHANNEL_ID)
        if channel is None:
            logger.error(f"[JellyfinUptime] CHANNEL_ID {CHANNEL_ID} not found.")
            return None
        try:
            msg = await channel.send(view=view)
            return msg.id
        except discord.HTTPException as e:
            logger.error(f"[JellyfinUptime] Failed to post incident message: {type(e).__name__}: {e}")
            return None

    async def _delete_message(self, message_id: int):
        channel = self.bot.get_channel(CHANNEL_ID)
        if channel is None:
            return
        try:
            await channel.get_partial_message(message_id).delete()
        except discord.NotFound:
            pass
        except discord.HTTPException as e:
            logger.warning(f"[JellyfinUptime] Failed to delete superseded UP message {message_id}: {type(e).__name__}: {e}")

    # --- transitions ---

    async def _handle_down_transition(self, state: dict, first_failed_at: float, now: float, reason: str | None):
        last_transition = state.get("incident_last_transition_at")
        window_open = (
            state.get("incident_down_message_id") is not None
            and last_transition is not None
            and (now - last_transition) <= FLAP_WINDOW_SECONDS
        )

        if window_open:
            # Dropped out again — the previous UP was a false recovery.
            pending_up_id = state.get("incident_pending_up_message_id")
            if pending_up_id is not None:
                await self._delete_message(pending_up_id)
            state["incident_pending_up_message_id"] = None
            state["incident_flap_count"] = state.get("incident_flap_count", 1) + 1
        else:
            state["incident_flap_count"] = 1
            state["incident_total_downtime_seconds"] = 0.0
            state["incident_pending_up_message_id"] = None
            state["incident_started_at"] = first_failed_at
            state["incident_down_message_id"] = await self._post(self._down_card(now, first_failed_at, reason))

        state["incident_last_transition_at"] = now
        state["down_since"] = first_failed_at
        state["last_reason"] = reason
        state["status"] = "down"
        self.check_loop.change_interval(seconds=DOWN_CHECK_INTERVAL_SECONDS)

        logger.warning(
            f"[JellyfinUptime] DOWN confirmed ({reason}) — flap_count={state['incident_flap_count']}, "
            f"down_message={state['incident_down_message_id']}."
        )

    async def _handle_up_transition(self, state: dict, now: float):
        down_since = state.get("down_since")
        segment_seconds = (now - down_since) if down_since else 0.0
        state["incident_total_downtime_seconds"] = state.get("incident_total_downtime_seconds", 0.0) + segment_seconds
        started_at = state.get("incident_started_at") or down_since or now

        state["incident_pending_up_message_id"] = await self._post(self._up_card(
            now, started_at, state.get("incident_flap_count", 1), state["incident_total_downtime_seconds"],
        ))
        state["incident_last_transition_at"] = now
        state["down_since"] = None
        state["status"] = "up"
        self.check_loop.change_interval(seconds=CHECK_INTERVAL_SECONDS)

        logger.info(
            f"[JellyfinUptime] UP confirmed — segment {_format_duration(segment_seconds)}, "
            f"incident total {_format_duration(state['incident_total_downtime_seconds'])}, "
            f"flap_count={state.get('incident_flap_count', 1)}."
        )

    def _maybe_close_incident(self, state: dict, now: float):
        last_transition = state.get("incident_last_transition_at")
        if state.get("incident_down_message_id") is not None and last_transition is not None:
            if (now - last_transition) > FLAP_WINDOW_SECONDS:
                logger.info(f"[JellyfinUptime] Incident window expired (flap_count={state.get('incident_flap_count')}) — closing.")
                state["incident_down_message_id"] = None
                state["incident_pending_up_message_id"] = None
                state["incident_flap_count"] = 0
                state["incident_total_downtime_seconds"] = 0.0
                state["incident_last_transition_at"] = None
                state["incident_started_at"] = None

    # --- loop ---

    @tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
    async def check_loop(self):
        # Caught here, inside the loop: an exception escaping a tasks.loop
        # stops it for good (restarting from @loop.error doesn't work —
        # it runs inside the loop's own task, so is_running() is still
        # True there). A failed tick is logged and the next one runs.
        try:
            await self._tick()
        except Exception as e:
            logger.error(f"[JellyfinUptime] Check failed: {type(e).__name__}: {e}", exc_info=True)

    async def _tick(self):
        state = _load_state()
        if state["status"] == "down" and self.check_loop.seconds != DOWN_CHECK_INTERVAL_SECONDS:
            self.check_loop.change_interval(seconds=DOWN_CHECK_INTERVAL_SECONDS)  # restarted mid-outage

        first_check_at = time.time()
        is_up, fail_reason = await self._check_reachable()

        if is_up:
            if state["status"] == "down":
                await self._handle_up_transition(state, first_check_at)
            else:
                self._maybe_close_incident(state, first_check_at)
                state["status"] = "up"
            _save_state(state)
            return

        if state["status"] == "down":
            return  # still down — nothing to post, keep checking

        logger.warning(f"[JellyfinUptime] check failed, confirming in {CONFIRM_DELAY_SECONDS}s: {fail_reason}")
        await asyncio.sleep(CONFIRM_DELAY_SECONDS)
        is_up_confirm, confirm_reason = await self._check_reachable()
        now = time.time()

        if is_up_confirm:
            logger.info("[JellyfinUptime] confirm check succeeded — false alarm, not marking down.")
            self._maybe_close_incident(state, now)
            state["status"] = "up"
            _save_state(state)
            return

        if not await self._check_canary():
            logger.warning(
                "[JellyfinUptime] Both checks failed AND canary is unreachable — looks like OUR network, "
                "not Jellyfin. Skipping this tick without changing status."
            )
            return

        logger.warning(f"[JellyfinUptime] confirm check also failed: {confirm_reason}")
        # Downtime starts at the FIRST failed check, not the confirmation.
        await self._handle_down_transition(state, first_check_at, now, confirm_reason or fail_reason)
        _save_state(state)

    @check_loop.before_loop
    async def before_check_loop(self):
        await self.bot.wait_until_ready()


async def setup(bot) -> None:
    await bot.add_cog(JellyfinUptime(bot))