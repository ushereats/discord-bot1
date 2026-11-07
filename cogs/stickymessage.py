"""
Sticky Message Cog
  Keeps one configured message always at the BOTTOM of a channel — the
  standard "sticky message" pattern (same idea as Carl-bot/MEE6's sticky
  messages): whenever anyone else posts in the channel, the old sticky
  message is deleted and a fresh copy is sent, so it's always the newest
  message rather than getting buried under conversation.

  /stickymessage — [Manage Server] opens a single simple panel:
    - "Set Message" button: opens a modal, paste raw JSON (any format —
      Components V2, classic embed, plain content — same JSON your
      /embed command already accepts). Parsing is NOT reimplemented here
      — this cog imports prepare_message_payload/validate_json_syntax
      directly from cogs.embed, the exact same validated pipeline /embed
      itself uses, rather than duplicating ~900 lines of Components V2
      parsing in a second file.
    - "Toggle On/Off" button: turns the sticky behavior on (posts it
      fresh) or off (deletes the current sticky message from the
      channel, keeps the saved JSON for next time).
    - A channel select: pick which channel it lives in. Defaults to
      CHANNEL_ID below on first setup; changeable any time. Switching
      channels while enabled deletes the sticky from the old channel and
      posts fresh in the new one.

  PERFORMANCE: config is cached in memory (self.config on the cog),
  loaded once in cog_load and updated in place on every write — never
  re-read from disk. This matters specifically because on_message fires
  for EVERY message the bot can see, bot-wide, across every channel —
  hitting the database on every single one just to check "is this even
  the sticky channel" would be real, avoidable overhead in any busy
  server. The only DB activity in the hot path is the eventual write of
  a new message_id after a successful repost — everything read during
  on_message's own filtering is a plain in-memory dict lookup.

  REPOSTING ORDER: the new message is sent BEFORE the old one is
  deleted, not after. If sending the new one fails (permissions pulled,
  a transient Discord error, whatever), the old message — stale but
  still something — is left untouched rather than the channel being left
  with zero sticky message and no way to notice until someone next
  happens to post there. Worst case on a delete failure afterward is a
  harmless duplicate that self-heals on the next repost; that trade is
  strictly better than ever having nothing.

  REPOSTING is leading + trailing debounced, not a flat delay on every
  message. A single isolated message reposts INSTANTLY — no artificial
  wait for the common case of one message landing in an otherwise quiet
  channel. A burst of several rapid messages collapses into that first
  instant repost plus exactly ONE trailing repost STICKY_COOLDOWN_SECONDS
  after the burst settles, so the sticky still ends up at the true
  bottom once everyone's done talking, without a delete+send pair firing
  for every single message in between — the same reasoning
  /syncmedia's SYNC_PACE_SECONDS pacing exists for elsewhere in this
  codebase: unpaced delete/send pairs in a busy channel can realistically
  blow through Discord's per-channel rate-limit bucket.

  Single-row config (id=1) — this bot isn't multi-guild-ready anywhere
  else either, so no guild_id scoping was added here for one channel's
  sticky setup.
"""

import os
import time
import asyncio
import logging
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands

from cogs.embed import prepare_message_payload, validate_json_syntax

logger = logging.getLogger("sticky_message_cog")

# --- CONFIG ---
CHANNEL_ID = 1538371135362564096  # default channel — changeable via the panel's channel select afterward

DB_DIR = "/home/container/ushereats/DATABASES"
DB_PATH = os.path.join(DB_DIR, "sticky_message.db")

# Minimum time between reposts. A rapid burst of messages collapses into
# ONE repost this long after the burst settles, rather than a delete+send
# pair firing for every single message in a busy channel.
STICKY_COOLDOWN_SECONDS = 3

# Circuit breaker — see repost_now's docstring. A hard ceiling
# independent of the debounce/leading-edge logic above, so a bug neither
# of those catches can't spam the channel indefinitely.
MAX_REPOSTS_PER_WINDOW = 8
REPOST_WINDOW_SECONDS = 15


async def _ensure_db():
    os.makedirs(DB_DIR, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sticky_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                channel_id INTEGER,
                message_json TEXT,
                message_id INTEGER,
                enabled INTEGER NOT NULL DEFAULT 0
            )
        """)
        await conn.execute(
            "INSERT OR IGNORE INTO sticky_config (id, channel_id, enabled) VALUES (1, ?, 0)",
            (CHANNEL_ID,),
        )
        await conn.commit()


async def _load_config_from_db() -> dict:
    """Only ever called once, from cog_load — every other read in this
    file goes through the cog's in-memory self.config instead."""
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM sticky_config WHERE id = 1") as cursor:
            row = await cursor.fetchone()
    return dict(row)


async def _persist_to_db(**fields):
    if not fields:
        return
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(f"UPDATE sticky_config SET {set_clause} WHERE id = 1", tuple(fields.values()))
        await conn.commit()


def _payload_to_send_kwargs(payload: dict) -> dict | None:
    """Converts prepare_message_payload's result into kwargs ready for
    channel.send(**kwargs). Returns None for an error payload — callers
    check for that before calling this."""
    kind = payload["kind"]
    if kind == "cv2":
        return {"view": payload["view"]}
    if kind == "legacy":
        return {"content": payload["content"], "embeds": payload["embeds"], "view": payload["view"]}
    if kind == "classic":
        return {"content": payload["content"], "embeds": payload["embeds"]}
    return None


class StickyMessageModal(discord.ui.Modal, title="Set Sticky Message JSON"):
    def __init__(self, callback_view: "StickyPanelView"):
        super().__init__()
        self.callback_view = callback_view
        self.json_input = discord.ui.TextInput(
            label="Message JSON",
            style=discord.TextStyle.paragraph,
            placeholder='{"content": "..."} or a full Components V2 / embed payload — same format /embed accepts',
            required=True,
            max_length=4000,
        )
        self.add_item(self.json_input)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.json_input.value.strip()

        parsed, syntax_err = validate_json_syntax(raw)
        if syntax_err:
            return await interaction.response.send_message(f"❌ {syntax_err}", ephemeral=True)

        payload = prepare_message_payload(parsed)
        if payload["kind"] == "error":
            return await interaction.response.send_message(f"❌ {payload['message']}", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        cog = self.callback_view.cog
        await cog.persist(message_json=raw)

        if cog.config["enabled"] and cog.config["channel_id"]:
            ok = await cog.repost_now(cog.config["channel_id"], reason="panel:set_message")
            note = " Reposted with the new content." if ok else " ⚠️ Saved, but the repost failed — check the channel/permissions."
        else:
            note = " Sticky isn't enabled yet — toggle it on to post this."

        await interaction.followup.send(f"✅ Sticky message content saved.{note}", ephemeral=True)
        await self.callback_view.refresh(interaction)


class StickyChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, callback_view: "StickyPanelView"):
        super().__init__(
            placeholder="Select the sticky message's channel...",
            channel_types=[discord.ChannelType.text, discord.ChannelType.news],
            row=1,
        )
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        new_channel_id = int(self.values[0].id)
        await interaction.response.defer(ephemeral=True)
        cog = self.callback_view.cog

        old_channel_id = cog.config["channel_id"]
        was_enabled = cog.config["enabled"]

        if was_enabled and old_channel_id and old_channel_id != new_channel_id:
            await cog.delete_current_sticky(old_channel_id)

        await cog.persist(channel_id=new_channel_id, message_id=None)

        note = ""
        if was_enabled and cog.config["message_json"]:
            ok = await cog.repost_now(new_channel_id, reason="panel:channel_change")
            note = " Posted there now." if ok else " ⚠️ Channel updated, but posting failed — check permissions there."

        await interaction.followup.send(f"✅ Sticky channel set to <#{new_channel_id}>.{note}", ephemeral=True)
        await self.callback_view.refresh(interaction)


class SetMessageButton(discord.ui.Button):
    def __init__(self, callback_view: "StickyPanelView"):
        super().__init__(label="📝 Set Message", style=discord.ButtonStyle.primary, row=0)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(StickyMessageModal(self.callback_view))


class ToggleButton(discord.ui.Button):
    def __init__(self, callback_view: "StickyPanelView", enabled: bool):
        super().__init__(
            label="🔴 Turn Off" if enabled else "🟢 Turn On",
            style=discord.ButtonStyle.danger if enabled else discord.ButtonStyle.success,
            row=0,
        )
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        cog = self.callback_view.cog

        if cog.config["enabled"]:
            await cog.delete_current_sticky(cog.config["channel_id"])
            await cog.persist(enabled=0)
            await interaction.followup.send("✅ Sticky message turned off and removed from the channel.", ephemeral=True)
        else:
            if not cog.config["message_json"]:
                await interaction.followup.send("❌ Set a message first — there's nothing to post yet.", ephemeral=True)
            elif not cog.config["channel_id"]:
                await interaction.followup.send("❌ Pick a channel first.", ephemeral=True)
            else:
                await cog.persist(enabled=1)
                ok = await cog.repost_now(cog.config["channel_id"], reason="panel:toggle_on")
                if ok:
                    await interaction.followup.send("✅ Sticky message turned on and posted.", ephemeral=True)
                else:
                    await interaction.followup.send("⚠️ Turned on, but the initial post failed — check channel permissions.", ephemeral=True)

        await self.callback_view.refresh(interaction)


class StickyPanelView(discord.ui.View):
    """Deliberately flat — one button row, one channel select, no
    multi-page/multi-stage navigation. Rebuilds itself after every
    action via refresh() so the Toggle button's label/color always
    matches current state without needing a second /stickymessage run."""
    def __init__(self, cog: "StickyMessage"):
        super().__init__(timeout=180.0)
        self.cog = cog

    def _build_items(self):
        self.clear_items()
        self.add_item(SetMessageButton(self))
        self.add_item(ToggleButton(self, enabled=bool(self.cog.config["enabled"])))
        self.add_item(StickyChannelSelect(self))

    def build_embed(self) -> discord.Embed:
        config = self.cog.config
        status = "🟢 ON" if config["enabled"] else "🔴 OFF"
        channel_note = f"<#{config['channel_id']}>" if config["channel_id"] else "*not set*"
        message_note = "✅ set" if config["message_json"] else "❌ not set"

        embed = discord.Embed(title="📌 Sticky Message", color=discord.Color.blurple())
        embed.add_field(name="Status", value=status, inline=True)
        embed.add_field(name="Channel", value=channel_note, inline=True)
        embed.add_field(name="Message", value=message_note, inline=True)
        embed.set_footer(text=f"Reposts automatically, collapsing bursts of activity into one update every {STICKY_COOLDOWN_SECONDS}s.")
        return embed

    async def send_initial(self, interaction: discord.Interaction):
        self._build_items()
        await interaction.followup.send(embed=self.build_embed(), view=self, ephemeral=True)

    async def refresh(self, interaction: discord.Interaction):
        self._build_items()
        await interaction.edit_original_response(embed=self.build_embed(), view=self)


class StickyMessage(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Utility"
        self.config: dict = {}  # populated in cog_load; never None afterward
        self._repost_task: asyncio.Task | None = None
        self._repost_lock = asyncio.Lock()
        self._last_repost_at: float = 0.0  # 0 = "never" — always treated as long enough ago
        self._leading_repost_pending: bool = False  # set synchronously the instant a leading repost is decided on
        self._recent_repost_times: list[float] = []  # circuit breaker — see repost_now
        self._repost_in_flight: bool = False  # see on_message's comment — closes the gateway/REST ordering race

    async def cog_load(self):
        await _ensure_db()
        self.config = await _load_config_from_db()

    async def cog_unload(self):
        if self._repost_task is not None:
            self._repost_task.cancel()

    async def persist(self, **fields):
        """The only place DB writes happen. Updates the in-memory cache
        AND the database together, so every other read in this cog can
        trust self.config without ever touching disk itself."""
        await _persist_to_db(**fields)
        self.config.update(fields)

    async def delete_current_sticky(self, channel_id: int):
        message_id = self.config.get("message_id")
        if not message_id:
            return
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            return
        try:
            # get_partial_message deletes by ID directly — one API call,
            # not a fetch_message() + delete() pair. We never use
            # anything from the fetched message object except its
            # ability to be deleted, so fetching the full message first
            # was pure wasted overhead on every single deletion.
            await channel.get_partial_message(message_id).delete()
        except discord.NotFound:
            pass
        except discord.HTTPException as e:
            logger.warning(f"[StickyMessage] Failed to delete old sticky message {message_id}: {type(e).__name__}: {e}")
        await self.persist(message_id=None)

    async def repost_now(self, channel_id: int, reason: str = "unknown") -> bool:
        """Deletes the current sticky message (if any) and posts a fresh
        copy — used by both the debounced on_message trigger and the
        panel's own actions (Set Message, Toggle On, channel change).

        Sends the NEW message before deleting the OLD one, deliberately
        — if the send fails, the old message (stale but present) is left
        untouched instead of the channel being left with nothing at all.
        A delete failure AFTER a successful send just leaves a harmless
        duplicate that the next repost cleans up on its own.

        reason is diagnostic-only (logged, nothing else reads it) — pass
        a short label ("leading", "trailing", "panel:toggle_on", etc.) so
        the logs show WHAT triggered each repost, not just that one
        happened. Added specifically to debug reports of continuous
        reposting with no clear cause — this cog previously logged
        nothing at all on a successful repost, only on failures, making
        it impossible to tell from the logs alone whether reposts were
        coming from this process's own normal operation or from
        something else entirely (e.g. a second bot process running
        against the same token).

        CIRCUIT BREAKER: if this fires more than MAX_REPOSTS_PER_WINDOW
        times within REPOST_WINDOW_SECONDS, something is wrong — a bug
        neither the leading-edge race fix nor the debounce caught, a
        feedback loop with another bot, whatever — and it stops posting
        entirely rather than trusting the debounce logic alone to be the
        only thing standing between normal operation and spamming the
        channel forever. Logged at ERROR so it reaches the error channel;
        clears itself once REPOST_WINDOW_SECONDS passes with no more
        reposts."""
        now = time.time()
        self._recent_repost_times = [t for t in self._recent_repost_times if now - t < REPOST_WINDOW_SECONDS]
        if len(self._recent_repost_times) >= MAX_REPOSTS_PER_WINDOW:
            logger.error(
                f"[StickyMessage] Circuit breaker tripped — {len(self._recent_repost_times)} reposts in the last "
                f"{REPOST_WINDOW_SECONDS}s. Something is wrong (runaway loop?). Refusing to repost until this settles. "
                f"(triggering reason: {reason})"
            )
            return False
        self._recent_repost_times.append(now)

        async with self._repost_lock:
            self._repost_in_flight = True
            try:
                channel = self.bot.get_channel(channel_id)
                if channel is None:
                    logger.error(f"[StickyMessage] Channel {channel_id} not found — can't post sticky message. (reason: {reason})")
                    return False

                parsed, err = validate_json_syntax(self.config["message_json"])
                if err:
                    logger.error(f"[StickyMessage] Stored JSON became invalid: {err} (reason: {reason})")
                    return False
                payload = prepare_message_payload(parsed)
                if payload["kind"] == "error":
                    logger.error(f"[StickyMessage] Stored payload failed to build: {payload['message']} (reason: {reason})")
                    return False
                send_kwargs = _payload_to_send_kwargs(payload)

                old_id = self.config.get("message_id")

                try:
                    new_msg = await channel.send(**send_kwargs)
                except discord.HTTPException as e:
                    logger.error(f"[StickyMessage] Failed to post sticky message — leaving the previous one (if any) in place: {type(e).__name__}: {e} (reason: {reason})")
                    return False

                # Updated synchronously, immediately — no await between
                # this line and send() returning. This closes HALF the
                # race (our own code seeing the update in time); the
                # OTHER half — the gateway delivering MESSAGE_CREATE for
                # this exact message before send()'s REST response even
                # returns to us, which is possible since gateway and REST
                # are independent connections with no ordering guarantee
                # between them — is what _repost_in_flight above exists
                # to close (confirmed in production: on_message fired for
                # our own brand-new repost using the OLD message_id,
                # because the gateway event beat the REST response back).
                self.config["message_id"] = new_msg.id
                self._last_repost_at = time.time()

                logger.info(
                    f"[StickyMessage] Reposted — reason={reason}, channel={channel_id}, old_message={old_id}, "
                    f"new_message={new_msg.id}, process_id={os.getpid()}."
                )

                if old_id:
                    try:
                        # get_partial_message deletes by ID directly —
                        # one API call instead of fetch_message() +
                        # delete(). Fires on every single repost, so this
                        # halves the delete-side network cost across the
                        # cog's entire hot path.
                        await channel.get_partial_message(old_id).delete()
                    except discord.NotFound:
                        pass
                    except discord.HTTPException as e:
                        logger.warning(
                            f"[StickyMessage] New sticky posted, but failed to delete the old one ({old_id}) — "
                            f"it'll sit as a harmless duplicate until the next repost: {type(e).__name__}: {e}"
                        )

                await _persist_to_db(message_id=new_msg.id)
                return True
            finally:
                self._repost_in_flight = False

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        config = self.config

        # Excludes the sticky's own repost — but NOT by exact message_id
        # alone, since that's racy: Discord's gateway (which delivers
        # on_message) and the REST API (channel.send()'s HTTP response)
        # are independent connections with no ordering guarantee between
        # them. It's possible for the gateway to deliver MESSAGE_CREATE
        # for a message we JUST sent before that same send() call's
        # response even reaches us — meaning self.config["message_id"]
        # hasn't been updated to the new id yet when this check runs
        # (confirmed in production: this was the actual cause of
        # continuous spam — on_message firing for the sticky's own fresh
        # repost, treating it as a normal message, reposting again, which
        # then triggered the same race again, forever). _repost_in_flight
        # is set BEFORE send() is even called, so it's already true
        # during that entire window regardless of which network path
        # delivers first — closing the race the ID-only check couldn't.
        if message.author.id == self.bot.user.id:
            if self._repost_in_flight or message.id == config.get("message_id"):
                return

        # Every check here is a plain in-memory dict lookup — no database
        # access at all for the overwhelming majority of messages this
        # fires for (wrong channel, or sticky not enabled here).
        if not config["enabled"] or not config["channel_id"] or not config["message_json"]:
            return
        if message.channel.id != config["channel_id"]:
            return

        # Leading + trailing debounce: a single isolated message reposts
        # INSTANTLY — no artificial 3s wait for the common case of one
        # message landing in an otherwise quiet channel. A burst of
        # several messages arriving close together collapses into that
        # first instant repost PLUS exactly one trailing repost after the
        # burst settles, so the sticky still ends up back at the true
        # bottom once everyone's done talking — without a delete+send
        # pair firing for every single message in between, which is
        # exactly the rate-limit exposure this whole cooldown exists to
        # avoid (same reasoning /syncmedia's SYNC_PACE_SECONDS pacing
        # exists for elsewhere in this codebase).
        #
        # _leading_repost_pending is set SYNCHRONOUSLY, before any await
        # — this is what actually closes the race. _last_repost_at and
        # _repost_task only get updated once the network calls inside
        # repost_now() finish, which is too late: two messages arriving
        # within milliseconds of each other could otherwise both see
        # "nothing in progress" and both take the leading-edge path
        # concurrently (confirmed in production — this was firing
        # multiple simultaneous instant reposts). The flag closes that
        # window completely, since setting a plain bool never yields to
        # the event loop.
        trailing_pending = self._repost_task is not None and not self._repost_task.done()
        long_enough_since_last_repost = (time.time() - self._last_repost_at) >= STICKY_COOLDOWN_SECONDS

        logger.info(
            f"[StickyMessage] on_message trigger — msg_id={message.id}, author={message.author.id} "
            f"({'BOT' if message.author.bot else 'human'}), trailing_pending={trailing_pending}, "
            f"long_enough_since_last={long_enough_since_last_repost}, process_id={os.getpid()}."
        )

        if not trailing_pending and not self._leading_repost_pending and long_enough_since_last_repost:
            self._leading_repost_pending = True
            asyncio.create_task(self._leading_repost(config["channel_id"]))
        else:
            if trailing_pending:
                self._repost_task.cancel()
            self._repost_task = asyncio.create_task(self._debounced_repost(config["channel_id"]))

    async def _leading_repost(self, channel_id: int):
        try:
            await self.repost_now(channel_id, reason="leading")
        finally:
            self._leading_repost_pending = False

    async def _debounced_repost(self, channel_id: int):
        try:
            await asyncio.sleep(STICKY_COOLDOWN_SECONDS)
        except asyncio.CancelledError:
            return  # superseded by a newer message within the cooldown window
        await self.repost_now(channel_id, reason="trailing")

    @app_commands.command(name="stickymessage", description="[Manage Server] Set up a message that always stays at the bottom of a channel")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def stickymessage(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        view = StickyPanelView(self)
        await view.send_initial(interaction)

    @stickymessage.error
    async def stickymessage_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            msg = "❌ You need the **Manage Server** permission to use this."
        else:
            msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(StickyMessage(bot))