"""
Poll Suggestions Cog
  /poll — [Owner] Starts a new suggestion round in POLL_CHANNEL_ID. Posts
    a Components V2 container with 10 numbered slots and two buttons:
      - Suggest (anyone, unlimited people, one suggestion per person) —
        opens a modal asking for title + year. Disappears once all 10
        slots are filled; nothing left to put an 11th suggestion into.
      - Start Poll (owner only, always present regardless of slot count)
        — converts whatever's currently in the slots into a real native
        Discord poll.

  /pollcancel — [Owner] Wipes whatever's currently active. During the
    suggestion phase: edits the message to a cancelled state, releases
    the one-round-at-a-time guard. During the poll phase: deletes the
    poll message outright — per Discord's own Polls FAQ, deleting the
    message is the ONLY way to delete a poll; there is no other API for
    it, and a poll can't be edited or its options changed once created.

  WHY TWO MESSAGES, NOT ONE EDITED IN PLACE: confirmed against Discord's
  own docs — a poll can only be attached to a message at the moment it's
  first SENT, never via an edit to an existing message. So starting the
  poll edits the suggestion message to a closed/final state ("suggestions
  closed, poll started below") and sends the actual native poll as a
  brand-new message right after it, rather than literally replacing the
  suggestion container's content with a poll object (which the API does
  not support).

  AUTO-FINALIZATION: a background task loop (_check_expired_polls) runs
  every CHECK_INTERVAL_SECONDS and looks for any round in 'polling' phase
  whose poll has passed its expiry. When found:
    1. Fetch the poll message fresh (never trust a cached object for vote
       counts — Discord updates these live and there's no push event for
       "poll ended", only the expiry timestamp itself).
    2. Read final answer_counts, sort descending, mark top vote count as
       winner(s) (ties handled — all top-count answers get marked).
    3. Delete the poll message. Per Discord's own Polls FAQ this is the
       ONLY way to remove a poll; there is no other API for it, ended or
       not.
    4. Re-edit the ORIGINAL suggestion message (same message_id used for
       every prior state) to a two-container view: container 1 = header
       swapped to "poll ended", container 2 = results list. No new
       message is sent for this step.
    5. Mark the round phase 'finished' in the DB. Since
       _get_active_round() only matches ('suggesting', 'polling'), this
       alone frees up /poll for a new round — no /pollcancel needed.
  If the poll message was deleted out-of-band (manually, channel gone,
  etc.) before the loop caught the natural expiry, that 404 is treated
  as "already ended" and the round is finalized with no results rather
  than left stuck in 'polling' forever.

  ONE ROUND AT A TIME: enforced by checking for any poll_rounds row with
  phase IN ('suggesting', 'polling') before /poll will start a new one.

  PERSISTENCE / RESTART SAFETY: suggestions and round state live in
  poll.db (aiosqlite). Button dispatch is registered once via a
  persistent LayoutView in cog_load with fixed custom_ids — every button
  press looks up the CURRENT active round fresh from the database rather
  than trusting anything captured at registration time, so this works
  correctly on any message regardless of how long ago the bot restarted.
  The expiry-check loop provides the same restart safety for poll
  finalization: it re-queries 'polling' rounds from the DB on every tick
  rather than tracking anything in memory, so a bot restart mid-poll
  just resumes checking on the next tick.

Config: set POLL_CHANNEL_ID and OWNER_DISCORD_ID below.
"""

import os
import logging
import asyncio
from datetime import timedelta, datetime, timezone
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks

logger = logging.getLogger("poll_cog")

POLL_CHANNEL_ID = 1540678374861832213
OWNER_DISCORD_ID = 723417665087078451

MAX_SUGGESTIONS = 10
POLL_DURATION_HOURS = 12
POLL_ANSWER_MAX_CHARS = 55  # Discord's own cap on a single poll answer's text
CHECK_INTERVAL_SECONDS = 60  # how often the expiry-check loop runs

DB_DIR = "/home/container/ushereats/DATABASES"
DB_PATH = os.path.join(DB_DIR, "poll.db")

RESULT_MEDALS = ["🥇", "🥈", "🥉"]


# --- DATABASE ---

async def _ensure_db():
    os.makedirs(DB_DIR, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS poll_rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                phase TEXT NOT NULL DEFAULT 'suggesting',
                poll_message_id INTEGER,
                poll_expires_at TEXT,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS poll_suggestions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id INTEGER NOT NULL,
                discord_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                year TEXT NOT NULL,
                title_normalized TEXT NOT NULL,
                slot_number INTEGER NOT NULL,
                submitted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(round_id, discord_id),
                UNIQUE(round_id, title_normalized, year),
                UNIQUE(round_id, slot_number)
            )
        """)
        # Backfill for DBs created before poll_expires_at existed.
        try:
            await conn.execute("ALTER TABLE poll_rounds ADD COLUMN poll_expires_at TEXT")
        except aiosqlite.OperationalError:
            pass
        await conn.commit()


async def _get_active_round() -> dict | None:
    """The one round currently in 'suggesting' or 'polling' phase, if
    any — this IS the one-round-at-a-time guard. Every button callback
    re-fetches this fresh rather than trusting anything captured at
    message-build time, so behavior is always correct regardless of
    restarts or how stale the rendered message might be."""
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM poll_rounds WHERE phase IN ('suggesting', 'polling') ORDER BY id DESC LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()
    return dict(row) if row else None


async def _get_polling_rounds() -> list[dict]:
    """All rounds currently in 'polling' phase — used by the expiry-check
    loop. In normal operation there's only ever one (enforced by
    _get_active_round's guard), but the loop checks all of them anyway
    rather than assuming, since it queries the DB fresh every tick."""
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM poll_rounds WHERE phase = 'polling'") as cursor:
            rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _get_round(round_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM poll_rounds WHERE id = ?", (round_id,)) as cursor:
            row = await cursor.fetchone()
    return dict(row) if row else None


async def _create_round(channel_id: int, message_id: int, created_by: int) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "INSERT INTO poll_rounds (channel_id, message_id, created_by) VALUES (?, ?, ?)",
            (channel_id, message_id, created_by),
        )
        await conn.commit()
        return cursor.lastrowid


async def _set_round_phase(round_id: int, phase: str, poll_message_id: int | None = None,
                            poll_expires_at: str | None = None):
    async with aiosqlite.connect(DB_PATH) as conn:
        if poll_message_id is not None:
            await conn.execute(
                "UPDATE poll_rounds SET phase = ?, poll_message_id = ?, poll_expires_at = ? WHERE id = ?",
                (phase, poll_message_id, poll_expires_at, round_id),
            )
        else:
            await conn.execute("UPDATE poll_rounds SET phase = ? WHERE id = ?", (phase, round_id))
        await conn.commit()


async def _get_suggestions(round_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM poll_suggestions WHERE round_id = ? ORDER BY slot_number ASC", (round_id,)
        ) as cursor:
            rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _add_suggestion(round_id: int, discord_id: int, title: str, year: str) -> tuple[bool, str | None, int | None]:
    """Returns (success, error_message, slot_number). Checks are done
    explicitly (not just relying on the UNIQUE constraints catching an
    IntegrityError) so the error message can be specific and accurate —
    the UNIQUE constraints on the table still exist as a final defense-in-
    depth safety net in case of a race, caught separately below."""
    normalized = title.strip().lower()

    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row

        async with conn.execute(
            "SELECT 1 FROM poll_suggestions WHERE round_id = ? AND discord_id = ?", (round_id, discord_id)
        ) as cursor:
            if await cursor.fetchone():
                return False, "You've already made a suggestion for this round.", None

        async with conn.execute(
            "SELECT 1 FROM poll_suggestions WHERE round_id = ? AND title_normalized = ? AND year = ?",
            (round_id, normalized, year),
        ) as cursor:
            if await cursor.fetchone():
                return False, f"**{title} ({year})** has already been suggested.", None

        async with conn.execute("SELECT COUNT(*) FROM poll_suggestions WHERE round_id = ?", (round_id,)) as cursor:
            count = (await cursor.fetchone())[0]
        if count >= MAX_SUGGESTIONS:
            return False, "All 10 suggestion slots are full.", None

        slot = count + 1
        try:
            await conn.execute(
                "INSERT INTO poll_suggestions (round_id, discord_id, title, year, title_normalized, slot_number) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (round_id, discord_id, title, year, normalized, slot),
            )
            await conn.commit()
        except aiosqlite.IntegrityError:
            # Final safety net if two submissions raced past the checks
            # above (the asyncio.Lock in the cog should already prevent
            # this in practice) — never silently corrupt slot numbering.
            return False, "That conflicted with another submission — please try again.", None

    return True, None, slot


# --- COMPONENTS V2 BUILDERS ---

def _build_suggestions_container(round_row: dict, suggestions: list[dict]) -> discord.ui.Container:
    container = discord.ui.Container()
    container.add_item(discord.ui.TextDisplay("-# JELLYFIN DOWNLOAD SUGGESTIONS\n# 🎬 What should we add?"))
    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

    by_slot = {s["slot_number"]: s for s in suggestions}
    lines = []
    for slot in range(1, MAX_SUGGESTIONS + 1):
        s = by_slot.get(slot)
        if s:
            lines.append(f"**{slot}.** {s['title']} ({s['year']}) — suggested by <@{s['discord_id']}>")
        else:
            lines.append(f"**{slot}.** *(empty)*")
    container.add_item(discord.ui.TextDisplay("\n".join(lines)))

    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
    container.add_item(discord.ui.TextDisplay(
        f"-# {len(suggestions)}/{MAX_SUGGESTIONS} slots filled · one suggestion per person\n"
        "-# The owner can start the poll at any time, regardless of slot count."
    ))

    container.add_item(PollActionRow(round_row["id"], len(suggestions)))
    return container


def _build_suggestions_view(round_row: dict, suggestions: list[dict]) -> discord.ui.LayoutView:
    view = discord.ui.LayoutView(timeout=None)
    view.add_item(_build_suggestions_container(round_row, suggestions))
    return view


def _build_simple_message_view(text: str) -> discord.ui.LayoutView:
    """Used for the closed/cancelled end states — same Container>TextDisplay
    shape as everything else in this bot, just no buttons."""
    view = discord.ui.LayoutView(timeout=None)
    container = discord.ui.Container()
    container.add_item(discord.ui.TextDisplay(text))
    view.add_item(container)
    return view


def _build_finished_view(round_row: dict, results: list[dict] | None, total_votes: int) -> discord.ui.LayoutView:
    """Two-container final state: header container says 'poll ended',
    second container shows results sorted highest-to-lowest votes with
    the winner(s) marked. `results` is a list of {"text": str, "count": int}
    already sorted descending, or None/empty if there was nothing to show
    (e.g. poll message vanished before the loop could read it)."""
    view = discord.ui.LayoutView(timeout=None)

    header = discord.ui.Container()
    header.add_item(discord.ui.TextDisplay(
        "-# JELLYFIN DOWNLOAD SUGGESTIONS\n# ✅ Suggestions closed — poll ended"
    ))
    view.add_item(header)

    results_container = discord.ui.Container()
    results_container.add_item(discord.ui.TextDisplay("# 🏆 Results"))
    results_container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

    if not results:
        results_container.add_item(discord.ui.TextDisplay("*No votes were recorded for this poll.*"))
    else:
        top_count = results[0]["count"]
        lines = []
        rank_counter = 0
        for r in results:
            is_winner = r["count"] == top_count and top_count > 0
            plural = "" if r["count"] == 1 else "s"
            if is_winner:
                lines.append(f"**🥇 {r['text']}** — {r['count']} vote{plural}")
            else:
                rank_counter += 1
                lines.append(f"**{rank_counter + 1}.** {r['text']} — {r['count']} vote{plural}")
        results_container.add_item(discord.ui.TextDisplay("\n".join(lines)))

    results_container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
    results_container.add_item(discord.ui.TextDisplay(
        f"-# Round #{round_row['id']} · Poll ran {POLL_DURATION_HOURS}h · {total_votes} total vote"
        f"{'' if total_votes == 1 else 's'} cast"
    ))
    view.add_item(results_container)

    return view


# --- MODAL ---

class SuggestModal(discord.ui.Modal, title="Suggest something to add"):
    def __init__(self, round_id: int):
        super().__init__()
        self.round_id = round_id
        self.title_input = discord.ui.TextInput(
            label="Movie / Show Name", required=True, max_length=100, placeholder="e.g. Dune: Part Two"
        )
        self.year_input = discord.ui.TextInput(
            label="Year", required=True, max_length=4, placeholder="e.g. 2024"
        )
        self.add_item(self.title_input)
        self.add_item(self.year_input)

    async def on_submit(self, interaction: discord.Interaction):
        title = self.title_input.value.strip()
        year_raw = self.year_input.value.strip()

        if not year_raw.isdigit() or not (1888 <= int(year_raw) <= 2100):
            return await interaction.response.send_message("❌ Enter a valid 4-digit year.", ephemeral=True)
        year = year_raw

        round_row = await _get_round(self.round_id)
        if round_row is None or round_row["phase"] != "suggesting":
            return await interaction.response.send_message("❌ Suggestions are closed for this round.", ephemeral=True)

        cog = interaction.client.get_cog("PollSuggestions")
        if cog is None:
            return await interaction.response.send_message("❌ Poll cog not loaded.", ephemeral=True)

        async with cog.suggestion_lock:
            ok, error, slot = await _add_suggestion(self.round_id, interaction.user.id, title, year)

        if not ok:
            return await interaction.response.send_message(f"❌ {error}", ephemeral=True)

        await interaction.response.send_message(f"✅ Added **{title} ({year})** as suggestion #{slot}.", ephemeral=True)

        fresh_round = await _get_round(self.round_id)
        await cog.refresh_suggestions_message(fresh_round)


# --- PERSISTENT BUTTON ROW ---

class PollActionRow(discord.ui.ActionRow):
    """Fixed custom_ids for both buttons so they keep working on old
    messages after a bot restart — registered once via a persistent
    LayoutView in cog_load. Every callback re-fetches the ACTIVE round
    fresh rather than trusting round_id captured at construction time,
    since the message this instance renders on may be stale relative to
    the database by the time someone actually clicks."""

    def __init__(self, round_id: int, suggestion_count: int):
        super().__init__()
        self.round_id = round_id
        if suggestion_count >= MAX_SUGGESTIONS:
            self.remove_item(self.suggest_button)

    @discord.ui.button(label="Suggest", style=discord.ButtonStyle.primary, custom_id="poll_suggest_button", emoji="🎬")
    async def suggest_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        round_row = await _get_active_round()
        if round_row is None or round_row["phase"] != "suggesting":
            return await interaction.response.send_message("❌ Suggestions are closed.", ephemeral=True)
        await interaction.response.send_modal(SuggestModal(round_row["id"]))

    @discord.ui.button(label="Start Poll", style=discord.ButtonStyle.success, custom_id="poll_start_button", emoji="🗳️")
    async def start_poll_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ Only the owner can start the poll.", ephemeral=True)

        round_row = await _get_active_round()
        if round_row is None or round_row["phase"] != "suggesting":
            return await interaction.response.send_message("❌ No active suggestion round to start.", ephemeral=True)

        suggestions = await _get_suggestions(round_row["id"])
        if len(suggestions) < 2:
            return await interaction.response.send_message("❌ Need at least 2 suggestions to start a poll.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        cog = interaction.client.get_cog("PollSuggestions")
        if cog is None:
            return await interaction.followup.send("❌ Poll cog not loaded.", ephemeral=True)
        await cog.start_poll(interaction, round_row, suggestions)


# --- COG ---

class PollSuggestions(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Admin"
        # Serializes suggestion inserts — cheap insurance against two
        # people submitting at the exact same instant both computing the
        # same next slot_number before either write lands. The UNIQUE
        # constraints in the DB are the final backstop if this somehow
        # doesn't fully prevent it.
        self.suggestion_lock = asyncio.Lock()
        # Prevents the loop from double-processing the same round if a
        # tick takes longer than CHECK_INTERVAL_SECONDS to run.
        self._finalizing: set[int] = set()

    async def cog_load(self):
        await _ensure_db()
        # Registers both custom_ids for dispatch on ANY message, old or
        # new — the actual round_id/count passed here are placeholders,
        # never trusted; every real callback re-fetches fresh state.
        self.bot.add_view(self._persistent_view())
        self._check_expired_polls.start()

    async def cog_unload(self):
        self._check_expired_polls.cancel()

    def _persistent_view(self) -> discord.ui.LayoutView:
        view = discord.ui.LayoutView(timeout=None)
        container = discord.ui.Container()
        container.add_item(discord.ui.TextDisplay("placeholder"))
        container.add_item(PollActionRow(round_id=0, suggestion_count=0))
        view.add_item(container)
        return view

    async def refresh_suggestions_message(self, round_row: dict):
        suggestions = await _get_suggestions(round_row["id"])
        view = _build_suggestions_view(round_row, suggestions)
        channel = self.bot.get_channel(round_row["channel_id"])
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            return
        try:
            msg = await channel.fetch_message(round_row["message_id"])
            await msg.edit(view=view)
        except discord.HTTPException as e:
            logger.error(f"[PollSuggestions] Failed to refresh suggestions message: {type(e).__name__}: {e}")

    async def start_poll(self, interaction: discord.Interaction, round_row: dict, suggestions: list[dict]):
        """Sends the actual native poll as a NEW message (Discord doesn't
        allow attaching a poll via edit — see module docstring), then
        edits the original suggestion message to a closed/final state.
        Order matters: post the poll first, so if that fails, the
        suggestion message is left untouched and /poll can just be
        retried rather than being left in a broken half-closed state."""
        channel = self.bot.get_channel(round_row["channel_id"])
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            return await interaction.followup.send("❌ Couldn't find the poll channel.", ephemeral=True)

        duration = timedelta(hours=POLL_DURATION_HOURS)
        poll = discord.Poll(
            question=discord.PollMedia(text="What should we add to Jellyfin?"),
            duration=duration,
            multiple=False,
        )
        for s in suggestions:
            answer_text = f"{s['title']} ({s['year']})"
            if len(answer_text) > POLL_ANSWER_MAX_CHARS:
                answer_text = answer_text[:POLL_ANSWER_MAX_CHARS - 1] + "…"
            poll.add_answer(text=answer_text)

        try:
            poll_msg = await channel.send(poll=poll)
        except discord.HTTPException as e:
            logger.error(f"[PollSuggestions] Failed to create poll: {type(e).__name__}: {e}")
            return await interaction.followup.send(f"❌ Failed to create the poll: `{e}`", ephemeral=True)

        try:
            old_msg = await channel.fetch_message(round_row["message_id"])
            closed_view = _build_simple_message_view(
                "-# JELLYFIN DOWNLOAD SUGGESTIONS\n# ✅ Suggestions closed — poll started below ⬇️"
            )
            await old_msg.edit(view=closed_view)
        except discord.HTTPException as e:
            logger.error(f"[PollSuggestions] Failed to close suggestions message: {type(e).__name__}: {e}")

        # Prefer Discord's own reported expiry over a locally-computed one
        # once it's available (poll_msg.poll.expires_at), falling back to
        # now+duration if the library doesn't surface it on the send
        # response for some reason.
        expires_at = getattr(getattr(poll_msg, "poll", None), "expires_at", None)
        if expires_at is None:
            expires_at = datetime.now(timezone.utc) + duration
        expires_at_iso = expires_at.isoformat()

        await _set_round_phase(round_row["id"], "polling", poll_message_id=poll_msg.id,
                                poll_expires_at=expires_at_iso)
        await interaction.followup.send(
            f"✅ Poll started with {len(suggestions)} option(s) — runs for {POLL_DURATION_HOURS}h.", ephemeral=True
        )
        logger.info(f"[PollSuggestions] Poll started for round {round_row['id']} with {len(suggestions)} option(s) by <@{interaction.user.id}>.")

    async def _finalize_round(self, round_row: dict):
        """Reads final vote counts, deletes the poll message, re-edits the
        original suggestion message into the two-container 'ended' state,
        and marks the round finished. Called by the expiry loop. Safe to
        call more than once for the same round — re-checks phase before
        acting and guards against concurrent ticks via self._finalizing."""
        round_id = round_row["id"]
        if round_id in self._finalizing:
            return
        self._finalizing.add(round_id)
        try:
            # Re-fetch fresh in case a manual /pollcancel raced this tick.
            current = await _get_round(round_id)
            if current is None or current["phase"] != "polling":
                return

            channel = self.bot.get_channel(current["channel_id"])
            if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                logger.error(f"[PollSuggestions] Round {round_id}: channel not found/valid, marking finished anyway.")
                await _set_round_phase(round_id, "finished")
                return

            results = None
            total_votes = 0

            if current.get("poll_message_id"):
                try:
                    poll_msg = await channel.fetch_message(current["poll_message_id"])
                    poll = poll_msg.poll
                    if poll is not None:
                        # discord.py's Poll object exposes vote counts
                        # directly on each PollAnswer via .vote_count —
                        # there is no separate poll.results object to read
                        # from. (A previous version of this code tried
                        # poll.results.channel_answer_counts as a first
                        # branch; Poll has no `results` attribute at all,
                        # so that `hasattr(poll.results, ...)` check itself
                        # raised AttributeError before hasattr could even
                        # run, crashing this loop every time a poll expired.)
                        counted = [
                            {"text": ans.text, "count": ans.vote_count} for ans in poll.answers
                        ]
                        counted.sort(key=lambda r: r["count"], reverse=True)
                        results = counted
                        total_votes = sum(r["count"] for r in counted)
                    try:
                        await poll_msg.delete()
                    except discord.HTTPException as e:
                        logger.error(f"[PollSuggestions] Round {round_id}: failed to delete poll message: {type(e).__name__}: {e}")
                except discord.NotFound:
                    # Poll message already gone (manual deletion, etc.) —
                    # treat as ended with no results rather than stalling.
                    logger.warning(f"[PollSuggestions] Round {round_id}: poll message missing, finalizing with no results.")
                except discord.HTTPException as e:
                    logger.error(f"[PollSuggestions] Round {round_id}: failed to fetch poll message: {type(e).__name__}: {e}")

            finished_view = _build_finished_view(current, results, total_votes)
            try:
                orig_msg = await channel.fetch_message(current["message_id"])
                await orig_msg.edit(view=finished_view)
            except discord.HTTPException as e:
                logger.error(f"[PollSuggestions] Round {round_id}: failed to edit suggestion message to finished state: {type(e).__name__}: {e}")

            await _set_round_phase(round_id, "finished")
            logger.info(f"[PollSuggestions] Round {round_id} finalized. {total_votes} total votes.")
        finally:
            self._finalizing.discard(round_id)

    @tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
    async def _check_expired_polls(self):
        try:
            polling_rounds = await _get_polling_rounds()
        except Exception as e:
            logger.error(f"[PollSuggestions] Failed to query polling rounds: {type(e).__name__}: {e}")
            return

        now = datetime.now(timezone.utc)
        for round_row in polling_rounds:
            expires_at_raw = round_row.get("poll_expires_at")
            if not expires_at_raw:
                # Older row from before this column existed / expiry wasn't
                # captured — fall back to fetching the poll to check its
                # own expiry directly rather than skipping it forever.
                channel = self.bot.get_channel(round_row["channel_id"])
                if isinstance(channel, (discord.TextChannel, discord.VoiceChannel)) and round_row.get("poll_message_id"):
                    try:
                        poll_msg = await channel.fetch_message(round_row["poll_message_id"])
                        if poll_msg.poll and poll_msg.poll.expires_at and now < poll_msg.poll.expires_at:
                            continue
                    except discord.NotFound:
                        pass  # gone — finalize below
                    except discord.HTTPException:
                        continue
                await self._finalize_round(round_row)
                continue

            expires_at = datetime.fromisoformat(expires_at_raw)
            if now >= expires_at:
                await self._finalize_round(round_row)

    @_check_expired_polls.before_loop
    async def _before_check_expired_polls(self):
        await self.bot.wait_until_ready()

    # --- /poll ---
    @app_commands.command(name="poll", description="[Owner] Start a new Jellyfin download-suggestion round")
    async def poll_command(self, interaction: discord.Interaction):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        active = await _get_active_round()
        if active is not None:
            return await interaction.response.send_message(
                "❌ There's already an active round or poll — use `/pollcancel` first if you want to start over.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True)

        channel = self.bot.get_channel(POLL_CHANNEL_ID)
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            return await interaction.followup.send("❌ POLL_CHANNEL_ID isn't a valid text channel — check config.", ephemeral=True)

        placeholder_view = _build_simple_message_view("Setting up...")
        try:
            msg = await channel.send(view=placeholder_view)
        except discord.HTTPException as e:
            return await interaction.followup.send(f"❌ Failed to post the suggestions message: `{e}`", ephemeral=True)

        round_id = await _create_round(channel.id, msg.id, interaction.user.id)
        round_row = await _get_round(round_id)
        real_view = _build_suggestions_view(round_row, [])
        try:
            await msg.edit(view=real_view)
        except discord.HTTPException as e:
            logger.error(f"[PollSuggestions] Failed to render initial suggestions view: {type(e).__name__}: {e}")

        await interaction.followup.send(f"✅ Suggestion round started in {channel.mention}.", ephemeral=True)
        logger.info(f"[PollSuggestions] New suggestion round {round_id} started by <@{interaction.user.id}>.")

    # --- /pollcancel ---
    @app_commands.command(name="pollcancel", description="[Owner] Cancel the current suggestion round or delete the active poll")
    async def pollcancel_command(self, interaction: discord.Interaction):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        active = await _get_active_round()
        if active is None:
            return await interaction.response.send_message("❌ No active round or poll to cancel.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        channel = self.bot.get_channel(active["channel_id"])

        if active["phase"] == "suggesting":
            if isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                try:
                    msg = await channel.fetch_message(active["message_id"])
                    cancelled_view = _build_simple_message_view(
                        "-# JELLYFIN DOWNLOAD SUGGESTIONS\n# ❌ Cancelled"
                    )
                    await msg.edit(view=cancelled_view)
                except discord.HTTPException as e:
                    logger.warning(f"[PollSuggestions] Couldn't edit cancelled suggestions message: {type(e).__name__}: {e}")
            await _set_round_phase(active["id"], "cancelled")
            await interaction.followup.send("✅ Suggestion round cancelled.", ephemeral=True)
            logger.info(f"[PollSuggestions] Round {active['id']} cancelled (suggestion phase) by <@{interaction.user.id}>.")

        elif active["phase"] == "polling":
            # Per Discord's own Polls FAQ: deleting the whole message is
            # the ONLY way to delete a poll — there is no other API for
            # it, and a poll can't be edited or partially removed.
            if active.get("poll_message_id") and isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                try:
                    poll_msg = await channel.fetch_message(active["poll_message_id"])
                    await poll_msg.delete()
                except discord.HTTPException as e:
                    await interaction.followup.send(f"⚠️ Couldn't delete the poll message: `{e}`", ephemeral=True)
                    logger.error(f"[PollSuggestions] Failed to delete poll message for round {active['id']}: {type(e).__name__}: {e}")
            await _set_round_phase(active["id"], "cancelled")
            await interaction.followup.send("✅ Poll deleted.", ephemeral=True)
            logger.info(f"[PollSuggestions] Round {active['id']} cancelled (poll deleted) by <@{interaction.user.id}>.")

    @poll_command.error
    @pollcancel_command.error
    async def poll_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(PollSuggestions(bot))