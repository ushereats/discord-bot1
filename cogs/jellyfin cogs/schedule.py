"""
Schedule Cog — post a message (raw Components V2 JSON) at a set time.

  /schedule post   channel  when  message_json   [Owner]
      when: "YYYY-MM-DD HH:MM" in Sydney time, e.g. "2026-10-06 00:01".
      message_json: the full message JSON exactly as you'd paste it into
      an embed builder ({"flags": 32768, "components": [...]}).
  /schedule list                                   [Owner]
      Lists every pending post, then shows a private preview of each one
      (up to MAX_PREVIEWS) exactly as it will look, labelled with its
      number, channel and time. Previews never ping. A preview Discord
      rejects means the post itself would fail — the error is shown so
      it can be fixed before it's due.
  /schedule cancel id                              [Owner]

Scheduled posts are stored in scheduled_posts.db, so they survive
restarts. The bot checks every CHECK_SECONDS; a post that came due while
the bot was offline is sent as soon as it's back.

@everyone / @here / role / user mentions in the JSON DO ping when the
post goes out (the bot needs "Mention Everyone" in that channel for
@everyone to work).
"""

import json
import os
import time
import logging
from datetime import datetime

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks

from utils.config import OWNER_DISCORD_ID, TIMEZONE, SCHEDULE_DB_PATH as DB_PATH

logger = logging.getLogger("schedule_cog")
CHECK_SECONDS = 30
MAX_PREVIEWS = 5
IS_COMPONENTS_V2 = 1 << 15
EPHEMERAL = 1 << 6

# Pings in scheduled posts are deliberate (announcements), so allow them.
DEFAULT_ALLOWED_MENTIONS = {"parse": ["everyone", "roles", "users"]}


def _parse_when(text: str) -> int | None:
    """'YYYY-MM-DD HH:MM' in Sydney time -> Unix timestamp."""
    try:
        local = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
    except ValueError:
        return None
    return int(local.timestamp())


def _parse_payload(text: str) -> tuple[dict | None, str | None]:
    """Returns (payload, error)."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as e:
        return None, f"That isn't valid JSON — {e.msg} (character {e.pos})."
    if not isinstance(payload, dict) or not (payload.get("components") or payload.get("content") or payload.get("embeds")):
        return None, "The JSON needs a `components`, `content` or `embeds` field."
    return payload, None


class Schedule(commands.Cog):
    group = app_commands.Group(name="schedule", description="[Owner] Schedule a message to post later")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Admin"
        self._db: aiosqlite.Connection | None = None

    async def cog_load(self):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        self._db = await aiosqlite.connect(DB_PATH)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                send_at INTEGER NOT NULL,
                payload TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT
            )
        """)
        await self._db.commit()
        if not self.send_due.is_running():
            self.send_due.start()

    async def cog_unload(self):
        self.send_due.cancel()
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != OWNER_DISCORD_ID:
            await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)
            return False
        return True

    # --- sending ---

    async def _send(self, channel_id: int, payload: dict):
        body = dict(payload)
        body.setdefault("allowed_mentions", DEFAULT_ALLOWED_MENTIONS)
        route = discord.http.Route("POST", "/channels/{channel_id}/messages", channel_id=channel_id)
        await self.bot.http.request(route, json=body)

    @tasks.loop(seconds=CHECK_SECONDS)
    async def send_due(self):
        try:
            async with self._db.execute(
                "SELECT id, channel_id, payload FROM scheduled_posts WHERE status = 'pending' AND send_at <= ? ORDER BY send_at",
                (int(time.time()),),
            ) as cursor:
                due = await cursor.fetchall()
            for post_id, channel_id, payload in due:
                try:
                    await self._send(channel_id, json.loads(payload))
                    status, error = "sent", None
                except discord.HTTPException as e:
                    status, error = "failed", f"{e.status} {e.text}"[:500]
                    logger.error(f"[Schedule] Post #{post_id} failed: {error}")
                await self._db.execute("UPDATE scheduled_posts SET status = ?, error = ? WHERE id = ?", (status, error, post_id))
                await self._db.commit()
        except Exception as e:
            logger.error(f"[Schedule] Check failed: {type(e).__name__}: {e}", exc_info=True)

    @send_due.before_loop
    async def before_send_due(self):
        await self.bot.wait_until_ready()

    # --- commands ---

    @group.command(name="post", description="Schedule a message (Components V2 JSON) for a Sydney date and time")
    @app_commands.describe(
        channel="Where to post it",
        when="Sydney time, YYYY-MM-DD HH:MM — e.g. 2026-10-06 00:01",
        message_json="The full message JSON ({\"flags\": 32768, \"components\": [...]})",
    )
    async def post(self, interaction: discord.Interaction, channel: discord.TextChannel, when: str, message_json: str):
        send_at = _parse_when(when)
        if send_at is None:
            return await interaction.response.send_message("❌ Use the format `YYYY-MM-DD HH:MM`, e.g. `2026-10-06 00:01`.", ephemeral=True)
        if send_at <= time.time():
            return await interaction.response.send_message(f"❌ <t:{send_at}:F> is already in the past.", ephemeral=True)

        payload, error = _parse_payload(message_json)
        if error:
            return await interaction.response.send_message(f"❌ {error}", ephemeral=True)

        cursor = await self._db.execute(
            "INSERT INTO scheduled_posts (channel_id, send_at, payload, created_by) VALUES (?, ?, ?, ?)",
            (channel.id, send_at, json.dumps(payload), interaction.user.id),
        )
        await self._db.commit()
        await interaction.response.send_message(
            f"✅ Scheduled **#{cursor.lastrowid}** for {channel.mention} at <t:{send_at}:F> (<t:{send_at}:R>).",
            ephemeral=True,
        )

    async def _send_private_preview(self, interaction: discord.Interaction, post_id: int, channel_id: int,
                                    send_at: int, payload: dict):
        """Posts the stored message as a private follow-up, exactly as it
        will look, with a small label on top. Mentions are disabled."""
        label = f"-# 🗓️ Preview of **#{post_id}** → <#{channel_id}> · <t:{send_at}:F> (<t:{send_at}:R>)"
        body = json.loads(json.dumps(payload))  # deep copy
        if body.get("flags", 0) & IS_COMPONENTS_V2:
            body["components"] = [{"type": 10, "content": label}] + list(body.get("components") or [])
        else:
            body["content"] = f"{label}\n{body.get('content') or ''}".strip()
        body["flags"] = body.get("flags", 0) | EPHEMERAL
        body["allowed_mentions"] = {"parse": []}
        route = discord.http.Route(
            "POST", "/webhooks/{webhook_id}/{webhook_token}",
            webhook_id=interaction.application_id, webhook_token=interaction.token,
        )
        await self.bot.http.request(route, json=body)

    @group.command(name="list", description="Show upcoming scheduled messages with a preview of each")
    async def list_(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        async with self._db.execute(
            "SELECT id, channel_id, send_at, payload FROM scheduled_posts WHERE status = 'pending' ORDER BY send_at"
        ) as cursor:
            rows = await cursor.fetchall()
        if not rows:
            return await interaction.followup.send("Nothing scheduled.", ephemeral=True)

        lines = [f"**#{pid}** → <#{cid}> at <t:{ts}:F> (<t:{ts}:R>)" for pid, cid, ts, _ in rows]
        if len(rows) > MAX_PREVIEWS:
            lines.append(f"-# Showing previews of the next {MAX_PREVIEWS}.")
        await interaction.followup.send("\n".join(lines)[:2000], ephemeral=True)

        for pid, cid, ts, payload in rows[:MAX_PREVIEWS]:
            try:
                await self._send_private_preview(interaction, pid, cid, ts, json.loads(payload))
            except discord.HTTPException as e:
                await interaction.followup.send(
                    f"⚠️ **#{pid}** can't be previewed — Discord rejected it, so it would fail when due too.\n"
                    f"-# {e.status}: {str(e.text)[:300]}",
                    ephemeral=True,
                )

    @group.command(name="cancel", description="Cancel a scheduled message by its number")
    @app_commands.describe(id="The number shown by /schedule list")
    async def cancel(self, interaction: discord.Interaction, id: int):
        cursor = await self._db.execute(
            "UPDATE scheduled_posts SET status = 'cancelled' WHERE id = ? AND status = 'pending'", (id,)
        )
        await self._db.commit()
        if cursor.rowcount:
            await interaction.response.send_message(f"🗑️ Cancelled **#{id}**.", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ No pending post **#{id}**.", ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(Schedule(bot))