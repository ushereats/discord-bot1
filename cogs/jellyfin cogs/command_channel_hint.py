"""
Command Channel Hint Cog

Any message posted in WATCH_CHANNEL_ID that starts with "/" gets a reply
pointing the author to COMMANDS_CHANNEL_ID:

    Commands live in #commands!

The reply pings the author and is never deleted.

Requires the Message Content intent (Developer Portal -> Bot -> Privileged
Gateway Intents, plus `intents.message_content = True` in main.py).
Without it every message's text arrives empty and this never triggers.
"""

import time
import logging

import discord
from discord.ext import commands

logger = logging.getLogger("command_channel_hint_cog")

WATCH_CHANNEL_ID = 1537028150783909999
COMMANDS_CHANNEL_ID = 1538371135362564096

# Minimum seconds between replies to the same user. 0 = reply every time.
REPLY_COOLDOWN_SECONDS = 0


class CommandChannelHint(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Utility"
        self._last_reply_at: dict[int, float] = {}

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.channel.id != WATCH_CHANNEL_ID:
            return
        if message.author.bot:
            return
        if not message.content.startswith("/"):
            return

        if REPLY_COOLDOWN_SECONDS > 0:
            now = time.monotonic()
            last = self._last_reply_at.get(message.author.id)
            if last is not None and now - last < REPLY_COOLDOWN_SECONDS:
                return
            self._last_reply_at[message.author.id] = now

        try:
            await message.reply(
                f"Commands live in <#{COMMANDS_CHANNEL_ID}>!",
                mention_author=True,
                # Set explicitly so a bot-wide allowed_mentions default can't
                # silently turn the reply ping off.
                allowed_mentions=discord.AllowedMentions(replied_user=True, users=False, roles=False, everyone=False),
            )
        except discord.HTTPException as e:
            logger.warning(f"[CommandChannelHint] Couldn't reply to {message.author.id}: {type(e).__name__}: {e}")


async def setup(bot) -> None:
    await bot.add_cog(CommandChannelHint(bot))