"""
Restart Cog
  /restart — [Owner] Fully restarts the bot process via os.execv,
  re-executing the same Python interpreter with the same arguments this
  process was started with. This is a REAL process restart, not a cog
  reload — every cog, every listener, the whole gateway connection gets
  torn down and rebuilt from scratch, same as manually stopping and
  starting the bot on Pterodactyl.

  Exists specifically because hot-reloading a cog (e.g. via jishaku's
  jsk reload) can leave a stale listener registered alongside the new
  one if the old cog instance wasn't fully torn down first — two
  on_message listeners both reacting to the same events has caused a
  real bug in this bot before (confirmed: it's what caused
  stickymessage.py to spam-repost with no real trigger — two live
  instances, each reacting to the other's own repost as if it were an
  ordinary new message). A full process restart guarantees exactly one
  instance of everything, which a cog reload does not.

  Nothing runs after os.execv() — it doesn't return. Modern Python
  (3.4+, PEP 446) marks socket file descriptors non-inheritable by
  default, so the old gateway connection's socket closes cleanly as
  part of the exec rather than leaking.
"""

import os
import sys
import logging
import discord
from discord import app_commands
from discord.ext import commands

logger = logging.getLogger("restart_cog")

OWNER_DISCORD_ID = 723417665087078451


class Restart(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Utility"

    @app_commands.command(name="restart", description="[Owner] Fully restart the bot process")
    async def restart(self, interaction: discord.Interaction):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        await interaction.response.send_message("🔄 Restarting the bot process now — back in a few seconds.", ephemeral=True)
        logger.info(f"[Restart] Full process restart triggered by <@{interaction.user.id}>.")

        # Replaces the current process image entirely — a genuine full
        # restart, not a reload. Nothing after this line ever runs; the
        # process is gone the instant this call succeeds.
        os.execv(sys.executable, [sys.executable] + sys.argv)

    @restart.error
    async def restart_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(Restart(bot))
