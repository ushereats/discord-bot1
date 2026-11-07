"""
Error Handler Cog
  Catches every category of Discord-facing failure across the whole bot
  and reports it to ERROR_CHANNEL_ID — one centralized place instead of
  errors only ever showing up in console logs nobody's watching live.

  Three separate mechanisms, because discord.py doesn't expose one single
  hook that covers everything:

  1. LOGGING HANDLER (covers almost everything that already exists):
     Every cog in this bot already calls logger.error()/logger.warning()
     constantly — rate limits, loop crashes, DB errors, Jellyfin API
     failures, reconciliation failures, all of it. A single custom
     logging.Handler attached to the ROOT logger catches every one of
     those calls, bot-wide, from every file, without touching any of
     those files. This alone covers discord.py's own internal rate-limit
     warnings (logged under the 'discord.http' logger) plus every
     logger.error/warning call already in the codebase.

  2. COMPONENT ERROR MONKEYPATCH (the actual gap):
     A button/select/modal callback that raises goes through
     discord.ui.View.on_error / discord.ui.Modal.on_error — which by
     default just print() a traceback to stderr, completely bypassing
     the logging module. No global hook exists for this in discord.py,
     so View.on_error and Modal.on_error are monkeypatched at the class
     level, once, here — this applies retroactively to every view/modal
     across every cog without editing any of them.

  3. APP COMMAND TREE FALLBACK:
     bot.tree.on_error runs for every slash command error that isn't
     already caught by a command's own local .error() handler or a cog's
     cog_app_command_error — several cogs here already handle their own
     command errors with a nice ephemeral embed, and this never touches
     those. main.py ALSO already had its own bot.tree.on_error installed
     (aussieglory.on_app_command_error, giving users real messages for
     missing permissions, cooldowns, check failures, etc.) — this cog
     does NOT replace that. It captures whatever was already in that
     slot on cog_load and CHAINS to it: the original handler always runs
     first (users see exactly what they saw before this cog existed),
     and channel reporting happens in addition, not instead.

  THROTTLING: the same error repeating rapidly (e.g. a burst of 429s)
  is deduplicated within DEDUP_WINDOW_SECONDS — only the first occurrence
  sends immediately, repeats are counted and folded into the next report
  as "(+N more in the last Ns)" instead of spamming the channel.

  SAFETY: every reporting path is wrapped so a failure to send the
  report can never itself raise or cascade — worst case, a report is
  silently dropped, never a new unhandled exception.

  HONEST LIMITATION: this reports failures to the channel below. It does
  NOT change what the user who triggered a failed interaction sees —
  Discord's own generic "This interaction failed" still shows on their
  end regardless. This is visibility for admins, not user-facing error
  messages.
"""

import time
import logging
import traceback
import asyncio
import discord
from discord.ext import commands
from discord import app_commands

from .utils.config import ERROR_CHANNEL_ID, WATCH_CHANNEL_ID, COMMANDS_CHANNEL_ID

logger = logging.getLogger("error_handler_cog")

DEDUP_WINDOW_SECONDS = 60      # same error key within this window gets folded, not resent
MAX_TRACEBACK_CHARS = 1000     # keep the code-block field under Discord's per-field limit

# Loggers whose records should never be forwarded (this module's own —
# prevents any possibility of a report-about-a-report feedback loop).
_EXCLUDED_LOGGER_NAMES = {"error_handler_cog"}

_bot_ref: commands.Bot | None = None
_recent_keys: dict[str, float] = {}     # dedup key -> last-sent timestamp
_suppressed_counts: dict[str, int] = {}  # dedup key -> count suppressed since last send

_original_view_on_error = discord.ui.View.on_error
_original_modal_on_error = discord.ui.Modal.on_error


def _should_send(key: str) -> tuple[bool, int]:
    """Returns (should_send_now, suppressed_count_to_report). Updates
    internal dedup state as a side effect."""
    now = time.time()
    last = _recent_keys.get(key)
    if last is not None and (now - last) < DEDUP_WINDOW_SECONDS:
        _suppressed_counts[key] = _suppressed_counts.get(key, 0) + 1
        return False, 0

    suppressed = _suppressed_counts.pop(key, 0)
    _recent_keys[key] = now
    return True, suppressed


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 20] + "\n...[truncated]"


async def _safe_send(embed: discord.Embed):
    """The one and only place that actually sends to Discord for this
    whole cog. Never raises — a failure here is a dropped report, never
    a new unhandled exception (which would be a special kind of ironic
    for an error-reporting system)."""
    if _bot_ref is None:
        return
    try:
        channel = _bot_ref.get_channel(ERROR_CHANNEL_ID)
        if channel is None:
            return
        await channel.send(embed=embed)
    except Exception:
        pass


def _build_embed(title: str, description: str, fields: dict[str, str] | None, suppressed: int) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        description=_truncate(description, 4000),
        color=discord.Color.red(),
        timestamp=discord.utils.utcnow(),
    )
    for name, value in (fields or {}).items():
        embed.add_field(name=name, value=_truncate(str(value), 1000) or "—", inline=False)
    if suppressed > 0:
        embed.set_footer(text=f"+{suppressed} similar report(s) suppressed in the last {DEDUP_WINDOW_SECONDS}s")
    return embed


# --- 1. LOGGING HANDLER ---

class ChannelLogHandler(logging.Handler):
    """Attached to the root logger — receives every log record from
    every logger in the process at WARNING level or above, from every
    cog, without any of them needing to know this exists."""

    def emit(self, record: logging.LogRecord):
        try:
            if record.name in _EXCLUDED_LOGGER_NAMES:
                return
            if record.levelno < logging.WARNING:
                return

            message = self.format(record)
            key = f"log:{record.name}:{record.levelname}:{message[:150]}"
            should_send, suppressed = _should_send(key)
            if not should_send:
                return

            if _bot_ref is None:
                return
            loop = _bot_ref.loop
            if loop is None or loop.is_closed():
                return

            level_label = record.levelname
            asyncio.run_coroutine_threadsafe(self._report(record, message, level_label, suppressed), loop)
        except Exception:
            pass  # the handler itself must never raise — that would break logging bot-wide

    async def _report(self, record: logging.LogRecord, message: str, level_label: str, suppressed: int):
        color_icon = "🟥" if level_label == "ERROR" or level_label == "CRITICAL" else "🟧"
        embed = _build_embed(
            title=f"{color_icon} {level_label} — {record.name}",
            description=message,
            fields=None,
            suppressed=suppressed,
        )
        await _safe_send(embed)


# --- 2. COMPONENT ERROR MONKEYPATCH ---

async def _patched_view_on_error(self: discord.ui.View, interaction: discord.Interaction, error: Exception, item: discord.ui.Item):
    tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    custom_id = getattr(item, "custom_id", None) or type(item).__name__
    key = f"view:{type(self).__name__}:{custom_id}:{type(error).__name__}"
    should_send, suppressed = _should_send(key)

    logger.error(f"[ErrorHandler] View '{type(self).__name__}' item '{custom_id}' raised {type(error).__name__}: {error}")

    if should_send:
        fields = {
            "View": type(self).__name__,
            "Item": custom_id,
            "User": f"{interaction.user} ({interaction.user.id})" if interaction.user else "Unknown",
            "Guild": str(interaction.guild_id) if interaction.guild_id else "DM",
            "Traceback": f"```py\n{tb}\n```",
        }
        embed = _build_embed(
            title=f"🔘 Button/Select failed — {type(self).__name__}",
            description=f"`{type(error).__name__}: {error}`",
            fields=fields,
            suppressed=suppressed,
        )
        await _safe_send(embed)

    # Preserve default behavior too (stderr traceback) for local console visibility.
    await _original_view_on_error(self, interaction, error, item)


async def _patched_modal_on_error(self: discord.ui.Modal, interaction: discord.Interaction, error: Exception):
    tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    key = f"modal:{type(self).__name__}:{type(error).__name__}"
    should_send, suppressed = _should_send(key)

    logger.error(f"[ErrorHandler] Modal '{type(self).__name__}' raised {type(error).__name__}: {error}")

    if should_send:
        fields = {
            "Modal": type(self).__name__,
            "User": f"{interaction.user} ({interaction.user.id})" if interaction.user else "Unknown",
            "Guild": str(interaction.guild_id) if interaction.guild_id else "DM",
            "Traceback": f"```py\n{tb}\n```",
        }
        embed = _build_embed(
            title=f"📝 Modal submit failed — {type(self).__name__}",
            description=f"`{type(error).__name__}: {error}`",
            fields=fields,
            suppressed=suppressed,
        )
        await _safe_send(embed)

    await _original_modal_on_error(self, interaction, error)


# --- 3. APP COMMAND TREE FALLBACK ---

# Captured in cog_load — whatever bot.tree.on_error was already set to
# BEFORE this cog loaded (main.py sets one in its __init__, giving users
# real messages for missing permissions/cooldowns/check failures). This
# module used to just overwrite that slot outright, which would have
# silently killed every one of those user-facing messages the moment
# this cog loaded — replaced by a version that only reported to the
# error channel and showed the user nothing. Fixed: the original handler
# now always runs first (users keep seeing exactly what they saw
# before), and reporting to the channel happens in addition to that, not
# instead of it.
_original_tree_on_error = None


async def _on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Fires for every slash command error, full stop — not just ones
    without a local handler, since this now always chains to whatever
    bot.tree.on_error was before. Commands with their OWN local .error()
    decorator or cog_app_command_error still never reach here at all —
    those take priority over bot.tree.on_error entirely, unaffected by
    any of this."""
    if _original_tree_on_error is not None:
        try:
            await _original_tree_on_error(interaction, error)
        except Exception as e:
            logger.error(f"[ErrorHandler] Original tree.on_error itself raised while handling a command error: {type(e).__name__}: {e}")

    tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    command_name = interaction.command.qualified_name if interaction.command else "unknown"
    key = f"cmd:{command_name}:{type(error).__name__}"
    should_send, suppressed = _should_send(key)

    logger.error(f"[ErrorHandler] Slash command '/{command_name}' raised {type(error).__name__}: {error}")

    if should_send:
        fields = {
            "Command": f"/{command_name}",
            "User": f"{interaction.user} ({interaction.user.id})" if interaction.user else "Unknown",
            "Guild": str(interaction.guild_id) if interaction.guild_id else "DM",
            "Traceback": f"```py\n{tb}\n```",
        }
        embed = _build_embed(
            title=f"⚡ Slash command failed — /{command_name}",
            description=f"`{type(error).__name__}: {error}`",
            fields=fields,
            suppressed=suppressed,
        )
        await _safe_send(embed)


# --- COG ---

REPLY_COOLDOWN_SECONDS = 0  # min seconds between command-channel hints to the same user


class ErrorHandler(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Admin"
        self._log_handler: ChannelLogHandler | None = None
        self._hint_cooldowns: dict[int, float] = {}  # user_id -> monotonic time

    async def cog_load(self):
        global _bot_ref, _original_tree_on_error
        _bot_ref = self.bot

        self._log_handler = ChannelLogHandler()
        self._log_handler.setFormatter(logging.Formatter("%(message)s"))
        logging.getLogger().addHandler(self._log_handler)

        discord.ui.View.on_error = _patched_view_on_error
        discord.ui.Modal.on_error = _patched_modal_on_error

        # Capture whatever handler was already installed (main.py's
        # aussieglory.on_app_command_error) BEFORE overwriting the slot —
        # this is what _on_app_command_error chains to, so nothing about
        # the existing user-facing error messages changes.
        _original_tree_on_error = self.bot.tree.on_error
        self.bot.tree.on_error = _on_app_command_error

        logger.info("[ErrorHandler] Installed — logging handler, View/Modal error monkeypatch, and tree.on_error chained (original handler preserved) all active.")

    # --- command-channel hint (merged from command_channel_hint.py) ---

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
            last = self._hint_cooldowns.get(message.author.id)
            if last is not None and now - last < REPLY_COOLDOWN_SECONDS:
                return
            self._hint_cooldowns[message.author.id] = now

        try:
            await message.reply(
                f"Commands live in <#{COMMANDS_CHANNEL_ID}>!",
                mention_author=True,
                allowed_mentions=discord.AllowedMentions(replied_user=True, users=False, roles=False, everyone=False),
            )
        except discord.HTTPException as e:
            logger.warning(f"[CommandChannelHint] Couldn't reply to {message.author.id}: {type(e).__name__}: {e}")

    async def cog_unload(self):
        if self._log_handler is not None:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler = None
        discord.ui.View.on_error = _original_view_on_error
        discord.ui.Modal.on_error = _original_modal_on_error
        if _original_tree_on_error is not None:
            self.bot.tree.on_error = _original_tree_on_error


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ErrorHandler(bot))