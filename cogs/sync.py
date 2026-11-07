"""
Sync Cog — /sync
  Reloads every cog in ./cogs whose file changed, loads new ones, unloads
  ones whose file was deleted, then syncs slash commands with Discord only
  if the command list actually changed.

  How "changed" is decided:
    - A cog reloaded by /sync before: its file's modified time vs. the one
      recorded at that reload.
    - A cog only ever loaded at bot startup: its file's modified time vs.
      the moment the bot started. Edited after startup = reload; otherwise
      it's still the exact code that's running. (Previously every cog got
      reloaded on the first /sync after a restart, changed or not.)

  This cog never reloads itself mid-run (that used to swap the running
  instance out from under the command and reset its lock). If sync.py
  itself changed, it's reloaded right after the report is posted.

  Unicode emoji only — the bot's custom emojis render as text like
  ":Online:" anywhere the bot can't use them.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import traceback

import discord
from discord import app_commands
from discord.ext import commands

logger = logging.getLogger("sync_cog")

COGS_DIR = "./cogs"
LOAD_TIMEOUT_SECONDS = 15
TREE_SYNC_TIMEOUT_SECONDS = 60
PROGRESS_EDIT_SECONDS = 1.5   # interaction edits are rate limited; ~1 edit per 1.5s is safe
TEXT_BUDGET = 3700            # Discord allows 4000 characters of text per message


def _process_start_time() -> float:
    """Unix time this bot process started. Read from /proc so it's right
    even when this cog is first loaded long after startup (e.g. swapped in
    by a /sync). Falls back to now."""
    try:
        with open("/proc/self/stat") as f:
            start_ticks = int(f.read().rsplit(")", 1)[1].split()[19])
        with open("/proc/stat") as f:
            btime = next(int(line.split()[1]) for line in f if line.startswith("btime"))
        return btime + start_ticks / os.sysconf("SC_CLK_TCK")
    except Exception:
        return time.time()


class sync_cog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.category = "Bot-Owner"
        # Everything that must survive this cog being reloaded lives on the
        # bot object, which is never recreated.
        if not hasattr(bot, "_sync_state"):
            bot._sync_state = {
                "boot_time": _process_start_time(),  # when the bot process started
                # extension -> file mtime at its last /sync (re)load; carries
                # over what the previous version of this cog recorded
                "mtimes": dict(getattr(bot, "_sync_last_reload_mtime", {}) or {}),
                "commands_hash": None,        # hash of the command list last pushed to Discord
                "lock": asyncio.Lock(),       # one /sync at a time, even across reloads of this cog
            }
        self.state = bot._sync_state
        self.own_extension = self.__module__

    async def cog_load(self):
        if self.state["commands_hash"] is None:
            asyncio.create_task(self._seed_commands_hash())

    async def _seed_commands_hash(self):
        """main.py already pushes every command to Discord on startup, so
        once the bot is ready the command list Discord has IS the current
        one. Recording its fingerprint here means the first /sync after a
        restart doesn't push the identical list a second time — that
        redundant push is what was hitting Discord's rate limit and timing
        out."""
        await self.bot.wait_until_ready()
        await asyncio.sleep(15)  # give main.py's startup sync time to finish
        if self.state["commands_hash"] is None:
            self.state["commands_hash"] = self._commands_hash()
            logger.info("[Sync] Recorded the startup command list — /sync will only push slash commands when they change.")

    # ------------------------------------------------------------------ UI
    def _view(self, body: str, footer: str | None = None) -> discord.ui.LayoutView:
        view = discord.ui.LayoutView(timeout=None)
        container = discord.ui.Container()
        container.add_item(discord.ui.Section(
            discord.ui.TextDisplay(body),
            accessory=discord.ui.Thumbnail(media=self.bot.user.display_avatar.url),
        ))
        if footer:
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
            container.add_item(discord.ui.TextDisplay(footer))
        view.add_item(container)
        return view

    @staticmethod
    def _bar(done: int, total: int, length: int = 14) -> str:
        filled = round(length * done / total) if total else length
        return "▓" * filled + "░" * (length - filled)

    # ------------------------------------------------------------------ discovery
    @staticmethod
    def _discover() -> dict[str, str]:
        """{extension_path: file_path} for every cog file under ./cogs,
        built the same way main.py builds extension names, so reload vs.
        fresh-load matching works for nested folders too."""
        found = {}
        for root, _, files in os.walk(COGS_DIR):
            for file in sorted(files):
                if file.endswith(".py") and not file.startswith("_"):
                    filepath = os.path.join(root, file)
                    ext = os.path.relpath(filepath, ".")[:-3].replace(os.sep, ".")
                    found[ext] = filepath
        return found

    def _has_changed(self, ext: str, filepath: str) -> bool:
        try:
            mtime = os.path.getmtime(filepath)
        except OSError:
            return True  # can't tell — reload rather than guess
        baseline = self.state["mtimes"].get(ext, self.state["boot_time"])
        return mtime > baseline

    def _commands_hash(self) -> str | None:
        """Fingerprint of every app command as Discord would receive it. If
        it matches what was last synced, tree.sync() is skipped — editing a
        cog's internals doesn't change its slash commands, and the sync
        endpoint is rate limited."""
        try:
            tree = self.bot.tree
            payload = sorted(
                (json.dumps(cmd.to_dict(tree), sort_keys=True, default=str) for cmd in tree.get_commands()),
            )
            return hashlib.sha256("\n".join(payload).encode()).hexdigest()
        except Exception as e:
            logger.warning(f"[Sync] Couldn't fingerprint commands, will sync anyway: {type(e).__name__}: {e}")
            return None

    # ------------------------------------------------------------------ command
    @app_commands.command(name="sync", description="Reload changed cogs and sync slash commands.")
    @app_commands.checks.has_permissions(administrator=True)
    async def sync_command(self, interaction: discord.Interaction):
        lock: asyncio.Lock = self.state["lock"]
        if lock.locked():
            return await interaction.response.send_message(
                view=self._view("### ⏳ Sync already running\nWait for it to finish before starting another."),
                ephemeral=True,
            )

        async with lock:
            started = time.perf_counter()
            await interaction.response.defer()

            discovered = self._discover()
            loaded = {e for e in self.bot.extensions if e.startswith("cogs.")}

            to_load = [e for e in discovered if e not in loaded]
            to_reload = [
                e for e in discovered
                if e in loaded and e != self.own_extension and self._has_changed(e, discovered[e])
            ]
            to_unload = sorted(loaded - set(discovered))
            unchanged = [
                e for e in discovered
                if e in loaded and e not in to_reload and e != self.own_extension
            ]
            self_changed = self.own_extension in discovered and self._has_changed(self.own_extension, discovered[self.own_extension])

            jobs = [("load", e) for e in to_load] + [("reload", e) for e in to_reload] + [("unload", e) for e in to_unload]
            total = len(jobs)

            # --- live progress (edits at most every PROGRESS_EDIT_SECONDS) ---
            progress = {"done": 0, "current": "scanning"}

            async def progress_loop():
                last = None
                while True:
                    if total:
                        text = (
                            f"### 🔄 Syncing\n"
                            f"`{self._bar(progress['done'], total)}` {progress['done']}/{total}\n"
                            f"-# {progress['current']}"
                        )
                    else:
                        text = f"### 🔄 Syncing\n-# {progress['current']}"
                    if text != last:
                        try:
                            await interaction.edit_original_response(view=self._view(text))
                        except discord.HTTPException:
                            return
                        last = text
                    await asyncio.sleep(PROGRESS_EDIT_SECONDS)

            progress_task = asyncio.create_task(progress_loop())

            done_ok: dict[str, list[str]] = {"load": [], "reload": [], "unload": []}
            failed: list[tuple[str, str, str]] = []  # (action, name, short error)

            try:
                for action, ext in jobs:
                    name = ext.split(".")[-1]
                    progress["current"] = f"{action}ing `{name}`"
                    try:
                        if action == "load":
                            await asyncio.wait_for(self.bot.load_extension(ext), timeout=LOAD_TIMEOUT_SECONDS)
                        elif action == "reload":
                            await asyncio.wait_for(self.bot.reload_extension(ext), timeout=LOAD_TIMEOUT_SECONDS)
                        else:
                            await asyncio.wait_for(self.bot.unload_extension(ext), timeout=LOAD_TIMEOUT_SECONDS)
                        done_ok[action].append(name)
                        if action != "unload":
                            try:
                                self.state["mtimes"][ext] = os.path.getmtime(discovered[ext])
                            except OSError:
                                pass
                        else:
                            self.state["mtimes"].pop(ext, None)
                    except asyncio.TimeoutError:
                        failed.append((action, name, f"timed out after {LOAD_TIMEOUT_SECONDS}s — a blocking call in setup()/cog_load()?"))
                    except Exception as e:
                        root = e.__cause__ or e  # discord.py wraps the real error in ExtensionFailed
                        logger.error(f"[Sync] {action} {ext} failed", exc_info=e)
                        failed.append((action, name, f"{type(root).__name__}: {root}".splitlines()[0][:160]))
                    progress["done"] += 1

                progress["current"] = "checking slash commands"
                new_hash = self._commands_hash()
                needs_push = new_hash is None or new_hash != self.state["commands_hash"]
            except Exception as e:
                # Never leave the message stuck on the progress screen.
                logger.error("[Sync] crashed mid-run", exc_info=e)
                progress_task.cancel()
                await self._post(interaction, self._view(f"### ❌ /sync crashed\n`{type(e).__name__}: {e}`"[:1900]))
                return
            finally:
                progress_task.cancel()

            # Let any in-flight progress edit finish first, so it can't land
            # after (and overwrite) the report.
            await asyncio.gather(progress_task, return_exceptions=True)

            elapsed = time.perf_counter() - started
            sync_line = "Pushing slash commands to Discord…" if needs_push else "Slash commands unchanged — nothing to push."

            # Post the cog report NOW — it no longer waits on Discord's
            # command-sync endpoint, which can stall for a long time when
            # Discord rate limits it.
            body, footer = self._report(done_ok, failed, unchanged, sync_line, True, self_changed, elapsed)
            await self._post(interaction, self._view(body, footer))

            if needs_push:
                logger.info("[Sync] Pushing slash commands to Discord…")
                try:
                    synced = await asyncio.wait_for(self.bot.tree.sync(), timeout=TREE_SYNC_TIMEOUT_SECONDS)
                    self.state["commands_hash"] = new_hash
                    sync_line, sync_ok = f"Synced {len(synced)} slash commands with Discord.", True
                    logger.info(f"[Sync] {sync_line}")
                except asyncio.TimeoutError:
                    sync_line = (
                        f"Slash command sync didn't finish within {TREE_SYNC_TIMEOUT_SECONDS}s — Discord is most likely "
                        f"rate limiting it. Cogs are reloaded; try /sync again in a few minutes."
                    )
                    sync_ok = False
                    logger.warning(f"[Sync] {sync_line}")
                except Exception as e:
                    logger.error("[Sync] tree.sync failed", exc_info=e)
                    sync_line, sync_ok = f"Slash command sync failed: {type(e).__name__}: {e}"[:200], False

                body, footer = self._report(done_ok, failed, unchanged, sync_line, sync_ok, self_changed, elapsed)
                await self._post(interaction, self._view(body, footer))

        # Outside the lock and after the report: reload this cog itself if it changed.
        if self_changed:
            asyncio.create_task(self._reload_self(discovered[self.own_extension]))

    async def _post(self, interaction: discord.Interaction, view: discord.ui.LayoutView):
        """Edits the /sync message; if that fails for any reason, sends the
        result as a new message instead so it's never silently lost."""
        try:
            await interaction.edit_original_response(view=view)
            return
        except Exception as e:
            logger.error(f"[Sync] Couldn't edit the /sync message: {type(e).__name__}: {e}")
        try:
            await interaction.followup.send(view=view)
        except Exception as e:
            logger.error(f"[Sync] Couldn't send the /sync report at all: {type(e).__name__}: {e}")

    async def _reload_self(self, filepath: str):
        await asyncio.sleep(1)
        try:
            await self.bot.reload_extension(self.own_extension)
            self.state["mtimes"][self.own_extension] = os.path.getmtime(filepath)
            logger.info("[Sync] Reloaded the sync cog itself.")
        except Exception as e:
            logger.error("[Sync] Reloading the sync cog itself failed", exc_info=e)

    # ------------------------------------------------------------------ report
    def _report(self, done_ok, failed, unchanged, sync_line, sync_ok, self_changed, elapsed):
        changed_count = sum(len(v) for v in done_ok.values()) + (1 if self_changed else 0)

        if not sync_ok:
            title = "### ❌ Sync finished with errors"
        elif failed:
            title = "### ⚠️ Sync finished — some cogs failed"
        elif changed_count == 0:
            title = "### ✅ Already up to date"
        else:
            title = "### ✅ Sync complete"

        summary_bits = []
        if done_ok["reload"] or self_changed:
            summary_bits.append(f"{len(done_ok['reload']) + (1 if self_changed else 0)} reloaded")
        if done_ok["load"]:
            summary_bits.append(f"{len(done_ok['load'])} new")
        if done_ok["unload"]:
            summary_bits.append(f"{len(done_ok['unload'])} removed")
        if failed:
            summary_bits.append(f"{len(failed)} failed")
        summary_bits.append(f"{len(unchanged)} unchanged")
        summary_bits.append(f"{elapsed:.1f}s")

        lines = [title, "-# " + " · ".join(summary_bits)]

        def names(items):
            return " ".join(f"`{n}`" for n in items)

        reloaded = list(done_ok["reload"]) + ([f"{self.own_extension.split('.')[-1]} (after this report)"] if self_changed else [])
        if reloaded:
            lines += ["", f"**Reloaded**", names(reloaded)]
        if done_ok["load"]:
            lines += ["", "**Loaded (new)**", names(done_ok["load"])]
        if done_ok["unload"]:
            lines += ["", "**Unloaded (file deleted)**", names(done_ok["unload"])]
        if failed:
            lines += ["", "**Failed** — the previous version keeps running"]
            for action, name, err in failed:
                lines.append(f"`{name}` · {action} · {err}")

        body = "\n".join(lines)
        if len(body) > TEXT_BUDGET:
            body = body[:TEXT_BUDGET - 40].rsplit("\n", 1)[0] + "\n-# …cut off, full details in the console"

        footer_lines = [f"-# {sync_line} · <t:{int(time.time())}:R>"]
        if unchanged:
            unchanged_text = "-# Unchanged: " + " ".join(e.split(".")[-1] for e in unchanged)
            if len(body) + len(unchanged_text) + len(footer_lines[0]) < 3900:
                footer_lines.insert(0, unchanged_text)
        return body, "\n".join(footer_lines)

    @sync_command.error
    async def sync_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            text = "### ⛔ Not allowed\nYou need Administrator to use `/sync`."
        else:
            original = error.original if isinstance(error, app_commands.CommandInvokeError) else error
            traceback.print_exception(type(original), original, original.__traceback__)
            text = f"### ❌ /sync crashed\n`{type(original).__name__}: {original}`"[:1900]
        try:
            if interaction.response.is_done():
                await interaction.followup.send(view=self._view(text), ephemeral=True)
            else:
                await interaction.response.send_message(view=self._view(text), ephemeral=True)
        except discord.HTTPException:
            pass


async def setup(bot) -> None:
    await bot.add_cog(sync_cog(bot))