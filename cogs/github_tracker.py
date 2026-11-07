"""
GitHub Release Tracker Cog

  /github — [Owner] opens a private dashboard:
      Set channel   where release notifications are posted
      Set ping      who gets pinged (a user or a role)
      ➕ Add repo    paste any GitHub repo link (…/releases, plain repo URL,
                    or owner/repo) — checked to exist before saving
      ➖ Remove repo
      🧪 Pre-releases  toggle per repo whether betas / RCs notify
      🔄 Check now

How releases are detected:
  Every CHECK_MINUTES the bot asks GitHub for each repo's newest releases.
  Adding a repo records everything already released as "seen", so you're
  only notified about releases published AFTER you add it. Each new
  release (oldest first, none skipped — even after downtime) is posted as a
  card in the channel, pinging the chosen user/role, with a View release
  button. Drafts are never shown.

Reliability:
  - Everything (channel, ping, repos, seen releases) lives in
    github_tracker.db, so restarts never lose state or re-send old releases.
  - Conditional requests (ETag): an unchanged repo answers 304, which
    GitHub does not count against the rate limit. If the limit is ever hit,
    the bot waits until GitHub's reset time instead of hammering it.
  - Optional GITHUB_TOKEN raises the limit from 60 to 5,000 requests/hour.
  - A failed check is retried next round; a deleted/renamed repo shows ⚠️
    on the dashboard instead of breaking anything.
"""

import os
import re
import time
import asyncio
import logging
from datetime import datetime

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks

logger = logging.getLogger("github_tracker_cog")

OWNER_DISCORD_ID = 723417665087078451
DB_PATH = os.path.join("/home/container/ushereats/DATABASES", "github_tracker.db")
GITHUB_TOKEN = ""  # optional: a GitHub personal access token (no scopes needed) for 5,000 req/hour
CHECK_MINUTES = 5
RELEASES_PER_CHECK = 10
NOTES_EXCERPT_CHARS = 700
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)

REPO_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)", re.I)
SHORT_PATTERN = re.compile(r"^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)$")


def _parse_repo(text: str) -> str | None:
    text = text.strip()
    match = REPO_PATTERN.search(text) or SHORT_PATTERN.match(text)
    if not match:
        return None
    owner, repo = match.group(1), match.group(2)
    if repo.endswith(".git"):
        repo = repo[:-4]
    return f"{owner}/{repo}"


def _iso_ts(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def _excerpt(body: str | None) -> str:
    text = (body or "").strip()
    if not text:
        return ""
    if len(text) > NOTES_EXCERPT_CHARS:
        text = text[:NOTES_EXCERPT_CHARS].rsplit("\n", 1)[0].rstrip() + "\n…"
    return text


class GitHubTracker(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.category = "Admin"
        self._db: aiosqlite.Connection | None = None
        self._rate_limited_until = 0.0
        self._check_lock = asyncio.Lock()

    async def cog_load(self):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        self._db = await aiosqlite.connect(DB_PATH)
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS repos (
                full_name TEXT PRIMARY KEY,
                include_prereleases INTEGER NOT NULL DEFAULT 1,
                etag TEXT,
                latest_tag TEXT,
                last_checked INTEGER,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS seen (
                full_name TEXT NOT NULL,
                release_id INTEGER NOT NULL,
                PRIMARY KEY (full_name, release_id)
            );
        """)
        await self._db.commit()
        if not self.check_loop.is_running():
            self.check_loop.start()

    async def cog_unload(self):
        self.check_loop.cancel()
        if self._db is not None:
            await self._db.close()
            self._db = None

    # --- storage ---

    async def get_config(self, key: str) -> str | None:
        async with self._db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
        return row[0] if row else None

    async def set_config(self, key: str, value: str | None):
        await self._db.execute(
            "INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await self._db.commit()

    async def repos(self) -> list[dict]:
        async with self._db.execute(
            "SELECT full_name, include_prereleases, etag, latest_tag, last_checked, error FROM repos ORDER BY full_name COLLATE NOCASE"
        ) as cursor:
            rows = await cursor.fetchall()
        keys = ("full_name", "include_prereleases", "etag", "latest_tag", "last_checked", "error")
        return [dict(zip(keys, row)) for row in rows]

    # --- GitHub API ---

    def _headers(self, etag: str | None = None) -> dict:
        headers = {"Accept": "application/vnd.github+json", "User-Agent": "ushereats-release-tracker",
                   "X-GitHub-Api-Version": "2022-11-28"}
        if GITHUB_TOKEN:
            headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
        if etag:
            headers["If-None-Match"] = etag
        return headers

    async def _get(self, path: str, etag: str | None = None, params: dict | None = None):
        """Returns (status, json, etag). Raises on network errors."""
        async with self.bot.http_session.get(
            f"https://api.github.com{path}", headers=self._headers(etag), params=params, timeout=REQUEST_TIMEOUT,
        ) as resp:
            if resp.status in (403, 429) and resp.headers.get("X-RateLimit-Remaining") == "0":
                reset = int(resp.headers.get("X-RateLimit-Reset") or 0)
                self._rate_limited_until = max(time.time() + 60, reset)
                logger.warning(f"[GitHub] Rate limited until <t:{reset}>")
            data = await resp.json(content_type=None) if resp.status == 200 else None
            return resp.status, data, resp.headers.get("ETag")

    async def _fetch_releases(self, full_name: str, etag: str | None):
        return await self._get(f"/repos/{full_name}/releases", etag=etag, params={"per_page": str(RELEASES_PER_CHECK)})

    # --- adding / removing ---

    async def add_repo(self, raw: str) -> str:
        full_name = _parse_repo(raw)
        if full_name is None:
            return "❌ That doesn't look like a GitHub repo link — e.g. `https://github.com/Moonfin-Client/Moonfin-Core/releases`."
        try:
            status, repo, _ = await self._get(f"/repos/{full_name}")
            if status == 404:
                return f"❌ `{full_name}` doesn't exist or isn't public."
            if status != 200:
                return f"⚠️ GitHub answered {status} — try again in a minute."
            full_name = repo.get("full_name") or full_name  # canonical capitalisation
            status, releases, etag = await self._fetch_releases(full_name, None)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return "⚠️ Couldn't reach GitHub — try again in a minute."

        async with self._db.execute("SELECT 1 FROM repos WHERE full_name = ? COLLATE NOCASE", (full_name,)) as cursor:
            if await cursor.fetchone():
                return f"ℹ️ `{full_name}` is already tracked."

        releases = [r for r in (releases or []) if not r.get("draft")]
        latest = releases[0].get("tag_name") if releases else None
        await self._db.execute(
            "INSERT INTO repos (full_name, etag, latest_tag, last_checked) VALUES (?, ?, ?, ?)",
            (full_name, etag, latest, int(time.time())),
        )
        # Everything already released counts as seen — only NEW releases notify.
        await self._db.executemany(
            "INSERT OR IGNORE INTO seen (full_name, release_id) VALUES (?, ?)",
            [(full_name, r["id"]) for r in releases],
        )
        await self._db.commit()
        return f"✅ Tracking `{full_name}`" + (f" — latest is **{latest}**." if latest else " — no releases yet.")

    async def remove_repo(self, full_name: str) -> str:
        await self._db.execute("DELETE FROM repos WHERE full_name = ?", (full_name,))
        await self._db.execute("DELETE FROM seen WHERE full_name = ?", (full_name,))
        await self._db.commit()
        return f"🗑️ Stopped tracking `{full_name}`."

    async def toggle_prereleases(self, full_name: str) -> str:
        await self._db.execute(
            "UPDATE repos SET include_prereleases = 1 - include_prereleases WHERE full_name = ?", (full_name,),
        )
        await self._db.commit()
        async with self._db.execute("SELECT include_prereleases FROM repos WHERE full_name = ?", (full_name,)) as cursor:
            row = await cursor.fetchone()
        return f"🧪 Pre-releases for `{full_name}` are now **{'on' if row and row[0] else 'off'}**."

    # --- checking ---

    async def _ping_text(self) -> str:
        kind, target = await self.get_config("ping_type"), await self.get_config("ping_id")
        if kind == "role" and target:
            return f"<@&{target}>"
        if kind == "user" and target:
            return f"<@{target}>"
        return ""

    def _release_card(self, full_name: str, release: dict, ping: str) -> discord.ui.LayoutView:
        tag = release.get("tag_name") or "new release"
        name = release.get("name") or ""
        repo_short = full_name.split("/", 1)[1]
        published = _iso_ts(release.get("published_at") or release.get("created_at"))
        author = (release.get("author") or {}).get("login")
        avatar = (release.get("author") or {}).get("avatar_url")

        head = (f"{ping}\n" if ping else "") + f"### 🚀 New release — {discord.utils.escape_markdown(repo_short)} {discord.utils.escape_markdown(tag)}"
        meta = []
        if release.get("prerelease"):
            meta.append("🧪 Pre-release")
        if name and name != tag:
            meta.append(discord.utils.escape_markdown(name))
        if published:
            meta.append(f"<t:{published}:R>")
        if author:
            meta.append(f"by {discord.utils.escape_markdown(author)}")
        meta.append(full_name)
        top = f"{head}\n-# " + " · ".join(meta)

        container = discord.ui.Container()
        if avatar:
            container.add_item(discord.ui.Section(discord.ui.TextDisplay(top), accessory=discord.ui.Thumbnail(media=avatar)))
        else:
            container.add_item(discord.ui.TextDisplay(top))
        notes = _excerpt(release.get("body"))
        if notes:
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
            container.add_item(discord.ui.TextDisplay(notes))
        container.add_item(discord.ui.ActionRow(discord.ui.Button(
            label="View release", emoji="📦", style=discord.ButtonStyle.link,
            url=release.get("html_url") or f"https://github.com/{full_name}/releases",
        )))
        view = discord.ui.LayoutView(timeout=None)
        view.add_item(container)
        return view

    async def check_all(self) -> int:
        """Checks every repo once; returns how many notifications went out."""
        async with self._check_lock:
            if time.time() < self._rate_limited_until:
                return 0
            channel_id = await self.get_config("channel_id")
            channel = self.bot.get_channel(int(channel_id)) if channel_id else None
            if channel is None:
                return 0
            ping = await self._ping_text()
            sent = 0
            for repo in await self.repos():
                full_name = repo["full_name"]
                try:
                    status, releases, etag = await self._fetch_releases(full_name, repo["etag"])
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"[GitHub] Check failed for {full_name}: {type(e).__name__}")
                    continue
                now = int(time.time())
                if status == 304:
                    await self._db.execute("UPDATE repos SET last_checked = ?, error = NULL WHERE full_name = ?", (now, full_name))
                    continue
                if status != 200:
                    error = "not found (deleted or renamed?)" if status == 404 else f"GitHub answered {status}"
                    await self._db.execute("UPDATE repos SET last_checked = ?, error = ? WHERE full_name = ?", (now, error, full_name))
                    if status in (403, 429):
                        break  # rate limited — stop this round
                    continue

                releases = [r for r in releases or [] if not r.get("draft")]
                async with self._db.execute("SELECT release_id FROM seen WHERE full_name = ?", (full_name,)) as cursor:
                    seen = {row[0] for row in await cursor.fetchall()}
                new = [r for r in releases if r["id"] not in seen]
                for release in sorted(new, key=lambda r: r.get("published_at") or r.get("created_at") or ""):
                    if release.get("prerelease") and not repo["include_prereleases"]:
                        pass  # still marked seen below, so it never notifies later either
                    else:
                        try:
                            await channel.send(
                                view=self._release_card(full_name, release, ping),
                                allowed_mentions=discord.AllowedMentions(users=True, roles=True, everyone=False),
                            )
                            sent += 1
                        except discord.HTTPException as e:
                            logger.error(f"[GitHub] Couldn't post {full_name} {release.get('tag_name')}: {e}")
                            continue  # not marked seen -> retried next round
                    await self._db.execute("INSERT OR IGNORE INTO seen (full_name, release_id) VALUES (?, ?)", (full_name, release["id"]))
                latest = releases[0].get("tag_name") if releases else repo["latest_tag"]
                await self._db.execute(
                    "UPDATE repos SET etag = ?, latest_tag = ?, last_checked = ?, error = NULL WHERE full_name = ?",
                    (etag, latest, now, full_name),
                )
            await self._db.commit()
            return sent

    @tasks.loop(minutes=CHECK_MINUTES)
    async def check_loop(self):
        try:
            await self.check_all()
        except Exception as e:
            logger.error(f"[GitHub] Check round failed: {type(e).__name__}: {e}", exc_info=True)

    @check_loop.before_loop
    async def before_check_loop(self):
        await self.bot.wait_until_ready()

    # --- dashboard ---

    async def build_dashboard(self, stage: str = "main", status: str | None = None) -> "Dashboard":
        repos = await self.repos()
        channel_id = await self.get_config("channel_id")
        ping = await self._ping_text()
        return Dashboard(self, stage, repos, channel_id, ping, status)

    @app_commands.command(name="github", description="[Owner] GitHub release tracker dashboard")
    async def github(self, interaction: discord.Interaction):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)
        await interaction.response.send_message(view=await self.build_dashboard(), ephemeral=True)


class AddRepoModal(discord.ui.Modal, title="Track a GitHub repo"):
    link = discord.ui.TextInput(label="Repo link", placeholder="https://github.com/Moonfin-Client/Moonfin-Core/releases", max_length=200)

    def __init__(self, cog: GitHubTracker):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        status = await self.cog.add_repo(self.link.value)
        await interaction.edit_original_response(view=await self.cog.build_dashboard(status=status))


class Dashboard(discord.ui.LayoutView):
    def __init__(self, cog: GitHubTracker, stage: str, repos: list[dict], channel_id: str | None, ping: str, status: str | None):
        super().__init__(timeout=900)
        self.cog = cog

        lines = []
        for repo in repos:
            checked = f"<t:{repo['last_checked']}:R>" if repo["last_checked"] else "not yet"
            latest = f"**{discord.utils.escape_markdown(repo['latest_tag'])}**" if repo["latest_tag"] else "no releases yet"
            pre = "" if repo["include_prereleases"] else " · 🧪 off"
            warn = f"\n-# ⚠️ {repo['error']}" if repo["error"] else ""
            lines.append(f"`{repo['full_name']}` · latest {latest} · checked {checked}{pre}{warn}")

        text = (
            "### 📦 GitHub Release Tracker\n"
            f"**Notify in:** {f'<#{channel_id}>' if channel_id else '*not set*'}\n"
            f"**Ping:** {ping or '*nobody*'}\n"
            f"-# Checks every {CHECK_MINUTES} min · only releases published after a repo is added notify"
        )
        container = discord.ui.Container()
        container.add_item(discord.ui.TextDisplay(text))
        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
        container.add_item(discord.ui.TextDisplay(
            f"**Tracking ({len(repos)})**\n" + ("\n".join(lines) if lines else "-# Nothing yet — press **Add repo**.")
        ))
        if status:
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
            container.add_item(discord.ui.TextDisplay(status))
        self.add_item(container)

        if stage == "channel":
            select = discord.ui.ChannelSelect(placeholder="Pick the notification channel…",
                                              channel_types=[discord.ChannelType.text, discord.ChannelType.news])
            select.callback = self._channel_chosen
            self.add_item(discord.ui.ActionRow(select))
            self.add_item(discord.ui.ActionRow(self._button("Back", "↩️", self._back)))
        elif stage == "ping":
            select = discord.ui.MentionableSelect(placeholder="Pick a user or role to ping…")
            select.callback = self._ping_chosen
            self.add_item(discord.ui.ActionRow(select))
            self.add_item(discord.ui.ActionRow(self._button("No ping", "🔕", self._no_ping), self._button("Back", "↩️", self._back)))
        elif stage in ("remove", "pre") and repos:
            select = discord.ui.Select(
                placeholder="Pick a repo to remove…" if stage == "remove" else "Pick a repo to toggle pre-releases…",
                options=[discord.SelectOption(label=r["full_name"][:100], value=r["full_name"][:100],
                                              description=("pre-releases on" if r["include_prereleases"] else "pre-releases off"))
                         for r in repos[:25]],
            )
            select.callback = self._remove_chosen if stage == "remove" else self._pre_chosen
            self.add_item(discord.ui.ActionRow(select))
            self.add_item(discord.ui.ActionRow(self._button("Back", "↩️", self._back)))
        else:
            self.add_item(discord.ui.ActionRow(
                self._button("Set channel", "📣", lambda i: self._stage(i, "channel")),
                self._button("Set ping", "🔔", lambda i: self._stage(i, "ping")),
                self._button("Add repo", "➕", self._add, style=discord.ButtonStyle.success),
                self._button("Remove repo", "➖", lambda i: self._stage(i, "remove"), disabled=not repos),
            ))
            self.add_item(discord.ui.ActionRow(
                self._button("Pre-releases", "🧪", lambda i: self._stage(i, "pre"), disabled=not repos),
                self._button("Check now", "🔄", self._check_now, disabled=not repos),
            ))

    @staticmethod
    def _button(label, emoji, callback, style=discord.ButtonStyle.secondary, disabled=False):
        button = discord.ui.Button(label=label, emoji=emoji, style=style, disabled=disabled)

        async def wrapped(interaction: discord.Interaction):
            await callback(interaction)
        button.callback = wrapped
        return button

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == OWNER_DISCORD_ID

    async def _show(self, interaction: discord.Interaction, stage: str = "main", status: str | None = None):
        self.stop()
        view = await self.cog.build_dashboard(stage, status)
        if interaction.response.is_done():
            await interaction.edit_original_response(view=view)
        else:
            await interaction.response.edit_message(view=view)

    async def _stage(self, interaction, stage):
        await self._show(interaction, stage)

    async def _back(self, interaction):
        await self._show(interaction)

    async def _add(self, interaction):
        self.stop()
        await interaction.response.send_modal(AddRepoModal(self.cog))

    async def _channel_chosen(self, interaction):
        channel_id = interaction.data["values"][0]
        await self.cog.set_config("channel_id", channel_id)
        await self._show(interaction, status=f"✅ Notifications will post in <#{channel_id}>.")

    async def _ping_chosen(self, interaction):
        target = int(interaction.data["values"][0])
        resolved = (interaction.data.get("resolved") or {})
        kind = "role" if str(target) in (resolved.get("roles") or {}) else "user"
        await self.cog.set_config("ping_type", kind)
        await self.cog.set_config("ping_id", str(target))
        mention = f"<@&{target}>" if kind == "role" else f"<@{target}>"
        await self._show(interaction, status=f"✅ New releases will ping {mention}.")

    async def _no_ping(self, interaction):
        await self.cog.set_config("ping_type", None)
        await self.cog.set_config("ping_id", None)
        await self._show(interaction, status="🔕 Releases will post without a ping.")

    async def _remove_chosen(self, interaction):
        await self._show(interaction, status=await self.cog.remove_repo(interaction.data["values"][0]))

    async def _pre_chosen(self, interaction):
        await self._show(interaction, status=await self.cog.toggle_prereleases(interaction.data["values"][0]))

    async def _check_now(self, interaction):
        await interaction.response.defer()
        if not await self.cog.get_config("channel_id"):
            return await self._show(interaction, status="ℹ️ Set a channel first.")
        sent = await self.cog.check_all()
        await self._show(interaction, status=f"🔄 Checked — {sent} new release{'s' if sent != 1 else ''} posted.")


async def setup(bot) -> None:
    await bot.add_cog(GitHubTracker(bot))