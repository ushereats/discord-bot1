"""
Jellyfin Registry Dashboard Cog
  - /users: configure a persistent, auto-refreshing dashboard message
    showing unlinked Jellyfin accounts and linked Discord<->Jellyfin pairs.

  Read-heavy, display-only. Never creates/deletes/links accounts — that's
  registry_commands.py's job. This cog only reads the links/config tables
  and renders them; refresh_dashboard() is called externally by the
  commands cog (and by whoami.py) after any mutation.

  Optimizations over the previous revision:
  1. Guild chunking — the member cache used to warm up one fetch_member
     call at a time (same pattern that was causing WatchingRole's 429s).
     Now the guild is bulk-chunked once via guild.chunk(), so
     guild.get_member() hits from cache almost every time afterward.
  2. Targeted single-user cache updates — refresh_dashboard() used to
     re-resolve EVERY linked user's membership status on every single
     /link, /unlink, /register, /unregister, or signup, even though only
     one person's status actually changed. Callers can now pass
     resolve_member_ids=[discord_id] to patch just that one entry instead
     of paying for a full pass. resolve_members=True (full pass) is kept
     for genuinely full scenarios: /users initial setup and the loop's
     first tick after a restart, when the whole cache is legitimately
     empty.
  3. One shared aiohttp.ClientSession for all Jellyfin API calls, opened
     in cog_load and closed in cog_unload, instead of a new connection
     per call.
  4. All database access goes through aiosqlite instead of blocking
     sqlite3, so a DB read/write here can't stall the bot's event loop.

Config: set JELLYFIN_URL and JELLYFIN_API_KEY below (same values as the
other Jellyfin cogs).
"""

import os
import logging
import aiohttp
import aiosqlite
import discord
from datetime import datetime
from discord import app_commands
from discord.ext import commands, tasks

logger = logging.getLogger("registry_dashboard_cog")

TARGET_GUILD_ID = 1537023658323869757
JELLYFIN_URL = "https://jellyfin.ushereats.com"
JELLYFIN_API_KEY = "9cb945d0d38d45cb84fcc8a2e1541bd8"

DB_DIR = "/home/container/ushereats/DATABASES"
DB_PATH = os.path.join(DB_DIR, "jellyfin_links.db")

DASHBOARD_LOOP_MINUTES = 5
PAGE_SIZE = 40
OWNER_DISCORD_ID = 723417665087078451

# Mentions on the dashboard render as normal blue clickable names but never
# ping anyone — passed to every send/edit of the dashboard message.
NO_PINGS = discord.AllowedMentions.none()


async def _ensure_db():
    """Creates all three registry tables, same as registry_commands.py.
    Both cogs call this independently on load — CREATE TABLE IF NOT EXISTS
    is idempotent, so it's safe regardless of which cog loads first, and
    neither depends on the other having run yet."""
    os.makedirs(DB_DIR, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS links (
                discord_id INTEGER PRIMARY KEY,
                jellyfin_user_id TEXT NOT NULL,
                linked_by INTEGER NOT NULL,
                linked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS registry_dashboard_config (
                guild_id INTEGER PRIMARY KEY,
                dashboard_channel_id INTEGER,
                dashboard_message_id INTEGER
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS signup_tokens (
                discord_id INTEGER PRIMARY KEY,
                issued_by INTEGER NOT NULL,
                issued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                message_id INTEGER
            )
        """)
        await conn.commit()


async def _get_all_links() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM links") as cursor:
            rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _get_dashboard_config(guild_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT guild_id, dashboard_channel_id, dashboard_message_id FROM registry_dashboard_config WHERE guild_id=?",
            (guild_id,),
        ) as cursor:
            row = await cursor.fetchone()
    if row is None:
        return {"guild_id": guild_id, "dashboard_channel_id": None, "dashboard_message_id": None}
    return dict(row)


async def _upsert_dashboard_config(guild_id: int, **fields):
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            "SELECT guild_id FROM registry_dashboard_config WHERE guild_id=?", (guild_id,)
        ) as cursor:
            existing = await cursor.fetchone()

        if existing is None:
            columns = ["guild_id"] + list(fields.keys())
            placeholders = ", ".join(["?"] * len(columns))
            values = [guild_id] + list(fields.values())
            await conn.execute(
                f"INSERT INTO registry_dashboard_config ({', '.join(columns)}) VALUES ({placeholders})",
                tuple(values),
            )
        else:
            set_clause = ", ".join(f"{k}=?" for k in fields.keys())
            values = list(fields.values()) + [guild_id]
            await conn.execute(f"UPDATE registry_dashboard_config SET {set_clause} WHERE guild_id=?", tuple(values))
        await conn.commit()


# --- SHARED JELLYFIN HTTP SESSION ---
# No longer owned by this module — shared across the whole bot process
# via bot.http_session (created once in main.py's setup_hook). _bot_ref
# is set once in cog_load; _get_jellyfin_session() keeps its original
# async signature so the one call site below needed zero changes.
_bot_ref = None


async def _get_jellyfin_session() -> aiohttp.ClientSession:
    return _bot_ref.http_session


async def _fetch_jellyfin_users() -> list[dict]:
    headers = {"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"', "accept": "application/json"}
    session = await _get_jellyfin_session()
    async with session.get(f"{JELLYFIN_URL}/Users", headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        resp.raise_for_status()
        return await resp.json()


def _last_activity_ts(user: dict) -> int | None:
    raw = user.get("LastActivityDate")
    if not raw:
        return None
    try:
        raw = raw.split(".")[0] + "Z" if "." in raw else raw
        dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ")
        return int(dt.timestamp())
    except Exception:
        return None


def _format_last_seen(ts: int | None) -> str:
    if ts is None:
        return "never"
    return f"<t:{ts}:R>"


# --- SNAPSHOT: only the fields that should trigger a dashboard edit.
def _registry_snapshot(jf_users: list[dict], links: list[dict]) -> tuple:
    users_part = tuple(sorted(
        (u.get("Id"), u.get("Name"), u.get("LastActivityDate")) for u in jf_users
    ))
    links_part = tuple(sorted(
        (l["discord_id"], l["jellyfin_user_id"]) for l in links
    ))
    return (users_part, links_part)


# Discord's real limit: the combined text across EVERY TextDisplay in one
# message/container is capped at 4000 characters total — not per-component.
# Leaving headroom below the hard cap for the title/unlinked block/footer
# that always get added regardless of how many linked accounts there are.
TEXT_BUDGET = 3800


# --- CONTAINER BUILDER (Components V2) ---
UNLINKED_SHOWN = 15  # unlinked list caps here, the rest collapse into "+ N more"
ANSI_ESC = "\u001b"


def _stats_block(total: int, linked: int, unlinked: int, left: int) -> str:
    e = ANSI_ESC
    return (
        "```ansi\n"
        f"{e}[1;37m{total:,}{e}[0m accounts   "
        f"{e}[1;32m{linked:,}{e}[0m linked   "
        f"{e}[1;33m{unlinked:,}{e}[0m unlinked   "
        f"{e}[1;31m{left:,}{e}[0m left server\n"
        "```"
    )


def build_registry_container(jf_users: list[dict], links: list[dict], member_cache: dict, page: int = 0) -> tuple[discord.ui.Container, int, int]:
    """member_cache: discord_id -> (mention_or_fallback_str, in_server_bool).
    Falls back to a raw-ID string if a linked user hasn't been resolved yet
    (resolution only happens on link/unlink or /users setup, never on the
    5-minute poll, to avoid a Discord API call per linked user per tick).

    Returns (container, clamped_page, total_pages) — the caller (the
    LayoutView) needs page/total_pages to render matching pagination
    buttons, and page may get clamped here if the link count shrank since
    the caller's last known page (e.g. after an /unregister)."""
    jf_users_by_id = {u["Id"]: u for u in jf_users}
    linked_jf_ids = {l["jellyfin_user_id"] for l in links}

    unlinked_jf_users = [u for u in jf_users if u["Id"] not in linked_jf_ids]
    unlinked_jf_users.sort(key=lambda u: _last_activity_ts(u) or -1, reverse=True)

    # Build linked rows first so the stats block can count who left.
    entries = []
    left_count = 0
    for link in links:
        jf_user = jf_users_by_id.get(link["jellyfin_user_id"])
        cached = member_cache.get(link["discord_id"])
        if cached:
            discord_display, in_server = cached
        else:
            discord_display, in_server = f"`Unknown ({link['discord_id']})`", False

        if jf_user:
            ts = _last_activity_ts(jf_user)
            if not in_server:
                left_count += 1
            dot = "🟢" if in_server else "🔴"
            line = f"{dot} {discord_display} › `{jf_user.get('Name', '?')}` · {_format_last_seen(ts)}"
        else:
            ts = None
            line = f"⚫ {discord_display} › ~~`{link['jellyfin_user_id']}`~~ · account not found"

        entries.append((ts if ts is not None else -1, line))

    entries.sort(key=lambda x: x[0], reverse=True)

    container = discord.ui.Container()
    used = 0

    def add_text(text: str):
        nonlocal used
        container.add_item(discord.ui.TextDisplay(text))
        used += len(text)

    add_text("### 📋 Jellyfin Registry\n-# UsherEats · every account, who owns it, when it was last used")
    add_text(_stats_block(len(jf_users), len(links), len(unlinked_jf_users), left_count))
    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

    # --- Unlinked ---
    if unlinked_jf_users:
        lines = [f"**⚠️ Unlinked** · {len(unlinked_jf_users):,}"]
        for u in unlinked_jf_users[:UNLINKED_SHOWN]:
            lines.append(f"`{u.get('Name', '?')}` · {_format_last_seen(_last_activity_ts(u))}")
        hidden = len(unlinked_jf_users) - UNLINKED_SHOWN
        if hidden > 0:
            lines.append(f"-# + {hidden:,} more")
        add_text("\n".join(lines))
    else:
        add_text("**⚠️ Unlinked** · 0\n-# none, everyone's linked")

    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

    # --- Linked (paginated) ---
    total_pages = max(1, (len(entries) + PAGE_SIZE - 1) // PAGE_SIZE)
    clamped_page = max(0, min(page, total_pages - 1))

    if entries:
        page_entries = entries[clamped_page * PAGE_SIZE:(clamped_page + 1) * PAGE_SIZE]
        footer_reserve = 150
        chunk_lines = [f"**✅ Linked** · page {clamped_page + 1}/{total_pages}"]
        current_len = len(chunk_lines[0])
        shown_count = 0
        for _, line in page_entries:
            line_len = len(line) + 1
            if used + current_len + line_len > TEXT_BUDGET - footer_reserve:
                break
            chunk_lines.append(line)
            current_len += line_len
            shown_count += 1
        remaining = len(page_entries) - shown_count
        if remaining:
            chunk_lines.append(f"-# …and {remaining} more on this page not shown (message full)")
        add_text("\n".join(chunk_lines))
    else:
        add_text("**✅ Linked** · 0\n-# none yet")

    container.add_item(discord.ui.Separator(visible=False, spacing=discord.SeparatorSpacing.small))
    now_ts = int(datetime.now().timestamp())
    add_text(f"-# 🟢 in server · 🔴 left server · ⚫ account not found · updated <t:{now_ts}:R>")
    return container, clamped_page, total_pages


class RegistryPaginationActionRow(discord.ui.ActionRow):
    """Owner-only Previous/Next buttons for the 40-per-page linked accounts
    list. Fixed custom_ids so this survives a bot restart as a persistent
    view (registered once in cog_load) — every actual instance still gets
    its own page/total_pages state via the constructor, same pattern as
    MovieListView's instance-level button mutation."""

    def __init__(self, page: int, total_pages: int):
        super().__init__()
        self.prev_button.disabled = page <= 0
        self.next_button.disabled = page >= total_pages - 1
        self.page_label.label = f"{page + 1} / {total_pages}"

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary, custom_id="registry_page_prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._change_page(interaction, -1)

    @discord.ui.button(label="1 / 1", style=discord.ButtonStyle.grey, custom_id="registry_page_label", disabled=True)
    async def page_label(self, interaction: discord.Interaction, button: discord.ui.Button):
        pass  # decorative only — always disabled, never actually clickable

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary, custom_id="registry_page_next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._change_page(interaction, 1)

    async def _change_page(self, interaction: discord.Interaction, delta: int):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ Only the owner can use this.", ephemeral=True)

        cog = interaction.client.get_cog("JellyfinRegistryDashboard")
        if cog is None:
            return await interaction.response.send_message("❌ Dashboard cog not loaded.", ephemeral=True)

        await interaction.response.defer()
        await cog.change_page(interaction, delta)


class RegistryDashboardLayoutView(discord.ui.LayoutView):
    def __init__(self, jf_users: list[dict], links: list[dict], member_cache: dict, page: int = 0):
        super().__init__(timeout=None)
        container, clamped_page, total_pages = build_registry_container(jf_users, links, member_cache, page)
        self.page = clamped_page
        self.total_pages = total_pages
        self.add_item(container)
        self.add_item(RegistryPaginationActionRow(clamped_page, total_pages))


# --- SETUP PANEL (/users) ---
class SetMessageIdModal(discord.ui.Modal, title="Set Dashboard Message"):
    def __init__(self, callback_view):
        super().__init__()
        self.callback_view = callback_view
        self.message_id_input = discord.ui.TextInput(
            label="Existing message ID",
            placeholder="Paste the ID of a message already sent in this server",
            required=True,
            max_length=25
        )
        self.add_item(self.message_id_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            msg_id_int = int(self.message_id_input.value.strip())
        except ValueError:
            return await interaction.response.send_message("❌ Invalid message ID format.", ephemeral=True)

        msg = None
        for ch in interaction.guild.text_channels:
            perms = ch.permissions_for(interaction.guild.me)
            if not perms.view_channel or not perms.read_message_history:
                continue
            try:
                msg = await ch.fetch_message(msg_id_int)
                break
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue

        if msg is None:
            return await interaction.response.send_message("❌ Message not found anywhere in this server.", ephemeral=True)

        await _upsert_dashboard_config(interaction.guild.id, dashboard_channel_id=msg.channel.id, dashboard_message_id=msg.id)

        cog = interaction.client.get_cog("JellyfinRegistryDashboard")
        if cog is not None:
            await cog.refresh_dashboard(force=True)

        content, embeds = await self.callback_view.build_full_display()
        await interaction.response.edit_message(content=content, embeds=embeds, view=self.callback_view)


class DashboardChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, callback_view, current_channel_id: int = None):
        kwargs = dict(placeholder="Select dashboard channel...", channel_types=[discord.ChannelType.text], row=1)
        if current_channel_id:
            kwargs["default_values"] = [discord.Object(id=current_channel_id)]
        super().__init__(**kwargs)
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        channel = interaction.guild.get_channel(self.values[0].id)
        if channel is None:
            return await interaction.response.send_message("❌ Couldn't resolve that channel — try again.", ephemeral=True)

        perms = channel.permissions_for(interaction.guild.me)
        if not perms.send_messages or not perms.embed_links:
            return await interaction.response.send_message(
                f"❌ I lack `Send Messages` or `Embed Links` permission in {channel.mention}.", ephemeral=True
            )

        await interaction.response.defer()

        old_config = await _get_dashboard_config(interaction.guild.id)
        if old_config.get("dashboard_channel_id") and old_config.get("dashboard_message_id"):
            old_channel = interaction.guild.get_channel(old_config["dashboard_channel_id"])
            if old_channel is not None:
                try:
                    old_msg = await old_channel.fetch_message(old_config["dashboard_message_id"])
                    await old_msg.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass

        cog = interaction.client.get_cog("JellyfinRegistryDashboard")
        try:
            jf_users = await _fetch_jellyfin_users()
        except Exception as e:
            return await interaction.followup.send(f"❌ Couldn't reach Jellyfin: `{type(e).__name__}: {e}`", ephemeral=True)

        links = await _get_all_links()
        if cog is not None:
            await cog._refresh_member_cache_full(links)
            member_cache = cog._member_cache
        else:
            member_cache = {}

        dashboard_view = RegistryDashboardLayoutView(jf_users, links, member_cache, cog._current_page if cog else 0)
        try:
            new_msg = await channel.send(view=dashboard_view, allowed_mentions=NO_PINGS)
        except discord.HTTPException as e:
            return await interaction.followup.send(f"❌ Failed to post dashboard message: `{e}`", ephemeral=True)

        await _upsert_dashboard_config(interaction.guild.id, dashboard_channel_id=channel.id, dashboard_message_id=new_msg.id)
        if cog is not None:
            cog._dashboard_config_cache = None
            cog._last_snapshot = _registry_snapshot(jf_users, links)

        content, embeds = await self.callback_view.build_full_display()
        await interaction.edit_original_response(content=content, embeds=embeds, view=self.callback_view)


class DashboardConfigView(discord.ui.View):
    def __init__(self, guild_id: int, config: dict):
        super().__init__(timeout=300.0)
        self.guild_id = guild_id
        self.config = config
        self.add_item(DashboardChannelSelect(self, config.get("dashboard_channel_id")))

    async def build_full_display(self):
        self.config = await _get_dashboard_config(self.guild_id)
        channel_text = f"<#{self.config['dashboard_channel_id']}>" if self.config["dashboard_channel_id"] else "`Not Set`"
        message_text = f"`{self.config['dashboard_message_id']}`" if self.config["dashboard_message_id"] else "`Not Set`"

        embed = discord.Embed(
            title="⚙️ Jellyfin Registry Dashboard — Setup",
            description=(
                "Select a channel below to post a fresh registry dashboard there, or use **Set Message ID** "
                "to adopt an existing message instead of creating a new one.\n\n"
                f"Auto-refreshes every {DASHBOARD_LOOP_MINUTES} minutes, and instantly on `/link` or `/unlink`."
            ),
        )
        embed.add_field(name="Dashboard Channel", value=channel_text, inline=True)
        embed.add_field(name="Dashboard Message ID", value=message_text, inline=True)

        return None, [embed]

    @discord.ui.button(label="Set Message ID", style=discord.ButtonStyle.secondary, row=0)
    async def set_message_id_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetMessageIdModal(self))

    @discord.ui.button(label="Refresh Now", style=discord.ButtonStyle.success, row=0)
    async def refresh_now_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = await _get_dashboard_config(self.guild_id)
        if not config.get("dashboard_channel_id") or not config.get("dashboard_message_id"):
            return await interaction.response.send_message("⚠️ No dashboard message configured yet — pick a channel first.", ephemeral=True)

        # Ack immediately — refresh_dashboard below (Jellyfin fetch + DB +
        # a Discord message edit) can easily exceed the 3s window Discord
        # gives an unacknowledged interaction before the token expires.
        await interaction.response.defer()

        cog = interaction.client.get_cog("JellyfinRegistryDashboard")
        if cog is not None:
            await cog.refresh_dashboard(force=True)

        content, embeds = await self.build_full_display()
        await interaction.edit_original_response(content=content, embeds=embeds, view=self)
        await interaction.followup.send("✅ Dashboard refreshed.", ephemeral=True)


class JellyfinRegistryDashboard(commands.Cog):
    def __init__(self, bot: commands.AutoShardedBot):
        self.bot = bot
        self.category = "Admin"
        self._member_cache: dict[int, tuple[str, bool]] = {}
        self._last_snapshot = None
        self._dashboard_config_cache: dict | None = None
        self._did_initial_resolve = False
        self._current_page = 0
        # _ensure_db() is async (aiosqlite) now — moved to cog_load.

    async def cog_load(self):
        global _bot_ref
        _bot_ref = self.bot
        await _ensure_db()
        # Registers the fixed custom_ids (registry_page_prev/label/next) so
        # clicks on an already-sent dashboard message still work immediately
        # after a restart, before the first registry_loop tick has had a
        # chance to re-render it with a freshly bound view.
        self.bot.add_view(RegistryDashboardLayoutView([], [], {}, 0))
        if not self.registry_loop.is_running():
            self.registry_loop.start()

    async def cog_unload(self):
        self.registry_loop.cancel()
        # No session close here — main.py owns bot.http_session.

    async def _ensure_guild_chunked(self):
        """Bulk-loads the entire member list into the local cache in one
        shot, instead of letting it warm up one fetch_member call at a
        time during a full resolution pass. Same fix applied to
        WatchingRole for the same reason — a full pass over hundreds of
        linked users doing individual member fetches reliably triggers
        Discord rate limits. Safe to call repeatedly; no-ops once chunked."""
        guild = self.bot.get_guild(TARGET_GUILD_ID)
        if guild is None or guild.chunked:
            return
        try:
            await guild.chunk(cache=True)
            logger.info(f"[JellyfinRegistryDashboard] Guild {guild.id} chunked — {len(guild.members)} members cached.")
        except Exception as e:
            logger.warning(f"[JellyfinRegistryDashboard] Guild chunk failed, falling back to individual fetch_member calls: {type(e).__name__}: {e}")

    async def _resolve_member(self, discord_id: int):
        guild = self.bot.get_guild(TARGET_GUILD_ID)
        member = None
        if guild:
            member = guild.get_member(discord_id)
            if member is None:
                try:
                    member = await guild.fetch_member(discord_id)
                except Exception:
                    member = None

        in_server = member is not None
        discord_obj = member
        if discord_obj is None:
            try:
                discord_obj = await self.bot.fetch_user(discord_id)
            except Exception:
                discord_obj = None

        return discord_obj, in_server

    async def update_member_cache_for(self, discord_id: int):
        """Targeted single-user cache patch — resolves and updates just
        this one entry instead of re-resolving every linked user. This is
        the fast path every mutation (link/unlink/register/unregister/
        signup) should use via refresh_dashboard(resolve_member_ids=[id]);
        the full _refresh_member_cache_full pass is reserved for /users
        setup and the loop's first post-restart tick, where the cache is
        genuinely empty and there's no single relevant user to target."""
        discord_obj, in_server = await self._resolve_member(discord_id)
        display = discord_obj.mention if discord_obj else f"`Unknown ({discord_id})`"
        self._member_cache[discord_id] = (display, in_server)

    async def _refresh_member_cache_full(self, links: list[dict]):
        """Full resolution pass — every linked user, one by one. Reserved
        for /users setup and the first registry_loop tick after a restart,
        where the whole cache is legitimately empty. Chunks the guild
        first so most of these resolve from the local cache for free."""
        await self._ensure_guild_chunked()
        for link in links:
            discord_obj, in_server = await self._resolve_member(link["discord_id"])
            display = discord_obj.mention if discord_obj else f"`Unknown ({link['discord_id']})`"
            self._member_cache[link["discord_id"]] = (display, in_server)
        # drop cache entries for links that no longer exist
        current_ids = {l["discord_id"] for l in links}
        for stale_id in list(self._member_cache.keys()):
            if stale_id not in current_ids:
                del self._member_cache[stale_id]

    async def get_dashboard_config(self, force: bool = False) -> dict | None:
        guild = discord.utils.get(self.bot.guilds, id=TARGET_GUILD_ID)
        if guild is None:
            return None
        if force or self._dashboard_config_cache is None:
            self._dashboard_config_cache = await _get_dashboard_config(TARGET_GUILD_ID)
        return self._dashboard_config_cache

    async def refresh_dashboard(
        self,
        force: bool = False,
        resolve_members: bool = False,
        resolve_member_ids: list[int] | None = None,
    ):
        """Fetches fresh state, compares against the last broadcast snapshot,
        and only edits the dashboard message if something actually changed
        (or force=True).

        Two ways to update the member cache before rendering:
        - resolve_member_ids=[...]: FAST PATH. Patches only the given
          discord_ids into the cache. This is what registry_commands.py
          should pass on /link, /unlink, /register, /unregister, and
          signup completion — those mutations only ever change ONE
          person's linked/membership status, so there's no reason to
          re-resolve everyone else too.
        - resolve_members=True: SLOW PATH, full re-resolution of every
          linked user. Reserved for /users initial setup and the loop's
          first tick after a restart, where the cache starts genuinely
          empty. Takes priority if both are somehow passed together."""
        config = await self.get_dashboard_config()
        if not config or not config.get("dashboard_channel_id") or not config.get("dashboard_message_id"):
            return

        try:
            jf_users = await _fetch_jellyfin_users()
        except Exception as e:
            logger.error(f"[JellyfinRegistryDashboard] Failed to fetch Jellyfin users, skipping this refresh: {type(e).__name__}: {e}")
            return

        links = await _get_all_links()

        if resolve_members:
            await self._refresh_member_cache_full(links)
        elif resolve_member_ids:
            for discord_id in resolve_member_ids:
                await self.update_member_cache_for(discord_id)

        new_snapshot = _registry_snapshot(jf_users, links)
        if not force and new_snapshot == self._last_snapshot:
            return

        self._last_snapshot = new_snapshot

        channel = self.bot.get_channel(config["dashboard_channel_id"])
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            return

        try:
            msg = await channel.fetch_message(config["dashboard_message_id"])
            dashboard_view = RegistryDashboardLayoutView(jf_users, links, self._member_cache, self._current_page)
            self._current_page = dashboard_view.page  # sync back in case it got clamped (e.g. after an unregister)
            await msg.edit(content=None, embeds=[], view=dashboard_view, allowed_mentions=NO_PINGS)
        except discord.NotFound:
            await _upsert_dashboard_config(TARGET_GUILD_ID, dashboard_channel_id=None, dashboard_message_id=None)
            self._dashboard_config_cache = None
        except discord.HTTPException as e:
            logger.error(f"[JellyfinRegistryDashboard] Failed to edit dashboard message: {type(e).__name__}: {e}")

    async def change_page(self, interaction: discord.Interaction, delta: int):
        """Owner-only pagination. Fetches fresh state and edits the message
        in place — deliberately bypasses the snapshot-diff check in
        refresh_dashboard, since a page change must always visibly update
        the message regardless of whether the underlying data changed."""
        try:
            jf_users = await _fetch_jellyfin_users()
        except Exception as e:
            return await interaction.followup.send(f"❌ Couldn't reach Jellyfin: `{type(e).__name__}: {e}`", ephemeral=True)

        links = await _get_all_links()
        total_pages = max(1, (len(links) + PAGE_SIZE - 1) // PAGE_SIZE)
        self._current_page = max(0, min(self._current_page + delta, total_pages - 1))

        dashboard_view = RegistryDashboardLayoutView(jf_users, links, self._member_cache, self._current_page)
        self._current_page = dashboard_view.page
        try:
            await interaction.edit_original_response(view=dashboard_view, allowed_mentions=NO_PINGS)
        except discord.HTTPException as e:
            logger.error(f"[JellyfinRegistryDashboard] Failed to edit dashboard message on page change: {type(e).__name__}: {e}")

    @tasks.loop(minutes=DASHBOARD_LOOP_MINUTES)
    async def registry_loop(self):
        # First tick after startup/reload does a full member-resolution
        # pass — the in-memory cache is empty at that point regardless of
        # what was linked before restart. Every tick after that skips
        # resolution as normal to avoid a Discord API call per linked user
        # every 5 minutes.
        need_resolve = not self._did_initial_resolve
        await self.refresh_dashboard(force=need_resolve, resolve_members=need_resolve)
        if need_resolve:
            self._did_initial_resolve = True

    @registry_loop.before_loop
    async def before_registry_loop(self):
        await self.bot.wait_until_ready()

    @registry_loop.error
    async def registry_loop_error(self, error: BaseException):
        # tasks.loop silently STOPS forever on an unhandled exception —
        # this is what was causing the dashboard to go stale for hours
        # after a single transient failure (a locked DB, a Jellyfin
        # timeout not already caught elsewhere, etc.). Log it and restart
        # the loop instead of letting it die permanently.
        logger.error(f"[JellyfinRegistryDashboard] registry_loop crashed: {type(error).__name__}: {error}", exc_info=error)
        if not self.registry_loop.is_running():
            logger.warning("[JellyfinRegistryDashboard] Restarting registry_loop after crash.")
            self.registry_loop.start()

    @app_commands.command(name="users", description="[Admin] Set up the persistent Jellyfin registry dashboard")
    async def users(self, interaction: discord.Interaction):
        if interaction.user.id != OWNER_DISCORD_ID:
            await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        config = await _get_dashboard_config(interaction.guild.id)
        view = DashboardConfigView(interaction.guild.id, config)
        content, embeds = await view.build_full_display()
        await interaction.followup.send(content=content, embeds=embeds, view=view, ephemeral=True)

    @users.error
    async def users_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(JellyfinRegistryDashboard(bot))