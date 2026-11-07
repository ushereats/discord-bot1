"""
Welcome Cog — automated join greetings, using the SAME JSON pipeline as
embeds.py: classic embeds, legacy (classic + action-row buttons), and
Components V2 are all accepted, auto-detected, and built via
prepare_message_payload() from embeds.py. This means the welcome template
can be ANY of the three formats embeds.py supports (including raw
Discord-client dumps with rawTitle/rawDescription/rawName/rawValue), and
the same validation/limit-checking (embed char limits, CV2's real 40-node
total, action-row rules, etc.) applies here too, instead of a separate,
weaker copy of those checks.

Placeholder variables ({member.mention}, {guild.name}, etc.) are substituted
into the RAW JSON STRING before it's ever parsed — this happens identically
regardless of which of the three formats the template uses, since it's pure
text substitution. After substitution the string is re-parsed as JSON and
handed to prepare_message_payload() exactly like /embed does.

NOTE: this file imports `prepare_message_payload` from `cogs.embed` (the
file at cogs/embed.py) — update this import if that file is ever renamed
or moved.
"""

import discord
from discord.ext import commands
from discord import app_commands
import json
import aiosqlite
import logging
import datetime
import os

from cogs.embed import prepare_message_payload

logger = logging.getLogger("welcome_cog")

os.makedirs("DATABASES", exist_ok=True)
DB_NAME = os.path.join("DATABASES", "welcome.db")


# --- SYSTEM DATABASE LAYER (aiosqlite — fully async) ---
async def run_query(query, params=(), fetch=False, fetchall=False):
    """Returns (result, error). error is None on success."""
    try:
        async with aiosqlite.connect(DB_NAME) as conn:
            cursor = await conn.execute(query, params)
            if fetchall:
                result = await cursor.fetchall()
            elif fetch:
                result = await cursor.fetchone()
            else:
                result = None
            await cursor.close()
            await conn.commit()
            return result, None
    except aiosqlite.Error as e:
        logger.error(f"DB error on query [{query}]: {e}")
        return None, str(e)


async def init_db():
    await run_query(
        "CREATE TABLE IF NOT EXISTS welcome_config ("
        "guild_id INTEGER PRIMARY KEY, channel_id INTEGER, embed TEXT, enabled INTEGER DEFAULT 0)"
    )


# --- LIGHTWEIGHT JSON SYNTAX CHECK (structure/limit validation now lives in
# embeds.py's prepare_message_payload — no need to duplicate it here) ---
def validate_json_syntax(raw: str):
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON syntax at line {e.lineno}, column {e.colno}: {e.msg}"
    if not isinstance(parsed, dict):
        return None, f"JSON must be an object (`{{...}}`), not a {type(parsed).__name__}."
    return parsed, None


def safe_reparse_after_substitution(processed_string: str):
    try:
        parsed = json.loads(processed_string)
    except json.JSONDecodeError as e:
        return None, (
            "JSON became invalid after inserting live values "
            f"(likely a member/guild name containing a quote or backslash): {e.msg}"
        )
    if not isinstance(parsed, dict):
        return None, "Processed JSON is not an object after variable substitution."
    return parsed, None


# --- HELPER: JOIN POSITION ---
def get_join_position(member: discord.Member) -> str:
    """Returns this member's approximate join position (e.g. '1,204th')
    based on how many CURRENT members joined before them. This shifts if
    people leave the server — it is not a permanent historical record."""
    if member.joined_at is None:
        return "Unknown"

    earlier_count = sum(
        1 for m in member.guild.members
        if m.joined_at is not None and m.joined_at < member.joined_at
    )
    position = earlier_count + 1

    if 10 <= position % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(position % 10, "th")

    return f"{position:,}{suffix}"


def get_account_age_days(member: discord.Member) -> str:
    """Days between account creation and joining this server."""
    if member.joined_at is None:
        return "Unknown"
    delta = member.joined_at - member.created_at
    return str(delta.days)


# --- HELPER ROUTINE: PLACEHOLDER PARSER ---
def parse_welcome_variables(template_str: str, member: discord.Member) -> str:
    """Replaces every supported placeholder with a live value pulled from the
    member/guild at the moment this is called. Values that can legitimately
    be unset (banners, vanity URLs, server descriptions) fall back to a
    human-readable 'None set' string instead of an empty/broken result.

    This runs on the RAW TEMPLATE STRING, before any JSON parsing — so it
    works identically no matter which of the three message formats
    (classic, legacy, Components V2) the template actually is."""
    if not template_str:
        return ""

    guild = member.guild
    now = datetime.datetime.now(datetime.timezone.utc)

    try:
        replacements = {
            # --- Member: identity ---
            "{member}": member.mention,
            "{member.mention}": member.mention,
            "{member.name}": member.name,
            "{member.display_name}": member.display_name,
            "{member.global_name}": member.global_name if member.global_name else member.name,
            "{member.id}": str(member.id),
            "{member.discriminator}": getattr(member, "discriminator", "0") or "0",
            "{member.avatar}": str(member.display_avatar.url),
            "{member.banner}": str(member.banner.url) if getattr(member, "banner", None) else "None set",
            "{member.is_bot}": "Yes" if member.bot else "No",

            # --- Member: timing ---
            "{member.created_at}": discord.utils.format_dt(member.created_at, style="F"),
            "{member.joined_at}": discord.utils.format_dt(member.joined_at, style="F") if member.joined_at else "N/A",
            "{member.join.position}": get_join_position(member),
            "{member.account_age}": get_account_age_days(member),

            # --- Member: roles ---
            "{member.top_role}": member.top_role.mention if member.top_role != guild.default_role else "No Custom Roles",
            "{member.top_role.name}": member.top_role.name if member.top_role != guild.default_role else "None",
            "{member.role_count}": str(max(len(member.roles) - 1, 0)),  # exclude @everyone

            # --- Guild: identity ---
            "{guild}": guild.name,
            "{guild.name}": guild.name,
            "{guild.id}": str(guild.id),
            "{guild.icon}": str(guild.icon.url) if guild.icon else "None set",
            "{guild.banner}": str(guild.banner.url) if guild.banner else "None set",
            "{guild.description}": guild.description if guild.description else "None set",
            "{guild.vanity_url}": f"discord.gg/{guild.vanity_url_code}" if getattr(guild, "vanity_url_code", None) else "None set",

            # --- Guild: counts ---
            "{guild.count}": str(guild.member_count),
            "{guild.count.humans}": str(sum(1 for m in guild.members if not m.bot)),
            "{guild.count.bots}": str(sum(1 for m in guild.members if m.bot)),
            "{guild.channel_count}": str(len(guild.channels)),
            "{guild.role_count}": str(len(guild.roles)),
            "{guild.emoji_count}": str(len(guild.emojis)),

            # --- Guild: ownership/boost ---
            "{guild.owner_id}": str(guild.owner_id),
            "{guild.owner_mention}": f"<@{guild.owner_id}>",
            "{guild.boost_level}": str(guild.premium_tier),
            "{guild.boost_count}": str(guild.premium_subscription_count),
            "{guild.created_at}": discord.utils.format_dt(guild.created_at, style="F"),

            # --- Time (current moment, not member/guild specific) ---
            "{date}": now.strftime("%B %d, %Y"),
            "{time}": now.strftime("%I:%M %p UTC"),
            "{timestamp}": discord.utils.format_dt(now, style="F"),
        }
    except AttributeError as e:
        logger.error(f"Attribute resolution failure while building placeholders: {e}")
        raise

    processed = template_str
    for placeholder, live_value in sorted(replacements.items(), key=lambda kv: -len(kv[0])):
        processed = processed.replace(placeholder, live_value)
    return processed


def render_welcome_payload(raw_template: str, member: discord.Member):
    """The single shared pipeline used by BOTH the live preview and the real
    on_member_join send: substitute variables -> re-parse as JSON -> run
    through embeds.py's prepare_message_payload() (handles format detection,
    normalization, and building for all three formats). Returns
    (result_dict_or_None, error_string_or_None) — result_dict is exactly
    what prepare_message_payload() returns (kind: classic/legacy/cv2/error)."""
    try:
        processed_string = parse_welcome_variables(raw_template, member)
    except AttributeError as e:
        return None, f"Failed reading member/guild attributes: `{e}`"

    parsed, reparse_error = safe_reparse_after_substitution(processed_string)
    if reparse_error:
        return None, reparse_error

    result = prepare_message_payload(parsed)
    if result["kind"] == "error":
        return None, result["message"]

    return result, None


async def send_rendered_payload(destination, result: dict):
    """Sends a prepare_message_payload() result to a destination that
    supports .send() (a TextChannel) or .edit() semantics — used for both
    the real welcome message and (via a thin wrapper) the live preview.
    Returns the sent discord.Message."""
    if result["kind"] == "cv2":
        return await destination.send(view=result["view"])
    elif result["kind"] == "legacy":
        return await destination.send(content=result["content"], embeds=result["embeds"], view=result["view"])
    else:
        return await destination.send(content=result["content"], embeds=result["embeds"])


# --- MODAL: JSON SPECIFICATION EDITOR ---
class WelcomeJSONModal(discord.ui.Modal):
    def __init__(self, callback_view):
        super().__init__(title="View / Edit Welcome JSON Template")
        self.callback_view = callback_view

        current = callback_view.config
        default_json = current.get("embed") if current and current.get("embed") else """{
  "content": "Welcome {member.mention}!",
  "embed": {
    "title": "Welcome to {guild.name}",
    "description": "You are member #{guild.count}"
  }
}"""

        self.json_input = discord.ui.TextInput(
            label="Current JSON (edit and submit to save)",
            style=discord.TextStyle.long,
            default=default_json[:4000],
            required=True,
            max_length=4000
        )
        self.add_item(self.json_input)

    async def on_submit(self, interaction: discord.Interaction):
        raw_json = self.json_input.value
        bot = interaction.client

        # Early validation on the RAW TEMPLATE (placeholders still in place —
        # they're just plain text inside string values, so this validates
        # fine without needing live member/guild data). Reuses embeds.py's
        # full validator (classic/legacy/CV2, all limits) instead of a
        # separate, weaker copy of those checks.
        parsed, syntax_error = validate_json_syntax(raw_json)
        if syntax_error:
            embed = discord.Embed(
                description=f"{bot.cross_emoji}  **Broken JSON — you entered invalid JSON.**\n```\n{syntax_error}\n```Check for missing commas, unmatched braces/brackets, or unescaped quotes.",
                color=bot.red_color
            )
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        validation_result = prepare_message_payload(parsed)
        if validation_result["kind"] == "error":
            embed = discord.Embed(
                description=f"{bot.cross_emoji}  **JSON is syntactically valid but structurally invalid:**\n```\n{validation_result['message']}\n```",
                color=bot.red_color
            )
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        guild_id = interaction.guild.id
        row, db_error = await run_query("SELECT channel_id FROM welcome_config WHERE guild_id=?", (guild_id,), fetch=True)
        if db_error:
            embed = discord.Embed(
                description=f"{bot.cross_emoji}  Database error while reading existing config: `{db_error}`",
                color=bot.red_color
            )
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        if row:
            _, db_error = await run_query("UPDATE welcome_config SET embed=? WHERE guild_id=?", (raw_json, guild_id))
        else:
            sys_ch = interaction.guild.system_channel.id if interaction.guild.system_channel else interaction.channel.id
            _, db_error = await run_query(
                "INSERT INTO welcome_config (guild_id, channel_id, embed, enabled) VALUES (?, ?, ?, 1)",
                (guild_id, sys_ch, raw_json)
            )

        if db_error:
            embed = discord.Embed(
                description=f"{bot.cross_emoji}  Database error while saving config: `{db_error}`",
                color=bot.red_color
            )
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        await self.callback_view.update_panel(interaction)
        await self.callback_view.update_preview(interaction)


# --- INPUT MENUS: DYNAMIC CHANNEL SELECTOR ---
class WelcomeChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, callback_view):
        super().__init__(
            placeholder="Select notification feed text channel...",
            channel_types=[discord.ChannelType.text],
            row=0
        )
        self.callback_view = callback_view

    async def callback(self, interaction: discord.Interaction):
        bot = interaction.client
        try:
            raw_channel = self.values[0]
            channel = interaction.guild.get_channel(raw_channel.id)

            if channel is None:
                embed = discord.Embed(
                    description=f"{bot.cross_emoji}  Couldn't resolve that channel — try selecting it again.",
                    color=bot.red_color
                )
                return await interaction.response.send_message(embed=embed, ephemeral=True)

            guild_id = interaction.guild.id

            perms = channel.permissions_for(interaction.guild.me)
            if not perms.send_messages or not perms.embed_links:
                embed = discord.Embed(
                    description=f"{bot.cross_emoji}  I lack `Send Messages` or `Embed Links` permission in {channel.mention}. Grant those permissions before selecting this channel.",
                    color=bot.red_color
                )
                return await interaction.response.send_message(embed=embed, ephemeral=True)

            row, db_error = await run_query("SELECT embed FROM welcome_config WHERE guild_id=?", (guild_id,), fetch=True)
            if db_error:
                embed = discord.Embed(
                    description=f"{bot.cross_emoji}  Database error while reading config: `{db_error}`",
                    color=bot.red_color
                )
                return await interaction.response.send_message(embed=embed, ephemeral=True)

            if row:
                _, db_error = await run_query("UPDATE welcome_config SET channel_id=? WHERE guild_id=?", (channel.id, guild_id))
            else:
                default_json = '{"content": "Welcome {member} to {guild}!"}'
                _, db_error = await run_query(
                    "INSERT INTO welcome_config (guild_id, channel_id, embed, enabled) VALUES (?, ?, ?, 1)",
                    (guild_id, channel.id, default_json)
                )

            if db_error:
                embed = discord.Embed(
                    description=f"{bot.cross_emoji}  Database error while saving channel: `{db_error}`",
                    color=bot.red_color
                )
                return await interaction.response.send_message(embed=embed, ephemeral=True)

            await interaction.response.defer()
            await self.callback_view.update_panel(interaction)
            await self.callback_view.update_preview(interaction)

        except Exception as e:
            logger.error(f"[WelcomeChannelSelect] Unhandled exception: {e}", exc_info=True)
            embed = discord.Embed(
                description=f"{bot.cross_emoji}  Something broke selecting the channel: `{type(e).__name__}: {e}`",
                color=bot.red_color
            )
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(embed=embed, ephemeral=True)
                else:
                    await interaction.response.send_message(embed=embed, ephemeral=True)
            except discord.HTTPException:
                pass


# --- DASHBOARD CONTROL INTERFACE ---
# NOTE: the interactive control panel (channel select + buttons) and the
# LIVE PREVIEW are now TWO SEPARATE MESSAGES. Discord doesn't allow mixing
# Components V2 with classic embeds/buttons in one message, and a legacy
# (buttons+embeds) preview needs its own `view` slot — which would collide
# with the panel's own control buttons if merged into one message the way
# the old classic-only version did. Splitting them is what makes ALL THREE
# formats work as the live preview without format conflicts.
class WelcomeControlPanel(discord.ui.View):
    def __init__(self, bot, guild_id, member_for_preview):
        super().__init__(timeout=300.0)
        self.bot = bot
        self.guild_id = guild_id
        self.member_for_preview = member_for_preview
        self.config = {}
        self.preview_message: discord.Message | None = None
        self.add_item(WelcomeChannelSelect(self))

    async def get_guild_data(self):
        row, db_error = await run_query(
            "SELECT channel_id, embed, enabled FROM welcome_config WHERE guild_id=?",
            (self.guild_id,), fetch=True
        )
        if db_error:
            logger.error(f"get_guild_data DB error: {db_error}")
            return {"channel_id": None, "embed": None, "enabled": 0, "db_error": db_error}
        if row:
            return {"channel_id": row[0], "embed": row[1], "enabled": row[2], "db_error": None}
        return {"channel_id": None, "embed": None, "enabled": 0, "db_error": None}

    def build_panel_embed(self, data):
        status_text = f"{self.bot.check_emoji} **ENABLED**" if data["enabled"] == 1 else f"{self.bot.cross_emoji} **DISABLED**"
        channel_mention = f"<#{data['channel_id']}>" if data["channel_id"] else "`Not Assigned`"
        msg_status = f"{self.bot.check_emoji} *Configured & Validated*" if data["embed"] else f"{self.bot.error_emoji} *No JSON Set*"

        embed = discord.Embed(
            title="⚙️ Welcome Module Management Console",
            description=(
                "Manage automated greetings from this interface control deck.\n"
                "The live preview below (as its own message) always mirrors exactly "
                "what a real member will see on join — classic embed, legacy buttons, "
                "or Components V2, whichever your JSON uses."
            ),
            color=self.bot.green_color if data["enabled"] == 1 else self.bot.red_color
        )
        embed.add_field(name="System Status", value=status_text, inline=True)
        embed.add_field(name="Target Channel", value=channel_mention, inline=True)
        embed.add_field(name="Configuration Status", value=msg_status, inline=False)
        if data.get("db_error"):
            embed.add_field(name=f"{self.bot.error_emoji} Database Warning", value=f"```{data['db_error']}```", inline=False)
        embed.set_footer(text=f"Guild ID Node: {self.guild_id}")
        return embed

    async def build_panel_display(self):
        """Returns (content, embeds) for the CONTROL PANEL message only —
        this message always keeps `self` as its view (channel select +
        buttons), so it always stays classic-embed-based regardless of what
        format the welcome template itself uses."""
        data = await self.get_guild_data()
        self.config = data
        return None, [self.build_panel_embed(data)]

    async def build_preview_result(self):
        """Returns a prepare_message_payload()-shaped result dict (or an
        error dict) for the LIVE PREVIEW message, rendered with real data
        from self.member_for_preview exactly as a genuine join would be."""
        data = self.config or await self.get_guild_data()
        if not data.get("embed"):
            return {"kind": "classic", "content": None, "embeds": [
                discord.Embed(
                    description=f"{self.bot.error_emoji}  No JSON template set yet — use **View / Edit JSON** below.",
                    color=self.bot.yellow_color
                )
            ]}

        result, error = render_welcome_payload(data["embed"], self.member_for_preview)
        if error:
            return {"kind": "classic", "content": None, "embeds": [
                discord.Embed(description=f"{self.bot.cross_emoji}  {error}", color=self.bot.red_color)
            ]}
        return result

    async def send_preview(self, channel: discord.abc.Messageable):
        """Initial send of the preview message — called once, right after
        the panel itself is sent."""
        result = await self.build_preview_result()
        self.preview_message = await send_rendered_payload(channel, result)

    async def update_panel(self, interaction: discord.Interaction):
        content, embeds = await self.build_panel_display()
        try:
            await interaction.edit_original_response(content=content, embeds=embeds, view=self)
        except discord.NotFound:
            logger.warning("Original panel message not found — likely deleted or expired.")
        except discord.HTTPException as e:
            logger.error(f"Failed to edit panel: {e}")

    async def update_preview(self, interaction: discord.Interaction):
        """Re-renders and edits the SEPARATE preview message in place,
        switching cleanly between classic/legacy/CV2 as needed (clearing
        content/embeds when switching TO Components V2, same rule
        embeds.py's own edit path follows)."""
        if self.preview_message is None:
            return
        result = await self.build_preview_result()
        try:
            if result["kind"] == "cv2":
                await self.preview_message.edit(content=None, embeds=[], view=result["view"])
            elif result["kind"] == "legacy":
                await self.preview_message.edit(
                    content=result["content"] if result["content"] is not None else "",
                    embeds=result["embeds"],
                    view=result["view"]
                )
            else:
                await self.preview_message.edit(
                    content=result["content"] if result["content"] is not None else "",
                    embeds=result["embeds"],
                    view=None
                )
        except discord.NotFound:
            logger.warning("Preview message not found — likely deleted or expired.")
            self.preview_message = None
        except discord.HTTPException as e:
            logger.error(f"Failed to edit preview message: {e}")

    @discord.ui.button(label="View / Edit JSON", style=discord.ButtonStyle.secondary, row=1)
    async def edit_json_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(WelcomeJSONModal(self))
        except discord.InteractionResponded:
            pass
        except discord.HTTPException as e:
            embed = discord.Embed(
                description=f"{self.bot.cross_emoji}  Failed to open editor: `{e}`",
                color=self.bot.red_color
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="Toggle System Status", style=discord.ButtonStyle.primary, row=1)
    async def toggle_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.get_guild_data()
        new_state = 0 if data["enabled"] == 1 else 1

        _, db_error = await run_query("UPDATE welcome_config SET enabled=? WHERE guild_id=?", (new_state, self.guild_id))
        if db_error:
            embed = discord.Embed(
                description=f"{self.bot.cross_emoji}  Database error while toggling status: `{db_error}`",
                color=self.bot.red_color
            )
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        await interaction.response.defer()
        await self.update_panel(interaction)

    @discord.ui.button(label="Variable Tags List", style=discord.ButtonStyle.danger, row=1)
    async def help_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="📋 Universal Parsing Variable Tags",
            color=self.bot.embed_color,
            description=(
                "Inject these strings inside your values to dynamically pull active profile and server metrics.\n\n"
                "Works the same regardless of format — classic embed, legacy buttons, or Components V2. "
                "Placeholders are substituted as plain text before the JSON is parsed, so they work anywhere "
                "a string value appears (embed description, a TextDisplay's content, a button label, etc.)."
            )
        )
        embed.add_field(
            name="👤 Member — Identity",
            value=(
                "`{member}` / `{member.mention}` — Mentions the joining user\n"
                "`{member.name}` — Account username\n"
                "`{member.display_name}` — Server nickname\n"
                "`{member.global_name}` — Discord global display name\n"
                "`{member.id}` — User ID\n"
                "`{member.discriminator}` — Legacy 4-digit tag\n"
                "`{member.avatar}` — Avatar URL\n"
                "`{member.banner}` — Profile banner URL (if set)\n"
                "`{member.is_bot}` — Yes/No"
            ),
            inline=False
        )
        embed.add_field(
            name="🕒 Member — Timing",
            value=(
                "`{member.created_at}` — Account creation date\n"
                "`{member.joined_at}` — Server join date\n"
                "`{member.join.position}` — Join rank among current members\n"
                "`{member.account_age}` — Account age in days at time of joining"
            ),
            inline=False
        )
        embed.add_field(
            name="🎖️ Member — Roles",
            value=(
                "`{member.top_role}` — Mention of highest role\n"
                "`{member.top_role.name}` — Name of highest role\n"
                "`{member.role_count}` — Number of roles held"
            ),
            inline=False
        )
        embed.add_field(
            name="🏰 Guild — Identity",
            value=(
                "`{guild}` / `{guild.name}` — Server name\n"
                "`{guild.id}` — Server ID\n"
                "`{guild.icon}` — Server icon URL\n"
                "`{guild.banner}` — Server banner URL (if set)\n"
                "`{guild.description}` — Server description (Community servers)\n"
                "`{guild.vanity_url}` — Vanity invite link (if set)"
            ),
            inline=False
        )
        embed.add_field(
            name="🔢 Guild — Counts",
            value=(
                "`{guild.count}` — Total member count\n"
                "`{guild.count.humans}` — Human members only\n"
                "`{guild.count.bots}` — Bot accounts only\n"
                "`{guild.channel_count}` — Total channels\n"
                "`{guild.role_count}` — Total roles\n"
                "`{guild.emoji_count}` — Total custom emojis"
            ),
            inline=False
        )
        embed.add_field(
            name="👑 Guild — Ownership/Boost",
            value=(
                "`{guild.owner_id}` — Owner's user ID\n"
                "`{guild.owner_mention}` — Mention of the owner\n"
                "`{guild.boost_level}` — Server boost tier (0-3)\n"
                "`{guild.boost_count}` — Active boost count\n"
                "`{guild.created_at}` — When the server was created"
            ),
            inline=False
        )
        embed.add_field(
            name="🕐 Time",
            value=(
                "`{date}` — Today's date\n"
                "`{time}` — Current time\n"
                "`{timestamp}` — Dynamic Discord timestamp of now"
            ),
            inline=False
        )
        embed.add_field(
            name=f"{self.bot.error_emoji} Warning",
            value="Values containing double quotes (`\"`) or backslashes (`\\`) can break the JSON after substitution. Avoid nicknames/values with these characters in critical templates, or the welcome message will silently fail.",
            inline=False
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


# --- MAIN SYSTEM INTEGRATION COG ---
class welcome(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.category = "Server Setup"

    async def cog_load(self):
        # Table creation moved here from __init__ — aiosqlite connections
        # can only be opened from inside a running event loop.
        await init_db()

    @app_commands.command(name="welcome", description="Open the welcome system module control dashboard panel.")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def welcome_command(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        if interaction.guild is None:
            embed = discord.Embed(
                description=f"{self.bot.cross_emoji}  This command only works inside a server.",
                color=self.bot.red_color
            )
            return await interaction.followup.send(embed=embed, ephemeral=True)

        guild_id = interaction.guild.id
        panel_view = WelcomeControlPanel(self.bot, guild_id, interaction.user)

        data = await panel_view.get_guild_data()
        if data.get("db_error"):
            embed = discord.Embed(
                description=f"{self.bot.cross_emoji}  Database error while loading config: `{data['db_error']}`",
                color=self.bot.red_color
            )
            return await interaction.followup.send(embed=embed, ephemeral=True)

        content, embeds = await panel_view.build_panel_display()
        await interaction.followup.send(content=content, embeds=embeds, view=panel_view, ephemeral=True)

        preview_result = await panel_view.build_preview_result()
        if preview_result["kind"] == "cv2":
            preview_msg = await interaction.followup.send(view=preview_result["view"], ephemeral=True, wait=True)
        elif preview_result["kind"] == "legacy":
            preview_msg = await interaction.followup.send(
                content=preview_result["content"], embeds=preview_result["embeds"],
                view=preview_result["view"], ephemeral=True, wait=True
            )
        else:
            preview_msg = await interaction.followup.send(
                content=preview_result["content"], embeds=preview_result["embeds"],
                ephemeral=True, wait=True
            )
        panel_view.preview_message = preview_msg

    @welcome_command.error
    async def welcome_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            msg = f"{self.bot.cross_emoji}  You need `Manage Channels` permission to use this command."
        elif isinstance(error, app_commands.CommandInvokeError):
            logger.error(f"welcome command invoke error: {error.original}")
            msg = f"{self.bot.cross_emoji}  Unexpected error: `{error.original}`"
        else:
            logger.error(f"welcome command error: {error}")
            msg = f"{self.bot.cross_emoji}  Unexpected error: `{error}`"

        embed = discord.Embed(description=msg, color=self.bot.red_color)

        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        guild_id = member.guild.id
        row, db_error = await run_query(
            "SELECT channel_id, embed, enabled FROM welcome_config WHERE guild_id=?",
            (guild_id,), fetch=True
        )

        if db_error:
            logger.error(f"[Welcome Engine] DB read failure for guild {guild_id}: {db_error}")
            return

        if not row or row[2] != 1:
            return

        channel_id, raw_string_template = row[0], row[1]
        if not channel_id or not raw_string_template:
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            logger.warning(f"[Welcome Engine] Configured channel {channel_id} not found in guild {guild_id}.")
            return

        perms = channel.permissions_for(member.guild.me)
        if not perms.send_messages or not perms.embed_links:
            logger.warning(f"[Welcome Engine] Missing permissions in channel {channel_id}, guild {guild_id}.")
            return

        result, error = render_welcome_payload(raw_string_template, member)
        if error:
            logger.error(f"[Welcome Engine] {error} (guild {guild_id})")
            return

        try:
            await send_rendered_payload(channel, result)
        except discord.Forbidden:
            logger.error(f"[Welcome Engine] Forbidden sending in channel {channel_id}, guild {guild_id}.")
        except discord.HTTPException as e:
            logger.error(f"[Welcome Engine] HTTP error sending welcome in guild {guild_id}: {e}")
        except Exception as e:
            logger.error(f"[Welcome Engine] Unexpected failure in guild {guild_id}: {e}")


async def setup(bot) -> None:
    await bot.add_cog(welcome(bot))