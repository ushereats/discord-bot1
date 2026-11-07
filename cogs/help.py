import discord
from discord.ext import commands
from discord import app_commands
import datetime
import psutil
import os


class HelpPaginatorView(discord.ui.View):
    def __init__(self, bot, embeds: list[discord.Embed], invoker_id: int):
        super().__init__(timeout=120)
        self.bot = bot
        self.embeds = embeds
        self.invoker_id = invoker_id
        self.current_page = 0

        self.prev_button.emoji = bot.left
        self.next_button.emoji = bot.right

        if len(embeds) <= 1:
            self.prev_button.disabled = True
            self.next_button.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            embed = discord.Embed(
                description=f"{self.bot.error_emoji}  Only the person who ran this command can use these buttons.",
                color=self.bot.yellow_color
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return False
        return True

    @discord.ui.button(style=discord.ButtonStyle.grey, custom_id="help_prev", row=0)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = (self.current_page - 1) % len(self.embeds)
        await interaction.response.edit_message(embed=self.embeds[self.current_page])

    @discord.ui.button(style=discord.ButtonStyle.red, emoji="🏠", custom_id="help_home", row=0)
    async def home_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = 0
        await interaction.response.edit_message(embed=self.embeds[self.current_page])

    @discord.ui.button(style=discord.ButtonStyle.grey, custom_id="help_next", row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = (self.current_page + 1) % len(self.embeds)
        await interaction.response.edit_message(embed=self.embeds[self.current_page])

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class helpcog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.process = psutil.Process(os.getpid())

    def get_bot_uptime(self):
        delta_uptime = datetime.datetime.now(datetime.timezone.utc) - self.bot.start_time
        hours, remainder = divmod(int(delta_uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        days, hours = divmod(hours, 24)
        return f'{days}d:{hours}h:{minutes}m:{seconds}s'

    def build_overview_embed(self, total_commands: int, total_pages: int):
        embed = discord.Embed(
            color=self.bot.embed_color,
            description=f"Welcome to **{self.bot.bot_name}**'s help menu. Use the buttons below to browse commands."
        )

        try:
            guild_count = len(self.bot.guilds)
            user_count = len(set(self.bot.get_all_members()))
        except Exception:
            guild_count = "n/a"
            user_count = "n/a"

        embed.add_field(
            name="Status",
            inline=True,
            value=(
                "```ansi\n"
                f"\u001b[1;36mUsers: {user_count}\n"
                f"Guilds: {guild_count}\n"
                f"Uptime: {self.get_bot_uptime()}\u001b[0m\n"
                "```"
            )
        )
        embed.add_field(
            name="Links",
            inline=True,
            value=(
                "```ansi\n"
                f"\u001b[1;36mCommands: {total_commands}\n"
                f"Latency: {round(self.bot.latency * 1000)}ms\n"
                f"Version: 3.10.10\u001b[0m\n"
                "```"
            )
        )
        embed.set_author(name=f"{self.bot.bot_name}'s Help Menu", icon_url=self.bot.user.display_avatar.url)
        embed.set_thumbnail(url=self.bot.user.display_avatar.url)
        embed.set_footer(text=f"Page 1/{total_pages}")
        return embed

    def get_command_status_emoji(self, cog, command_name: str):
        """Looks for an opt-in health convention on the cog:
          self.broken_commands = ["cmdname", ...]   -> red cross_emoji
          self.degraded_commands = ["cmdname", ...] -> yellow error_emoji
        Returns None if the cog doesn't flag this command as broken/degraded."""
        broken = getattr(cog, "broken_commands", [])
        degraded = getattr(cog, "degraded_commands", [])

        if command_name in broken:
            return self.bot.cross_emoji
        if command_name in degraded:
            return self.bot.error_emoji
        return None

    def build_command_pages(self, commands_per_page: int = 8):
        """Flat list of every slash command across every cog — no category
        grouping, just paginated straight through."""
        all_lines = []

        for cog_name, cog in self.bot.cogs.items():
            app_cmds = cog.get_app_commands()
            if not app_cmds:
                continue

            for c in app_cmds:
                status_emoji = self.get_command_status_emoji(cog, c.name)
                prefix = f"{status_emoji} " if status_emoji else ""
                all_lines.append((f"{prefix}`/{c.name}`", c.description or "No description."))

        embeds = []
        for i in range(0, len(all_lines), commands_per_page):
            chunk = all_lines[i:i + commands_per_page]
            embed = discord.Embed(
                title="Commands",
                color=self.bot.embed_color
            )
            for name, desc in chunk:
                embed.add_field(name=name, value=f"> {desc}", inline=True)
            embed.set_footer(text=f"Total Commands: {len(chunk)}")
            embeds.append(embed)

        return embeds

    @app_commands.command(name="help", description="View all commands, or get details on a specific one.")
    @app_commands.describe(command="Get detailed info on a specific command (optional).")
    async def help_command(self, interaction: discord.Interaction, command: str = None):
        await interaction.response.defer()

        if command is not None:
            target = self.bot.tree.get_command(command)
            if target is None:
                embed = discord.Embed(
                    description=f"{self.bot.error_emoji}  No command named **`{command}`** found.",
                    color=self.bot.yellow_color
                )
                return await interaction.followup.send(embed=embed, ephemeral=True)

            cog_name = target.binding.__class__.__name__ if target.binding else "Unknown"
            embed = discord.Embed(
                description=f"**Description:** {target.description or 'No description.'}",
                color=self.bot.embed_color
            )
            embed.set_author(name=f"Command: /{target.name}", icon_url=self.bot.user.display_avatar.url)
            embed.set_footer(text=f"Module: {cog_name}")
            return await interaction.followup.send(embed=embed, ephemeral=True)

        try:
            command_pages = self.build_command_pages()
            total_commands = sum(int(e.footer.text.split(": ")[1]) for e in command_pages)
            total_pages = 1 + len(command_pages)

            overview = self.build_overview_embed(total_commands, total_pages)
            all_pages = [overview] + command_pages

            for i, embed in enumerate(all_pages, start=1):
                base_footer = embed.footer.text or ""
                if i == 1:
                    embed.set_footer(text=f"Page {i}/{total_pages}")
                else:
                    embed.set_footer(text=f"Page {i}/{total_pages} • {base_footer}")

            view = HelpPaginatorView(self.bot, all_pages, interaction.user.id)
            await interaction.followup.send(embed=all_pages[0], view=view)

        except discord.HTTPException as e:
            embed = discord.Embed(
                description=f"{self.bot.cross_emoji}  Failed to send help menu: `{e}`",
                color=self.bot.red_color
            )
            await interaction.followup.send(embed=embed)
        except Exception as e:
            embed = discord.Embed(
                description=f"{self.bot.cross_emoji}  Unexpected error building help menu: `{e}`",
                color=self.bot.red_color
            )
            await interaction.followup.send(embed=embed)

    @help_command.error
    async def help_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        embed = discord.Embed(
            description=f"{self.bot.cross_emoji}  Unexpected error: `{type(error).__name__}: {error}`",
            color=self.bot.red_color
        )
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(helpcog(bot))