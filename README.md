# UsherEats Jellyfin Discord Bot

Discord bot for managing a private Jellyfin media server. Handles user registration, playback controls, stats, leaderboards, and automated media announcements.

## Cogs

### registry-commands.py
Core account management. Handles Jellyfin user creation, linking Discord accounts to Jellyfin accounts, signup tokens, password resets, username changes, and automated cleanup of inactive or left users.

| Command | Description |
|---------|-------------|
| `/jellyfin` | View your Jellyfin account details, reset password, or change username |
| `/leaveserver` | Voluntarily delete your own Jellyfin account |
| `/manage` | [Admin] Manage accounts, signup tokens, and left-user cleanup |
| `/exempt` | [Owner] Add/remove a user from the leave-deletion exemption list |

### registry-dashboard.py
Persistent, auto-refreshing dashboard showing all linked and unlinked Jellyfin accounts. Read-only display — never creates or deletes accounts.

| Command | Description |
|---------|-------------|
| `/users` | [Admin] Set up the persistent registry dashboard in a channel |

### dashboard.py
Live "Now Playing" dashboard. Shows who's currently watching what on Jellyfin in real time via a persistent, auto-updating embed.

| Command | Description |
|---------|-------------|
| `/dashboard` | Configure the live Now Playing dashboard channel and message |

### remote.py
Playback remote control. Lets users control their Jellyfin devices from Discord — play/pause, seek, stop, search, and send media to their TV.

| Command | Description |
|---------|-------------|
| `/remote` | Control playback on your Jellyfin devices, or send something to your TV |

### stats.py
Per-user watch statistics. Shows total watch time, recent history, and detailed breakdowns by media type pulled from Jellyfin's Playback Reporting plugin.

| Command | Description |
|---------|-------------|
| `/stats` | View a linked user's Jellyfin watch stats (self or by username) |

### leaderboard.py
Persistent daily/weekly/lifetime watch-time leaderboards. Auto-refreshing messages that rank users by hours watched.

| Command | Description |
|---------|-------------|
| `/daily` | Set the channel for the live leaderboard report |

### trending.py
Netflix-style trending lists. Shows the top 10 most-watched movies and shows over configurable time windows.

| Command | Description |
|---------|-------------|
| `/trending` | Top 10 movies and shows, ranked by watch count and unique viewers |

### newreleases.py
Automated new media announcements. Listens for Jellyfin library events via Tracearr SSE and posts rich embeds when new movies or series are added, with quality-check screenshots.

| Command | Description |
|---------|-------------|
| `/syncmedia` | [Owner] Re-fetch metadata and refresh all posted announcements |
| `/redoscreenshot` | [Owner] Replace the quality-check screenshot on an announcement |
| `/adoptannouncement` | [Owner] Track an existing announcement message |
| `/fixscreenshots` | [Owner] Adopt untracked announcements and screenshot missing ones |
| `Toggle Sync Lock` | Context menu — lock/unlock an announcement from auto-sync |

### poll.py
Download suggestion rounds. Users suggest media, then vote in Discord polls to decide what gets added to the library.

| Command | Description |
|---------|-------------|
| `/poll` | [Owner] Start a new suggestion round |
| `/pollcancel` | [Owner] Cancel the current round or delete the active poll |

### schedule.py
Scheduled message posting. Queue messages (Components V2 JSON) to post at a specific Sydney time.

| Command | Description |
|---------|-------------|
| `/schedule post` | Schedule a message for a specific date and time |
| `/schedule list` | Show upcoming scheduled messages |
| `/schedule cancel` | Cancel a scheduled message by number |

### peak_sessions.py
Tracks concurrent stream peaks. Records the highest number of simultaneous Jellyfin sessions each day.

| Command | Description |
|---------|-------------|
| `/peak` | [Owner] Show peak concurrent streams per day for the last 7 days |

### uptime.py
Server uptime monitor. Pings Jellyfin periodically and posts/updates an uptime status message showing current availability and response time.

### watchingnowroleadder.py
Automatic role assignment. Grants a "Watching" role to users actively streaming on Jellyfin and a "Browsing" role to users with active sessions, removing them when activity stops.

### errorhandler.py
Global error handler. Catches unhandled command errors, logs them to a designated channel, and provides user-friendly error messages. Also handles command-channel restriction hints.

## Shared Utilities (`utils/`)

### config.py
All shared constants — Jellyfin URLs, API keys, Discord IDs (roles, channels, guild), database paths, and embed limits.

### jellyfin_api.py
`JellyfinClient` class wrapping Jellyfin's REST API. Provides cached user lookups, account management (create/delete/rename/password), session and device queries, and Playback Reporting plugin access. Uses the bot's shared `aiohttp.ClientSession`.

### db.py
Two database patterns:
- **`Database` class** — persistent aiosqlite connection with WAL mode and serialized writes. Used by cogs that keep a long-lived connection (registry-commands, remote).
- **Ad-hoc helpers** (`run_query`, `run_write`, `run_script`) — open-close-per-call pattern for cogs with lighter DB needs.
