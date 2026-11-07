"""
New Media Announcer Cog (Tracearr SSE-driven)
  Watches for new items added to the Jellyfin library and posts a
  Components V2 announcement in CHANNEL_ID.

  - New episodes are grouped by series: several episodes of the same show
    added close together (e.g. a whole season dump) become ONE message
    listing them, not one message per episode.
  - Movies get their own message each.
  - Shows a quality badge next to the title — e.g. `4K HDR`, `1080p SDR`
    — read directly from Jellyfin's MediaStreams for the item. No file
    size shown.
  - Every announcement carries an owner-only "Add Quality Check" button,
    rendered in its own Container beneath the announcement Container.
    Clicking it opens a modal for an image URL; submitting adds that
    image to the message as a gallery item (appended after the footer)
    and removes the button.
  - /syncmedia: [Owner] re-fetches metadata for EVERY stored announcement
    and edits each one in place — no message_id needed. Useful when
    metadata wasn't available yet at scan time, or in bulk after a
    library-wide change. The original "Added X ago" timestamp and any
    quality-check image already added on each message are both preserved
    across the sync.

    A message confirmed genuinely gone (discord.NotFound on the edit) has
    its row DELETED from the database — it can never sync successfully
    again by definition, so keeping the row around forever is pure
    clutter (confirmed in production: 292 accumulated rows with no
    pruning at all before this, 75% of one batch turning out to be
    long-dead messages). A message whose underlying Jellyfin items are
    gone but which itself is still a real, visible message is treated
    differently — that row is KEPT (not pruned), since the announcement
    is still valid, it just can't be live-refreshed anymore.

    Shows LIVE progress in a single Components V2 message, edited as it
    goes rather than a silent loop with one summary at the end — each
    announcement gets its own status line: ⏳ Checking → 🔧 Fixed
    (something was actually different) or ➖ Nothing changed (a fresh
    signature — title/year/quality/episode-list fingerprint — matched
    what was stored from the last sync, so this was a genuine no-op, not
    a guess), plus 🔒 Locked, ⚠️ Missing items, 🗑️ Missing message, or
    ❌ Failed. Beyond SYNC_PROGRESS_LIST_CAP records the display becomes
    a scrolling window of the most recently processed entries rather
    than trying to list every single one — this table has no pruning,
    so it can hold more records than safely fit in one message.

    TWO independent rate-limit-safety mechanisms, not one: SYNC_PACE_SECONDS
    paces the actual per-announcement Discord edits (each is now a single
    get_partial_message().edit() call, not fetch_message()+edit() — that
    halving is what actually fixed /syncmedia occasionally hitting
    Discord's rate limit despite pacing; the pacing itself was always
    correct, there were just twice as many calls per record as
    necessary). SYNC_PROGRESS_UPDATE_MIN_INTERVAL_SECONDS separately
    paces edits to the LIVE PROGRESS message itself — without this,
    updating it after every single record would just move the exact same
    rate-limit problem onto a different message.
  - "Toggle Sync Lock" (message context menu, right-click → Apps): [Owner]
    marks a single announcement as locked, so /syncmedia skips it forever
    (or until toggled again) instead of overwriting it with freshly
    rebuilt Jellyfin data. For when an announcement's auto-generated data
    is wrong or incomplete and gets manually fixed (via /embededit or by
    hand) — without this, the very next /syncmedia run would silently
    revert that manual fix back to the auto-generated version.
  - /redoscreenshot <message_id>: [Owner] Adds a quality-check screenshot
    to an existing announcement that doesn't have one yet, or replaces
    the existing one with a freshly grabbed frame at a new random point
    (so the image is visibly different, not a re-fetch of the same shot)
    if it already has one. NOT gated by AUTO_QUALITY_SCREENSHOT_ENABLED
    — that flag only controls the automatic pipeline; this is an
    explicit manual action and works regardless of the toggle.
  - /adoptannouncement <message_id> <item_id>: [Owner] Registers an
    existing announcement message that has NO database row (posted, but
    the record step never ran) so every other command — /redoscreenshot,
    /syncmedia, the Quality Check button — can act on it again. Manual
    fallback only — AUTO-ADOPT below normally handles this on its own.

  AUTO-ADOPT: every screenshot sweep (periodic AND /fixscreenshots) first
  scans the last ADOPT_SCAN_LIMIT messages in CHANNEL_ID for this bot's
  own announcements that have no DB row, and registers them. The
  Jellyfin ID is read straight from the poster thumbnail URL already on
  the message (/Items/<id>/Images/Primary) — for a movie that IS the
  movie's item ID; for a series it's the SeriesId, and the episode item
  IDs are resolved by matching the message's `SxEy` codes against that
  series' episode list. An existing screenshot on the message is
  detected and recorded, so it isn't grabbed again. Messages younger
  than ADOPT_MIN_AGE_SECONDS are skipped so a post still in progress
  isn't adopted out from under itself.

  FLUSH TASK ISOLATION: the debounce timer (_debounce_task) and the
  actual posting work (_announce_items) run as SEPARATE tasks. queue_item()
  cancels and restarts the timer on every incoming event, which is
  correct while it's still waiting — but it used to be the SAME task
  that then went on to post, so a late event (Jellyfin fires
  library.item.added for Person/Studio/Genre records created as side
  effects of a real item) arriving mid-post cancelled the post itself.
  Confirmed in production: a movie message landed on Discord, but the
  cancel hit before _record_announcement() ran, leaving no DB row and no
  screenshot, with nothing logged. The posting work is now spawned into
  its own task the instant the timer fires, which nothing ever cancels
  except cog_unload.

  AUTO QUALITY-CHECK SCREENSHOT: when AUTO_QUALITY_SCREENSHOT_ENABLED,
  every new movie announcement and every brand-new (not edited) series
  announcement automatically gets a real frame grabbed from Jellyfin's
  direct stream via a bundled ffmpeg binary (imageio-ffmpeg — this
  container has no system ffmpeg at all, so imageio-ffmpeg's own
  downloaded-at-pip-install-time binary is what actually runs the grab,
  not any OS-level ffmpeg), fed through the bot's own aiohttp session
  rather than opened by ffmpeg itself (see _grab_quality_screenshot for
  why). Seeks to a random point within
  SCREENSHOT_WINDOW_START_SECONDS-SCREENSHOT_WINDOW_END_SECONDS — small
  and bounded on purpose, both to keep every grab fast regardless of the
  source's bitrate/length and so /redoscreenshot's re-grabs land on a
  visibly different frame each time. The manual "Add Quality Check"
  button stays available as a fallback either way — if the auto-grab
  fails for any reason (ffmpeg unavailable, stream unreachable, a
  timeout), nothing breaks, the button just remains for a manual swap-in
  exactly as if this feature didn't exist for that one item. A series
  announcement only gets ONE auto-grab, on its FIRST post — not
  re-attempted on every later edit as the rollup window extends, since
  one representative frame is enough and re-grabbing on every edit would
  add real latency for no benefit.

  RELIABILITY LAYER — a screenshot grab depends on several live things
  cooperating (network, Jellyfin's transcoder, ffmpeg, Discord's upload
  API), so "never fails" isn't realistic; what's built instead is
  "never fails PERMANENTLY":
    - _grab_quality_screenshot_with_retry: up to SCREENSHOT_RETRY_ATTEMPTS
      tries per grab, jittered delay between them (SCREENSHOT_RETRY_JITTER_FRACTION)
      so a batch of simultaneous failures doesn't retry in lockstep and
      pile onto Jellyfin again at the same instant.
    - screenshot_sweep_loop: a background pass every
      SCREENSHOT_SWEEP_INTERVAL_MINUTES that finds every announcement
      still missing a screenshot and tries again — this is what turns a
      failure that survived even the retries into a temporary one
      instead of a permanent one, with nobody needing to notice and run
      /redoscreenshot by hand.
    - Circuit breaker (SCREENSHOT_CIRCUIT_BREAKER_THRESHOLD /
      SCREENSHOT_CIRCUIT_BREAKER_COOLDOWN_SECONDS): if several attempts
      fail in a row, that's a signal Jellyfin itself is struggling right
      now, not that we're just unlucky — pauses ALL automatic attempts
      (on-post and the sweep) for a cooldown instead of continuing to
      hammer an already-strained server with retries likely to fail
      anyway. Manual /redoscreenshot and /testscreenshot are NOT gated
      by this — an admin explicitly asking for one right now should
      still be able to try.
    - /fixscreenshots: [Owner] force-runs the sweep immediately for
      every announcement currently missing a screenshot, instead of
      waiting for the next periodic pass — bypasses the circuit breaker
      entirely, since this is a deliberate one-off request, not the
      unattended automatic path the breaker exists to protect against.

  SERIES ROLLUP WINDOW: ROLLING, not fixed. When the first episode of a
  series is announced, that announcement is "open" for
  SERIES_ROLLUP_HOURS from that moment. Any further episode of the SAME
  series that arrives while the window is still open edits the same
  message in place AND extends the window another SERIES_ROLLUP_HOURS
  from THAT moment — so a season that trickles in over many hours (or
  days, if new episodes genuinely keep arriving within the window)
  still ends up as ONE continuously-updated announcement, rather than
  splintering into disconnected messages every time a fixed clock from
  the first episode ran out. The full episode list (old + new) is
  re-fetched and always re-sorted by (season, episode) before rendering,
  so episodes always display in order regardless of the order they were
  actually downloaded/announced in. Only once a full SERIES_ROLLUP_HOURS
  passes with NOTHING new for that series does the window finally close
  — the next episode after that starts a brand new announcement with
  its own fresh window.

  REQUIRES the Tracearr SSE plugin installed on the Jellyfin server (same
  requirement as the dashboard cog) — see docs.tracearr.com/sse-plugin.

  HTTP: uses the bot's one shared aiohttp session (bot.http_session,
  created once in main.py's setup_hook) instead of creating its own —
  self.session below is just a reference to that shared session, not
  something this cog creates or closes.

  ITEMS FETCHING: all /Items lookups by ID go through
  jellyfin_get_items_batched(), which chunks the Ids list into groups of
  ITEMS_CHUNK_SIZE instead of joining every ID into one query string.
  A single unchunked request (e.g. a large season dump, or /syncmedia
  across many stored announcements) can build an Ids param long enough
  to exceed the reverse proxy's/server's max URL or header size, which
  Jellyfin/the proxy returns as HTTP 414 — chunking keeps each request's
  URL short regardless of how many IDs are involved overall.

Config: set CHANNEL_ID, JELLYFIN_URL, and JELLYFIN_API_KEY below.
Requires imageio-ffmpeg in requirements.txt for the screenshot feature.
"""

import os
import tempfile
import io
import aiosqlite
import discord
import aiohttp
import asyncio
import time
import random
import re
import logging
import imageio_ffmpeg
from discord import app_commands
from discord.ext import commands, tasks

logger = logging.getLogger("new_media_cog")

# --- CONFIG ---
CHANNEL_ID = 1539939421032284190  # <-- set this to the channel you want new-media announcements posted in
JELLYFIN_URL = "https://jellyfin.ushereats.com"
JELLYFIN_API_KEY = "9cb945d0d38d45cb84fcc8a2e1541bd8"
OWNER_DISCORD_ID = 723417665087078451

# How long to wait after the last item-added event before actually posting
# — lets a whole season dump (many individual episode-added events) group
# into one message instead of spamming one per episode.
GROUP_DEBOUNCE_SECONDS = 10
MAX_EPISODES_SHOWN = 10  # kept for reference only — no longer applied to series (see build_new_media_container)

# Hard ceiling on how long a batch can be deferred, measured from the
# FIRST item queued since the last flush — not from the most recent one.
# Without this, queue_item()'s cancel-and-restart debounce can be reset
# indefinitely by a steady stream of ANY library.item.added event during
# a large scan, including non-media noise (Person/Studio/Season records
# created as side effects) — confirmed in production: a 30-episode
# import fragmented into scattered near-empty flushes instead of one
# clean batch, because brief quiet gaps kept triggering early flushes of
# whatever happened to be queued at that moment rather than the whole
# import. This forces a flush at MAX_GROUP_WAIT_SECONDS regardless of
# continued incoming events.
MAX_GROUP_WAIT_SECONDS = 45

# An Episode item fetched with no SeriesId yet means Jellyfin's metadata
# matching hasn't linked it to its parent series at scan time — not that
# it lacks one. Re-checked after this delay instead of ever being posted
# standalone (see _announce_items). Capped by MAX_SERIES_LINK_RETRIES so
# a genuinely unmatched episode doesn't retry forever.
SERIES_LINK_RETRY_SECONDS = 30
MAX_SERIES_LINK_RETRIES = 3

# How long a series announcement stays "open" to further edits, measured
# from the MOST RECENT episode added to it — see the module docstring's
# "SERIES ROLLUP WINDOW" section. Rolling, not fixed: every new episode
# for the same series pushes this out another SERIES_ROLLUP_HOURS from
# that moment, so the window only actually closes once nothing new has
# arrived for a full SERIES_ROLLUP_HOURS.
SERIES_ROLLUP_HOURS = 12

# Fields requested on every /Items fetch — MediaStreams is what the
# quality badge (resolution + HDR/SDR) is computed from. RunTimeTicks is
# what the quality-check screenshot uses to compute a sensible seek
# point (see SCREENSHOT_WINDOW_START_SECONDS below).
ITEM_FIELDS = "SeriesId,SeriesName,ParentIndexNumber,IndexNumber,ProductionYear,MediaStreams,RunTimeTicks"

# Max number of item IDs joined into a single /Items request's Ids param.
# Keeps the request URL well under typical reverse-proxy/server URL or
# header size limits (default nginx is ~8KB) even for large batches —
# see jellyfin_get_items_batched(). Lower this further if your proxy in
# front of JELLYFIN_URL enforces a tighter limit than default nginx.
ITEMS_CHUNK_SIZE = 40

# Delay between each record's Discord call inside /syncmedia. Only ONE
# Discord API call happens per record now (a direct get_partial_message()
# edit — see syncmedia below), not two (fetch_message() + edit(), the
# original design) — that halving is what actually fixed /syncmedia still
# occasionally hitting Discord's rate limit despite pacing; the pacing
# itself was never the problem, there were just twice as many calls per
# record as necessary. This value is set to roughly match the ORIGINAL
# design's effective calls-per-second (2 calls/0.75s ≈ 2.7/s before, vs
# 1 call/0.4s = 2.5/s now) — same safety margin that mostly worked
# before, but the whole sync finishes in roughly half the wall-clock
# time since there are half as many round trips to wait between.
SYNC_PACE_SECONDS = 0.4

# How many announcements to list individually, by status, in /syncmedia's
# live progress display. This table has no pruning (see the class
# docstring), so it can hold far more records than safely fit in one
# Discord message — beyond this cap, the display shows a scrolling
# window of the most recently processed entries instead of trying to
# keep every single one visible at once (both a 4000-char-per-block risk
# and a readability one for a very long list).
SYNC_PROGRESS_LIST_CAP = 25

# Minimum time between edits to the LIVE PROGRESS message itself —
# separate from and independent of SYNC_PACE_SECONDS, which paces the
# actual announcement edits. Editing the progress message on every
# single record would just move the exact rate-limit problem
# SYNC_PACE_SECONDS already solves onto a different message. Always
# overridden on the final record regardless of timing, so the finished
# state is never stale.
SYNC_PROGRESS_UPDATE_MIN_INTERVAL_SECONDS = 1.5

# --- AUTO QUALITY-CHECK SCREENSHOT CONFIG ---
AUTO_QUALITY_SCREENSHOT_ENABLED = True

# Random point within this window is the seek target — not a fixed
# fraction of runtime. /redoscreenshot picking a fresh random point in
# this same window is what makes a "redo" actually produce a visibly
# different frame each time, not just re-fetch the identical shot.
SCREENSHOT_WINDOW_START_SECONDS = 15
SCREENSHOT_WINDOW_END_SECONDS = 45

# The grab is a small, capped-bitrate 1080p TRANSCODE from Jellyfin —
# for every item, not just high-bitrate/HDR/Dolby Vision ones. This is
# what actually made the feature reliable after several earlier,
# narrower attempts (raw file passthrough, a live piped stream, a
# second full-bitrate transcode path reserved just for Dolby Vision
# content) each fixed one symptom while the underlying strategy — moving
# a lot of video data around on a container with real, limited disk
# space — kept causing new ones. A capped-bitrate transcode is always
# small regardless of the source's real resolution/bitrate, Jellyfin's
# own transcoder (not our ffmpeg) handles Dolby Vision decoding and HDR
# tone-mapping correctly as a normal part of producing it, and 1080p is
# still a clear, good-looking screenshot — the quality BADGE on the
# announcement already states the source's real quality; the screenshot
# doesn't need to match it pixel-for-pixel.
SCREENSHOT_MAX_HEIGHT = 1080
SCREENSHOT_BITRATE_BPS = 6_000_000  # 6 Mbps — clear, sharp 1080p; a ~45s window is only ~34MB at this rate

# How much EARLIER than the actual target point to start Jellyfin's
# transcode, giving its encoder a short runway to stabilize from its
# nearest preceding keyframe before our own ffmpeg skips forward this
# same amount and grabs a frame. Confirmed in production: grabbing frame
# one of a transcode started exactly AT the target point can land on a
# uniform (black) frame instead of real content, during Jellyfin's own
# encoder-stabilization window.
SCREENSHOT_STARTUP_BUFFER_SECONDS = 3.0

# Separate timeouts for the two distinct phases: downloading the (now
# small) transcoded prefix to a local temp file, then running ffmpeg
# against that real file.
#
# DOWNLOAD_IDLE_TIMEOUT_SECONDS is the one that actually matters day to
# day — it resets on every chunk received, only firing if data genuinely
# stops arriving for that long (see the download loop's comment).
# DOWNLOAD_TIMEOUT_SECONDS is a much longer OUTER ceiling, purely a
# last-resort safety net now that idle protection handles the normal
# "Jellyfin's transcode is just running slow" case.
DOWNLOAD_IDLE_TIMEOUT_SECONDS = 15
DOWNLOAD_TIMEOUT_SECONDS = 180
FFMPEG_TIMEOUT_SECONDS = 30

# Generous cap given the small transcode bitrate above — at
# SCREENSHOT_BITRATE_BPS, even the full window is well under this; kept
# as a hard ceiling in case a much longer/oddly-shaped item is ever hit.
DOWNLOAD_CAP_BYTES = 80_000_000  # 80MB

# Anything smaller than this from a clean ffmpeg exit is treated as a
# corrupted/partial frame, not a real screenshot, and discarded — a
# clean ffmpeg exit code alone was confirmed NOT sufficient to trust the
# result on its own.
MIN_SCREENSHOT_BYTES = 50_000

# See _grab_quality_screenshot_with_retry. Failures here have been
# confirmed to look transient (Jellyfin's transcode running slow under
# momentary server load), so a short automatic retry meaningfully
# reduces how often this needs a manual /redoscreenshot.
SCREENSHOT_RETRY_ATTEMPTS = 2
SCREENSHOT_RETRY_DELAY_SECONDS = 5
# Actual delay is jittered around this value (see
# _grab_quality_screenshot_with_retry) — if several screenshots fail
# around the same moment (e.g. a whole season's worth of episodes
# landing at once), jitter stops them all retrying in exact lockstep
# and piling onto Jellyfin again at the same instant during recovery.
SCREENSHOT_RETRY_JITTER_FRACTION = 0.5  # +/- 50% of the base delay

# Self-healing sweep — periodically retries every announcement still
# missing a screenshot (an earlier attempt, even after its own
# SCREENSHOT_RETRY_ATTEMPTS, failed). This is what turns a transient
# failure into a temporary one instead of a permanent one, with no one
# needing to notice and run /redoscreenshot manually.
SCREENSHOT_SWEEP_INTERVAL_MINUTES = 30
# Pacing between records during a sweep — same reasoning as
# SYNC_PACE_SECONDS: a batch of many missing screenshots processed
# back-to-back would hit Discord/Jellyfin harder than spacing them out.
SCREENSHOT_SWEEP_PACE_SECONDS = 2.0

# Circuit breaker — if this many consecutive screenshot attempts fail
# (across the automatic on-post path AND the sweep), that's a signal
# Jellyfin itself is struggling right now, not that we're just unlucky.
# Pauses ALL automatic attempts (on-post and sweep) for
# SCREENSHOT_CIRCUIT_BREAKER_COOLDOWN_SECONDS rather than continuing to
# hammer an already-strained server with retries likely to fail anyway.
# Manual commands (/redoscreenshot, /testscreenshot) are NOT gated by
# this — an admin explicitly asking for one right now should still be
# able to try, even while the automatic side is paused.
SCREENSHOT_CIRCUIT_BREAKER_THRESHOLD = 5
SCREENSHOT_CIRCUIT_BREAKER_COOLDOWN_SECONDS = 600  # 10 minutes

DB_DIR = "/home/container/ushereats/DATABASES"
DB_PATH = os.path.join(DB_DIR, "new_media.db")

# Screenshot temp files go HERE, not the system default
# (tempfile.gettempdir(), almost always /tmp) — /tmp inside this
# container turned out to be its own small, separate mount, unrelated to
# the main persistent volume the rest of this bot's data lives on.
# Sibling directory to DB_DIR so it's guaranteed to sit on that same
# volume. Downloads are small now regardless (see DOWNLOAD_CAP_BYTES),
# so this matters far less than it used to, but there's no reason to
# risk the wrong mount again.
SCREENSHOT_TMP_DIR = "/home/container/ushereats/TMP_SCREENSHOTS"

# Stored in quality_image_url when a screenshot upload succeeded but
# Discord's response didn't expose a resolvable URL for it (see
# _attach_screenshot_bytes) — deliberately NOT a real http(s) URL, so
# _preserve_or_refresh_screenshot can tell at a glance "there's an image
# here, but I can't re-download the exact same one — grab a fresh one
# instead" rather than attempting to fetch a URL that was never real.
# Also deliberately non-null/non-empty, so _get_announcements_missing_screenshot
# correctly treats this as "already has one" and leaves it alone.
SCREENSHOT_URL_UNKNOWN_SENTINEL = "unknown-see-message-attachment"

# --- AUTO-ADOPT CONFIG ---
# How many recent messages in CHANNEL_ID each sweep scans for untracked
# announcements. One history page is 100 messages, so 200 = 2 API calls.
ADOPT_SCAN_LIMIT = 200
# Messages newer than this are left alone — the normal posting path may
# still be mid-way through recording/screenshotting them.
ADOPT_MIN_AGE_SECONDS = 300

_POSTER_ID_RE = re.compile(r"/Items/([0-9a-fA-F]{32})/Images/Primary")
_EPISODE_CODE_RE = re.compile(r"S(\d+)E(\d+)")


def _extract_announcement_parts(message: discord.Message) -> tuple[str, list[str]]:
    """Walks a Components V2 message's component tree and returns
    (all TextDisplay content joined, every media URL found). Attribute-
    based rather than isinstance-based so it tolerates discord.py's
    component class names differing between versions."""
    texts: list[str] = []
    media_urls: list[str] = []
    stack = list(getattr(message, "components", None) or [])
    while stack:
        c = stack.pop()
        content = getattr(c, "content", None)
        if isinstance(content, str):
            texts.append(content)
        media = getattr(c, "media", None)
        url = getattr(media, "url", None) if media is not None else None
        if isinstance(url, str):
            media_urls.append(url)
        for attr in ("children", "components", "items"):
            sub = getattr(c, attr, None)
            if isinstance(sub, (list, tuple)):
                stack.extend(sub)
        accessory = getattr(c, "accessory", None)
        if accessory is not None:
            stack.append(accessory)
    return "\n".join(texts), media_urls


# --- DB (records what went into each posted announcement, for /syncmedia,
# for preserving an owner-added quality-check image across a sync, and now
# for tracking the open series-rollup window so later episodes of the same
# series edit the existing message instead of posting a new one) ---
#
# aiosqlite throughout, not blocking sqlite3 — a blocking DB call inside an
# async function freezes the ENTIRE bot's event loop for its duration, not
# just this cog. Every other cog with any DB access in this codebase
# (daily.py, stickymessage.py, the fixed watchingnowroleadder.py) already
# uses aiosqlite for exactly this reason; this file was the one holdout.
async def _ensure_db():
    os.makedirs(DB_DIR, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS announcements (
                message_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                group_type TEXT NOT NULL,
                item_ids TEXT NOT NULL,
                posted_at INTEGER NOT NULL,
                quality_image_url TEXT,
                series_id TEXT,
                rollup_expires_at INTEGER
            )
        """)
        # Backward-compatible migrations for a DB created before these
        # columns existed — each ignored individually if already present.
        for ddl in (
            "ALTER TABLE announcements ADD COLUMN quality_image_url TEXT",
            "ALTER TABLE announcements ADD COLUMN series_id TEXT",
            "ALTER TABLE announcements ADD COLUMN rollup_expires_at INTEGER",
            "ALTER TABLE announcements ADD COLUMN locked INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE announcements ADD COLUMN last_sync_signature TEXT",
        ):
            try:
                await conn.execute(ddl)
            except aiosqlite.OperationalError:
                pass
        await conn.commit()


async def _record_announcement(message_id: int, channel_id: int, group_type: str, item_ids: list[str],
                                posted_at: int, series_id: str | None = None, rollup_expires_at: int | None = None):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO announcements "
            "(message_id, channel_id, group_type, item_ids, posted_at, quality_image_url, series_id, rollup_expires_at) "
            "VALUES (?, ?, ?, ?, ?, (SELECT quality_image_url FROM announcements WHERE message_id = ?), ?, ?)",
            (message_id, channel_id, group_type, ",".join(item_ids), posted_at, message_id, series_id, rollup_expires_at),
        )
        await conn.commit()


async def _get_announcement(message_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM announcements WHERE message_id = ?", (message_id,)) as cursor:
            row = await cursor.fetchone()
    return dict(row) if row else None


async def _get_all_announcements() -> list[dict]:
    """Every stored announcement, oldest first — used by /syncmedia to
    refresh everything in one pass instead of one message_id at a time."""
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM announcements ORDER BY posted_at ASC") as cursor:
            rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def _get_open_series_announcement(series_id: str) -> dict | None:
    """The currently-open announcement for this series, if its rollup
    window hasn't expired yet. Only ever one should exist at a time per
    series in practice, but ordered by most recent just in case."""
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM announcements WHERE group_type = 'series' AND series_id = ? "
            "AND rollup_expires_at IS NOT NULL AND rollup_expires_at > ? "
            "ORDER BY posted_at DESC LIMIT 1",
            (series_id, now),
        ) as cursor:
            row = await cursor.fetchone()
    return dict(row) if row else None


async def _update_announcement_item_ids(message_id: int, item_ids: list[str], rollup_expires_at: int | None = None):
    """rollup_expires_at is optional so /syncmedia-style callers that only
    touch item_ids don't need to pass one — but _handle_series_batch's
    edit path always passes a freshly-computed one, which is what makes
    the rollup window ROLL forward on every new episode instead of
    staying fixed from whenever the announcement was first posted (see
    the module docstring's "SERIES ROLLUP WINDOW" section)."""
    async with aiosqlite.connect(DB_PATH) as conn:
        if rollup_expires_at is not None:
            await conn.execute(
                "UPDATE announcements SET item_ids = ?, rollup_expires_at = ? WHERE message_id = ?",
                (",".join(item_ids), rollup_expires_at, message_id),
            )
        else:
            await conn.execute("UPDATE announcements SET item_ids = ? WHERE message_id = ?", (",".join(item_ids), message_id))
        await conn.commit()


async def _set_quality_image(message_id: int, image_url: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("UPDATE announcements SET quality_image_url = ? WHERE message_id = ?", (image_url, message_id))
        await conn.commit()


async def _set_locked(message_id: int, locked: bool):
    """Locked announcements are skipped entirely by /syncmedia — see
    toggle_sync_lock. Lets an owner manually fix a broken announcement
    (wrong episode names, bad metadata, whatever) via /embededit or by
    hand, then protect that fix from being silently overwritten the next
    time /syncmedia rebuilds everything from stored Jellyfin data."""
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("UPDATE announcements SET locked = ? WHERE message_id = ?", (1 if locked else 0, message_id))
        await conn.commit()


async def _delete_announcement(message_id: int):
    """Only ever called when /syncmedia has just CONFIRMED via a live
    discord.NotFound that the message is genuinely gone — never
    speculative. This table has no other pruning at all (confirmed in
    production: 292 accumulated rows, 75% of the first batch checked
    turning out to be long-dead messages), so a row for a message that
    will never exist again just sits there forever otherwise, making
    every future /syncmedia run slower and its counts more confusing for
    no benefit — a dead row can never be synced again by definition."""
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("DELETE FROM announcements WHERE message_id = ?", (message_id,))
        await conn.commit()


async def _set_sync_signature(message_id: int, signature: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("UPDATE announcements SET last_sync_signature = ? WHERE message_id = ?", (signature, message_id))
        await conn.commit()


async def _get_announcements_missing_screenshot() -> list[dict]:
    """Every non-locked announcement without a quality-check screenshot
    yet — used by the periodic self-healing sweep to retry ones an
    earlier attempt (even after its own SCREENSHOT_RETRY_ATTEMPTS)
    failed on. Locked rows are respected the same way /syncmedia
    respects them — an owner locked it deliberately, this sweep
    shouldn't touch it either."""
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM announcements WHERE (quality_image_url IS NULL OR quality_image_url = '') AND locked = 0"
        ) as cursor:
            rows = await cursor.fetchall()
    return [dict(r) for r in rows]


def _compute_group_signature(group: dict) -> str:
    """A cheap fingerprint of everything /syncmedia would actually render
    differently if it changed — title, year, quality badge, and for a
    series every (season, episode, name) tuple. Compared against the
    PREVIOUS sync's stored signature to tell /syncmedia's live progress
    display apart "nothing changed" from "fixed": without this, every
    sync just overwrote blindly and called it "synced" whether or not
    anything was actually different, which isn't the same claim as
    "this was broken and I just fixed it." Not a cryptographic hash —
    collisions aren't a security concern here, just a display accuracy
    one, and a plain joined string is enough for that."""
    if group["type"] == "movie":
        parts = [group.get("title"), str(group.get("year")), group.get("quality_label")]
    else:
        parts = [group.get("title"), group.get("quality_label")]
        parts += [f"{s}x{e}:{n}" for s, e, n in group.get("episodes", [])]
    return "|".join(str(p) for p in parts)


async def jellyfin_get(session: aiohttp.ClientSession, path: str, params: dict = None):
    headers = {"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"', "accept": "application/json"}

    try:
        async with session.get(f"{JELLYFIN_URL}{path}", headers=headers, params=params or {}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None, f"Jellyfin returned status {resp.status} for `{path}`."
            return await resp.json(), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


async def jellyfin_get_items_batched(session: aiohttp.ClientSession, item_ids: list[str], fields: str) -> tuple[list[dict], str | None]:
    """Fetches /Items for item_ids in chunks of ITEMS_CHUNK_SIZE instead of
    joining every ID into a single query string, avoiding HTTP 414 (URI
    Too Long) on large batches — a full season dump or a /syncmedia run
    across many stored announcements can otherwise produce an Ids param
    long enough to exceed the reverse proxy's/server's max URL or header
    size. Returns (items, err) — on a chunk failure, err is set and items
    contains whatever was successfully fetched before that point."""
    all_items: list[dict] = []
    for i in range(0, len(item_ids), ITEMS_CHUNK_SIZE):
        chunk = item_ids[i:i + ITEMS_CHUNK_SIZE]
        data, err = await jellyfin_get(session, "/Items", params={"Ids": ",".join(chunk), "Fields": fields})
        if err:
            return all_items, err
        all_items.extend((data or {}).get("Items", []))
    return all_items, None


def get_poster_url(item_id: str) -> str:
    return f"{JELLYFIN_URL}/Items/{item_id}/Images/Primary"


def get_quality_label(item: dict) -> str | None:
    """Reads resolution + dynamic range straight from the item's video
    stream — e.g. '4K HDR', '1080p SDR'. Returns None if no video stream
    data is present (Fields=MediaStreams wasn't returned, or the item
    genuinely has none)."""
    streams = item.get("MediaStreams") or []
    video_stream = next((s for s in streams if s.get("Type") == "Video"), None)
    if not video_stream:
        return None

    width = video_stream.get("Width") or 0
    height = video_stream.get("Height") or 0
    if width >= 3840 or height >= 2160:
        res = "4K"
    elif width >= 1920 or height >= 1080:
        res = "1080p"
    elif width >= 1280 or height >= 720:
        res = "720p"
    elif height:
        res = f"{height}p"
    else:
        return None

    video_range = (video_stream.get("VideoRange") or "SDR").upper()
    dynamic = "SDR" if video_range == "SDR" else "HDR"
    return f"{res} {dynamic}"


# --- AUTO QUALITY-CHECK SCREENSHOT ---
_ffmpeg_path_cache: str | None = None


def _get_ffmpeg_path() -> str:
    """imageio-ffmpeg downloads and ships its OWN self-contained ffmpeg
    binary at pip-install time — this container has no system ffmpeg at
    all (confirmed: `ffmpeg -version` returns nothing), and no way to
    install one at the OS level, so this is what actually runs every
    frame grab, not any OS-level binary. Cached after the first call —
    get_ffmpeg_exe() does its own disk lookup internally, no reason to
    repeat that on every single screenshot."""
    global _ffmpeg_path_cache
    if _ffmpeg_path_cache is None:
        _ffmpeg_path_cache = imageio_ffmpeg.get_ffmpeg_exe()
    return _ffmpeg_path_cache


async def _grab_quality_screenshot(session: aiohttp.ClientSession, item_id: str, runtime_ticks: int | None) -> bytes | None:
    """Grabs a single quality-check frame for item_id. ONE simple path
    now, not branched by content type — always requests a modest,
    capped-bitrate 1080p transcode from Jellyfin (SCREENSHOT_MAX_HEIGHT /
    SCREENSHOT_BITRATE_BPS), downloads that small stream to a local temp
    file, then runs ffmpeg against it.

    This replaces several earlier, more complicated attempts (raw
    static-file passthrough with fast byte-range seeking; piping a live
    stream directly into ffmpeg's stdin; branching HDR/Dolby-Vision
    content onto a different, full-bitrate transcode path with a custom
    tone-mapping filter chain) that each fixed one specific symptom
    while the underlying strategy — moving a lot of video data around on
    a container with real, limited disk space — kept causing new ones:
    torn/corrupted frames from cutting a live pipe mid-decode, a
    confirmed HEVC decoder bug parsing Dolby Vision metadata, black
    frames from an unstabilized live transcode, and finally the
    container's disk filling outright from oversized downloads.

    A small, capped-bitrate transcode fixes all of it AT ONCE: it's
    always tiny regardless of the source file's real resolution or
    bitrate (a ~45s window at SCREENSHOT_BITRATE_BPS is only a few dozen
    MB), Jellyfin's own transcoder — not our ffmpeg — handles Dolby
    Vision decoding and HDR-to-SDR tone-mapping correctly as a normal
    part of producing that stream, and 1080p is still a clear, good-
    looking screenshot; the quality BADGE on the announcement already
    states the source's real quality, the screenshot doesn't need to
    match it pixel-for-pixel.

    The transcode is started SCREENSHOT_STARTUP_BUFFER_SECONDS before
    the actual target, and our own ffmpeg seeks forward that same small,
    fixed amount before grabbing a frame — confirmed in production that
    grabbing frame one of a transcode started exactly at the target can
    land during Jellyfin's own encoder-stabilization window (visible as
    a uniform/black frame) rather than on real content.

    Returns raw PNG bytes (lossless) read from ffmpeg's stdout, or None
    on ANY failure (download failure, ffmpeg failure, timeout,
    corrupted/undersized output) — this must never raise, since a
    screenshot failing must never break the actual announcement it's
    attached to; the manual "Add Quality Check" button stays available
    as a fallback regardless of why this returned None."""
    if runtime_ticks:
        runtime_seconds = runtime_ticks / 10_000_000  # Jellyfin ticks are 100ns units
        if runtime_seconds > SCREENSHOT_WINDOW_END_SECONDS + 5:
            seek_seconds = random.uniform(SCREENSHOT_WINDOW_START_SECONDS, SCREENSHOT_WINDOW_END_SECONDS)
        elif runtime_seconds > SCREENSHOT_WINDOW_START_SECONDS:
            seek_seconds = random.uniform(SCREENSHOT_WINDOW_START_SECONDS, max(SCREENSHOT_WINDOW_START_SECONDS, runtime_seconds - 5))
        else:
            seek_seconds = max(2.0, runtime_seconds * 0.3)
    else:
        seek_seconds = float(SCREENSHOT_WINDOW_START_SECONDS)

    start_seconds = max(0.0, seek_seconds - SCREENSHOT_STARTUP_BUFFER_SECONDS)
    start_ticks = int(start_seconds * 10_000_000)
    own_seek_seconds = seek_seconds - start_seconds
    stream_url = (
        f"{JELLYFIN_URL}/Videos/{item_id}/stream"
        f"?VideoCodec=h264&AudioCodec=aac"
        f"&MaxHeight={SCREENSHOT_MAX_HEIGHT}&VideoBitrate={SCREENSHOT_BITRATE_BPS}"
        f"&StartTimeTicks={start_ticks}&api_key={JELLYFIN_API_KEY}"
    )

    try:
        ffmpeg_path = _get_ffmpeg_path()
    except Exception as e:
        logger.error(f"[NewMedia] ffmpeg binary unavailable: {type(e).__name__}: {e}")
        return None

    tmp_path = None
    try:
        os.makedirs(SCREENSHOT_TMP_DIR, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(suffix=".mp4", prefix="quality_check_", dir=SCREENSHOT_TMP_DIR)
        os.close(fd)

        bytes_written = 0
        try:
            # sock_read is an IDLE timeout, not a flat one — it resets on
            # every chunk actually received, only firing if data
            # genuinely stops arriving for that long. total is kept as a
            # much longer outer ceiling purely as a last-resort safety
            # net. Confirmed in production: a flat total timeout cut off
            # a transcode that was still making real progress (7MB
            # received in 60s, well under the expected rate — Jellyfin's
            # own encode was just running slow under load, not stalled)
            # — an idle timeout lets a slow-but-progressing grab keep
            # going and finish instead of failing outright.
            timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_SECONDS, sock_read=DOWNLOAD_IDLE_TIMEOUT_SECONDS)
            async with session.get(stream_url, timeout=timeout) as resp:
                if resp.status != 200:
                    logger.warning(f"[NewMedia] Screenshot stream fetch for item {item_id} returned HTTP {resp.status}.")
                    return None
                with open(tmp_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(65536):
                        f.write(chunk)
                        bytes_written += len(chunk)
                        if bytes_written >= DOWNLOAD_CAP_BYTES:
                            break
        except asyncio.TimeoutError:
            logger.warning(f"[NewMedia] Screenshot download for item {item_id} timed out ({bytes_written} bytes received) — either no data for {DOWNLOAD_IDLE_TIMEOUT_SECONDS}s, or the overall {DOWNLOAD_TIMEOUT_SECONDS}s ceiling was hit.")
            return None
        except Exception as e:
            logger.warning(f"[NewMedia] Screenshot download failed for item {item_id}: {type(e).__name__}: {e}")
            return None

        if bytes_written == 0:
            logger.warning(f"[NewMedia] Screenshot download for item {item_id} produced no data.")
            return None

        ffmpeg_args = [
            ffmpeg_path,
            "-err_detect", "ignore_err",
            "-fflags", "+genpts+igndts",
            "-ss", str(int(round(own_seek_seconds))),
            "-i", tmp_path,
            "-an", "-sn",
            "-frames:v", "1",
            "-f", "image2pipe", "-vcodec", "png", "-",
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *ffmpeg_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_data, stderr_data = await asyncio.wait_for(process.communicate(), timeout=FFMPEG_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.warning(f"[NewMedia] ffmpeg screenshot timed out for item {item_id} after {FFMPEG_TIMEOUT_SECONDS}s.")
            try:
                process.kill()
            except Exception:
                pass
            return None
        except Exception as e:
            logger.error(f"[NewMedia] ffmpeg screenshot failed to run for item {item_id}: {type(e).__name__}: {e}")
            return None
    finally:
        if tmp_path is not None:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    if process.returncode != 0 or not stdout_data:
        tail = stderr_data[-300:].decode(errors="replace") if stderr_data else ""
        logger.warning(f"[NewMedia] ffmpeg produced no image for item {item_id} (exit {process.returncode}): {tail}")
        return None

    if not stdout_data.startswith(b"\x89PNG\r\n\x1a\n"):
        logger.warning(f"[NewMedia] ffmpeg exited cleanly for item {item_id} but output isn't a valid PNG — discarding.")
        return None
    if len(stdout_data) < MIN_SCREENSHOT_BYTES:
        logger.warning(f"[NewMedia] ffmpeg output for item {item_id} is suspiciously small ({len(stdout_data)} bytes) — likely a corrupted/partial frame, discarding.")
        return None

    return stdout_data


async def _grab_quality_screenshot_with_retry(session: aiohttp.ClientSession, item_id: str, runtime_ticks: int | None) -> bytes | None:
    """Wraps _grab_quality_screenshot with SCREENSHOT_RETRY_ATTEMPTS total
    tries, waiting SCREENSHOT_RETRY_DELAY_SECONDS between them. Added
    after confirming in production that failures here look transient
    (Jellyfin's own transcode running slower than expected under
    server-side load at that moment), not deterministic — a short-lived
    problem is very plausibly gone by the next attempt a few seconds
    later, so this is the difference between "occasionally needs a
    manual /redoscreenshot" and "usually just works on its own." Each
    retry picks a FRESH random seek point (since it re-enters
    _grab_quality_screenshot from scratch), which is a harmless side
    effect, not a problem — any point in the window is an equally valid
    screenshot."""
    for attempt in range(1, SCREENSHOT_RETRY_ATTEMPTS + 1):
        result = await _grab_quality_screenshot(session, item_id, runtime_ticks)
        if result is not None:
            return result
        if attempt < SCREENSHOT_RETRY_ATTEMPTS:
            # Jittered, not a fixed delay — see SCREENSHOT_RETRY_JITTER_FRACTION.
            jitter_range = SCREENSHOT_RETRY_DELAY_SECONDS * SCREENSHOT_RETRY_JITTER_FRACTION
            delay = random.uniform(SCREENSHOT_RETRY_DELAY_SECONDS - jitter_range, SCREENSHOT_RETRY_DELAY_SECONDS + jitter_range)
            logger.info(f"[NewMedia] Screenshot attempt {attempt}/{SCREENSHOT_RETRY_ATTEMPTS} failed for item {item_id} — retrying in {delay:.1f}s.")
            await asyncio.sleep(delay)
    return None


# --- CONTAINER BUILDER (Components V2) ---
# Used by the real pipeline AND /syncmedia — sync passes the ORIGINAL
# posted_at (and any owner-added quality_image_url) through so both
# survive a re-render instead of resetting.
def build_new_media_container(bot, group: dict, posted_at: int | None = None, quality_image_url: str | None = None) -> discord.ui.Container:
    """group shape:
    {
        "type": "series" | "movie",
        "title": str,
        "year": int | None,
        "poster_url": str,
        "quality_label": str | None,
        "episodes": [(season:int|None, episode:int|None, name:str), ...]  # only for "series"
    }
    posted_at: unix timestamp for the footer. Defaults to now.
    quality_image_url: if set, adds a MediaGallery item with this image
    BEFORE the footer, and the footer gains a '— Quality Check Approved'
    suffix. The button is then omitted entirely — its last appearance is
    the pre-submission state, added as the final item in this same
    container (no separate button container anymore).

    Layout order: header Section (eyebrow + title, poster accessory) ->
    metadata chip line -> one small Separator -> one TextDisplay PER
    SEASON (each headed "**Season N**", listing that season's episodes)
    -> quality-check gallery (only if present) -> footer -> button (only
    if no quality-check image yet).

    NOTE: `group["episodes"]` is expected to ALREADY be sorted by
    (season, episode) by the caller before this function ever sees it —
    both the initial post path and the series-rollup edit path sort the
    full combined episode list every time it changes, so display order
    is never dependent on the order episodes were downloaded/announced in.
    Grouping into per-season blocks below relies on that sort already
    being in place — it groups consecutive runs of the same season
    rather than re-sorting.
    """
    container = discord.ui.Container()

    title_line = f"# {group['title']}"
    if group.get("year"):
        title_line += f" {group['year']}"

    if group["type"] == "series":
        episodes = group["episodes"]
        header = f"-# NEW EPISODES\n{title_line}\n\n"
        container.add_item(discord.ui.Section(
            discord.ui.TextDisplay(header),
            accessory=discord.ui.Thumbnail(media=group["poster_url"]),
        ))

        meta_parts = []
        if group.get("quality_label"):
            meta_parts.append(f"`{group['quality_label']}`")
        seasons_seen = [e[0] for e in episodes if e[0] is not None]
        distinct_seasons = sorted(set(seasons_seen))
        if len(distinct_seasons) == 1:
            meta_parts.append(f"Season {distinct_seasons[0]}")
        elif len(distinct_seasons) > 1:
            meta_parts.append(f"Seasons {distinct_seasons[0]}-{distinct_seasons[-1]}")
        meta_parts.append(f"{len(episodes)} episode{'s' if len(episodes) != 1 else ''}")
        container.add_item(discord.ui.TextDisplay("  ·  ".join(meta_parts)))

        container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

        # One TextDisplay block per season, each with its own "**Season
        # N**" header, in the order episodes already appear (which is
        # always season-then-episode ascending per the caller's sort).
        # No cross-season cap — a season with an unusual episode count
        # just lists all of them; MAX_EPISODES_SHOWN is no longer used
        # for series since a full multi-season rollup is meant to show
        # everything grouped, not truncate.
        current_season = object()  # sentinel so the very first episode always starts a new block
        current_lines: list[str] = []
        for season, episode, name in episodes:
            if season != current_season:
                if current_lines:
                    season_label = f"**Season {current_season}**" if current_season is not None else "**Extras**"
                    container.add_item(discord.ui.TextDisplay(season_label + "\n" + "\n".join(current_lines)))
                current_season = season
                current_lines = []
            if season is not None and episode is not None:
                current_lines.append(f"• `S{season}E{episode}` — {name}")
            else:
                current_lines.append(f"• {name}")
        if current_lines:
            season_label = f"**Season {current_season}**" if current_season is not None else "**Extras**"
            container.add_item(discord.ui.TextDisplay(season_label + "\n" + "\n".join(current_lines)))

    else:  # movie
        header = f"-# NEW MOVIE\n{title_line}\n\n"
        container.add_item(discord.ui.Section(
            discord.ui.TextDisplay(header),
            accessory=discord.ui.Thumbnail(media=group["poster_url"]),
        ))

        if group.get("quality_label"):
            container.add_item(discord.ui.TextDisplay(f"`{group['quality_label']}`"))
            container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.large))

    if quality_image_url:
        container.add_item(discord.ui.MediaGallery(
            discord.MediaGalleryItem(media=quality_image_url)
        ))

    footer_ts = posted_at if posted_at is not None else int(time.time())
    footer_text = f"-# Added <t:{footer_ts}:R>"
    if quality_image_url:
        footer_text += " — Quality Check Approved"
    container.add_item(discord.ui.TextDisplay(footer_text))

    if quality_image_url is None:
        container.add_item(NewMediaActionRow())

    return container


# --- OWNER-ONLY "ADD QUALITY CHECK" BUTTON ---
class AddQualityCheckModal(discord.ui.Modal, title="Add Quality Check Image"):
    def __init__(self, message_id: int):
        super().__init__()
        self.message_id = message_id
        self.image_url_input = discord.ui.TextInput(
            label="Image URL",
            placeholder="https://...",
            required=True,
            max_length=500,
        )
        self.add_item(self.image_url_input)

    async def on_submit(self, interaction: discord.Interaction):
        url = self.image_url_input.value.strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            return await interaction.response.send_message("❌ That doesn't look like a valid URL.", ephemeral=True)

        record = await _get_announcement(self.message_id)
        if record is None:
            return await interaction.response.send_message("❌ No stored record for this message — can't rebuild it.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        cog = interaction.client.get_cog("NewMediaAnnouncer")
        if cog is None:
            return await interaction.followup.send("❌ NewMedia cog not loaded.", ephemeral=True)

        group = await cog._rebuild_group_from_record(record)
        if group is None:
            return await interaction.followup.send("❌ Couldn't re-fetch the original items — they may have been removed from the library.", ephemeral=True)

        channel = interaction.client.get_channel(record["channel_id"])
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            return await interaction.followup.send("❌ Couldn't find the channel that message was posted in.", ephemeral=True)

        try:
            msg = await channel.fetch_message(self.message_id)
            await msg.edit(view=NewMediaLayoutView(interaction.client, group, posted_at=record["posted_at"], quality_image_url=url, message_id=self.message_id))
        except discord.HTTPException as e:
            return await interaction.followup.send(f"❌ Failed to edit the message: `{e}`", ephemeral=True)

        await _set_quality_image(self.message_id, url)
        await interaction.followup.send("✅ Quality check image added.", ephemeral=True)


class NewMediaActionRow(discord.ui.ActionRow):
    """Fixed custom_id so this survives a bot restart as a persistent
    view target — every message's button routes through here, and the
    click handler looks up which message/announcement it belongs to via
    interaction.message.id, not anything encoded per-instance."""

    @discord.ui.button(label="Quality Check", style=discord.ButtonStyle.secondary, custom_id="new_media_add_quality_check")
    async def add_quality_check_click(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)
        await interaction.response.send_modal(AddQualityCheckModal(interaction.message.id))


class NewMediaLayoutView(discord.ui.LayoutView):
    def __init__(self, bot, group: dict, posted_at: int | None = None, quality_image_url: str | None = None, message_id: int | None = None):
        super().__init__(timeout=None)
        # Button is added as the last item inside the announcement
        # Container itself (see build_new_media_container) — omitted
        # once a quality-check image has been added, so it disappears
        # from the message the moment the modal is submitted.
        self.add_item(build_new_media_container(bot, group, posted_at=posted_at, quality_image_url=quality_image_url))


# --- "TOGGLE SYNC LOCK" MESSAGE CONTEXT MENU ---
# discord.py does not allow @app_commands.context_menu on a method inside
# a Cog class (confirmed in production: "TypeError: context menus cannot
# be defined inside a class") — it must be a plain module-level function,
# manually registered to the tree in cog_load/removed in cog_unload,
# unlike @app_commands.command which IS allowed as a Cog method.
@app_commands.context_menu(name="Toggle Sync Lock")
async def toggle_sync_lock(interaction: discord.Interaction, message: discord.Message):
    if interaction.user.id != OWNER_DISCORD_ID:
        return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

    record = await _get_announcement(message.id)
    if record is None:
        return await interaction.response.send_message(
            "❌ This message isn't a tracked new-media announcement — nothing to lock.", ephemeral=True,
        )

    new_locked = not bool(record.get("locked"))
    await _set_locked(message.id, new_locked)

    if new_locked:
        await interaction.response.send_message(
            "🔒 Locked — `/syncmedia` will skip this announcement from now on, so any manual edits you make stay put. "
            "Run this again on the same message to unlock it.",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "🔓 Unlocked — `/syncmedia` will refresh this announcement with fresh Jellyfin data again on its next run.",
            ephemeral=True,
        )


@toggle_sync_lock.error
async def toggle_sync_lock_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)


# --- /syncmedia LIVE PROGRESS DISPLAY ---
_SYNC_STATUS_EMOJI = {
    "checking": "⏳",
    "fixed": "🔧",
    "unchanged": "➖",
    "locked": "🔒",
    "missing_items": "⚠️",
    "missing_message": "🗑️",
    "failed": "❌",
}
_SYNC_STATUS_LABEL = {
    "checking": "Checking...",
    "fixed": "Fixed",
    "unchanged": "Nothing changed",
    "locked": "Locked, skipped",
    "missing_items": "Items missing from library",
    "missing_message": "Message no longer exists — removed from database",
    "failed": "Failed",
}


def _build_sync_progress_view(entries: list[dict], done: int, total: int, counts: dict, started_at: int, finished: bool) -> discord.ui.LayoutView:
    """entries: [{"label": str, "status": one of _SYNC_STATUS_EMOJI's keys}, ...]
    — mutated in place by the caller as each record gets processed, so
    passing the SAME list on every call naturally reflects live progress
    without needing to rebuild it from scratch each time."""
    container = discord.ui.Container()

    title = "✅ Sync complete" if finished else "🔄 Syncing announcements..."
    container.add_item(discord.ui.TextDisplay(f"## {title}\nProgress: {done}/{total}"))
    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))

    if len(entries) <= SYNC_PROGRESS_LIST_CAP:
        visible = entries
        overflow_note = ""
    else:
        # Scrolling window of the most recently touched entries, not the
        # full list — see SYNC_PROGRESS_LIST_CAP's comment for why.
        visible = entries[-SYNC_PROGRESS_LIST_CAP:]
        overflow_note = f"\n-# ...and {len(entries) - SYNC_PROGRESS_LIST_CAP} more not shown individually (see totals below)."

    lines = [f"{_SYNC_STATUS_EMOJI[e['status']]} {e['label']} — {_SYNC_STATUS_LABEL[e['status']]}" for e in visible]
    container.add_item(discord.ui.TextDisplay("\n".join(lines) + overflow_note))

    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
    container.add_item(discord.ui.TextDisplay(
        f"🔧 {counts['fixed']} fixed  ·  ➖ {counts['unchanged']} unchanged  ·  🔒 {counts['locked']} locked  ·  "
        f"⚠️ {counts['missing_items']} missing items  ·  🗑️ {counts['missing_message']} missing message  ·  "
        f"❌ {counts['failed']} failed"
    ))

    container.add_item(discord.ui.Separator(spacing=discord.SeparatorSpacing.small))
    ts_label = "Finished" if finished else "Started"
    ts_value = int(time.time()) if finished else started_at
    container.add_item(discord.ui.TextDisplay(f"-# {ts_label} <t:{ts_value}:R>"))

    view = discord.ui.LayoutView(timeout=None)
    view.add_item(container)
    return view


# --- COG ---
class NewMediaAnnouncer(commands.Cog):
    """No longer opens its own SSE connection — dashboard.py owns the one
    shared connection to Tracearr and forwards library.item.added events
    here directly via queue_item(). This cog still needs an aiohttp
    session for the REST calls that fetch new items' details (title,
    poster, quality, episode numbers) — that's the bot's one shared
    session (bot.http_session), not one this cog creates or closes
    itself."""

    def __init__(self, bot):
        self.bot = bot
        self.category = "Utility"
        self.session: aiohttp.ClientSession | None = None
        self._pending_item_ids: set[str] = set()
        self._debounce_task: asyncio.Task | None = None
        # In-flight _announce_items runs, spawned by _debounced_flush once
        # its timer fires — deliberately SEPARATE from _debounce_task so
        # queue_item()'s cancel-and-restart only ever hits the timer,
        # never a post already in progress (see the module docstring's
        # "FLUSH TASK ISOLATION" section). Held here as strong refs so
        # the event loop can't garbage-collect them mid-run.
        self._announce_tasks: set[asyncio.Task] = set()
        # Timestamp of the first item queued since the last flush — used
        # to enforce MAX_GROUP_WAIT_SECONDS regardless of how many times
        # the debounce timer below gets reset by further incoming items.
        self._first_pending_at: float | None = None
        # item_id -> attempts so far, for episodes requeued because their
        # SeriesId wasn't linked yet at fetch time. Capped by
        # MAX_SERIES_LINK_RETRIES so a genuinely unmatched episode is
        # dropped (logged) instead of retried forever.
        self._series_link_retry_counts: dict[str, int] = {}
        # Serializes the check-open-announcement -> edit-or-post sequence
        # per series, so two episodes of the same show landing in
        # overlapping flushes can't race each other into posting two
        # separate messages or corrupting the stored item_ids list.
        self._series_locks: dict[str, asyncio.Lock] = {}
        # Circuit breaker state — see SCREENSHOT_CIRCUIT_BREAKER_THRESHOLD's
        # comment. Shared across the automatic on-post path and the
        # sweep loop; NOT consulted by manual commands (/redoscreenshot,
        # /testscreenshot).
        self._consecutive_screenshot_failures = 0
        self._circuit_breaker_open_until: float = 0.0
        # _ensure_db() moved to cog_load — see below. __init__ cannot
        # await, and _ensure_db is now async (aiosqlite).

    def _lock_for_series(self, series_id: str) -> asyncio.Lock:
        lock = self._series_locks.get(series_id)
        if lock is None:
            lock = asyncio.Lock()
            self._series_locks[series_id] = lock
        return lock

    async def cog_load(self):
        self.session = self.bot.http_session
        await _ensure_db()
        # Registers the fixed custom_id so the button on any already-sent
        # message keeps working after a restart — same pattern as the
        # registry cog's TokenMessageView registration. The group/data
        # here is throwaway; only the custom_id inside matters.
        # poster_url must be a real string, never None — discord.ui.Thumbnail
        # requires an actual media URL to construct. Passing None here used
        # to raise right inside this call, meaning add_view() below never
        # ran and the button's custom_id never got registered — the "Add
        # Quality Check" button would silently stop working after every
        # single restart. This placeholder is never actually rendered or
        # sent anywhere; it only needs to exist long enough to construct
        # without error so the persistent-view registration succeeds.
        placeholder_group = {"type": "movie", "title": "placeholder", "year": None, "poster_url": "https://cdn.discordapp.com/embed/avatars/0.png", "quality_label": None}
        self.bot.add_view(NewMediaLayoutView(self.bot, placeholder_group))

        # Context menus aren't auto-registered by add_cog() the way
        # regular @app_commands.command methods are (they can't even be
        # defined as methods at all — see the comment above
        # toggle_sync_lock) — added to the tree manually here instead.
        # Guarded in case of a reload where cog_unload's remove_command
        # somehow didn't run first — already-registered is not a reason
        # to fail the whole cog load.
        try:
            self.bot.tree.add_command(toggle_sync_lock)
        except app_commands.CommandAlreadyRegistered:
            pass

        if not self.screenshot_sweep_loop.is_running():
            self.screenshot_sweep_loop.start()

    async def cog_unload(self):
        if self._debounce_task is not None:
            self._debounce_task.cancel()
        for task in list(self._announce_tasks):
            task.cancel()
        self.screenshot_sweep_loop.cancel()
        self.bot.tree.remove_command(toggle_sync_lock.name, type=toggle_sync_lock.type)
        # No session close here — main.py owns bot.http_session.

    def queue_item(self, item_id: str):
        """Called by dashboard.py's shared SSE connection whenever it sees
        a library.item.added event — this is the only entry point into
        this cog's pipeline now.

        Cancelling _debounce_task here is only ever safe because that
        task is PURELY the timer now — see _debounced_flush. Any post
        already in progress lives in its own task in _announce_tasks and
        is untouched by this."""
        self._pending_item_ids.add(item_id)
        if self._first_pending_at is None:
            self._first_pending_at = time.time()
        if self._debounce_task is not None and not self._debounce_task.done():
            self._debounce_task.cancel()
        self._debounce_task = asyncio.create_task(self._debounced_flush())

    async def _debounced_flush(self):
        # Quiet-period debounce, capped by MAX_GROUP_WAIT_SECONDS measured
        # from the FIRST item in this batch — a steady stream of incoming
        # items (including non-media noise from a large scan) can reset
        # the quiet-period wait indefinitely otherwise. Each restart of
        # this task recomputes how much of the ceiling is left, so the
        # cap holds regardless of how many times queue_item() cancels and
        # restarts this coroutine.
        wait = GROUP_DEBOUNCE_SECONDS
        if self._first_pending_at is not None:
            elapsed = time.time() - self._first_pending_at
            wait = max(0.0, min(wait, MAX_GROUP_WAIT_SECONDS - elapsed))
        try:
            await asyncio.sleep(wait)
        except asyncio.CancelledError:
            return  # superseded by a newer item within the window

        # No await anywhere between the sleep returning and create_task
        # below — a cancel from queue_item() cannot land inside this
        # section. The actual posting work is spawned as its OWN task,
        # independent of this one, so once it's running no later
        # queue_item() call can ever cancel it (see the module
        # docstring's "FLUSH TASK ISOLATION" section for the production
        # bug this fixes).
        item_ids = list(self._pending_item_ids)
        self._pending_item_ids.clear()
        self._first_pending_at = None
        if item_ids:
            task = asyncio.create_task(self._announce_items(item_ids))
            self._announce_tasks.add(task)
            task.add_done_callback(self._on_announce_task_done)

    def _on_announce_task_done(self, task: asyncio.Task):
        """Surfaces anything that killed an _announce_items run. Before
        this, an exception or cancel inside that task vanished with no
        log line at all — which is exactly how a posted-but-unrecorded
        announcement went unnoticed."""
        self._announce_tasks.discard(task)
        if task.cancelled():
            logger.warning("[NewMedia] Announce task was cancelled before finishing.")
            return
        exc = task.exception()
        if exc is not None:
            logger.error(f"[NewMedia] Announce task crashed: {type(exc).__name__}: {exc}", exc_info=exc)

    async def _requeue_after_delay(self, item_ids: list[str], delay: float = SERIES_LINK_RETRY_SECONDS):
        """An Episode with no SeriesId yet just means Jellyfin hasn't
        linked it to its parent series at fetch time — this resolves
        itself shortly after. Requeues through the normal queue_item()
        pipeline (same debounce/grouping path as any other item) rather
        than retrying inline."""
        await asyncio.sleep(delay)
        for item_id in item_ids:
            self.queue_item(item_id)

    def _circuit_breaker_is_open(self) -> bool:
        return time.time() < self._circuit_breaker_open_until

    def _record_screenshot_result(self, success: bool):
        """Updates the shared consecutive-failure counter that drives the
        circuit breaker. Called from _apply_auto_screenshot regardless of
        which caller triggered it (automatic on-post, the sweep, or a
        manual /redoscreenshot) — every real outcome counts toward the
        same tally, but only the AUTOMATIC on-post path and the sweep
        loop actually check _circuit_breaker_is_open() before attempting
        anything; manual commands stay available even while the breaker
        is open."""
        if success:
            self._consecutive_screenshot_failures = 0
            return
        self._consecutive_screenshot_failures += 1
        if self._consecutive_screenshot_failures >= SCREENSHOT_CIRCUIT_BREAKER_THRESHOLD and not self._circuit_breaker_is_open():
            self._circuit_breaker_open_until = time.time() + SCREENSHOT_CIRCUIT_BREAKER_COOLDOWN_SECONDS
            logger.warning(
                f"[NewMedia] Screenshot circuit breaker tripped after {self._consecutive_screenshot_failures} consecutive "
                f"failures — pausing automatic screenshot attempts (on-post and sweep) for {SCREENSHOT_CIRCUIT_BREAKER_COOLDOWN_SECONDS}s. "
                f"Manual /redoscreenshot and /testscreenshot are unaffected."
            )

    async def _attach_screenshot_bytes(self, channel, message_id: int, group: dict, posted_at: int, image_bytes: bytes) -> str | None:
        """Shared low-level attach step — used both for a freshly-grabbed
        screenshot (_apply_auto_screenshot) and by
        _preserve_or_refresh_screenshot's fallback path. Attaches
        image_bytes to message_id as part of THIS edit and returns a
        value the caller should persist as "this announcement has a
        screenshot" — either a real, reusable CDN URL, or
        SCREENSHOT_URL_UNKNOWN_SENTINEL if the upload succeeded but no
        resolvable URL could be determined for it. Returns None ONLY on
        a genuine failure — the edit call itself raising a real,
        non-NotFound error.

        Confirmed in production: Discord's response to a Components V2
        message edit can report an empty attachments list even though
        the upload genuinely succeeded and is visibly showing on the
        message right this moment — this happened via TWO different
        lookup methods tried at different points (reading the edit
        call's own return value, and a separate fetch_message()
        afterward), so it isn't a timing/propagation issue retries would
        fix, it's Discord/discord.py not reliably exposing this specific
        piece of information for this message type. Treating that as
        "the attach failed" was the actual root cause of two real bugs:
        the announcement never got marked as having a screenshot in the
        database (so the sweep and /fixscreenshots kept re-touching an
        announcement that already had a perfectly good image), and a
        safety-fallback elsewhere would then actively blank the image
        Discord was already correctly displaying. Both are fixed by this
        function no longer conflating "couldn't determine the URL" with
        "the attach didn't work."

        Two-step edit when a real URL IS found: the first edit attaches
        the file using Discord's attachment://filename scheme (required
        for a component to reference a file uploaded in the SAME
        request); a second edit substitutes the real URL in once known,
        so future re-renders reference a durable link instead of the
        request-scoped attachment:// placeholder. Skipped entirely when
        no real URL could be determined — there's nothing to swap in.

        discord.NotFound is deliberately let through, not caught here —
        callers that need to distinguish "message genuinely doesn't
        exist anymore" from "attach failed for some other reason" need
        that distinction preserved, not collapsed into one generic
        result."""
        try:
            file = discord.File(io.BytesIO(image_bytes), filename="quality_check.png")
            edited_message = await channel.get_partial_message(message_id).edit(
                attachments=[file],
                view=NewMediaLayoutView(self.bot, group, posted_at=posted_at, quality_image_url="attachment://quality_check.png"),
            )

            real_url = edited_message.attachments[0].url if edited_message.attachments else None

            if real_url is None:
                # Worth one more attempt via a fresh fetch — it might
                # still resolve it — but its failure no longer means the
                # operation itself failed, only that we're storing a
                # placeholder instead of a directly reusable link.
                try:
                    refetched = await channel.fetch_message(message_id)
                    if refetched.attachments:
                        real_url = refetched.attachments[0].url
                except discord.HTTPException:
                    pass

            if real_url is None:
                logger.info(
                    f"[NewMedia] Screenshot for message {message_id} uploaded successfully, but no resolvable URL "
                    "was exposed for it — storing a placeholder rather than treating this as a failure."
                )
                return SCREENSHOT_URL_UNKNOWN_SENTINEL

            await channel.get_partial_message(message_id).edit(view=NewMediaLayoutView(self.bot, group, posted_at=posted_at, quality_image_url=real_url))
            return real_url
        except discord.NotFound:
            raise
        except discord.HTTPException as e:
            logger.warning(f"[NewMedia] Screenshot attach failed for message {message_id}: {type(e).__name__}: {e}")
            return None

    async def _apply_auto_screenshot(self, channel, message_id: int, group: dict, posted_at: int, item_id_for_screenshot: str | None, runtime_ticks: int | None) -> bool:
        """Grabs a FRESH screenshot and attaches it — used both by the
        automatic pipeline (a just-posted announcement) and by
        /redoscreenshot (an admin manually adding or replacing one).
        Never raises — a screenshot failing leaves the manual "Add
        Quality Check" button available on the message exactly as if
        this feature didn't exist for that one item. Returns True only
        if a screenshot was actually attached and persisted;
        /redoscreenshot uses this to report success/failure back to the
        admin, the automatic call sites just fire-and-forget it.

        Takes channel/message_id rather than a discord.Message object so
        it works identically whether the message was JUST created in
        this same call stack (the automatic path) or is an arbitrary
        existing announcement looked up by ID (/redoscreenshot) — doesn't
        need the caller to already hold a live Message object.

        AUTO_QUALITY_SCREENSHOT_ENABLED is deliberately NOT checked here
        — that flag controls whether the AUTOMATIC pipeline fires on its
        own, not whether the feature can ever be used at all; an admin
        running /redoscreenshot should still work even with the automatic
        toggle off. Each automatic call site checks the flag itself
        before calling this.

        Every real outcome here feeds _record_screenshot_result — the
        shared circuit-breaker tally — regardless of which caller
        triggered this (automatic on-post, the sweep, or a manual
        /redoscreenshot); a trivial "nothing to do" case (no item_id, or
        the message itself no longer exists) does NOT count, since
        neither reflects whether Jellyfin or Discord are actually
        healthy right now."""
        if not item_id_for_screenshot:
            return False

        screenshot_bytes = await _grab_quality_screenshot_with_retry(self.session, item_id_for_screenshot, runtime_ticks)
        if screenshot_bytes is None:
            self._record_screenshot_result(False)
            return False  # ffmpeg failed for any reason (even after retrying) — manual button stays as the fallback

        try:
            real_url = await self._attach_screenshot_bytes(channel, message_id, group, posted_at, screenshot_bytes)
        except discord.NotFound:
            logger.warning(f"[NewMedia] Message {message_id} no longer exists — can't attach a screenshot to it.")
            return False
        if real_url is None:
            self._record_screenshot_result(False)
            return False

        await _set_quality_image(message_id, real_url)
        logger.info(f"[NewMedia] Quality-check screenshot attached for message {message_id}.")
        self._record_screenshot_result(True)
        return True

    async def _reattach_existing_screenshot(self, channel, message_id: int, group: dict, posted_at: int, existing_url: str) -> str | None:
        """Re-downloads an ALREADY-STORED screenshot and re-attaches it
        as part of THIS content edit, instead of just carrying the old
        URL string forward inside the new view and hoping Discord keeps
        showing it. Confirmed in production: editing a Components V2
        message's content (a series rollup picking up a new episode,
        or /syncmedia refreshing metadata) WITHOUT re-attaching the
        image in that same edit call made the screenshot silently
        disappear — 100% of the time on every /syncmedia run and every
        series-rollup edit, not an occasional URL-expiry fluke.

        Only ever called with a real http(s) URL — see
        _preserve_or_refresh_screenshot, which is what callers should
        actually use; it handles the case where there's nothing real to
        download (SCREENSHOT_URL_UNKNOWN_SENTINEL) by falling back to a
        fresh grab instead.

        Returns the new stable CDN URL to store going forward, or None
        if the re-download or re-attach failed — the caller (
        _preserve_or_refresh_screenshot) treats that as "couldn't
        preserve the exact image" and falls back to a fresh grab, not as
        a reason to remove the screenshot entirely."""
        try:
            async with self.session.get(existing_url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    logger.warning(f"[NewMedia] Couldn't re-download existing screenshot for message {message_id} (HTTP {resp.status}).")
                    return None
                image_bytes = await resp.read()
        except Exception as e:
            logger.warning(f"[NewMedia] Failed to re-download existing screenshot for message {message_id}: {type(e).__name__}: {e}")
            return None

        return await self._attach_screenshot_bytes(channel, message_id, group, posted_at, image_bytes)

    async def _preserve_or_refresh_screenshot(self, channel, message_id: int, group: dict, posted_at: int, existing_url: str | None, item_id_for_screenshot: str | None, runtime_ticks: int | None) -> str | None:
        """The single entry point any content edit should use to keep
        SOME screenshot showing on an announcement that already has one
        (a series rollup picking up a new episode, /syncmedia refreshing
        metadata). Tries, in order:
          1. Preserve the EXACT existing image, if existing_url is a
             real http(s) link — re-downloads and re-attaches it.
          2. If that's not possible (existing_url is
             SCREENSHOT_URL_UNKNOWN_SENTINEL, or step 1 genuinely
             failed), grab a COMPLETELY FRESH screenshot instead.

        This is what guarantees an announcement that already has a
        screenshot NEVER ends up with none automatically — the caller
        only sees None back from this method if EVERY option failed
        (network down, ffmpeg down, Discord rejecting the edit, all at
        once), and even then should leave the announcement's CURRENT
        stored value untouched rather than actively blank it — see the
        module docstring's "never actively wipe an existing screenshot"
        note."""
        if existing_url and existing_url.startswith("http"):
            preserved_url = await self._reattach_existing_screenshot(channel, message_id, group, posted_at, existing_url)
            if preserved_url:
                return preserved_url
            logger.info(f"[NewMedia] Couldn't preserve the exact existing screenshot for message {message_id} — grabbing a fresh one instead.")

        if not item_id_for_screenshot:
            return None

        screenshot_bytes = await _grab_quality_screenshot_with_retry(self.session, item_id_for_screenshot, runtime_ticks)
        if screenshot_bytes is None:
            return None

        return await self._attach_screenshot_bytes(channel, message_id, group, posted_at, screenshot_bytes)

    async def _run_screenshot_sweep(self, bypass_circuit_breaker: bool = False) -> tuple[int, int, int]:
        """Core sweep logic, shared by the periodic screenshot_sweep_loop
        and /fixscreenshots's manual force-run. Finds every
        announcement still missing a screenshot and retries each one,
        pacing between attempts. Returns (succeeded, failed, total_checked).

        bypass_circuit_breaker=True (used by /fixscreenshots) skips
        the breaker check entirely — an admin explicitly forcing this to
        run right now should get exactly that, not be silently blocked
        by a breaker meant to protect against UNATTENDED automatic
        hammering, not a deliberate one-off request."""
        if not bypass_circuit_breaker and self._circuit_breaker_is_open():
            logger.info("[NewMedia] Screenshot sweep skipped — circuit breaker is currently open.")
            return 0, 0, 0

        records = await _get_announcements_missing_screenshot()
        if not records:
            return 0, 0, 0

        succeeded = 0
        failed = 0
        for record in records:
            if not bypass_circuit_breaker and self._circuit_breaker_is_open():
                logger.info(f"[NewMedia] Screenshot sweep stopping early — circuit breaker tripped mid-sweep ({succeeded} succeeded, {failed} failed so far).")
                break

            channel = self.bot.get_channel(record["channel_id"])
            if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                continue

            group = await self._rebuild_group_from_record(record)
            if group is None:
                continue  # underlying Jellyfin item(s) gone — nothing to screenshot, not a failure worth counting

            item_id = group.get("_screenshot_item_id")
            runtime_ticks = group.get("_screenshot_runtime_ticks")
            success = await self._apply_auto_screenshot(channel, record["message_id"], group, record["posted_at"], item_id, runtime_ticks)
            if success:
                succeeded += 1
            else:
                failed += 1

            await asyncio.sleep(SCREENSHOT_SWEEP_PACE_SECONDS)

        return succeeded, failed, len(records)

    async def _adopt_message(self, msg: discord.Message) -> bool:
        """Registers one untracked announcement message. Returns True if a
        row was written. Returns False (no row) if the message isn't
        recognisable as an announcement or its Jellyfin items can't be
        resolved — never guesses."""
        text, media_urls = _extract_announcement_parts(msg)

        if "NEW MOVIE" in text:
            group_type = "movie"
        elif "NEW EPISODES" in text:
            group_type = "series"
        else:
            return False

        poster_id = None
        for u in media_urls:
            m = _POSTER_ID_RE.search(u)
            if m:
                poster_id = m.group(1)
                break
        if not poster_id:
            return False

        posted_at = int(msg.created_at.timestamp())

        if group_type == "movie":
            items, err = await jellyfin_get_items_batched(self.session, [poster_id], ITEM_FIELDS)
            if err or not items or items[0].get("Type") != "Movie":
                return False
            item_ids = [poster_id]
            series_id = None
            rollup_expires_at = None
        else:
            codes = {(int(s), int(e)) for s, e in _EPISODE_CODE_RE.findall(text)}
            if not codes:
                return False
            data, err = await jellyfin_get(
                self.session, f"/Shows/{poster_id}/Episodes",
                params={"Fields": "ParentIndexNumber,IndexNumber"},
            )
            if err or not data:
                return False
            item_ids = [
                i["Id"] for i in data.get("Items", [])
                if (i.get("ParentIndexNumber"), i.get("IndexNumber")) in codes
            ]
            if not item_ids:
                return False
            series_id = poster_id
            # Reopen the rollup window only if it would still be open had
            # the message been recorded normally.
            rollup_expires_at = posted_at + SERIES_ROLLUP_HOURS * 3600
            if rollup_expires_at <= int(time.time()):
                rollup_expires_at = None

        await _record_announcement(msg.id, msg.channel.id, group_type, item_ids, posted_at,
                                   series_id=series_id, rollup_expires_at=rollup_expires_at)

        # Already has a screenshot? Record it so the sweep doesn't grab
        # another one over the top of it.
        other_urls = [u for u in media_urls if not _POSTER_ID_RE.search(u)]
        if other_urls or msg.attachments or "Quality Check Approved" in text:
            screenshot_url = next((u for u in other_urls if u.startswith("http")), None)
            if screenshot_url is None:
                screenshot_url = msg.attachments[0].url if msg.attachments else SCREENSHOT_URL_UNKNOWN_SENTINEL
            await _set_quality_image(msg.id, screenshot_url)

        logger.info(f"[NewMedia] Auto-adopted untracked {group_type} announcement {msg.id} ({len(item_ids)} item(s)).")
        return True

    async def _auto_adopt_untracked(self) -> int:
        """Scans recent messages in CHANNEL_ID for this bot's announcements
        with no DB row and adopts them. Returns how many were adopted.
        Never raises."""
        channel = self.bot.get_channel(CHANNEL_ID)
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)) or self.bot.user is None:
            return 0

        try:
            tracked = {r["message_id"] for r in await _get_all_announcements()}
        except Exception as e:
            logger.error(f"[NewMedia] Auto-adopt: couldn't read DB: {type(e).__name__}: {e}")
            return 0

        now = time.time()
        adopted = 0
        try:
            async for msg in channel.history(limit=ADOPT_SCAN_LIMIT):
                if msg.author.id != self.bot.user.id or msg.id in tracked:
                    continue
                if now - msg.created_at.timestamp() < ADOPT_MIN_AGE_SECONDS:
                    continue
                try:
                    if await self._adopt_message(msg):
                        adopted += 1
                except Exception as e:
                    logger.error(f"[NewMedia] Auto-adopt failed for message {msg.id}: {type(e).__name__}: {e}")
        except discord.HTTPException as e:
            logger.error(f"[NewMedia] Auto-adopt: couldn't read channel history: {e}")

        if adopted:
            logger.info(f"[NewMedia] Auto-adopt: registered {adopted} untracked announcement(s).")
        return adopted

    @tasks.loop(minutes=SCREENSHOT_SWEEP_INTERVAL_MINUTES)
    async def screenshot_sweep_loop(self):
        """Self-healing pass: finds every announcement still missing a
        quality-check screenshot (the automatic on-post attempt failed,
        even after its own retries) and tries again. This is what turns
        a transient failure into a temporary one instead of a permanent
        one — if Jellyfin was under load at the exact moment an episode
        was announced, this sweep tries again once that's passed,
        without anyone needing to notice and run /redoscreenshot by
        hand. Respects the circuit breaker — see _run_screenshot_sweep."""
        await self._auto_adopt_untracked()
        if not AUTO_QUALITY_SCREENSHOT_ENABLED:
            return
        succeeded, failed, total = await self._run_screenshot_sweep(bypass_circuit_breaker=False)
        if succeeded or failed:
            logger.info(f"[NewMedia] Screenshot sweep complete — {succeeded} fixed, {failed} still missing (of {total} checked).")

    @screenshot_sweep_loop.before_loop
    async def before_screenshot_sweep_loop(self):
        await self.bot.wait_until_ready()

    @screenshot_sweep_loop.error
    async def screenshot_sweep_loop_error(self, error: BaseException):
        logger.error(f"[NewMedia] screenshot_sweep_loop crashed: {type(error).__name__}: {error}", exc_info=error)
        if not self.screenshot_sweep_loop.is_running():
            self.screenshot_sweep_loop.start()

    async def _announce_items(self, item_ids: list[str]):
        # Batched fetch (chunked internally) for every queued item instead
        # of one call per item, and instead of one unchunked call that can
        # 414 on a large batch.
        item_list, err = await jellyfin_get_items_batched(self.session, item_ids, ITEM_FIELDS)
        if err:
            logger.error(f"[NewMedia] Failed to fetch new item details: {err}")
            return
        if not item_list:
            logger.error("[NewMedia] Failed to fetch new item details: no items returned.")
            return

        # Jellyfin fires library.item.added for supporting metadata too —
        # a new Studio/Person/Genre/MusicAlbum record created as a side
        # effect of adding real media, not real watchable media itself.
        # "Series" is deliberately EXCLUDED here — Jellyfin can fire a
        # separate library.item.added for the bare Series metadata object
        # itself (not any specific episode) when a new episode of a
        # previously-unindexed show comes in. That Series item was
        # falling through to the standalone/movie branch below (nothing
        # excluded it), so a single new episode could produce TWO cards:
        # a correct "NEW EPISODES" one from the real Episode, and a
        # phantom "NEW MOVIE" one from the Series stub itself (rendered
        # with the show's premiere year as if it were a movie release —
        # confirmed in production: one Reacher S4E4 episode produced both
        # a correct episode card AND a fake "Reacher 2022" movie card).
        # A bare Series object carries no watchable content of its own —
        # the episode already has SeriesName/SeriesId, which is all the
        # grouped card needs — so Series is treated the same as any other
        # non-announceable supporting metadata: skipped, logged, no card.
        ANNOUNCEABLE_TYPES = {"Movie", "Episode"}
        skipped = [i for i in item_list if i.get("Type") not in ANNOUNCEABLE_TYPES]
        if skipped:
            skipped_desc = [(i.get("Type"), i.get("Id")) for i in skipped]
            logger.info(f"[NewMedia] Skipped {len(skipped)} non-media item(s) from this batch (not announceable): {skipped_desc}")
        item_list = [i for i in item_list if i.get("Type") in ANNOUNCEABLE_TYPES]

        if not item_list:
            logger.info("[NewMedia] Nothing announceable in this batch after filtering — no message sent.")
            return

        series_new_item_ids: dict[str, list[str]] = {}
        standalone: list[dict] = []
        retry_item_ids: list[str] = []

        for item in item_list:
            item_type = item.get("Type")
            item_id = item.get("Id")
            if item_type == "Episode":
                sid = item.get("SeriesId")
                if sid:
                    series_new_item_ids.setdefault(sid, []).append(item_id)
                    self._series_link_retry_counts.pop(item_id, None)  # succeeded — drop any stale retry count
                else:
                    # SeriesId not linked yet at fetch time — never post
                    # an Episode as a standalone "movie" card. At this
                    # unmatched stage its raw Name is often an unprocessed
                    # release filename, not clean metadata (confirmed in
                    # production: an episode posted as "NEW MOVIE — Silo
                    # - S02E01 - The Engineer WEBDL"). Requeue instead.
                    attempts = self._series_link_retry_counts.get(item_id, 0)
                    if attempts >= MAX_SERIES_LINK_RETRIES:
                        logger.warning(f"[NewMedia] Episode {item_id} still has no SeriesId after {attempts} retries — giving up, not posting.")
                        self._series_link_retry_counts.pop(item_id, None)
                    else:
                        self._series_link_retry_counts[item_id] = attempts + 1
                        retry_item_ids.append(item_id)
            else:  # Movie — the only other type ANNOUNCEABLE_TYPES admits
                standalone.append(item)

        if retry_item_ids:
            logger.info(f"[NewMedia] {len(retry_item_ids)} episode(s) missing SeriesId — requeuing in {SERIES_LINK_RETRY_SECONDS}s: {retry_item_ids}")
            asyncio.create_task(self._requeue_after_delay(retry_item_ids))

        channel = self.bot.get_channel(CHANNEL_ID)
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            logger.error(f"[NewMedia] CHANNEL_ID {CHANNEL_ID} isn't a valid text channel — check config.")
            return

        for sid, new_ids in series_new_item_ids.items():
            await self._handle_series_batch(channel, sid, new_ids)

        for item in standalone:
            group = {
                "type": "movie",
                "title": item.get("Name", "Unknown title"),
                "year": item.get("ProductionYear"),
                "poster_url": get_poster_url(item.get("Id")),
                "quality_label": get_quality_label(item),
            }
            posted_at = int(time.time())

            # Send, record, and screenshot are separate steps with their
            # own error handling — a DB failure after a successful send
            # used to escape as an uncaught non-HTTPException and kill the
            # whole task silently, leaving a live message with no row.
            try:
                msg = await channel.send(view=NewMediaLayoutView(self.bot, group, posted_at=posted_at))
            except discord.HTTPException as e:
                logger.error(f"[NewMedia] Failed to post movie announcement for '{group['title']}': {e}")
                continue
            logger.info(f"[NewMedia] Posted movie announcement for '{group['title']}' — message {msg.id}.")

            try:
                await _record_announcement(msg.id, channel.id, "movie", [item.get("Id")], posted_at)
            except Exception as e:
                logger.error(
                    f"[NewMedia] Posted message {msg.id} for '{group['title']}' but FAILED to record it in the DB: "
                    f"{type(e).__name__}: {e} — use /adoptannouncement {msg.id} {item.get('Id')} to recover it."
                )
                continue

            if AUTO_QUALITY_SCREENSHOT_ENABLED and not self._circuit_breaker_is_open():
                await self._apply_auto_screenshot(channel, msg.id, group, posted_at, item.get("Id"), item.get("RunTimeTicks"))

    async def _handle_series_batch(self, channel, series_id: str, new_item_ids: list[str]):
        """Either edits an already-open announcement for this series (its
        rollup window hasn't closed yet) with the combined + re-sorted
        episode list, or posts a brand new announcement and opens a fresh
        window for it. Locked per-series so overlapping flushes for the
        same show can't race each other.

        The edit path always recomputes rollup_expires_at to
        now + SERIES_ROLLUP_HOURS — this is what makes the window ROLL
        forward on every new episode instead of staying fixed from
        whenever the announcement was first posted (see the module
        docstring's "SERIES ROLLUP WINDOW" section)."""
        async with self._lock_for_series(series_id):
            open_record = await _get_open_series_announcement(series_id)

            if open_record is not None:
                existing_ids = open_record["item_ids"].split(",") if open_record["item_ids"] else []
                # Dedupe in case the same item_id somehow gets queued twice
                # (e.g. a duplicate SSE event).
                combined_ids = list(dict.fromkeys(existing_ids + new_item_ids))

                group = await self._build_series_group(combined_ids)
                if group is None:
                    logger.error(f"[NewMedia] Series {series_id}: failed to re-fetch combined item list, skipping edit.")
                    return

                new_rollup_expires_at = int(time.time()) + SERIES_ROLLUP_HOURS * 3600
                existing_url = open_record.get("quality_image_url")

                try:
                    if existing_url:
                        # Tries hardest to preserve the exact existing
                        # image, falling back to a fresh grab if that's
                        # not possible — see
                        # _preserve_or_refresh_screenshot's docstring.
                        # discord.NotFound propagates up through this if
                        # the message is genuinely gone, handled below
                        # same as the plain-edit path.
                        item_id = group.get("_screenshot_item_id")
                        runtime_ticks = group.get("_screenshot_runtime_ticks")
                        new_url = await self._preserve_or_refresh_screenshot(
                            channel, open_record["message_id"], group, open_record["posted_at"],
                            existing_url, item_id, runtime_ticks,
                        )
                        if new_url:
                            await _set_quality_image(open_record["message_id"], new_url)
                        else:
                            # EVERYTHING failed (couldn't preserve, and
                            # couldn't grab fresh either). Per the "never
                            # actively wipe an existing screenshot" rule
                            # — confirmed in production this exact
                            # fallback path used to blank a working
                            # image over a false-negative verification
                            # failure — this edits the content using the
                            # OLD stored reference as-is rather than
                            # removing it. It may not resolve perfectly,
                            # but that's strictly better than definitely
                            # destroying a working image.
                            logger.warning(f"[NewMedia] Couldn't preserve or refresh the screenshot for message {open_record['message_id']} — leaving the previous reference in place rather than removing it.")
                            await channel.get_partial_message(open_record["message_id"]).edit(
                                view=NewMediaLayoutView(self.bot, group, posted_at=open_record["posted_at"], quality_image_url=existing_url)
                            )
                    else:
                        await channel.get_partial_message(open_record["message_id"]).edit(
                            view=NewMediaLayoutView(self.bot, group, posted_at=open_record["posted_at"], quality_image_url=None)
                        )

                    await _update_announcement_item_ids(open_record["message_id"], combined_ids, rollup_expires_at=new_rollup_expires_at)
                    logger.info(
                        f"[NewMedia] Edited series announcement for '{group['title']}' "
                        f"(message {open_record['message_id']}) — now {len(combined_ids)} episode(s), "
                        f"window extended to {SERIES_ROLLUP_HOURS}h from now."
                    )
                except discord.NotFound:
                    # The open message was deleted out-of-band — the DB
                    # thinks a window is still open pointing at nothing.
                    # Fall through to posting a fresh announcement instead
                    # of failing silently forever on every future episode.
                    logger.warning(f"[NewMedia] Series {series_id}: open announcement message missing, posting a new one instead.")
                    await self._post_new_series_announcement(channel, series_id, combined_ids)
                except discord.HTTPException as e:
                    logger.error(f"[NewMedia] Series {series_id}: failed to edit open announcement: {e}")
                return

            # No open window for this series — start a fresh one.
            await self._post_new_series_announcement(channel, series_id, new_item_ids)

    async def _post_new_series_announcement(self, channel, series_id: str, item_ids: list[str]):
        group = await self._build_series_group(item_ids)
        if group is None:
            logger.error(f"[NewMedia] Series {series_id}: failed to fetch item details, no announcement posted.")
            return

        posted_at = int(time.time())
        rollup_expires_at = posted_at + SERIES_ROLLUP_HOURS * 3600

        # Same send / record / screenshot split as the movie path — see
        # the comment there.
        try:
            msg = await channel.send(view=NewMediaLayoutView(self.bot, group, posted_at=posted_at))
        except discord.HTTPException as e:
            logger.error(f"[NewMedia] Failed to post series announcement for '{group['title']}': {e}")
            return
        logger.info(f"[NewMedia] Posted series announcement for '{group['title']}' ({len(group['episodes'])} episode(s)) — message {msg.id}.")

        try:
            await _record_announcement(msg.id, channel.id, "series", item_ids, posted_at,
                                        series_id=series_id, rollup_expires_at=rollup_expires_at)
        except Exception as e:
            logger.error(
                f"[NewMedia] Posted series message {msg.id} for '{group['title']}' but FAILED to record it in the DB: "
                f"{type(e).__name__}: {e} — use /adoptannouncement {msg.id} {item_ids[0]} to recover it."
            )
            return

        # Only on the FIRST post of a series announcement — see
        # _apply_auto_screenshot's docstring and the module
        # docstring's "AUTO QUALITY-CHECK SCREENSHOT" section for why
        # this is never re-attempted on later rollup edits.
        if AUTO_QUALITY_SCREENSHOT_ENABLED and not self._circuit_breaker_is_open():
            await self._apply_auto_screenshot(
                channel, msg.id, group, posted_at,
                group.get("_screenshot_item_id"), group.get("_screenshot_runtime_ticks"),
            )

    async def _build_series_group(self, item_ids: list[str]) -> dict | None:
        """Fetches fresh metadata for the given item IDs and builds a
        series group dict with episodes ALWAYS sorted by (season, episode)
        — used for both a brand new announcement and an edit of an
        already-open one, so display order never depends on the order
        episodes were downloaded or announced in.

        _screenshot_item_id/_screenshot_runtime_ticks are internal keys,
        not read by build_new_media_container — they exist purely so
        _post_new_series_announcement can auto-grab a representative
        screenshot from the FIRST episode without this function's return
        signature needing to change everywhere else it's used
        (_handle_series_batch's edit path and _rebuild_group_from_record
        both ignore these two keys entirely)."""
        item_list, err = await jellyfin_get_items_batched(self.session, item_ids, ITEM_FIELDS)
        if err:
            logger.error(f"[NewMedia] Failed to fetch series item details: {err}")
            return None
        if not item_list:
            return None

        first = item_list[0]
        episodes = sorted(
            [(i.get("ParentIndexNumber"), i.get("IndexNumber"), i.get("Name", "Unknown episode")) for i in item_list],
            key=lambda e: (e[0] if e[0] is not None else 0, e[1] if e[1] is not None else 0),
        )
        return {
            "type": "series",
            "title": first.get("SeriesName", "Unknown Series"),
            "year": None,
            "poster_url": get_poster_url(first.get("SeriesId")) if first.get("SeriesId") else get_poster_url(item_ids[0]),
            "quality_label": get_quality_label(first),
            "episodes": episodes,
            "_screenshot_item_id": first.get("Id"),
            "_screenshot_runtime_ticks": first.get("RunTimeTicks"),
        }

    async def _rebuild_group_from_record(self, record: dict) -> dict | None:
        """Shared by /syncmedia, the Add Quality Check modal, and
        /redoscreenshot — all three need to reconstruct a fresh group
        dict from a stored announcement record. For a series record this
        reflects whatever the CURRENT stored item_ids list is, which
        keeps growing as long as its rollup window stays open — not a
        frozen snapshot from first post.

        _screenshot_item_id/_screenshot_runtime_ticks (movie branch too,
        matching _build_series_group's series branch) are internal keys
        /redoscreenshot reads to know which single item to grab a frame
        from — not read by build_new_media_container itself."""
        item_ids = record["item_ids"].split(",")

        if record["group_type"] == "series":
            return await self._build_series_group(item_ids)

        item_list, err = await jellyfin_get_items_batched(self.session, item_ids, ITEM_FIELDS)
        if err or not item_list:
            return None
        first = item_list[0]
        return {
            "type": "movie",
            "title": first.get("Name", "Unknown title"),
            "year": first.get("ProductionYear"),
            "poster_url": get_poster_url(first.get("Id")),
            "quality_label": get_quality_label(first),
            "_screenshot_item_id": first.get("Id"),
            "_screenshot_runtime_ticks": first.get("RunTimeTicks"),
        }

    # --- /syncmedia ---
    @app_commands.command(name="syncmedia", description="[Owner] Re-fetch metadata and refresh every posted new-media announcement, showing live progress")
    async def syncmedia(self, interaction: discord.Interaction):
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        records = await _get_all_announcements()
        if not records:
            return await interaction.followup.send("❌ No stored announcements to sync.", ephemeral=True)

        started_at = int(time.time())
        counts = {"fixed": 0, "unchanged": 0, "locked": 0, "missing_items": 0, "missing_message": 0, "failed": 0}

        # Placeholder label until each record is actually processed and
        # we learn its real title — replaced in place at that point.
        entries: list[dict] = [
            {"label": f"{'📺 Series' if r['group_type'] == 'series' else '🎬 Movie'} (msg {r['message_id']})", "status": "checking"}
            for r in records
        ]

        await interaction.followup.send(
            view=_build_sync_progress_view(entries, 0, len(records), counts, started_at, finished=False),
            ephemeral=True,
        )
        last_progress_push = time.time()

        for i, record in enumerate(records):
            entry = entries[i]

            if record.get("locked"):
                # Owner manually fixed this one (via /embededit or by
                # hand) and locked it — never touch it again. No Discord
                # call made here at all, so no pacing needed either.
                entry["status"] = "locked"
                counts["locked"] += 1
            else:
                channel = self.bot.get_channel(record["channel_id"])
                if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                    entry["status"] = "failed"
                    counts["failed"] += 1
                    logger.error(f"[NewMedia] syncmedia: channel {record['channel_id']} not found for message {record['message_id']}.")
                else:
                    # Rebuilding the group first (a Jellyfin call, not a
                    # Discord one — no rate-limit cost here) means the
                    # missing-items case is caught before ever touching
                    # Discord at all.
                    group = await self._rebuild_group_from_record(record)
                    if group is None:
                        entry["status"] = "missing_items"
                        counts["missing_items"] += 1
                    else:
                        entry["label"] = f"{'📺' if group['type'] == 'series' else '🎬'} {group['title']}"
                        new_signature = _compute_group_signature(group)
                        changed = new_signature != record.get("last_sync_signature")

                        existing_url = record.get("quality_image_url")

                        try:
                            if existing_url:
                                # Tries hardest to preserve the exact
                                # existing image, falling back to a
                                # fresh grab if that's not possible — see
                                # _preserve_or_refresh_screenshot's
                                # docstring. Confirmed in production:
                                # /syncmedia stripped the screenshot off
                                # every single announcement it touched,
                                # 100% of the time, when it only
                                # referenced the old URL in the new view
                                # instead of re-attaching it.
                                item_id = group.get("_screenshot_item_id")
                                runtime_ticks = group.get("_screenshot_runtime_ticks")
                                new_url = await self._preserve_or_refresh_screenshot(
                                    channel, record["message_id"], group, record["posted_at"],
                                    existing_url, item_id, runtime_ticks,
                                )
                                if new_url:
                                    await _set_quality_image(record["message_id"], new_url)
                                else:
                                    # EVERYTHING failed. Per the "never
                                    # actively wipe an existing
                                    # screenshot" rule — confirmed in
                                    # production this exact fallback used
                                    # to blank a working image over a
                                    # false-negative verification failure
                                    # — this edits the content using the
                                    # OLD stored reference as-is rather
                                    # than removing it.
                                    logger.warning(f"[NewMedia] Couldn't preserve or refresh the screenshot for message {record['message_id']} — leaving the previous reference in place rather than removing it.")
                                    await channel.get_partial_message(record["message_id"]).edit(view=NewMediaLayoutView(
                                        self.bot, group, posted_at=record["posted_at"], quality_image_url=existing_url,
                                    ))
                            else:
                                # get_partial_message().edit() edits by ID
                                # directly — ONE Discord API call, not
                                # fetch_message() + edit() (two). Fires
                                # once per record across the whole sync,
                                # halving the total Discord call volume
                                # for the entire run (confirmed as the
                                # actual fix for /syncmedia occasionally
                                # hitting Discord's rate limit despite
                                # pacing — the pacing was always correct,
                                # there were just twice as many calls per
                                # record as necessary).
                                await channel.get_partial_message(record["message_id"]).edit(view=NewMediaLayoutView(
                                    self.bot, group, posted_at=record["posted_at"], quality_image_url=None,
                                ))
                            await _set_sync_signature(record["message_id"], new_signature)
                            entry["status"] = "fixed" if changed else "unchanged"
                            counts["fixed" if changed else "unchanged"] += 1
                        except discord.NotFound:
                            # Confirmed dead — this row can never sync
                            # successfully again, so it's removed here
                            # rather than left to accumulate forever.
                            await _delete_announcement(record["message_id"])
                            entry["status"] = "missing_message"
                            counts["missing_message"] += 1
                        except discord.HTTPException as e:
                            entry["status"] = "failed"
                            counts["failed"] += 1
                            logger.error(f"[NewMedia] syncmedia: failed to edit message {record['message_id']}: {e}")

                    await asyncio.sleep(SYNC_PACE_SECONDS)

            # Progress-message edits are throttled INDEPENDENTLY of
            # SYNC_PACE_SECONDS above — editing THIS message on every
            # single record would just move the exact same rate-limit
            # problem onto a different message. Always pushes on the
            # final record regardless of timing, so the finished state
            # is never stale.
            is_last = (i == len(records) - 1)
            if is_last or (time.time() - last_progress_push) >= SYNC_PROGRESS_UPDATE_MIN_INTERVAL_SECONDS:
                await interaction.edit_original_response(
                    view=_build_sync_progress_view(entries, i + 1, len(records), counts, started_at, finished=is_last)
                )
                last_progress_push = time.time()

        logger.info(
            f"[NewMedia] syncmedia run by <@{interaction.user.id}>: {counts['fixed']} fixed, {counts['unchanged']} unchanged, "
            f"{counts['locked']} locked, {counts['missing_items']} missing items, {counts['missing_message']} missing message, "
            f"{counts['failed']} failed."
        )

    @syncmedia.error
    async def syncmedia_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


    # --- /redoscreenshot ---
    @app_commands.command(name="redoscreenshot", description="[Owner] Add or replace the quality-check screenshot on an existing announcement")
    @app_commands.describe(message_id="The announcement message's ID")
    async def redoscreenshot(self, interaction: discord.Interaction, message_id: str):
        """Works whether or not AUTO_QUALITY_SCREENSHOT_ENABLED is on —
        this is an explicit manual action, not the automatic pipeline, so
        it bypasses that flag entirely (see _apply_auto_screenshot's
        docstring). If the announcement has no screenshot yet, this adds
        one. If it already has one, this replaces it with a freshly
        grabbed frame at a new random point in the same
        SCREENSHOT_WINDOW_START_SECONDS-SCREENSHOT_WINDOW_END_SECONDS
        window — so the image is visibly different each time, not a
        re-fetch of the identical shot."""
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        try:
            message_id_int = int(message_id.strip())
        except ValueError:
            return await interaction.response.send_message("❌ That doesn't look like a valid message ID.", ephemeral=True)

        record = await _get_announcement(message_id_int)
        if record is None:
            return await interaction.response.send_message(
                "❌ No tracked announcement found for that message ID. If the message exists but was never recorded, "
                "run `/adoptannouncement` on it first.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True)

        channel = self.bot.get_channel(record["channel_id"])
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            return await interaction.followup.send("❌ Couldn't find the channel that message was posted in.", ephemeral=True)

        group = await self._rebuild_group_from_record(record)
        if group is None:
            return await interaction.followup.send("❌ Couldn't re-fetch the underlying Jellyfin item(s) — they may have been removed from the library.", ephemeral=True)

        item_id = group.get("_screenshot_item_id")
        runtime_ticks = group.get("_screenshot_runtime_ticks")
        if not item_id:
            return await interaction.followup.send("❌ Couldn't determine which item to screenshot for this announcement.", ephemeral=True)

        had_image_before = bool(record.get("quality_image_url"))

        success = await self._apply_auto_screenshot(channel, message_id_int, group, record["posted_at"], item_id, runtime_ticks)
        if not success:
            return await interaction.followup.send(
                "❌ Couldn't grab or attach a screenshot — check the logs (ffmpeg failure, or the message/channel is gone).",
                ephemeral=True,
            )

        verb = "Replaced" if had_image_before else "Added"
        await interaction.followup.send(f"✅ {verb} the quality-check screenshot for **{group.get('title', 'this announcement')}**.", ephemeral=True)

    @redoscreenshot.error
    async def redoscreenshot_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    # --- /adoptannouncement ---
    @app_commands.command(name="adoptannouncement", description="[Owner] Track an existing announcement message that has no database record")
    @app_commands.describe(
        message_id="The announcement message's ID",
        item_id="Jellyfin item ID — the id=... value in the item's Jellyfin web URL",
    )
    async def adoptannouncement(self, interaction: discord.Interaction, message_id: str, item_id: str):
        """Recovery path for a message that made it onto Discord but never
        got its DB row (the send succeeded, the record step didn't run).
        Without a row, /redoscreenshot, /syncmedia, the Quality Check
        button and Toggle Sync Lock all treat the message as unknown.

        posted_at is taken from the message's own creation time so the
        "Added X ago" footer stays accurate. A series message is adopted
        with rollup_expires_at=None — it is never reopened as a rollup
        target; new episodes of that show start a fresh announcement."""
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        try:
            message_id_int = int(message_id.strip())
        except ValueError:
            return await interaction.response.send_message("❌ That doesn't look like a valid message ID.", ephemeral=True)

        item_id = item_id.strip()
        if not item_id:
            return await interaction.response.send_message("❌ Item ID can't be empty.", ephemeral=True)

        existing = await _get_announcement(message_id_int)
        if existing is not None:
            return await interaction.response.send_message("ℹ️ That message is already tracked — nothing to adopt.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        channel = self.bot.get_channel(CHANNEL_ID)
        if not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
            return await interaction.followup.send(f"❌ CHANNEL_ID {CHANNEL_ID} isn't a valid text channel.", ephemeral=True)

        try:
            msg = await channel.fetch_message(message_id_int)
        except discord.NotFound:
            return await interaction.followup.send("❌ No message with that ID in the announcement channel.", ephemeral=True)
        except discord.HTTPException as e:
            return await interaction.followup.send(f"❌ Couldn't fetch the message: `{e}`", ephemeral=True)

        if msg.author.id != self.bot.user.id:
            return await interaction.followup.send("❌ That message wasn't posted by this bot.", ephemeral=True)

        items, err = await jellyfin_get_items_batched(self.session, [item_id], ITEM_FIELDS)
        if err or not items:
            return await interaction.followup.send(f"❌ Jellyfin lookup failed: `{err or 'no item with that ID'}`", ephemeral=True)

        item = items[0]
        item_type = item.get("Type")
        if item_type == "Movie":
            group_type, series_id = "movie", None
        elif item_type == "Episode":
            group_type, series_id = "series", item.get("SeriesId")
        else:
            return await interaction.followup.send(
                f"❌ That Jellyfin item is a `{item_type}`, not a Movie or Episode.", ephemeral=True,
            )

        try:
            await _record_announcement(
                message_id_int, channel.id, group_type, [item["Id"]], int(msg.created_at.timestamp()),
                series_id=series_id, rollup_expires_at=None,
            )
        except Exception as e:
            return await interaction.followup.send(f"❌ DB write failed: `{type(e).__name__}: {e}`", ephemeral=True)

        title = item.get("SeriesName") if group_type == "series" else item.get("Name")
        logger.info(f"[NewMedia] Adopted message {message_id_int} as {group_type} '{title}' (item {item['Id']}).")
        await interaction.followup.send(
            f"✅ Now tracking message `{message_id_int}` as {group_type} **{title}**. `/redoscreenshot` will work on it now.",
            ephemeral=True,
        )

    @adoptannouncement.error
    async def adoptannouncement_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    # --- /fixscreenshots ---
    @app_commands.command(name="fixscreenshots", description="[Owner] Adopt untracked announcements, then screenshot every one missing a screenshot")
    async def fixscreenshots(self, interaction: discord.Interaction):
        """Manually forces _run_screenshot_sweep to run immediately,
        instead of waiting for the next periodic
        SCREENSHOT_SWEEP_INTERVAL_MINUTES pass. Bypasses the circuit
        breaker entirely — this is an explicit, one-off admin request,
        not the unattended automatic path the breaker exists to
        protect against."""
        if interaction.user.id != OWNER_DISCORD_ID:
            return await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        adopted = await self._auto_adopt_untracked()
        adopted_note = f"📥 Adopted {adopted} untracked announcement(s).\n" if adopted else ""

        missing = await _get_announcements_missing_screenshot()
        if not missing:
            return await interaction.followup.send(f"{adopted_note}✅ Nothing missing a screenshot right now.", ephemeral=True)

        await interaction.followup.send(
            f"{adopted_note}🔄 Force-running now for {len(missing)} announcement(s) missing a screenshot. This bypasses the "
            "circuit breaker, so it'll attempt every one regardless of recent failures — may take a while.",
            ephemeral=True,
        )

        succeeded, failed, total = await self._run_screenshot_sweep(bypass_circuit_breaker=True)

        await interaction.followup.send(f"✅ Done — {succeeded} fixed, {failed} still failing (of {total} checked).", ephemeral=True)

    @fixscreenshots.error
    async def fixscreenshots_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        msg = f"❌ Unexpected error: `{type(error).__name__}: {error}`"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(NewMediaAnnouncer(bot))