"""
Embeds Cog — create, extract, and edit Discord messages via raw JSON.
Accepts THREE input shapes, auto-detected:
  - Classic embeds:            {"content": ..., "embed"/"embeds": [...]}
  - Classic + legacy buttons:  {"content": ..., "embeds": [...], "components": [...]}  (no CV2 flag)
  - Components V2:             {"flags": 32768, "components": [...]}  (no content/embeds allowed)

Also accepts raw JSON copied straight out of the Discord client (camelCase
internals, "rawDescription", CSS color strings, and dozens of irrelevant
metadata fields like author/reactions/state) — these get normalized down
to the sendable essentials before anything else runs. Note this covers the
common/stable shapes; Discord's internal client JSON isn't a documented,
frozen format and can add new undocumented fields at any time, so no
normalizer can promise literally every JSON shape the client has ever
produced, only the ones that actually show up in practice.

  - /embed        create and post a new message from JSON (any shape above).
                   json: paste up to 6000 characters straight into the
                   command; file: attach a .json/.txt of any size. With
                   neither, falls back to "send it as your next message"
                   (that message is deleted once it's read successfully).
                   Shows a private preview with Post / Cancel first
                   (preview:False skips it), can post to another channel
                   (channel:), and never pings unless pings:True.
  - /json         extract an existing message back into sendable JSON —
                   slimmed to only what's needed to resend it. Accepts a
                   message link (one API call) or a bare ID (searches the
                   server). Only works on channels YOU can read.
  - /embededit    edit one of the bot's own messages. Opens an edit window
                   prefilled with compact JSON when it fits (4000 chars);
                   otherwise sends the JSON as a file to edit and re-upload
                   via /embededit file:.

Components V2 note: link buttons (style 5, has a "url") are fully
functional since they need no callback logic. Any OTHER interactive
component — a button with a custom_id, or ANY select menu (string, user,
role, mentionable, or channel select) — pasted from raw JSON is rendered
but DISABLED, in both the CV2 and legacy paths. There is no way to know
what a custom_id button or a select menu was originally supposed to do
without real Python callback code attached to it, and a
broken-looking-functional component is worse than an honestly-disabled one.

File components (type 13) cannot be built from pasted JSON at all — Discord
requires an actual multipart-uploaded attachment behind an "attachment://"
reference, which a text-only JSON paste can never provide. These are
skipped with a warning surfaced to the user rather than silently dropped.
"""

import discord
from discord.ext import commands
from discord import app_commands
import json
import logging
import io
import re
import colorsys
import asyncio

# --- CONFIG ---
# When True, /json and /embededit are removed from the slash-command list
# entirely — the "View as JSON" / "Edit Message" right-click context menus
# (Apps) keep working exactly the same, since they're independently
# registered and share no state with these two slash commands (no shared
# logic was factored out between them — each context menu duplicates its
# slash command's body rather than calling it). /embed has no context-menu
# equivalent at all — it creates a brand-new message rather than acting on
# an existing one, which a message context menu structurally can't
# represent — so it's unaffected by this toggle either way.
DISABLE_JSON_AND_EMBEDEDIT_SLASH_COMMANDS = False

logger = logging.getLogger("embeds_cog")


# --- VALIDATION LAYER (classic embeds only — CV2 has its own validation path) ---
MAX_CONTENT_LEN = 2000
MAX_EMBED_TITLE = 256
MAX_EMBED_DESC = 4096
MAX_EMBED_FIELDS = 25
MAX_EMBED_FIELD_NAME = 256
MAX_EMBED_FIELD_VALUE = 1024
MAX_EMBED_FOOTER = 2048
MAX_EMBED_AUTHOR = 256
MAX_EMBED_TOTAL = 6000
MAX_EMBEDS_PER_MESSAGE = 10

# Legacy (non-CV2) action row limits — same numeric limits as CV2 ActionRows,
# just enforced on the discord.ui.View path instead of LayoutView.
MAX_LEGACY_ACTION_ROWS = 5
MAX_COMPONENTS_PER_ACTION_ROW = 5

# How long /embed waits for the person's follow-up JSON message/file.
INPUT_WAIT_TIMEOUT_SECONDS = 300


def validate_json_syntax(raw: str):
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON syntax at line {e.lineno}, column {e.colno}: {e.msg}"
    if not isinstance(parsed, dict):
        return None, f"JSON must be an object (`{{...}}`), not a {type(parsed).__name__}."
    return parsed, None


# ============================================================
# NORMALIZATION — raw Discord-client dumps → clean sendable JSON
# ============================================================

def _parse_hex_color(s: str):
    s = s.lstrip('#')
    try:
        return int(s, 16)
    except ValueError:
        return None


def _parse_rgb_color(s: str):
    nums = re.findall(r'[\d.]+', s)
    if len(nums) < 3:
        return None
    try:
        r, g, b = (max(0, min(255, int(float(n)))) for n in nums[:3])
        return (r << 16) + (g << 8) + b
    except ValueError:
        return None


def _parse_hsl_color(s: str):
    # The Discord client renders "no color explicitly set" as an hsla()
    # string containing a CSS calc()/var() — that's a theme placeholder,
    # not a real value, and can't be converted to an integer at all.
    if 'var(' in s or 'calc(' in s:
        return None
    nums = re.findall(r'[\d.]+', s)
    if len(nums) < 3:
        return None
    try:
        h = float(nums[0]) / 360.0
        sat = float(nums[1]) / 100.0
        light = float(nums[2]) / 100.0
        r, g, b = colorsys.hls_to_rgb(h, light, sat)
        return (int(r * 255) << 16) + (int(g * 255) << 8) + int(b * 255)
    except ValueError:
        return None


def normalize_embed_color(value):
    """Accepts an int, hex string, rgb()/rgba(), hsl()/hsla(), or Discord's
    client-internal 'no color set' CSS placeholder. Returns a Discord
    integer color, or None if the value can't be resolved to one (in which
    case the embed is simply sent with no color, matching what an unset
    color would render as anyway)."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip()
        if s.startswith('#'):
            return _parse_hex_color(s)
        if s.startswith('rgb'):
            return _parse_rgb_color(s)
        if s.startswith('hsl'):
            return _parse_hsl_color(s)
        try:
            return int(s, 0)
        except ValueError:
            return None
    return None


def normalize_embed_dict(raw: dict) -> dict:
    """Accepts either an already-clean API-shaped embed dict or a raw
    Discord-client internal dump (camelCase keys, rawTitle/rawDescription,
    rawName/rawValue on fields, CSS color strings, proxy URLs,
    contentScanVersion, etc.) and returns a clean dict safe to pass to
    discord.Embed.from_dict(). Unknown/internal-only keys are simply
    dropped rather than causing a build failure."""
    if not isinstance(raw, dict):
        return raw

    clean = {}

    title = raw.get("title")
    if title is None:
        title = raw.get("rawTitle")
    if title is not None:
        clean["title"] = title

    description = raw.get("description")
    if description is None:
        description = raw.get("rawDescription")
    if description is not None:
        clean["description"] = description

    url = raw.get("url")
    if url is not None:
        clean["url"] = url

    raw_color = raw.get("color") if raw.get("color") is not None else raw.get("colour")
    color = normalize_embed_color(raw_color)
    if color is not None:
        clean["color"] = color

    timestamp = raw.get("timestamp")
    if timestamp is not None:
        clean["timestamp"] = timestamp

    footer = raw.get("footer")
    if isinstance(footer, dict):
        f = {}
        text = footer.get("text")
        if text is not None:
            f["text"] = text
        icon = footer.get("icon_url") or footer.get("iconURL") or footer.get("proxy_icon_url") or footer.get("proxyIconURL")
        if icon:
            f["icon_url"] = icon
        if f:
            clean["footer"] = f

    author = raw.get("author")
    if isinstance(author, dict):
        a = {}
        name = author.get("name")
        if name is not None:
            a["name"] = name
        a_url = author.get("url")
        if a_url:
            a["url"] = a_url
        icon = author.get("icon_url") or author.get("iconURL") or author.get("proxy_icon_url") or author.get("proxyIconURL")
        if icon:
            a["icon_url"] = icon
        if a:
            clean["author"] = a

    thumbnail = raw.get("thumbnail")
    if isinstance(thumbnail, dict):
        t_url = thumbnail.get("url") or thumbnail.get("proxyURL") or thumbnail.get("proxy_url")
        if t_url:
            clean["thumbnail"] = {"url": t_url}

    image = raw.get("image")
    if isinstance(image, dict):
        i_url = image.get("url") or image.get("proxyURL") or image.get("proxy_url")
        if i_url:
            clean["image"] = {"url": i_url}

    fields = raw.get("fields")
    if isinstance(fields, list):
        clean_fields = []
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = f.get("name")
            if name is None:
                name = f.get("rawName", "")
            value = f.get("value")
            if value is None:
                value = f.get("rawValue", "")
            clean_fields.append({
                "name": name,
                "value": value,
                "inline": bool(f.get("inline", False))
            })
        if clean_fields:
            clean["fields"] = clean_fields

    return clean


def normalize_incoming_payload(parsed: dict) -> dict:
    """Accepts either already-clean sendable JSON or a raw dump copied
    straight out of the Discord client and returns a payload containing only
    content/embed(s)/components/flags. Everything else — author, reactions,
    timestamps, IDs, guild member avatars, and every other piece of metadata
    that describes a message that already exists — is dropped, since none
    of it has any meaning when creating or editing a *new* message. This
    runs unconditionally; on already-clean input it's a harmless passthrough."""
    normalized = {}

    content = parsed.get("content")
    if content is not None:
        normalized["content"] = content

    embeds_raw = parsed.get("embeds")
    if isinstance(embeds_raw, list) and embeds_raw:
        normalized["embeds"] = [normalize_embed_dict(e) for e in embeds_raw]
    elif "embed" in parsed:
        normalized["embed"] = normalize_embed_dict(parsed["embed"])

    components = parsed.get("components")
    if isinstance(components, list) and components:
        normalized["components"] = slim_components(components)

    flags = parsed.get("flags")
    if flags is not None:
        normalized["flags"] = flags

    return normalized


# Keys Discord (API or client) attaches to RECEIVED components that mean
# nothing when sending: component ids, image proxy/size/scan metadata.
_COMPONENT_JUNK_KEYS = {
    "id", "proxy_url", "proxyUrl", "proxyURL", "width", "height",
    "content_type", "contentType", "placeholder", "placeholder_version",
    "placeholderVersion", "loading_state", "loadingState",
    "content_scan_metadata", "contentScanMetadata", "attachment_id", "attachmentId",
}
# Client dumps use camelCase for a few keys the API spells in snake_case.
_CAMEL_TO_SNAKE = {
    "accentColor": "accent_color", "customId": "custom_id",
    "minValues": "min_values", "maxValues": "max_values",
}
# Keys whose False value is the default anyway — dropped to save space.
_DROP_IF_FALSE = {"spoiler", "disabled"}


def _slim(obj, parent_key: str | None = None):
    if isinstance(obj, list):
        return [_slim(x, parent_key) for x in obj]
    if not isinstance(obj, dict):
        return obj
    if parent_key == "media":
        url = obj.get("url") or obj.get("proxy_url") or obj.get("proxyUrl") or obj.get("proxyURL")
        return {"url": url} if url else {}
    if parent_key == "emoji":
        # Emoji ids are real data (custom emoji) — keep them here.
        return {k: obj[k] for k in ("id", "name", "animated") if obj.get(k) not in (None, False)}
    out = {}
    for k, v in obj.items():
        k = _CAMEL_TO_SNAKE.get(k, k)
        if k in _COMPONENT_JUNK_KEYS or v is None:
            continue
        if k in _DROP_IF_FALSE and v is False:
            continue
        out[k] = _slim(v, k)
    return out


def slim_components(components: list) -> list:
    """Strips received-message metadata from a components array, leaving
    only what's needed to send it again. Typically cuts an exported
    Components V2 message by a third to a half."""
    return _slim(components)


def is_components_v2_payload(parsed: dict) -> bool:
    """True only for genuine Components V2 payloads. Requires either the
    IS_COMPONENTS_V2 flag bit (1 << 15) explicitly set, OR a components
    array with no content/embed(s) present at all (ambiguous bare-components
    paste, treated as CV2 for backward compatibility with payloads produced
    by this cog's own /json output). Critically: a components array
    alongside content or embeds with NO flag set is the legacy
    action-row-plus-embed format, not CV2 — conflating the two silently
    drops the embed/content when building, which is exactly the bug this
    check exists to prevent."""
    comps = parsed.get("components")
    if not (isinstance(comps, list) and len(comps) > 0):
        return False

    flags = parsed.get("flags", 0) or 0
    if isinstance(flags, int) and (flags & (1 << 15)):
        return True

    has_body = parsed.get("content") is not None or "embed" in parsed or "embeds" in parsed
    return not has_body


def compute_embed_char_total(embed_dict: dict) -> int:
    total = 0
    total += len(embed_dict.get("title", "") or "")
    total += len(embed_dict.get("description", "") or "")
    footer = embed_dict.get("footer", {})
    if isinstance(footer, dict):
        total += len(footer.get("text", "") or "")
    author = embed_dict.get("author", {})
    if isinstance(author, dict):
        total += len(author.get("name", "") or "")
    for field in embed_dict.get("fields", []) or []:
        if isinstance(field, dict):
            total += len(field.get("name", "") or "")
            total += len(field.get("value", "") or "")
    return total


def validate_embed_structure(embed_dict: dict, index: int = 0):
    if not isinstance(embed_dict, dict):
        return f"Embed #{index+1} must be an object, not a {type(embed_dict).__name__}."

    title = embed_dict.get("title")
    if title is not None:
        if not isinstance(title, str):
            return f"Embed #{index+1}: `title` must be a string."
        if len(title) > MAX_EMBED_TITLE:
            return f"Embed #{index+1}: `title` exceeds {MAX_EMBED_TITLE} characters."

    description = embed_dict.get("description")
    if description is not None:
        if not isinstance(description, str):
            return f"Embed #{index+1}: `description` must be a string."
        if len(description) > MAX_EMBED_DESC:
            return f"Embed #{index+1}: `description` exceeds {MAX_EMBED_DESC} characters."

    color = embed_dict.get("color")
    if color is not None and not isinstance(color, int):
        return f"Embed #{index+1}: `color` must be an integer (e.g. 65280), not a {type(color).__name__}."

    fields = embed_dict.get("fields")
    if fields is not None:
        if not isinstance(fields, list):
            return f"Embed #{index+1}: `fields` must be an array."
        if len(fields) > MAX_EMBED_FIELDS:
            return f"Embed #{index+1}: `fields` exceeds {MAX_EMBED_FIELDS} entries ({len(fields)} given)."
        for i, field in enumerate(fields):
            if not isinstance(field, dict):
                return f"Embed #{index+1}, field #{i+1}: must be an object."
            if "name" not in field or "value" not in field:
                return f"Embed #{index+1}, field #{i+1}: missing required `name` or `value` key."
            if not isinstance(field["name"], str) or not isinstance(field["value"], str):
                return f"Embed #{index+1}, field #{i+1}: `name` and `value` must be strings."
            if len(field["name"]) > MAX_EMBED_FIELD_NAME:
                return f"Embed #{index+1}, field #{i+1}: `name` exceeds {MAX_EMBED_FIELD_NAME} characters."
            if len(field["value"]) > MAX_EMBED_FIELD_VALUE:
                return f"Embed #{index+1}, field #{i+1}: `value` exceeds {MAX_EMBED_FIELD_VALUE} characters."
            if "inline" in field and not isinstance(field["inline"], bool):
                return f"Embed #{index+1}, field #{i+1}: `inline` must be true or false."

    footer = embed_dict.get("footer")
    if footer is not None:
        if not isinstance(footer, dict):
            return f"Embed #{index+1}: `footer` must be an object."
        if "text" not in footer:
            return f"Embed #{index+1}: `footer` must contain a `text` key."
        if len(footer.get("text", "")) > MAX_EMBED_FOOTER:
            return f"Embed #{index+1}: `footer.text` exceeds {MAX_EMBED_FOOTER} characters."

    author = embed_dict.get("author")
    if author is not None:
        if not isinstance(author, dict):
            return f"Embed #{index+1}: `author` must be an object."
        if "name" not in author:
            return f"Embed #{index+1}: `author` must contain a `name` key."
        if len(author.get("name", "")) > MAX_EMBED_AUTHOR:
            return f"Embed #{index+1}: `author.name` exceeds {MAX_EMBED_AUTHOR} characters."

    if compute_embed_char_total(embed_dict) > MAX_EMBED_TOTAL:
        return f"Embed #{index+1}: combined character count exceeds Discord's {MAX_EMBED_TOTAL}-character limit."

    return None


def validate_full_payload(parsed: dict):
    """Validates the content/embed/embeds portion of a payload — used for
    the classic path AND the legacy (classic+buttons) path, since both send
    real content/embeds. CV2 payloads are validated by
    validate_components_v2_structure() instead."""
    if not parsed:
        return "JSON object is empty. At minimum provide `content`, `embed`, or `embeds`."

    content = parsed.get("content")
    if content is not None:
        if not isinstance(content, str):
            return "`content` must be a string."
        if len(content) > MAX_CONTENT_LEN:
            return f"`content` exceeds Discord's {MAX_CONTENT_LEN}-character limit ({len(content)} given)."

    has_embed = "embed" in parsed
    has_embeds = "embeds" in parsed

    if has_embed and has_embeds:
        return "Use either `embed` (single object) or `embeds` (array), not both."

    if has_embed:
        err = validate_embed_structure(parsed["embed"], 0)
        if err:
            return err

    if has_embeds:
        embeds_list = parsed["embeds"]
        if not isinstance(embeds_list, list):
            return "`embeds` must be an array of embed objects."
        if len(embeds_list) > MAX_EMBEDS_PER_MESSAGE:
            return f"`embeds` exceeds Discord's {MAX_EMBEDS_PER_MESSAGE}-embed-per-message limit ({len(embeds_list)} given)."
        for i, emb in enumerate(embeds_list):
            err = validate_embed_structure(emb, i)
            if err:
                return err

    if content is None and not has_embed and not has_embeds and not parsed.get("components"):
        return "JSON must contain at least one of: `content`, `embed`, `embeds`, `components`."

    return None


def build_embeds_from_payload(parsed: dict):
    """Returns (embeds_list, error). Converts raw JSON dicts into real
    discord.Embed objects — .send()/.edit() cannot take plain dicts."""
    embeds = []
    try:
        if "embed" in parsed:
            embeds.append(discord.Embed.from_dict(parsed["embed"]))
        elif "embeds" in parsed:
            for emb_dict in parsed["embeds"]:
                embeds.append(discord.Embed.from_dict(emb_dict))
    except (KeyError, TypeError, ValueError) as e:
        return None, f"Discord rejected the embed structure: {e}"
    return embeds, None


def clean_embed_dict(embed: discord.Embed) -> dict:
    """Strips a discord.Embed.to_dict() output down to only fields valid for
    sending/editing — no proxy_url, width/height, type, flags, etc."""
    raw = embed.to_dict()
    clean = {}

    if "title" in raw:
        clean["title"] = raw["title"]
    if "description" in raw:
        clean["description"] = raw["description"]
    if "url" in raw:
        clean["url"] = raw["url"]
    if "color" in raw:
        clean["color"] = raw["color"]

    if "author" in raw:
        author = raw["author"]
        clean["author"] = {
            k: v for k, v in {
                "name": author.get("name"),
                "url": author.get("url"),
                "icon_url": author.get("icon_url") or author.get("proxy_icon_url"),
            }.items() if v is not None
        }

    if "footer" in raw:
        footer = raw["footer"]
        clean["footer"] = {
            k: v for k, v in {
                "text": footer.get("text"),
                "icon_url": footer.get("icon_url") or footer.get("proxy_icon_url"),
            }.items() if v is not None
        }

    if "image" in raw:
        clean["image"] = {"url": raw["image"].get("url")}
    if "thumbnail" in raw:
        clean["thumbnail"] = {"url": raw["thumbnail"].get("url")}

    if "fields" in raw:
        clean["fields"] = [
            {
                "name": f.get("name", ""),
                "value": f.get("value", ""),
                "inline": f.get("inline", False)
            }
            for f in raw["fields"]
        ]

    return clean


# ============================================================
# COMPONENTS — shared button/select builders (used by BOTH the
# Components V2 path and the legacy action-row path)
# ============================================================

CV2_TYPE_ACTION_ROW = 1
CV2_TYPE_BUTTON = 2
CV2_TYPE_STRING_SELECT = 3
CV2_TYPE_USER_SELECT = 5
CV2_TYPE_ROLE_SELECT = 6
CV2_TYPE_MENTIONABLE_SELECT = 7
CV2_TYPE_CHANNEL_SELECT = 8
CV2_TYPE_SECTION = 9
CV2_TYPE_TEXT_DISPLAY = 10
CV2_TYPE_THUMBNAIL = 11
CV2_TYPE_MEDIA_GALLERY = 12
CV2_TYPE_FILE = 13
CV2_TYPE_SEPARATOR = 14
CV2_TYPE_CONTAINER = 17

# All select menu types — every one is built and rendered, but always
# disabled=True. A pasted-JSON select menu has no attached callback logic
# regardless of which of the five kinds it is, so none of them can
# meaningfully respond to a real selection.
CV2_SELECT_TYPES = {
    CV2_TYPE_STRING_SELECT,
    CV2_TYPE_USER_SELECT,
    CV2_TYPE_ROLE_SELECT,
    CV2_TYPE_MENTIONABLE_SELECT,
    CV2_TYPE_CHANNEL_SELECT,
}

# Discord.py classes for the four "auto-populated" select types — these
# pull their choices live from Discord (users/roles/channels), so unlike
# String Select there are no "options" to read from JSON at all.
CV2_AUTO_SELECT_CLASSES = {
    CV2_TYPE_USER_SELECT: discord.ui.UserSelect,
    CV2_TYPE_ROLE_SELECT: discord.ui.RoleSelect,
    CV2_TYPE_MENTIONABLE_SELECT: discord.ui.MentionableSelect,
    CV2_TYPE_CHANNEL_SELECT: discord.ui.ChannelSelect,
}

# --- Discord's documented Components V2 / component limits ---
CV2_MAX_TOP_LEVEL_COMPONENTS = 40
CV2_MAX_TOTAL_NESTED_COMPONENTS = 40  # Discord's real limit: EVERY component in the whole message, nested or not
CV2_MAX_CONTAINER_CHILDREN = 10
CV2_MAX_ACTION_ROWS_PER_CONTAINER = 5
CV2_MAX_BUTTONS_PER_ACTION_ROW = 5
CV2_MAX_SECTION_TEXT_DISPLAYS = 3
CV2_MAX_MEDIA_GALLERY_ITEMS = 10
CV2_MAX_TEXT_DISPLAY_LEN = 4000
CV2_MAX_TOTAL_TEXT = 4000  # Discord's cap on ALL TextDisplay text in one message, combined
CV2_MAX_BUTTON_LABEL = 80
CV2_MAX_CUSTOM_ID = 100
CV2_MIN_SELECT_OPTIONS = 2
CV2_MAX_SELECT_OPTIONS = 25
CV2_MAX_SELECT_PLACEHOLDER = 150
CV2_MIN_SELECT_VALUES_FLOOR = 0
CV2_MAX_SELECT_VALUES_CEILING = 25
CV2_MAX_THUMBNAIL_DESCRIPTION = 1024
CV2_MAX_GALLERY_ITEM_DESCRIPTION = 1024


class ComponentBuildError(Exception):
    """Raised with a specific, human-readable reason when a single
    component JSON object can't be turned into a real discord.ui object —
    caught at the top level so ONE bad component gives a precise error
    instead of a generic traceback."""
    pass


def _validate_component(comp: dict, counter: list):
    """Returns an error string or None. `counter` is a single-element list
    used as a mutable running total — every component, at every nesting
    depth, increments it once. This is what lets
    validate_components_v2_structure() check Discord's REAL limit (40
    components total across the whole message, nested included), not just
    the top-level array length, which is a materially smaller number for
    anything using Containers/Sections/ActionRows.

    Recurses into Containers, Sections, and ActionRows so a violation
    buried three levels deep still surfaces a precise, human-readable
    message instead of a raw Discord HTTPException/ValueError. Used for the
    CV2 path; the legacy path has its own row/count checks in
    build_legacy_view_from_components since its structure is flatter."""
    if not isinstance(comp, dict):
        return f"Each component must be an object, not a {type(comp).__name__}."

    counter[0] += 1
    ctype = comp.get("type")

    if ctype == CV2_TYPE_TEXT_DISPLAY:
        content = comp.get("content", "") or ""
        if len(content) > CV2_MAX_TEXT_DISPLAY_LEN:
            return f"TextDisplay content exceeds {CV2_MAX_TEXT_DISPLAY_LEN} characters."

    elif ctype == CV2_TYPE_BUTTON:
        label = comp.get("label") or ""
        if len(label) > CV2_MAX_BUTTON_LABEL:
            return f"Button label exceeds {CV2_MAX_BUTTON_LABEL} characters."
        custom_id = comp.get("custom_id")
        if custom_id and len(custom_id) > CV2_MAX_CUSTOM_ID:
            return f"Button custom_id exceeds {CV2_MAX_CUSTOM_ID} characters."

    elif ctype in CV2_SELECT_TYPES:
        placeholder = comp.get("placeholder") or ""
        if len(placeholder) > CV2_MAX_SELECT_PLACEHOLDER:
            return f"Select menu placeholder exceeds {CV2_MAX_SELECT_PLACEHOLDER} characters."
        min_values = comp.get("min_values", 1)
        max_values = comp.get("max_values", 1)
        if not (CV2_MIN_SELECT_VALUES_FLOOR <= min_values <= CV2_MAX_SELECT_VALUES_CEILING):
            return f"Select menu min_values must be between {CV2_MIN_SELECT_VALUES_FLOOR} and {CV2_MAX_SELECT_VALUES_CEILING}."
        if not (CV2_MIN_SELECT_VALUES_FLOOR <= max_values <= CV2_MAX_SELECT_VALUES_CEILING):
            return f"Select menu max_values must be between {CV2_MIN_SELECT_VALUES_FLOOR} and {CV2_MAX_SELECT_VALUES_CEILING}."
        if min_values > max_values:
            return "Select menu min_values cannot exceed max_values."

        if ctype == CV2_TYPE_STRING_SELECT:
            opts = comp.get("options", []) or []
            if not (CV2_MIN_SELECT_OPTIONS <= len(opts) <= CV2_MAX_SELECT_OPTIONS):
                return (
                    f"Select menu must have between {CV2_MIN_SELECT_OPTIONS} and "
                    f"{CV2_MAX_SELECT_OPTIONS} options ({len(opts)} given)."
                )

    elif ctype == CV2_TYPE_THUMBNAIL:
        description = comp.get("description") or ""
        if len(description) > CV2_MAX_THUMBNAIL_DESCRIPTION:
            return f"Thumbnail description exceeds {CV2_MAX_THUMBNAIL_DESCRIPTION} characters."

    elif ctype == CV2_TYPE_MEDIA_GALLERY:
        items = comp.get("items", []) or []
        if len(items) > CV2_MAX_MEDIA_GALLERY_ITEMS:
            return f"MediaGallery exceeds {CV2_MAX_MEDIA_GALLERY_ITEMS} items ({len(items)} given)."
        for i, item in enumerate(items):
            if isinstance(item, dict):
                desc = item.get("description") or ""
                if len(desc) > CV2_MAX_GALLERY_ITEM_DESCRIPTION:
                    return f"MediaGallery item #{i+1} description exceeds {CV2_MAX_GALLERY_ITEM_DESCRIPTION} characters."

    elif ctype == CV2_TYPE_CONTAINER:
        children = comp.get("components", []) or []
        if len(children) > CV2_MAX_CONTAINER_CHILDREN:
            return f"Container exceeds {CV2_MAX_CONTAINER_CHILDREN} child components ({len(children)} given)."
        action_row_count = sum(1 for c in children if isinstance(c, dict) and c.get("type") == CV2_TYPE_ACTION_ROW)
        if action_row_count > CV2_MAX_ACTION_ROWS_PER_CONTAINER:
            return f"Container exceeds {CV2_MAX_ACTION_ROWS_PER_CONTAINER} action rows ({action_row_count} given)."
        for c in children:
            err = _validate_component(c, counter)
            if err:
                return err

    elif ctype == CV2_TYPE_SECTION:
        section_children = comp.get("components", []) or []
        text_display_count = sum(1 for c in section_children if isinstance(c, dict) and c.get("type") == CV2_TYPE_TEXT_DISPLAY)
        if text_display_count > CV2_MAX_SECTION_TEXT_DISPLAYS:
            return f"Section exceeds {CV2_MAX_SECTION_TEXT_DISPLAYS} TextDisplay components ({text_display_count} given)."
        if text_display_count == 0:
            return "Section needs at least one TextDisplay (type 10) inside its `components`."
        acc_check = comp.get("accessory")
        if not isinstance(acc_check, dict) or acc_check.get("type") not in (CV2_TYPE_THUMBNAIL, CV2_TYPE_BUTTON):
            return (
                "Section needs an `accessory` — a Thumbnail (type 11) or a Button (type 2). "
                "For text without one, use a plain TextDisplay (type 10) instead of a Section."
            )
        for c in section_children:
            err = _validate_component(c, counter)
            if err:
                return err
        acc = comp.get("accessory")
        if acc:
            err = _validate_component(acc, counter)
            if err:
                return err

    elif ctype == CV2_TYPE_ACTION_ROW:
        row_children = comp.get("components", []) or []
        select_children = [c for c in row_children if isinstance(c, dict) and c.get("type") in CV2_SELECT_TYPES]
        button_children = [c for c in row_children if isinstance(c, dict) and c.get("type") == CV2_TYPE_BUTTON]
        if select_children and len(row_children) > 1:
            return f"ActionRow containing a select menu cannot include any other components ({len(row_children)} given)."
        if len(button_children) > CV2_MAX_BUTTONS_PER_ACTION_ROW:
            return f"ActionRow exceeds {CV2_MAX_BUTTONS_PER_ACTION_ROW} buttons ({len(button_children)} given)."
        for c in row_children:
            err = _validate_component(c, counter)
            if err:
                return err

    return None


def _total_text_len(components) -> int:
    """Sum of every TextDisplay's content at any nesting depth."""
    total = 0
    for c in components or []:
        if not isinstance(c, dict):
            continue
        if c.get("type") == CV2_TYPE_TEXT_DISPLAY:
            total += len(c.get("content") or "")
        total += _total_text_len(c.get("components"))
    return total


def validate_components_v2_structure(parsed: dict):
    """Pre-flight structural validation for a CV2 payload — same purpose as
    validate_full_payload() on the classic path. Catches Discord's documented
    limits up front instead of letting a raw HTTPException/ValueError reach
    the user. Checks BOTH the top-level array length and the true total
    nested component count (Container/Section/ActionRow children all count
    toward Discord's real 40-component ceiling, not just what's at the top
    level — a message built mostly of Containers can exceed 40 total nodes
    while its top-level array has only 3-4 entries)."""
    top = parsed.get("components", [])
    if not isinstance(top, list):
        return "`components` must be an array."
    if len(top) > CV2_MAX_TOP_LEVEL_COMPONENTS:
        return f"Total top-level components exceed Discord's {CV2_MAX_TOP_LEVEL_COMPONENTS}-component limit ({len(top)} given)."

    counter = [0]
    for comp in top:
        err = _validate_component(comp, counter)
        if err:
            return err

    total_text = _total_text_len(top)
    if total_text > CV2_MAX_TOTAL_TEXT:
        return (
            f"Total text across the whole message is {total_text:,} characters — Discord's limit is "
            f"{CV2_MAX_TOTAL_TEXT:,} for ALL TextDisplays combined, not per block. Trim some text."
        )

    if counter[0] > CV2_MAX_TOTAL_NESTED_COMPONENTS:
        return (
            f"Total components (including everything nested inside Containers/Sections/ActionRows) "
            f"exceed Discord's {CV2_MAX_TOTAL_NESTED_COMPONENTS}-component limit ({counter[0]} given). "
            f"Combine or remove some TextDisplays/buttons/etc to bring it under the limit."
        )
    return None


def _build_button(data: dict) -> discord.ui.Button:
    style_map = {
        1: discord.ButtonStyle.primary,
        2: discord.ButtonStyle.secondary,
        3: discord.ButtonStyle.success,
        4: discord.ButtonStyle.danger,
        5: discord.ButtonStyle.link,
    }
    style_val = data.get("style", 5)
    is_link = style_val == 5 and bool(data.get("url"))

    emoji = None
    if data.get("emoji"):
        e = data["emoji"]
        if e.get("id"):
            emoji = f"<{'a' if e.get('animated') else ''}:{e.get('name', '')}:{e['id']}>"
        else:
            emoji = e.get("name")

    try:
        if is_link:
            # Link buttons need no callback — fully functional as pasted.
            return discord.ui.Button(
                style=discord.ButtonStyle.link,
                label=data.get("label"),
                url=data["url"],
                emoji=emoji,
                disabled=data.get("disabled", False)
            )
        else:
            # Non-link buttons (custom_id) have no real callback available
            # from pure JSON — rendered visible but disabled, honestly,
            # rather than pretending it would work.
            return discord.ui.Button(
                style=style_map.get(style_val, discord.ButtonStyle.secondary),
                label=data.get("label") or "Unavailable",
                emoji=emoji,
                disabled=True
            )
    except Exception as e:
        raise ComponentBuildError(f"Button: {type(e).__name__}: {e}")


def _build_select_menu(data: dict):
    """Builds any of the five select menu types. All are forced disabled —
    a pasted-JSON select has no callback logic attached, matching how
    non-link buttons are already handled above."""
    ctype = data.get("type")
    placeholder = data.get("placeholder")
    min_values = data.get("min_values", 1)
    max_values = data.get("max_values", 1)

    try:
        if ctype == CV2_TYPE_STRING_SELECT:
            options = []
            for opt in data.get("options", []) or []:
                emoji = None
                if opt.get("emoji"):
                    e = opt["emoji"]
                    if e.get("id"):
                        emoji = f"<{'a' if e.get('animated') else ''}:{e.get('name', '')}:{e['id']}>"
                    else:
                        emoji = e.get("name")
                options.append(discord.SelectOption(
                    label=(opt.get("label") or "Option")[:100],
                    value=(opt.get("value") or opt.get("label") or "option")[:100],
                    description=opt.get("description"),
                    emoji=emoji,
                    default=opt.get("default", False)
                ))
            if not options:
                options = [discord.SelectOption(label="No options provided", value="none")]
            return discord.ui.Select(
                placeholder=placeholder,
                min_values=min_values,
                max_values=max_values,
                options=options,
                disabled=True
            )

        select_cls = CV2_AUTO_SELECT_CLASSES.get(ctype)
        if select_cls is None:
            raise ComponentBuildError(f"Unknown select menu type: {ctype}")
        # User/Role/Mentionable/Channel selects pull live data from Discord —
        # there's no "options" list to read from JSON for these.
        return select_cls(
            placeholder=placeholder,
            min_values=min_values,
            max_values=max_values,
            disabled=True
        )
    except ComponentBuildError:
        raise
    except Exception as e:
        raise ComponentBuildError(f"Select menu: {type(e).__name__}: {e}")


# ============================================================
# COMPONENTS V2 — building a discord.ui.LayoutView from pasted JSON
# ============================================================

def _build_action_row(data: dict) -> discord.ui.ActionRow:
    try:
        row = discord.ui.ActionRow()
    except Exception as e:
        raise ComponentBuildError(f"ActionRow: {type(e).__name__}: {e}")

    for comp in data.get("components", []) or []:
        comp_type = comp.get("type")
        if comp_type == CV2_TYPE_BUTTON:
            row.add_item(_build_button(comp))
        elif comp_type in CV2_SELECT_TYPES:
            row.add_item(_build_select_menu(comp))
    return row


def _build_text_display(data: dict) -> discord.ui.TextDisplay:
    try:
        return discord.ui.TextDisplay(data.get("content", ""))
    except Exception as e:
        raise ComponentBuildError(f"TextDisplay: {type(e).__name__}: {e}")


def _build_thumbnail(data: dict) -> discord.ui.Thumbnail:
    media = data.get("media", {}) or {}
    url = media.get("url", "")
    try:
        return discord.ui.Thumbnail(
            url,
            description=data.get("description"),
            spoiler=data.get("spoiler", False)
        )
    except Exception as e:
        raise ComponentBuildError(f"Thumbnail: {type(e).__name__}: {e}")


def _build_media_gallery(data: dict) -> discord.ui.MediaGallery:
    try:
        items = []
        for item in data.get("items", []) or []:
            media = item.get("media", {}) or {}
            items.append(discord.MediaGalleryItem(
                media.get("url", ""),
                description=item.get("description"),
                spoiler=item.get("spoiler", False)
            ))
        return discord.ui.MediaGallery(*items)
    except Exception as e:
        raise ComponentBuildError(f"MediaGallery: {type(e).__name__}: {e}")


def _build_separator(data: dict) -> discord.ui.Separator:
    try:
        spacing_val = data.get("spacing", 1)
        spacing = discord.SeparatorSpacing.large if spacing_val == 2 else discord.SeparatorSpacing.small
        return discord.ui.Separator(visible=data.get("divider", True), spacing=spacing)
    except Exception as e:
        raise ComponentBuildError(f"Separator: {type(e).__name__}: {e}")


def _build_section(data: dict) -> discord.ui.Section:
    try:
        children = []
        for c in data.get("components", []) or []:
            if c.get("type") == CV2_TYPE_TEXT_DISPLAY:
                children.append(_build_text_display(c))

        accessory = None
        acc = data.get("accessory")
        if acc:
            if acc.get("type") == CV2_TYPE_THUMBNAIL:
                accessory = _build_thumbnail(acc)
            elif acc.get("type") == CV2_TYPE_BUTTON:
                accessory = _build_button(acc)

        if accessory is None:
            raise ComponentBuildError("Section needs an accessory (a Thumbnail or a Button).")
        return discord.ui.Section(*children, accessory=accessory)
    except ComponentBuildError:
        raise
    except Exception as e:
        raise ComponentBuildError(f"Section: {type(e).__name__}: {e}")


def _build_container(data: dict, warnings: list) -> discord.ui.Container:
    try:
        accent_color = None
        if data.get("accent_color") is not None:
            accent_color = discord.Colour(data["accent_color"])
        container = discord.ui.Container(accent_color=accent_color, spoiler=data.get("spoiler", False))
    except Exception as e:
        raise ComponentBuildError(f"Container: {type(e).__name__}: {e}")

    for child in data.get("components", []) or []:
        item = _build_cv2_component(child, warnings)
        if item is not None:
            container.add_item(item)
    return container


def _build_cv2_component(data: dict, warnings: list):
    """Dispatches a single Components V2 JSON object to the matching
    discord.ui builder, based on its numeric 'type' field. Returns None for
    types that are known but deliberately unbuildable (File — Discord only
    allows attachment:// references for it, backed by a real multipart
    upload, which a plain-text JSON paste can never supply). Skipped types
    append a plain-language note to `warnings` so the person finds out why,
    instead of the component just quietly not being there."""
    comp_type = data.get("type")

    if comp_type == CV2_TYPE_ACTION_ROW:
        return _build_action_row(data)
    elif comp_type == CV2_TYPE_SECTION:
        return _build_section(data)
    elif comp_type == CV2_TYPE_TEXT_DISPLAY:
        return _build_text_display(data)
    elif comp_type == CV2_TYPE_THUMBNAIL:
        return _build_thumbnail(data)
    elif comp_type == CV2_TYPE_MEDIA_GALLERY:
        return _build_media_gallery(data)
    elif comp_type == CV2_TYPE_SEPARATOR:
        return _build_separator(data)
    elif comp_type == CV2_TYPE_CONTAINER:
        return _build_container(data, warnings)
    elif comp_type == CV2_TYPE_FILE:
        warnings.append(
            "A File component was skipped — Discord requires an actual uploaded attachment "
            "behind an `attachment://` reference, which can't be created from pasted JSON alone."
        )
        return None
    else:
        raise ComponentBuildError(f"Unknown/unsupported component type: {comp_type}")


def build_layout_view_from_payload(parsed: dict):
    """Returns (view, error, warnings). Builds a discord.ui.LayoutView from a
    Components V2 JSON payload's top-level 'components' array. `error` halts
    the whole send/edit; `warnings` are non-fatal notes (currently only ever
    populated by skipped File components) shown alongside a successful send."""
    try:
        view = discord.ui.LayoutView(timeout=None)
    except AttributeError:
        return None, (
            "Your installed discord.py version doesn't support Components V2 "
            "(discord.ui.LayoutView doesn't exist). Upgrade discord.py to use this format."
        ), []

    structure_error = validate_components_v2_structure(parsed)
    if structure_error:
        return None, structure_error, []

    warnings = []
    try:
        for comp_data in parsed.get("components", []):
            item = _build_cv2_component(comp_data, warnings)
            if item is not None:
                view.add_item(item)
    except ComponentBuildError as e:
        return None, str(e), warnings
    except Exception as e:
        return None, f"Unexpected error building components: {type(e).__name__}: {e}", warnings

    return view, None, warnings


# ============================================================
# LEGACY PATH — classic content/embeds + a discord.ui.View of
# action-row buttons/selects, NOT wrapped in Components V2
# ============================================================

def build_legacy_view_from_components(components_list: list):
    """Returns (view, error, warnings). Builds a normal discord.ui.View from
    a top-level 'components' array of action rows — the pre-CV2 format,
    where buttons/selects sit alongside real content/embeds rather than
    replacing them. Row position is preserved explicitly via each item's
    .row so multi-row layouts render exactly as authored."""
    if len(components_list) > MAX_LEGACY_ACTION_ROWS:
        return None, f"Messages allow a maximum of {MAX_LEGACY_ACTION_ROWS} action rows ({len(components_list)} given).", []

    view = discord.ui.View(timeout=None)
    warnings = []

    for row_index, row_data in enumerate(components_list):
        if not isinstance(row_data, dict):
            return None, f"Component #{row_index+1} must be an object.", []
        if row_data.get("type") != CV2_TYPE_ACTION_ROW:
            return None, (
                f"Component #{row_index+1}: top-level components in this (non-Components-V2) "
                f"format must all be Action Rows (type 1)."
            ), []

        row_children = row_data.get("components", []) or []
        if len(row_children) > MAX_COMPONENTS_PER_ACTION_ROW:
            return None, (
                f"Action row #{row_index+1} exceeds {MAX_COMPONENTS_PER_ACTION_ROW} components "
                f"({len(row_children)} given)."
            ), []

        select_count = sum(1 for c in row_children if isinstance(c, dict) and c.get("type") in CV2_SELECT_TYPES)
        button_count = sum(1 for c in row_children if isinstance(c, dict) and c.get("type") == CV2_TYPE_BUTTON)
        if select_count and len(row_children) > 1:
            return None, (
                f"Action row #{row_index+1}: a row containing a select menu cannot include any other "
                f"components ({len(row_children)} given)."
            ), []
        if button_count > CV2_MAX_BUTTONS_PER_ACTION_ROW:
            return None, (
                f"Action row #{row_index+1} exceeds {CV2_MAX_BUTTONS_PER_ACTION_ROW} buttons ({button_count} given)."
            ), []

        for comp in row_children:
            comp_type = comp.get("type")
            try:
                if comp_type == CV2_TYPE_BUTTON:
                    label = comp.get("label") or ""
                    if len(label) > CV2_MAX_BUTTON_LABEL:
                        return None, f"Button label exceeds {CV2_MAX_BUTTON_LABEL} characters.", warnings
                    item = _build_button(comp)
                elif comp_type in CV2_SELECT_TYPES:
                    item = _build_select_menu(comp)
                else:
                    warnings.append(f"Unknown/unsupported component type `{comp_type}` in action row #{row_index+1} was skipped.")
                    continue
            except ComponentBuildError as e:
                return None, str(e), warnings

            item.row = row_index
            view.add_item(item)

    return view, None, warnings


# ============================================================
# PAYLOAD DISPATCH — classifies a normalized payload and builds
# everything needed to send or edit it
# ============================================================

def prepare_message_payload(parsed: dict):
    """Normalizes then classifies an incoming JSON payload and builds
    whatever's needed to send/edit it. Returns one of:
      {"kind": "error", "message": str}
      {"kind": "cv2", "view": LayoutView, "warnings": [...]}
      {"kind": "legacy", "content": str|None, "embeds": [...], "view": View, "warnings": [...]}
      {"kind": "classic", "content": str|None, "embeds": [...]}
    """
    parsed = normalize_incoming_payload(parsed)
    has_components = isinstance(parsed.get("components"), list) and len(parsed["components"]) > 0

    if is_components_v2_payload(parsed):
        view, build_error, warnings = build_layout_view_from_payload(parsed)
        if build_error:
            return {"kind": "error", "message": build_error}
        return {"kind": "cv2", "view": view, "warnings": warnings}

    structure_error = validate_full_payload(parsed)
    if structure_error:
        return {"kind": "error", "message": structure_error}

    embeds, embed_error = build_embeds_from_payload(parsed)
    if embed_error:
        return {"kind": "error", "message": embed_error}

    if has_components:
        view, view_error, warnings = build_legacy_view_from_components(parsed["components"])
        if view_error:
            return {"kind": "error", "message": view_error}
        return {"kind": "legacy", "content": parsed.get("content"), "embeds": embeds, "view": view, "warnings": warnings}

    return {"kind": "classic", "content": parsed.get("content"), "embeds": embeds}


# ============================================================
# COMPONENTS V2 — extracting JSON FROM an existing message
# ============================================================

def is_components_v2_message(message: discord.Message) -> bool:
    try:
        return bool(message.flags.value & (1 << 15))
    except Exception:
        return False


def _serialize_component(component) -> dict:
    """Best-effort recursive serialization of a received discord.py component
    object back into the raw Components V2 JSON shape. Prefers the object's
    own to_dict() (these are thin wrappers around Discord's actual API
    schema, so to_dict() should already match); falls back to reading
    common attributes directly if to_dict() isn't available on a given
    component type in the installed discord.py version."""
    if hasattr(component, "to_dict"):
        try:
            return component.to_dict()
        except Exception:
            pass

    data = {"type": getattr(component, "type", None)}
    if hasattr(component, "content"):
        data["content"] = component.content
    if hasattr(component, "components"):
        try:
            data["components"] = [_serialize_component(c) for c in component.components]
        except Exception:
            pass
    return data


def _dump(payload: dict, compact: bool) -> str:
    if compact:
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return json.dumps(payload, indent=2, ensure_ascii=False)


def build_components_v2_json_from_message(message: discord.Message, compact: bool = False) -> str:
    payload = {
        "flags": 32768,
        "components": slim_components([_serialize_component(c) for c in message.components])
    }
    return _dump(payload, compact)


def build_json_from_message(message: discord.Message, compact: bool = False) -> str:
    """Builds sendable JSON from an EXISTING message — used both by /json's
    extraction output and by /embededit's modal prefill. Detects whether the
    message actually used Components V2 and exports the matching shape.
    A legacy (classic + action-row buttons) message naturally falls through
    to the else branch, since is_components_v2_message() checks the real
    IS_COMPONENTS_V2 flag bit, not just component presence."""
    if is_components_v2_message(message):
        return build_components_v2_json_from_message(message, compact)

    payload = {}
    if message.content:
        payload["content"] = message.content
    if message.embeds:
        payload["embeds"] = [clean_embed_dict(e) for e in message.embeds]
    if message.components:
        payload["components"] = slim_components([_serialize_component(c) for c in message.components])
    return _dump(payload, compact)


# ============================================================
# MENTIONS — never ping by default
# ============================================================

# Mentions in posted/edited messages render as normal blue names but
# notify nobody, unless /embed is run with pings:True.
NO_PINGS = discord.AllowedMentions.none()
ALL_PINGS = discord.AllowedMentions(everyone=True, users=True, roles=True, replied_user=False)

MAX_JSON_FILE_BYTES = 2 * 1024 * 1024   # 2 MB is far beyond any real message JSON
MODAL_MAX_CHARS = 4000                  # Discord's cap on one modal text field
SLASH_JSON_MAX_CHARS = 6000             # Discord's cap on one slash-command string option
PREVIEW_TIMEOUT_SECONDS = 600


def _err_embed(bot, text: str) -> discord.Embed:
    return discord.Embed(description=f"{bot.cross_emoji}  {text}", color=bot.red_color)


def _warn_embed(bot, text: str) -> discord.Embed:
    return discord.Embed(description=f"{bot.error_emoji}  {text}", color=bot.yellow_color)


def _ok_embed(bot, text: str, warnings: list | None = None) -> discord.Embed:
    desc = f"{bot.check_emoji}  {text}"
    if warnings:
        desc += f"\n\n{bot.error_emoji}  Notes:\n" + "\n".join(f"- {w}" for w in warnings)
    return discord.Embed(description=desc, color=bot.green_color)


async def _reply(interaction: discord.Interaction, **kwargs):
    """Responds or follows up, whichever is valid right now. Always ephemeral."""
    kwargs.setdefault("ephemeral", True)
    if interaction.response.is_done():
        return await interaction.followup.send(**kwargs)
    return await interaction.response.send_message(**kwargs)


# ============================================================
# FINDING A MESSAGE — link (one call) or bare ID (server search)
# ============================================================

MESSAGE_LINK_RE = re.compile(r"discord(?:app)?\.com/channels/(\d+|@me)/(\d+)/(\d+)")


async def search_guild_for_message(guild: discord.Guild, msg_id_int: int):
    """Fallback for a bare ID: tries every text channel and every active
    thread the bot can read. One API call per channel, so a message link
    is always preferred. Returns (message_or_None, search_errors_list)."""
    search_errors = []
    channels = list(guild.text_channels) + list(guild.threads)
    for ch in channels:
        perms = ch.permissions_for(guild.me)
        if not perms.view_channel or not perms.read_message_history:
            continue
        try:
            return await ch.fetch_message(msg_id_int), search_errors
        except (discord.NotFound, discord.Forbidden):
            continue
        except discord.HTTPException as e:
            search_errors.append(f"#{ch.name}: {e}")
    return None, search_errors


async def resolve_message(guild: discord.Guild, ref: str):
    """Accepts a message link (Right-click → Copy Message Link) or a bare
    message ID. Returns (message, error_text)."""
    ref = (ref or "").strip()
    match = MESSAGE_LINK_RE.search(ref)
    if match:
        guild_part, channel_id, message_id = match.groups()
        if guild_part != str(guild.id):
            return None, "That link points to a message in a different server."
        channel = guild.get_channel_or_thread(int(channel_id))
        if channel is None:
            try:
                channel = await guild.fetch_channel(int(channel_id))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                return None, "I can't see the channel that link points to."
        try:
            return await channel.fetch_message(int(message_id)), None
        except discord.NotFound:
            return None, "That message doesn't exist any more."
        except discord.Forbidden:
            return None, "I don't have permission to read that channel."
        except discord.HTTPException as e:
            return None, f"Discord error fetching that message: `{e}`"

    try:
        msg_id_int = int(ref)
    except ValueError:
        return None, "That isn't a message link or a message ID. Right-click the message → **Copy Message Link**."

    message, search_errors = await search_guild_for_message(guild, msg_id_int)
    if message is None:
        note = ""
        if search_errors:
            note = "\n```\n" + "\n".join(search_errors[:5]) + "\n```"
        return None, f"No message with ID `{msg_id_int}` found in this server.{note}"
    return message, None


def user_can_read(member: discord.Member, channel) -> bool:
    perms = channel.permissions_for(member)
    return perms.view_channel and perms.read_message_history


# ============================================================
# INPUT — slash option, attached file, or (fallback) next message
# ============================================================

async def _read_attachment(attachment: discord.Attachment):
    """Returns (text, error)."""
    name = (attachment.filename or "").lower()
    if not (name.endswith(".txt") or name.endswith(".json")):
        return None, f"`{attachment.filename}` isn't a `.json` or `.txt` file."
    if attachment.size > MAX_JSON_FILE_BYTES:
        return None, f"`{attachment.filename}` is too big ({attachment.size // 1024:,} KB). Max is {MAX_JSON_FILE_BYTES // 1024 // 1024} MB."
    try:
        raw_bytes = await attachment.read()
    except discord.HTTPException as e:
        return None, f"Couldn't download the attached file: `{e}`"
    try:
        return raw_bytes.decode("utf-8-sig"), None
    except UnicodeDecodeError:
        return None, "The attached file isn't valid UTF-8 text."


async def _read_json_source(message: discord.Message):
    """Next-message fallback: prefers an attached file (what a long paste
    becomes automatically), otherwise the message text."""
    if message.attachments:
        return await _read_attachment(message.attachments[0])
    if message.content:
        return message.content, None
    return None, "That message had no text and no `.txt`/`.json` attachment."


async def collect_embed_json(bot, interaction: discord.Interaction):
    """Fallback when /embed is run with neither json: nor file:. Waits for
    the person's next message in the channel. Returns
    (raw_text, source_message, error)."""
    def check(m: discord.Message) -> bool:
        return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

    try:
        reply_message = await bot.wait_for("message", check=check, timeout=INPUT_WAIT_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        return None, None, f"No JSON received within {INPUT_WAIT_TIMEOUT_SECONDS // 60} minutes — cancelled."

    raw_text, read_error = await _read_json_source(reply_message)
    if read_error:
        return None, reply_message, read_error
    return raw_text, reply_message, None


async def _delete_quietly(message: discord.Message | None):
    """Removes the pasted-JSON message once it's been read successfully.
    Needs Manage Messages in that channel; silently skipped without it."""
    if message is None:
        return
    try:
        await message.delete()
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        pass


# ============================================================
# SENDING / EDITING
# ============================================================

async def send_prepared(target, result: dict, mentions: discord.AllowedMentions):
    """Sends a prepare_message_payload() result to a channel, a followup
    webhook, or anything else with .send(). Returns the sent message."""
    if result["kind"] == "cv2":
        return await target.send(view=result["view"], allowed_mentions=mentions)
    if result["kind"] == "legacy":
        return await target.send(content=result["content"], embeds=result["embeds"], view=result["view"], allowed_mentions=mentions)
    return await target.send(content=result["content"], embeds=result["embeds"], allowed_mentions=mentions)


async def edit_prepared(message: discord.Message, result: dict):
    if result["kind"] == "cv2":
        # Switching a message TO Components V2 requires clearing content/embeds in the same edit.
        await message.edit(content=None, embeds=[], view=result["view"], allowed_mentions=NO_PINGS)
    elif result["kind"] == "legacy":
        await message.edit(
            content=result["content"] if result["content"] is not None else "",
            embeds=result["embeds"], view=result["view"], allowed_mentions=NO_PINGS,
        )
    else:
        # view=None clears any leftover buttons from a previous version.
        await message.edit(
            content=result["content"] if result["content"] is not None else "",
            embeds=result["embeds"], view=None, allowed_mentions=NO_PINGS,
        )


def parse_and_prepare(raw_text: str):
    """Returns (parsed, result, error_text). parsed is kept so the payload
    can be rebuilt fresh (new view objects) when it's actually posted."""
    parsed, syntax_error = validate_json_syntax(raw_text)
    if syntax_error:
        return None, None, f"**Invalid JSON.**\n```\n{syntax_error}\n```"
    result = prepare_message_payload(parsed)
    if result["kind"] == "error":
        return None, None, result["message"]
    return parsed, result, None


class PostPreviewView(discord.ui.View):
    """Post / Cancel buttons shown under the private preview. Only the
    person who ran /embed can press them."""

    def __init__(self, owner_id: int, parsed: dict, target: discord.abc.Messageable, mentions: discord.AllowedMentions):
        super().__init__(timeout=PREVIEW_TIMEOUT_SECONDS)
        self.owner_id = owner_id
        self.parsed = parsed
        self.target = target
        self.mentions = mentions
        self.message: discord.WebhookMessage | None = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.owner_id

    async def _finish(self, interaction: discord.Interaction, embed: discord.Embed):
        self.stop()
        await interaction.response.edit_message(content=None, embed=embed, view=None)

    @discord.ui.button(label="Post", style=discord.ButtonStyle.success)
    async def post(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = interaction.client
        result = prepare_message_payload(self.parsed)  # fresh view objects for the real post
        if result["kind"] == "error":
            return await self._finish(interaction, _err_embed(bot, result["message"]))
        try:
            sent = await send_prepared(self.target, result, self.mentions)
        except discord.Forbidden:
            return await self._finish(interaction, _err_embed(bot, f"I can't send messages in {self.target.mention}."))
        except discord.HTTPException as e:
            return await self._finish(interaction, _err_embed(bot, f"Discord rejected the message: `{e}`"))
        await self._finish(interaction, _ok_embed(bot, f"Posted → {sent.jump_url}", result.get("warnings")))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._finish(interaction, _warn_embed(interaction.client, "Cancelled — nothing was posted."))

    async def on_timeout(self):
        if self.message is not None:
            try:
                await self.message.edit(content="Preview expired — nothing was posted.", embed=None, view=None)
            except discord.HTTPException:
                pass


# ============================================================
# EDITING — modal when it fits, file round-trip when it doesn't
# ============================================================

class EmbedEditModal(discord.ui.Modal, title="Edit Message"):
    def __init__(self, message_to_edit: discord.Message, current_json: str):
        super().__init__()
        self.message_to_edit = message_to_edit
        self.embed_json = discord.ui.TextInput(
            label="Current JSON (edit and submit to save)",
            style=discord.TextStyle.paragraph,
            default=current_json,
            required=True,
            max_length=MODAL_MAX_CHARS,
        )
        self.add_item(self.embed_json)

    async def on_submit(self, interaction: discord.Interaction):
        bot = interaction.client
        _, result, error = parse_and_prepare(self.embed_json.value)
        if error:
            return await interaction.response.send_message(embed=_err_embed(bot, error), ephemeral=True)
        try:
            await edit_prepared(self.message_to_edit, result)
        except discord.Forbidden:
            return await interaction.response.send_message(embed=_err_embed(bot, "I lack permission to edit that message."), ephemeral=True)
        except discord.HTTPException as e:
            return await interaction.response.send_message(embed=_err_embed(bot, f"Discord rejected the edit: `{e}`"), ephemeral=True)
        await interaction.response.send_message(embed=_ok_embed(bot, "Message updated.", result.get("warnings")), ephemeral=True)


async def open_editor(interaction: discord.Interaction, message: discord.Message):
    """Opens the edit modal prefilled with compact JSON if it fits in
    Discord's 4000-character modal field. If it doesn't, NEVER truncates
    (that used to corrupt the message on save) — sends the full JSON as a
    file instead, to edit and upload back with /embededit file:."""
    bot = interaction.client
    try:
        compact = build_json_from_message(message, compact=True)
    except Exception as e:
        return await _reply(interaction, embed=_err_embed(bot, f"Failed to read that message as JSON: `{e}`"))

    if len(compact) <= MODAL_MAX_CHARS:
        try:
            return await interaction.response.send_modal(EmbedEditModal(message, compact))
        except discord.HTTPException as e:
            return await _reply(interaction, embed=_err_embed(bot, f"Failed to open editor: `{e}`"))

    pretty = build_json_from_message(message)
    file = discord.File(io.BytesIO(pretty.encode("utf-8")), filename=f"message_{message.id}.json")
    await _reply(
        interaction,
        content=(
            f"{bot.error_emoji} This message's JSON is {len(compact):,} characters — too long for the edit window "
            f"({MODAL_MAX_CHARS:,} max).\nEdit this file, then run `/embededit message:{message.jump_url} file:` "
            f"and attach it."
        ),
        file=file,
    )


def _check_editable(bot, interaction: discord.Interaction, message: discord.Message) -> str | None:
    if message.author.id != bot.user.id:
        return "I can only edit my own messages."
    perms = message.channel.permissions_for(interaction.user)
    if not (perms.view_channel and perms.manage_messages):
        return "You need **Manage Messages** in that channel to edit it."
    return None


# ============================================================
# EXPORT OUTPUT — shared by /json and View as JSON
# ============================================================

async def send_json_export(interaction: discord.Interaction, message: discord.Message):
    bot = interaction.client
    if not message.embeds and not message.content and not message.components:
        return await _reply(interaction, embed=_warn_embed(bot, "That message has no content, embeds, or components."))
    try:
        json_str = build_json_from_message(message)
    except Exception as e:
        return await _reply(interaction, embed=_err_embed(bot, f"Failed to serialize message to JSON: `{e}`"))

    format_note = "Components V2" if is_components_v2_message(message) else "classic embed"
    location_note = f"#{message.channel.name} · {format_note} · {len(json_str):,} chars"
    try:
        if len(json_str) > 1900:
            file = discord.File(io.BytesIO(json_str.encode("utf-8")), filename=f"message_{message.id}.json")
            await _reply(interaction, content=f"{bot.check_emoji} {location_note}", file=file)
        else:
            await _reply(interaction, content=f"{bot.check_emoji} {location_note}\n```json\n{json_str}\n```")
    except discord.HTTPException as e:
        await _reply(interaction, embed=_err_embed(bot, f"Failed to send the JSON: `{e}`"))


# ============================================================
# CONTEXT MENUS
# ============================================================
# discord.py does not allow @app_commands.context_menu on a Cog method
# ("context menus cannot be defined inside a class") — these are
# module-level functions, added to the tree in Embeds.cog_load and removed
# in cog_unload. Right-clicking hands over the exact message, so no
# link/ID lookup is needed.

@app_commands.context_menu(name="View as JSON")
async def view_json_context_menu(interaction: discord.Interaction, message: discord.Message):
    await interaction.response.defer(ephemeral=True)
    if interaction.guild is None:
        return await _reply(interaction, embed=_err_embed(interaction.client, "This only works inside a server."))
    await send_json_export(interaction, message)


@view_json_context_menu.error
async def view_json_context_menu_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    await _reply(interaction, embed=_err_embed(interaction.client, f"Unexpected error: `{type(error).__name__}: {error}`"))


@app_commands.context_menu(name="Edit Message")
@app_commands.checks.has_permissions(manage_messages=True)
async def edit_message_context_menu(interaction: discord.Interaction, message: discord.Message):
    bot = interaction.client
    if interaction.guild is None:
        return await _reply(interaction, embed=_err_embed(bot, "This only works inside a server."))
    problem = _check_editable(bot, interaction, message)
    if problem:
        return await _reply(interaction, embed=_warn_embed(bot, problem))
    await open_editor(interaction, message)


@edit_message_context_menu.error
async def edit_message_context_menu_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    bot = interaction.client
    if isinstance(error, app_commands.MissingPermissions):
        text = "You need `Manage Messages` permission to use this."
    else:
        text = f"Unexpected error: `{type(error).__name__}: {error}`"
    await _reply(interaction, embed=_err_embed(bot, text))


# ============================================================
# COG
# ============================================================

class Embeds(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.category = "Utility"

    async def cog_load(self):
        for ctx_menu in (view_json_context_menu, edit_message_context_menu):
            try:
                self.bot.tree.add_command(ctx_menu)
            except app_commands.CommandAlreadyRegistered:
                pass

        if DISABLE_JSON_AND_EMBEDEDIT_SLASH_COMMANDS:
            self.bot.tree.remove_command(self.json_command.name)
            self.bot.tree.remove_command(self.embededit_command.name)

    async def cog_unload(self):
        for ctx_menu in (view_json_context_menu, edit_message_context_menu):
            self.bot.tree.remove_command(ctx_menu.name, type=ctx_menu.type)

    # --- /embed ---
    @app_commands.command(name="embed", description="Post a message from JSON — paste it, attach a file, or send it next.")
    @app_commands.rename(json_text="json")
    @app_commands.describe(
        json_text="Paste the JSON here (up to 6000 characters).",
        file="Or attach a .json / .txt file (any size).",
        channel="Where to post it (default: this channel).",
        pings="Let @mentions in the message actually ping (default: off).",
        preview="Show a private preview with Post / Cancel first (default: on).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def embed_command(
        self,
        interaction: discord.Interaction,
        json_text: app_commands.Range[str, 1, SLASH_JSON_MAX_CHARS] | None = None,
        file: discord.Attachment | None = None,
        channel: discord.TextChannel | None = None,
        pings: bool = False,
        preview: bool = True,
    ):
        bot = interaction.client
        target = channel or interaction.channel
        mentions = ALL_PINGS if pings else NO_PINGS

        target_perms = target.permissions_for(interaction.guild.me)
        if not (target_perms.view_channel and target_perms.send_messages):
            return await _reply(interaction, embed=_err_embed(bot, f"I can't send messages in {target.mention}."))

        source_message = None
        if file is not None:
            await interaction.response.defer(ephemeral=True, thinking=True)
            raw_text, read_error = await _read_attachment(file)
        elif json_text is not None:
            await interaction.response.defer(ephemeral=True, thinking=True)
            raw_text, read_error = json_text, None
        else:
            prompt = discord.Embed(
                description=(
                    f"{bot.check_emoji}  Send your JSON as your **next message** in this channel "
                    f"(paste it, or attach a `.json`/`.txt`). It's deleted once it's read.\n\n"
                    f"-# Tip: `/embed json:` or `/embed file:` skips this step.\n"
                    f"-# Cancels after {INPUT_WAIT_TIMEOUT_SECONDS // 60} minutes."
                ),
                color=bot.embed_color,
            )
            await interaction.response.send_message(embed=prompt, ephemeral=True)
            raw_text, source_message, read_error = await collect_embed_json(bot, interaction)

        if read_error:
            return await _reply(interaction, embed=_err_embed(bot, read_error))

        parsed, result, error = parse_and_prepare(raw_text)
        if error:
            # The pasted message is kept on an error so it can be copied and fixed.
            return await _reply(interaction, embed=_err_embed(bot, error))

        await _delete_quietly(source_message)

        if not preview:
            try:
                sent = await send_prepared(target, result, mentions)
            except discord.Forbidden:
                return await _reply(interaction, embed=_err_embed(bot, f"I can't send messages in {target.mention}."))
            except discord.HTTPException as e:
                return await _reply(interaction, embed=_err_embed(bot, f"Discord rejected the message: `{e}`"))
            return await _reply(interaction, embed=_ok_embed(bot, f"Posted → {sent.jump_url}", result.get("warnings")))

        # Private preview of exactly what will be posted, then the controls.
        try:
            await _send_preview(interaction, result)
        except discord.HTTPException as e:
            return await _reply(interaction, embed=_err_embed(bot, f"Discord rejected the message: `{e}`"))

        controls = PostPreviewView(interaction.user.id, parsed, target, mentions)
        where = "here" if target.id == interaction.channel.id else target.mention
        ping_note = "pings **on**" if pings else "pings off"
        controls.message = await interaction.followup.send(
            content=f"↑ Preview. Post it {where}? ({ping_note})",
            view=controls, ephemeral=True, wait=True,
        )

    @embed_command.error
    async def embed_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            text = "You need `Manage Server` permission to use this command."
        else:
            text = f"Unexpected error: `{type(error).__name__}: {error}`"
        await _reply(interaction, embed=_err_embed(self.bot, text))

    # --- /json ---
    @app_commands.command(name="json", description="Export a message as sendable JSON.")
    @app_commands.describe(message="Message link (Right-click → Copy Message Link) or message ID.")
    async def json_command(self, interaction: discord.Interaction, message: str):
        await interaction.response.defer(ephemeral=True)
        bot = interaction.client
        if interaction.guild is None:
            return await _reply(interaction, embed=_err_embed(bot, "This command only works inside a server."))

        found, error = await resolve_message(interaction.guild, message)
        if error:
            return await _reply(interaction, embed=_err_embed(bot, error))
        # Only export from channels the PERSON can read — the bot can see
        # private/staff channels they can't.
        if not user_can_read(interaction.user, found.channel):
            return await _reply(interaction, embed=_err_embed(bot, "You don't have access to the channel that message is in."))
        await send_json_export(interaction, found)

    @json_command.error
    async def json_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        await _reply(interaction, embed=_err_embed(self.bot, f"Unexpected error: `{type(error).__name__}: {error}`"))

    # --- /embededit ---
    @app_commands.command(name="embededit", description="Edit one of the bot's messages from JSON.")
    @app_commands.describe(
        message="Message link (Right-click → Copy Message Link) or message ID.",
        file="Optional: an edited .json / .txt to apply directly (for messages too big for the edit window).",
    )
    @app_commands.checks.has_permissions(manage_messages=True)
    async def embededit_command(self, interaction: discord.Interaction, message: str, file: discord.Attachment | None = None):
        bot = interaction.client
        if interaction.guild is None:
            return await _reply(interaction, embed=_err_embed(bot, "This command only works inside a server."))

        if file is not None:
            await interaction.response.defer(ephemeral=True, thinking=True)

        # A modal must be the FIRST response, so without a file there's no
        # defer here — a message link resolves in one call, well inside
        # Discord's 3-second window. A bare ID searches the server and may not.
        found, error = await resolve_message(interaction.guild, message)
        if error:
            return await _reply(interaction, embed=_err_embed(bot, error))
        problem = _check_editable(bot, interaction, found)
        if problem:
            return await _reply(interaction, embed=_warn_embed(bot, problem))

        if file is None:
            return await open_editor(interaction, found)

        raw_text, read_error = await _read_attachment(file)
        if read_error:
            return await _reply(interaction, embed=_err_embed(bot, read_error))
        _, result, error = parse_and_prepare(raw_text)
        if error:
            return await _reply(interaction, embed=_err_embed(bot, error))
        try:
            await edit_prepared(found, result)
        except discord.Forbidden:
            return await _reply(interaction, embed=_err_embed(bot, "I lack permission to edit that message."))
        except discord.HTTPException as e:
            return await _reply(interaction, embed=_err_embed(bot, f"Discord rejected the edit: `{e}`"))
        await _reply(interaction, embed=_ok_embed(bot, f"Message updated → {found.jump_url}", result.get("warnings")))

    @embededit_command.error
    async def embededit_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            text = "You need `Manage Messages` permission to use this command."
        else:
            text = f"Unexpected error: `{type(error).__name__}: {error}`"
        await _reply(interaction, embed=_err_embed(self.bot, text))


async def _send_preview(interaction: discord.Interaction, result: dict):
    """Ephemeral copy of exactly what /embed is about to post. Ephemeral
    messages never ping anyone."""
    if result["kind"] == "cv2":
        await interaction.followup.send(view=result["view"], ephemeral=True)
    elif result["kind"] == "legacy":
        await interaction.followup.send(content=result["content"], embeds=result["embeds"], view=result["view"], ephemeral=True)
    else:
        await interaction.followup.send(content=result["content"], embeds=result["embeds"], ephemeral=True)


async def setup(bot) -> None:
    await bot.add_cog(Embeds(bot))