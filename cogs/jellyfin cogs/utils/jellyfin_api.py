"""
Shared Jellyfin API client.

Every cog that talks to Jellyfin imports from here instead of building
its own headers, timeouts, and request helpers. The client wraps the
bot's shared aiohttp.ClientSession (bot.http_session, created once in
main.py's setup_hook).

Usage in a cog:
    from utils.jellyfin_api import JellyfinClient

    class MyCog(commands.Cog):
        async def cog_load(self):
            # Use the shared instance (one user cache, one semaphore)
            self.jf = getattr(self.bot, "jf", None) or JellyfinClient(self.bot)

        async def some_method(self):
            users = await self.jf.get_users()
            sessions = await self.jf.get_sessions(active_within=600)
"""

import asyncio
import logging
import time
from typing import Any

import aiohttp

from .config import JELLYFIN_URL, JELLYFIN_API_KEY

logger = logging.getLogger("jellyfin_api")

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=10)
DEFAULT_CONCURRENCY = 5  # max parallel requests to Jellyfin


def _build_headers(api_key: str | None = None) -> dict[str, str]:
    key = api_key or JELLYFIN_API_KEY
    return {
        "Authorization": f'MediaBrowser Token="{key}"',
        "accept": "application/json",
    }


class JellyfinClient:
    """Thin async wrapper around Jellyfin's REST API.

    Intended as a shared instance on the bot (bot.jf) so all cogs share
    one user cache and one concurrency semaphore. All requests go through
    the bot's shared aiohttp.ClientSession — this class never creates or
    closes one.
    """

    def __init__(self, bot, *, api_key: str | None = None, timeout: aiohttp.ClientTimeout | None = None,
                 concurrency: int = DEFAULT_CONCURRENCY):
        self._bot = bot
        self._headers = _build_headers(api_key)
        self._timeout = timeout or DEFAULT_TIMEOUT
        self._sem = asyncio.Semaphore(concurrency)
        # Cached /Users list — shared across lookups and duplicate-name checks.
        self._users_cache: list[dict] | None = None
        self._users_cache_time: float = 0.0
        self._users_cache_ttl: float = 30.0

    @property
    def session(self) -> aiohttp.ClientSession:
        return self._bot.http_session

    # --- low-level request ---

    async def request(self, method: str, path: str, *,
                      params: dict | None = None,
                      json: Any = None,
                      timeout: aiohttp.ClientTimeout | None = None,
                      headers: dict | None = None,
                      raise_for_status: bool = True) -> aiohttp.ClientResponse:
        """Fire a request and return the response. Caller must use `async with`
        or close the response. Semaphore-gated to limit parallel requests."""
        url = f"{JELLYFIN_URL}{path}"
        async with self._sem:
            resp = await self.session.request(
                method, url,
                headers=headers or self._headers,
                params=params,
                json=json,
                timeout=timeout or self._timeout,
            )
            if raise_for_status:
                resp.raise_for_status()
            return resp

    async def get_json(self, path: str, *, params: dict | None = None,
                       timeout: aiohttp.ClientTimeout | None = None) -> Any:
        async with await self.request("GET", path, params=params, timeout=timeout) as resp:
            return await resp.json()

    async def post_json(self, path: str, *, json: Any = None, params: dict | None = None,
                        timeout: aiohttp.ClientTimeout | None = None) -> Any:
        async with await self.request("POST", path, json=json, params=params, timeout=timeout) as resp:
            return await resp.json()

    # --- Users ---

    async def get_users(self) -> list[dict]:
        return await self.get_json("/Users")

    async def get_users_cached(self, ttl: float | None = None) -> list[dict]:
        """Cached /Users for lookups and duplicate checks."""
        now = time.monotonic()
        cache_ttl = ttl if ttl is not None else self._users_cache_ttl
        if self._users_cache is not None and (now - self._users_cache_time) < cache_ttl:
            return self._users_cache
        users = await self.get_users()
        self._users_cache = users
        self._users_cache_time = now
        return users

    def invalidate_users_cache(self):
        self._users_cache = None
        self._users_cache_time = 0.0

    async def get_user(self, user_id: str) -> dict | None:
        """GET /Users/{id} — returns None on 404."""
        try:
            return await self.get_json(f"/Users/{user_id}")
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return None
            raise

    async def get_user_strict(self, user_id: str) -> dict | None:
        """GET /Users/{id} — returns None on ANY error (404, 403, timeout)."""
        try:
            return await self.get_json(f"/Users/{user_id}")
        except Exception:
            return None

    async def create_user(self, username: str, password: str) -> dict:
        """POST /Users/New — returns the new user object."""
        result = await self.post_json("/Users/New", json={"Name": username, "Password": password})
        self.invalidate_users_cache()
        return result

    async def set_password(self, user_id: str, new_password: str):
        """POST /Users/{id}/Password — sets a new password directly."""
        async with await self.request(
            "POST", f"/Users/{user_id}/Password",
            json={"NewPw": new_password, "ResetPassword": False},
        ):
            pass

    async def delete_user(self, user_id: str) -> str:
        """DELETE /Users/{id} — returns 'deleted' or 'already_gone'."""
        try:
            async with await self.request("DELETE", f"/Users/{user_id}") as resp:
                self.invalidate_users_cache()
                return "deleted"
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return "already_gone"
            raise

    async def rename_user(self, user_id: str, new_name: str):
        """Rename by fetching the full user object, changing Name, and POSTing it back."""
        user = await self.get_json(f"/Users/{user_id}")
        user["Name"] = new_name
        async with await self.request("POST", f"/Users/{user_id}", json=user):
            pass
        self.invalidate_users_cache()

    async def grant_full_library_access(self, user_id: str):
        """Enable all folders and features for a user."""
        user = await self.get_json(f"/Users/{user_id}")
        policy = user.get("Policy", {})
        policy["EnableAllFolders"] = True
        policy["EnableAllChannels"] = True
        policy["EnableAllDevices"] = True
        async with await self.request("POST", f"/Users/{user_id}/Policy", json=policy):
            pass

    # --- Sessions ---

    async def get_sessions(self, active_within: int | None = None) -> list[dict]:
        params = {}
        if active_within is not None:
            params["activeWithinSeconds"] = str(active_within)
        return await self.get_json("/Sessions", params=params)

    # --- Devices ---

    async def get_user_devices(self, user_id: str) -> list[dict]:
        """GET /Devices filtered by userId. Returns the Items array."""
        data = await self.get_json("/Devices", params={"userId": user_id})
        return data.get("Items", [])

    async def delete_device(self, device_id: str):
        """DELETE /Devices?id= — removes the device and its login token."""
        async with await self.request("DELETE", "/Devices", params={"id": device_id}):
            pass

    # --- Playback Reporting (custom query) ---

    async def playback_query(self, sql: str, *, timeout: aiohttp.ClientTimeout | None = None) -> list[dict]:
        """POST to the Playback Reporting plugin's custom query endpoint.
        Returns the result rows as dicts keyed by column name."""
        data = await self.post_json(
            "/user_usage_stats/submit_custom_query",
            json={"CustomQueryString": sql, "ReplaceUserId": False},
            timeout=timeout,
        )
        # Jellyfin Playback Reporting uses the typo "colums" (no 'n')
        raw_cols = data.get("colums") or data.get("columns") or []
        columns = [col.get("Name", f"col{i}") if isinstance(col, dict) else str(col)
                    for i, col in enumerate(raw_cols)]
        return [dict(zip(columns, row)) for row in data.get("results", [])]

    # --- Items / Images ---

    @staticmethod
    def image_url(item_id: str, *, height: int = 300, quality: int = 80, tag: str | None = None) -> str:
        url = f"{JELLYFIN_URL}/Items/{item_id}/Images/Primary?fillHeight={height}&quality={quality}"
        if tag:
            url += f"&tag={tag}"
        return url

    @staticmethod
    def user_avatar_url(user_id: str) -> str:
        return f"{JELLYFIN_URL}/Users/{user_id}/Images/Primary"


# No-op setup so the extension loader skips this without error.
async def setup(bot):  # noqa: ARG001
    pass
