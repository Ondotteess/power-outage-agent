"""Minimal async client for Sber GigaChat API.

Only handles transport: OAuth token caching, HTTP errors, JSON parsing.
Business logic (prompt building, response validation) lives in
`app/normalization/llm.py`.

Reference: https://developers.sber.ru/docs/ru/gigachat/individuals-quickstart
"""

from __future__ import annotations

import base64
import logging
import time
from uuid import uuid4

import httpx

logger = logging.getLogger(__name__)


# Refresh token a bit before actual expiry to avoid races on the boundary.
_TOKEN_REFRESH_MARGIN_SEC = 60


class GigaChatError(Exception):
    """Base for all GigaChat client errors."""


class GigaChatAuthError(GigaChatError):
    """OAuth flow failed (bad key, bad scope, network)."""


class GigaChatHTTPError(GigaChatError):
    """Chat completion endpoint returned a non-2xx response."""


class GigaChatInvalidResponseError(GigaChatError):
    """Response from GigaChat was not valid JSON / not in expected shape."""


class GigaChatClient:
    def __init__(
        self,
        *,
        scope: str,
        base_url: str,
        oauth_url: str,
        model: str,
        auth_key: str = "",
        client_id: str = "",
        client_secret: str = "",
        verify_ssl: bool = True,
        timeout: float = 60.0,
    ) -> None:
        """Provide either `auth_key` (pre-encoded base64 of "id:secret") or
        both `client_id` and `client_secret` (will be base64-encoded here).
        If both forms are supplied, `auth_key` wins (more explicit).
        """
        resolved_key = auth_key or self._encode_basic(client_id, client_secret)
        if not resolved_key:
            raise GigaChatAuthError(
                "GigaChat credentials missing — set GIGACHAT_AUTH_KEY "
                "or both GIGACHAT_CLIENT_ID and GIGACHAT_CLIENT_SECRET in .env"
            )
        self._auth_key = resolved_key
        self._scope = scope
        self._base_url = base_url.rstrip("/")
        self._oauth_url = oauth_url
        self._model = model
        self._verify_ssl = verify_ssl
        self._timeout = timeout

        # In-memory token cache. Re-issued lazily on expiry.
        self._token: str | None = None
        self._token_expires_at: float = 0.0  # unix seconds

    @staticmethod
    def _encode_basic(client_id: str, client_secret: str) -> str:
        """base64('client_id:client_secret') — matches what Sber shows as Authorization Key."""
        if not client_id or not client_secret:
            return ""
        token = f"{client_id}:{client_secret}".encode()
        return base64.b64encode(token).decode("ascii")

    async def _get_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expires_at - _TOKEN_REFRESH_MARGIN_SEC:
            return self._token

        rquid = str(uuid4())
        logger.debug("GigaChat  oauth  RqUID=%s  scope=%s", rquid, self._scope)

        async with httpx.AsyncClient(timeout=self._timeout, verify=self._verify_ssl) as client:
            response = await client.post(
                self._oauth_url,
                headers={
                    "Authorization": f"Basic {self._auth_key}",
                    "RqUID": rquid,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
                data={"scope": self._scope},
            )

        if response.status_code != 200:
            # Body may contain error description but never auth_key (it's only in headers).
            raise GigaChatAuthError(
                f"OAuth failed: HTTP {response.status_code}: {response.text[:200]}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise GigaChatAuthError(f"OAuth response is not JSON: {exc}") from exc

        token = payload.get("access_token")
        expires_at_ms = payload.get("expires_at")
        if not token or not expires_at_ms:
            raise GigaChatAuthError(
                f"OAuth response missing access_token / expires_at: keys={list(payload)}"
            )

        self._token = token
        # expires_at from Sber is in milliseconds since epoch.
        self._token_expires_at = float(expires_at_ms) / 1000.0
        ttl = self._token_expires_at - now
        logger.info("GigaChat  token issued  ttl=%.0fs  RqUID=%s", ttl, rquid)
        return token

    async def chat_completion(
        self,
        *,
        messages: list[dict],
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> dict:
        """Send a chat completion request. Returns the parsed JSON response.

        Raises GigaChatHTTPError on non-2xx, GigaChatInvalidResponseError on bad JSON.
        """
        token = await self._get_token()
        url = f"{self._base_url}/chat/completions"

        body: dict = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
        }
        if max_tokens is not None:
            body["max_tokens"] = max_tokens

        logger.debug(
            "GigaChat  chat  model=%s  temp=%.2f  msgs=%d", self._model, temperature, len(messages)
        )

        async with httpx.AsyncClient(timeout=self._timeout, verify=self._verify_ssl) as client:
            response = await client.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json=body,
            )

        if response.status_code != 200:
            raise GigaChatHTTPError(
                f"chat/completions HTTP {response.status_code}: {response.text[:500]}"
            )

        try:
            return response.json()
        except ValueError as exc:
            raise GigaChatInvalidResponseError(
                f"chat/completions response is not JSON: {exc}"
            ) from exc

    @staticmethod
    def extract_message_content(response: dict) -> str:
        """Pull the assistant message text out of an OpenAI-style chat response."""
        try:
            return response["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise GigaChatInvalidResponseError(
                f"unexpected chat response shape: {list(response)[:5]}"
            ) from exc

    @staticmethod
    def extract_usage(response: dict) -> dict[str, int]:
        """Pull `usage` token counts out of the response, defaulting to zero.

        GigaChat returns OpenAI-style `{prompt_tokens, completion_tokens,
        total_tokens}`. Missing fields are treated as zero rather than as an
        error: token tracking is a metrics-only concern and should never break
        normalization.
        """
        usage = response.get("usage") if isinstance(response, dict) else None
        if not isinstance(usage, dict):
            return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        prompt = int(usage.get("prompt_tokens") or 0)
        completion = int(usage.get("completion_tokens") or 0)
        total = int(usage.get("total_tokens") or (prompt + completion))
        return {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": total,
        }

    @property
    def model_name(self) -> str:
        return self._model
