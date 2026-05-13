from __future__ import annotations

from typing import Any

import httpx


class TelegramDeliveryError(RuntimeError):
    """Raised when Telegram rejects or cannot receive a message."""


class TelegramSender:
    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        *,
        client: httpx.AsyncClient | None = None,
        base_url: str = "https://api.telegram.org",
        timeout: float = 10.0,
    ) -> None:
        self._bot_token = bot_token.strip()
        self._chat_id = chat_id.strip()
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    @property
    def configured(self) -> bool:
        return bool(self._bot_token and self._chat_id)

    async def send_message(self, text: str) -> None:
        if not self.configured:
            raise TelegramDeliveryError("Telegram bot token or chat id is not configured")

        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        url = f"{self._base_url}/bot{self._bot_token}/sendMessage"

        if self._client is not None:
            await self._send_with_client(self._client, url, payload)
            return

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            await self._send_with_client(client, url, payload)

    async def _send_with_client(
        self,
        client: httpx.AsyncClient,
        url: str,
        payload: dict[str, Any],
    ) -> None:
        try:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            detail = _shorten(exc.response.text)
            raise TelegramDeliveryError(
                f"Telegram sendMessage failed with HTTP {status}: {detail}"
            ) from exc
        except httpx.HTTPError as exc:
            raise TelegramDeliveryError(
                f"Telegram sendMessage request failed: {exc.__class__.__name__}"
            ) from exc
        except ValueError as exc:
            raise TelegramDeliveryError("Telegram sendMessage returned invalid JSON") from exc

        if not isinstance(data, dict) or data.get("ok") is not True:
            description = data.get("description") if isinstance(data, dict) else None
            raise TelegramDeliveryError(
                f"Telegram sendMessage was rejected: {description or 'unknown error'}"
            )


def _shorten(value: str, limit: int = 240) -> str:
    text = value.strip().replace("\n", " ")
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."
