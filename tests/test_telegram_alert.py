from __future__ import annotations

import httpx
import pytest

from app.alerts.telegram import TelegramDeliveryError, TelegramSender


async def test_telegram_sender_posts_send_message_payload():
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["payload"] = request.read()
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = TelegramSender("token-123", "chat-42", client=client)

        await sender.send_message("hello")

    assert seen["url"] == "https://api.telegram.org/bottoken-123/sendMessage"
    assert (
        seen["payload"] == b'{"chat_id":"chat-42","text":"hello","disable_web_page_preview":true}'
    )


async def test_telegram_sender_rejects_non_ok_response():
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"ok": False, "description": "chat not found"})
    )
    async with httpx.AsyncClient(transport=transport) as client:
        sender = TelegramSender("token-123", "chat-42", client=client)

        with pytest.raises(TelegramDeliveryError, match="chat not found"):
            await sender.send_message("hello")


async def test_telegram_sender_wraps_http_errors():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection failed", request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        sender = TelegramSender("token-123", "chat-42", client=client)

        with pytest.raises(TelegramDeliveryError, match="request failed"):
            await sender.send_message("hello")


async def test_telegram_sender_requires_configuration():
    sender = TelegramSender("", "chat-42")

    with pytest.raises(TelegramDeliveryError, match="not configured"):
        await sender.send_message("hello")
