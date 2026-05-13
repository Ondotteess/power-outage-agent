from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=(".env", ".env.local"), extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/outage_agent"

    # Generic OpenAI-compatible LLM settings — kept as a baseline for future provider
    # switching (DeepSeek / OpenAI-compatible gateways). Not used by the active
    # GigaChat normalizer (see fields below).
    llm_base_url: str = "https://api.deepseek.com"
    llm_api_key: str = ""
    llm_model: str = "deepseek-chat"
    # Cost guardrail: LLM normalization is opt-in. Keep this disabled for normal
    # checkpoint/demo runs so large source payloads cannot enqueue thousands of
    # paid model calls by accident.
    llm_normalization_enabled: bool = False
    llm_normalization_max_per_raw: int = 5

    # GigaChat — currently the only active LLM provider in the normalizer.
    # Provide EITHER `gigachat_auth_key` (base64 of "client_id:client_secret",
    # shown in personal cabinet as "Authorization Key")
    # OR `gigachat_client_id` + `gigachat_client_secret` separately.
    gigachat_auth_key: str = ""
    gigachat_client_id: str = ""
    gigachat_client_secret: str = ""
    gigachat_scope: str = "GIGACHAT_API_PERS"
    gigachat_base_url: str = "https://gigachat.devices.sberbank.ru/api/v1"
    gigachat_oauth_url: str = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    gigachat_model: str = "GigaChat-2"  # base tier of v2 family (no separate "Lite" anymore)
    gigachat_verify_ssl: bool = True

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    log_level: str = "INFO"


settings = Settings()
