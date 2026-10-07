from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    SNOWFLAKE_ACCOUNT: str = ""
    SNOWFLAKE_USER: str = ""
    SNOWFLAKE_PASSWORD: SecretStr = SecretStr("")
    SNOWFLAKE_ROLE: str = ""  # set explicitly; prefer a read-only role
    SNOWFLAKE_WAREHOUSE: str = "COMPUTE_WH"
    SNOWFLAKE_DATABASE: str = "ECOMM_DATA_LAKE"
    SNOWFLAKE_SCHEMA: str = "CONFORMED"
    SNOWFLAKE_QUERY_TIMEOUT_SECONDS: int = 25
    LLM_MODEL: str = "MiniMax-M2.7-highspeed"
    LLM_API_KEY: SecretStr = SecretStr("")
    LLM_BASE_URL: str = "https://api.minimaxi.chat/v1"
    LLM_TIMEOUT_SECONDS: float = 20.0
    LLM_MAX_TOKENS: int = 1024
    # Optional shared secret for /chat. When empty the endpoint is open (local demo mode).
    ECOMBOT_API_KEY: SecretStr = SecretStr("")
    APP_PORT: int = 8010

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


settings = Settings()
