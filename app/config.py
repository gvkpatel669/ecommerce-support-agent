from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SNOWFLAKE_ACCOUNT: str = ""
    SNOWFLAKE_USER: str = ""
    SNOWFLAKE_PASSWORD: str = ""
    SNOWFLAKE_ROLE: str = "ACCOUNTADMIN"
    SNOWFLAKE_WAREHOUSE: str = "COMPUTE_WH"
    SNOWFLAKE_DATABASE: str = "ECOMM_DATA_LAKE"
    SNOWFLAKE_SCHEMA: str = "CONFORMED"
    LLM_MODEL: str = "MiniMax-M2.7-highspeed"
    LLM_API_KEY: str = ""
    LLM_BASE_URL: str = "https://api.minimaxi.chat/v1"
    APP_PORT: int = 8010

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
