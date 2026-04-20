from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SNOWFLAKE_ACCOUNT: str = ""
    SNOWFLAKE_USER: str = ""
    SNOWFLAKE_PASSWORD: str = ""
    SNOWFLAKE_ROLE: str = "ACCOUNTADMIN"
    SNOWFLAKE_WAREHOUSE: str = "COMPUTE_WH"
    SNOWFLAKE_DATABASE: str = "ECOMM_DATA_LAKE"
    SNOWFLAKE_SCHEMA: str = "CONFORMED"
    LLM_MODEL: str = "gpt-4o-mini"
    LLM_API_KEY: str = ""
    APP_PORT: int = 8010

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
