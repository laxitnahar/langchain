from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    shopify_api_version: str = Field(..., alias="SHOPIFY_API_VERSION")
    shopify_access_token: str = Field(..., alias="SHOPIFY_ACCESS_TOKEN")

    pg_host: str = Field(..., alias="PG_HOST")
    pg_port: int = Field(..., alias="PG_PORT")
    pg_database: str = Field(..., alias="PG_DATABASE")
    pg_user: str = Field(..., alias="PG_USER")
    pg_password: str = Field(..., alias="PG_PASSWORD")
    pg_ssl: bool = Field(False, alias="PG_SSL")

    cors_origins: str = Field(
        "http://127.0.0.1:5173,http://localhost:5173",
        alias="CORS_ORIGINS",
    )
    request_timeout_seconds: float = Field(30.0, alias="REQUEST_TIMEOUT_SECONDS")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    @property
    def postgres_dsn(self) -> str:
        sslmode = "require" if self.pg_ssl else "disable"
        return (
            f"host={self.pg_host} "
            f"port={self.pg_port} "
            f"dbname={self.pg_database} "
            f"user={self.pg_user} "
            f"password={self.pg_password} "
            f"sslmode={sslmode}"
        )

    @property
    def allowed_origins(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

