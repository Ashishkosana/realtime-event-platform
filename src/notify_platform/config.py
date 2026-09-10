from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://workflow:workflow@127.0.0.1:5432/notify"
    api_host: str = "0.0.0.0"
    api_port: int = 43200
    demo_mode: bool = False
    log_level: str = "INFO"
    fanout_chunk: int = 200
    replay_cap: int = 500
    poll_interval_seconds: float = 0.15
    lease_ttl_seconds: float = 15.0
    webhook_max_attempts: int = 5
    wake_mode: str = "listen"
    max_body_bytes: int = 200_000
    worker_concurrency: int = 2
    base_backoff_seconds: float = 0.4
    max_backoff_seconds: float = 30.0

    @property
    def lease_ttl_sql(self) -> str:
        return f"{self.lease_ttl_seconds} seconds"


def load_settings() -> Settings:
    return Settings()
