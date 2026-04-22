from __future__ import annotations

import getpass
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    source_name: str = "sonxeber.az"
    base_url: str = "https://sonxeber.az"
    default_image_url: str = "https://sonxeber.az/images/fbcover.jpg"
    postgres_host: str = os.getenv("SONXEBER_PGHOST", "127.0.0.1")
    postgres_port: int = int(os.getenv("SONXEBER_PGPORT", "5432"))
    postgres_user: str = os.getenv("SONXEBER_PGUSER", getpass.getuser())
    postgres_password: str = os.getenv("SONXEBER_PGPASSWORD", "")
    postgres_dbname: str = os.getenv("SONXEBER_PGDATABASE", "sonxeber_scraper")
    postgres_admin_dbname: str = os.getenv("SONXEBER_PGADMIN_DATABASE", "postgres")
    postgres_timezone: str = os.getenv("SONXEBER_PGTIMEZONE", "Asia/Baku")
    pid_path: Path = Path(os.getenv("SONXEBER_PID_PATH", "data/sonxeber.pid"))
    log_path: Path = Path(os.getenv("SONXEBER_LOG_PATH", "data/sonxeber.log"))
    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    request_timeout_seconds: int = int(os.getenv("SONXEBER_REQUEST_TIMEOUT_SECONDS", "20"))
    poll_interval_seconds: int = int(os.getenv("SONXEBER_POLL_INTERVAL_SECONDS", "60"))
    listing_page_count: int = int(os.getenv("SONXEBER_LISTING_PAGE_COUNT", "5"))
    reconcile_page_count: int = int(os.getenv("SONXEBER_RECONCILE_PAGE_COUNT", "10"))
    reconcile_every_cycles: int = int(os.getenv("SONXEBER_RECONCILE_EVERY_CYCLES", "10"))
    forward_probe_window: int = int(os.getenv("SONXEBER_FORWARD_PROBE_WINDOW", "20"))
    user_agent: str = os.getenv(
        "SONXEBER_USER_AGENT",
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
    )

    def ensure_paths(self) -> None:
        self.pid_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def postgres_connect_kwargs(self, *, admin: bool = False) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "user": self.postgres_user,
            "dbname": self.postgres_admin_dbname if admin else self.postgres_dbname,
        }
        if self.postgres_password:
            kwargs["password"] = self.postgres_password
        return kwargs

    def database_display_name(self) -> str:
        return (
            f"postgresql://{self.postgres_user}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.postgres_dbname}"
        )
