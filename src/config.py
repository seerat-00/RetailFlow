"""Configuration management and logging setup for RetailFlow."""

import os
from pathlib import Path
from dataclasses import dataclass
import logging
from datetime import datetime
from dotenv import load_dotenv

# Base paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load environment variables from .env file
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    """Application settings loaded from environment variables."""

    # Database Settings
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "retailflow_db")
    db_user: str = os.getenv("DB_USER", "chehak")
    db_password: str = os.getenv("DB_PASSWORD", "")
    db_schema: str = os.getenv("DB_SCHEMA", "retailflow")

    # Paths
    raw_data_path: Path = PROJECT_ROOT / os.getenv("DATA_RAW_PATH", "data/raw/online_retail_II.csv")
    processed_data_dir: Path = PROJECT_ROOT / os.getenv("DATA_PROCESSED_PATH", "data/processed")
    logs_dir: Path = PROJECT_ROOT / os.getenv("LOG_DIR", "logs")

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    def get_db_url(self) -> str:
        """Construct PostgreSQL connection URL for SQLAlchemy and Psycopg3."""
        return (
            f"postgresql+psycopg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def __repr__(self) -> str:
        """Safe representation masking password."""
        return (
            f"Settings(db_host='{self.db_host}', db_port={self.db_port}, "
            f"db_name='{self.db_name}', db_user='{self.db_user}', "
            f"db_schema='{self.db_schema}', db_password='***MASKED***', "
            f"raw_data_path='{self.raw_data_path}', processed_data_dir='{self.processed_data_dir}')"
        )


settings = Settings()


def setup_logging(level: str = None) -> logging.Logger:
    """Configure console and file logging."""
    log_level_name = level or settings.log_level
    numeric_level = getattr(logging, log_level_name.upper(), logging.INFO)

    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d")
    log_file = settings.logs_dir / f"retailflow_{today_str}.log"

    logger = logging.getLogger("retailflow")
    logger.setLevel(numeric_level)

    # Avoid duplicate handlers if setup_logging is called multiple times
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # File Handler
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
