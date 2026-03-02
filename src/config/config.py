"""Configuration loader for environment parameters."""
import os
from dataclasses import dataclass
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # If python-dotenv is not installed, environment variables must be provided by the runtime.
    pass


@dataclass
class Config:
    snowflake_user: str = os.getenv("SNOWFLAKE_USER")
    snowflake_password: str = os.getenv("SNOWFLAKE_PASSWORD")
    snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE")
    snowflake_role: str = os.getenv("SNOWFLAKE_ROLE", "etl_service_role")
    tolerance_amount: float = float(os.getenv("TOLERANCE_AMOUNT", "5.0"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


def get_config() -> Config:
    cfg = Config()
    # Basic validation
    # no validation here; other modules can decide what to do when missing
    return cfg


def has_snowflake_credentials(cfg: Config = None) -> bool:
    """Return True if minimal Snowflake credentials appear to be configured."""
    if cfg is None:
        cfg = get_config()
    return bool(
        cfg.snowflake_account and cfg.snowflake_user and cfg.snowflake_password
    )
