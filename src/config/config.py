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
    snowflake_user: str | None
    snowflake_password: str | None
    snowflake_account: str | None
    snowflake_warehouse: str | None
    snowflake_database: str | None
    snowflake_role: str
    tolerance_amount: float
    log_level: str


def get_config() -> Config:
    # construct a fresh Config instance each time so changes to environment
    # variables (e.g. via testing monkeypatches) are respected.  Previously the
    # dataclass defaults were evaluated at import time, meaning dynamic updates
    # to ``os.environ`` were ignored.
    return Config(
        snowflake_user=os.getenv("SNOWFLAKE_USER"),
        snowflake_password=os.getenv("SNOWFLAKE_PASSWORD"),
        snowflake_account=os.getenv("SNOWFLAKE_ACCOUNT"),
        snowflake_warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        snowflake_database=os.getenv("SNOWFLAKE_DATABASE"),
        snowflake_role=os.getenv("SNOWFLAKE_ROLE", "etl_service_role"),
        tolerance_amount=float(os.getenv("TOLERANCE_AMOUNT", "5.0")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


def has_snowflake_credentials(cfg: Config = None) -> bool:
    """Return True if minimal Snowflake credentials appear to be configured."""
    if cfg is None:
        cfg = get_config()
    return bool(
        cfg.snowflake_account and cfg.snowflake_user and cfg.snowflake_password
    )
