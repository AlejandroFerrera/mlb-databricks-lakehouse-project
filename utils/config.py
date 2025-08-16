import os
from dataclasses import dataclass

@dataclass
class Config:
    """
    Configuration class for application settings.
    """

    # API settings
    MLB_STATS_API_BASE_URL: str = os.getenv(
        "MLB_STATS_API_BASE_URL", "https://statsapi.mlb.com"
    )

    API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "30"))

    # Application settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()
