from pathlib import Path
from pydantic import BaseModel, validator, Field
from enum import Enum
from utils import get_project_root

PROJECT_ROOT = get_project_root()


class DayOfWeek(str, Enum):
    MONDAY = "MONDAY"
    TUESDAY = "TUESDAY"
    WEDNESDAY = "WEDNESDAY"
    THURSDAY = "THURSDAY"
    FRIDAY = "FRIDAY"
    SATURDAY = "SATURDAY"
    SUNDAY = "SUNDAY"


class BaseParserConfig(BaseModel):
    """
    Base config for parsers
    """

    SPREADSHEET_ID: str | None
    """Spreadsheet ID from Google Sheets URL"""
    TARGET_RANGES: list[str] = Field(default_factory=list)
    """Target ranges from spreadsheet"""
    TARGET_SHEET_TITLES: list[str] = Field(default_factory=list)
    """Target sheet titles from spreadsheet"""
    SAVE_ICS_PATH: Path
    """Path to directory to save .ics files"""
    SAVE_JSON_PATH: Path
    """Path to save .json file"""

    CREDENTIALS_PATH: Path = "credentials.json"
    """Path to credentials.json file for Google Sheets API"""
    TOKEN_PATH: Path = "token.json"
    """Path to token.json file for Google Sheets API"""

    API_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    """API scopes for Google Sheets API"""
    TIMEZONE = "Europe/Moscow"
    """Timezone for events"""
    TIMEZONE_DELTA = "+03:00"
    """Timezone delta for events"""

    DAYS = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ]
    """Days of week"""

    @validator(
        "CREDENTIALS_PATH", "TOKEN_PATH", "SAVE_ICS_PATH", "SAVE_JSON_PATH",
        pre=True, always=True)
    def relative_path(cls, v):
        """If not absolute path, then with respect to main directory"""
        v = Path(v)
        if not v.is_absolute():
            v = PROJECT_ROOT / v
        return v

    @validator("CREDENTIALS_PATH",
               "TOKEN_PATH", always=True)
    def file_exists(cls, v: Path):
        """Check if file exists"""
        if not v.exists():
            raise ValueError(f"File {v.absolute()} does not exist")
        return v


__all__ = [
    "BaseParserConfig",
]

if __name__ == "__main__":
    cfg = BaseParserConfig(
        SAVE_ICS_PATH=Path(""),
        SAVE_JSON_PATH=Path(""),
    )
    print(cfg.CREDENTIALS_PATH.absolute())
