from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, validator

from schedule.utils import get_project_root

PROJECT_ROOT = get_project_root()


class DayOfWeek(StrEnum):
    MONDAY = "MONDAY"
    TUESDAY = "TUESDAY"
    WEDNESDAY = "WEDNESDAY"
    THURSDAY = "THURSDAY"
    FRIDAY = "FRIDAY"
    SATURDAY = "SATURDAY"
    SUNDAY = "SUNDAY"


class VDayOfWeek(StrEnum):
    MONDAY = "MO"
    TUESDAY = "TU"
    WEDNESDAY = "WE"
    THURSDAY = "TH"
    FRIDAY = "FR"
    SATURDAY = "SA"
    SUNDAY = "SU"

    @classmethod
    def __len__(cls):
        return 7

    @classmethod
    def get_by_index(cls, idx: int):
        return list(cls.__members__.values())[idx]


class CSS3Color(StrEnum):
    BROWN = "brown"
    CADETBLUE = "cadetblue"
    CHOCOLATE = "chocolate"
    DARK_CYAN = "darkcyan"
    DARK_GREEN = "darkgreen"
    DARK_MAGENTA = "darkmagenta"
    DARK_OLIVE_GREEN = "darkolivegreen"
    DARK_RED = "darkred"
    DARK_SLATE_BLUE = "darkslateblue"
    DARK_SLATE_GRAY = "darkslategray"
    DIM_GRAY = "dimgray"
    FIREBRICK = "firebrick"
    FOREST_GREEN = "forestgreen"
    GRAY = "gray"
    INDIAN_RED = "indianred"
    LIGHT_SLATE_GRAY = "lightslategray"
    MAROON = "maroon"
    MEDIUM_VIOLET_RED = "mediumvioletred"
    MIDNIGHT_BLUE = "midnightblue"
    INDIGO = "indigo"
    REBECCA_PURPLE = "rebeccapurple"
    SEA_GREEN = "seagreen"
    TEAL = "teal"

    @classmethod
    def __len__(cls):
        return len(cls.__members__)

    @classmethod
    def get_by_index(cls, idx: int):
        return list(cls.__members__.values())[idx]


class VeryBaseParserConfig(BaseModel):
    MOUNT_POINT: Path = PROJECT_ROOT / "output"
    """Mount point for output files"""
    SAVE_ICS_PATH: Path
    """Path to directory to save .ics files relative to MOUNT_POINT"""
    SAVE_JSON_PATH: Path
    """Path to save .json file"""

    @validator("SAVE_JSON_PATH", "SAVE_ICS_PATH", pre=True, always=True)
    def relative_path_ics(cls, v, values):
        """If not absolute path, then with respect to the main directory"""
        v = Path(v)
        if not v.is_absolute():
            v = values["MOUNT_POINT"] / v

        # if not children of mount point, then raise error
        if not v.is_relative_to(values["MOUNT_POINT"]):
            raise ValueError(
                f"SAVE_ICS_PATH must be children of MOUNT_POINT, but got {v}"
            )
        return v

    @validator("SAVE_JSON_PATH", pre=False, always=True)
    def create_parent_dir(cls, v, values):
        """Create parent directory if not exists"""
        v = Path(v)
        v.parent.mkdir(parents=True, exist_ok=True)
        return v

    @validator("SAVE_ICS_PATH", pre=False, always=True)
    def create_dir(cls, v, values):
        """Create directory if not exists"""
        v = Path(v)
        v.mkdir(parents=True, exist_ok=True)
        return v


class GoogleSpreadsheetConfig(VeryBaseParserConfig):
    """
    Base config for parsers
    """

    CREDENTIALS_PATH: Path = "credentials.json"
    """Path to credentials.json file for Google Sheets API"""
    TOKEN_PATH: Path = "token.json"
    """Path to token.json file for Google Sheets API"""

    API_SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    """API scopes for Google Sheets API"""
    TIMEZONE_DELTA = "+03:00"
    """Timezone delta for events"""

    @validator(
        "CREDENTIALS_PATH",
        "TOKEN_PATH",
        "SAVE_JSON_PATH",
        pre=True,
        always=True,
    )
    def relative_path(cls, v):
        """If not absolute path, then with respect to the main directory"""
        v = Path(v)
        if not v.is_absolute():
            v = PROJECT_ROOT / v
        return v


__all__ = [
    "GoogleSpreadsheetConfig",
    "VeryBaseParserConfig",
    "CSS3Color",
    "DayOfWeek",
]

if __name__ == "__main__":
    cfg = GoogleSpreadsheetConfig(
        SAVE_ICS_PATH=Path(""),
        SAVE_JSON_PATH=Path(""),
    )
    print(cfg.CREDENTIALS_PATH.absolute())
