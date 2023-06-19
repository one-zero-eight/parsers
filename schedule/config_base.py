from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, validator, Field

from utils import get_project_root

PROJECT_ROOT = get_project_root()


class DayOfWeek(StrEnum):
    MONDAY = "MONDAY"
    TUESDAY = "TUESDAY"
    WEDNESDAY = "WEDNESDAY"
    THURSDAY = "THURSDAY"
    FRIDAY = "FRIDAY"
    SATURDAY = "SATURDAY"
    SUNDAY = "SUNDAY"


class CSS3Color(StrEnum):
    ALICE_BLUE = "aliceblue"
    ANTIQUE_WHITE = "antiquewhite"
    AQUA = "aqua"
    AQUAMARINE = "aquamarine"
    AZURE = "azure"
    BEIGE = "beige"
    BISQUE = "bisque"
    BLACK = "black"
    BLANCHED_ALMOND = "blanchedalmond"
    BLUE = "blue"
    BLUE_VIOLET = "blueviolet"
    BROWN = "brown"
    BURLY_WOOD = "burlywood"
    CADET_BLUE = "cadetblue"
    CHARTREUSE = "chartreuse"
    CHOCOLATE = "chocolate"
    CORAL = "coral"
    CORNFLOWER_BLUE = "cornflowerblue"
    CORNSILK = "cornsilk"
    CRIMSON = "crimson"
    CYAN = "cyan"
    DARK_BLUE = "darkblue"
    DARK_CYAN = "darkcyan"
    DARK_GOLDENROD = "darkgoldenrod"
    DARK_GRAY = "darkgray"
    DARK_GREEN = "darkgreen"
    DARK_KHAKI = "darkkhaki"
    DARK_MAGENTA = "darkmagenta"
    DARK_OLIVE_GREEN = "darkolivegreen"
    DARK_ORANGE = "darkorange"
    DARK_ORCHID = "darkorchid"
    DARK_RED = "darkred"
    DARK_SALMON = "darksalmon"
    DARK_SEA_GREEN = "darkseagreen"
    DARK_SLATE_BLUE = "darkslateblue"
    DARK_SLATE_GRAY = "darkslategray"
    DARK_TURQUOISE = "darkturquoise"
    DARK_VIOLET = "darkviolet"
    DEEP_PINK = "deeppink"
    DEEP_SKY_BLUE = "deepskyblue"
    DIM_GRAY = "dimgray"
    DODGER_BLUE = "dodgerblue"
    FIREBRICK = "firebrick"
    FLORAL_WHITE = "floralwhite"
    FOREST_GREEN = "forestgreen"
    FUCHSIA = "fuchsia"
    GAINSBORO = "gainsboro"
    GHOST_WHITE = "ghostwhite"
    GOLD = "gold"
    GOLDENROD = "goldenrod"
    GRAY = "gray"
    GREEN = "green"
    GREEN_YELLOW = "greenyellow"
    HONEYDEW = "honeydew"
    HOT_PINK = "hotpink"
    INDIAN_RED = "indianred"
    INDIGO = "indigo"
    IVORY = "ivory"
    KHAKI = "khaki"
    LAVENDER = "lavender"
    LAVENDER_BLUSH = "lavenderblush"
    LAWN_GREEN = "lawngreen"
    LEMON_CHIFFON = "lemonchiffon"
    LIGHT_BLUE = "lightblue"
    LIGHT_CORAL = "lightcoral"
    LIGHT_CYAN = "lightcyan"
    LIGHT_GOLDENROD_YELLOW = "lightgoldenrodyellow"
    LIGHT_GRAY = "lightgray"
    LIGHT_GREEN = "lightgreen"
    LIGHT_PINK = "lightpink"
    LIGHT_SALMON = "lightsalmon"
    LIGHT_SEA_GREEN = "lightseagreen"
    LIGHT_SKY_BLUE = "lightskyblue"

    @classmethod
    def __len__(cls):
        return len(cls.__members__)

    @classmethod
    def get_by_index(cls, idx: int):
        return list(cls.__members__.values())[idx]


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

    @validator(
        "CREDENTIALS_PATH", "TOKEN_PATH", "SAVE_ICS_PATH", "SAVE_JSON_PATH",
        pre=True, always=True)
    def relative_path(cls, v):
        """If not absolute path, then with respect to the main directory"""
        v = Path(v)
        if not v.is_absolute():
            v = PROJECT_ROOT / v
        return v

    @validator("CREDENTIALS_PATH", always=True)
    def file_exists(cls, v: Path):
        """Check if the file exists"""
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
