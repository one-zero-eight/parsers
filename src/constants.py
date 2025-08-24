from enum import StrEnum

WEEKDAYS = [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
]

ICS_WEEKDAYS = [
    "MO",
    "TU",
    "WE",
    "TH",
    "FR",
    "SA",
    "SU",
]


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
