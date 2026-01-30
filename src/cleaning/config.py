import datetime

from pydantic import BaseModel


class LinenChangeEntry(BaseModel):
    name: str = "Linen change"
    location: str
    rrule: dict[str, str]


class CleaningParserConfig(BaseModel):
    start_date: datetime.date
    cleaning_spreadsheet_url: str
    cleaning_spreadsheet_id: str
    linen_change_entries: list[LinenChangeEntry]
