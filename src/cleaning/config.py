import datetime

from pydantic import BaseModel, model_validator


class LinenChangeEntry(BaseModel):
    name: str = "Linen change"
    location: str
    rrule: dict[str, str | int] | None = None
    dates: list[datetime.date] | None = None
    description: str

    @model_validator(mode="after")
    def validate_schedule_source(self):
        if self.rrule is None and self.dates is None:
            raise ValueError("Either rrule or dates must be set for linen change entry")
        if self.rrule is not None and self.dates is not None:
            raise ValueError("Only one of rrule or dates can be set for linen change entry")
        return self


class CleaningParserConfig(BaseModel):
    start_date: datetime.date
    cleaning_spreadsheet_url: str
    cleaning_spreadsheet_id: str
    linen_change_entries: list[LinenChangeEntry]
