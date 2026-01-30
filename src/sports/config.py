import datetime

from pydantic import BaseModel, ConfigDict


class SportsParserConfig(BaseModel):
    start_of_semester: datetime.date
    end_of_semester: datetime.date

    website_url: str = "https://sport.innopolis.university"
    api_url: str = "https://sport.innopolis.university/api"
    model_config = ConfigDict(validate_assignment=True)
