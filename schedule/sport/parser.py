import datetime
from pprint import pprint
from typing import Optional

import requests
from pydantic import BaseModel

from schedule.sport.config import sports_config as config


class ResponseSports(BaseModel):
    class Sports(BaseModel):
        id: int
        name: str
        special: bool

    sports: list[Sports]


class ResponseSportSchedule(BaseModel):
    class SportScheduleEvent(BaseModel):
        class ExtendedProps(BaseModel):
            group_id: int
            training_class: str
            current_load: int
            capacity: int

        title: Optional[str]
        daysOfWeek: list[int]
        startTime: str
        endTime: str
        extendedProps: ExtendedProps

    __root__: list[SportScheduleEvent]


def get_sports(session: requests.Session) -> ResponseSports:
    r = session.get(f'{config.api_url}/sports')
    response_schema = ResponseSports.parse_raw(r.text, content_type="application/json")
    return response_schema


def get_sport_schedule(sport_id: int, session: requests.Session):
    dateTime = datetime.date
    finalDate = '2099-12-30'  # may be have to be set up in config
    url = f'{config.api_url}/calendar/{sport_id}/schedule?start={dateTime.today()}T00%3A00&end={finalDate}T00%3A00'
    r = session.get(url)
    response_schema = ResponseSportSchedule.parse_raw(r.text, content_type="application/json")
    return response_schema


if __name__ == '__main__':
    session = requests.Session()
    session.headers.update({'Content-Type': 'application/json'})
    session.headers.update({'Authorization': f'Bearer {config.token}'})
    sports_id = (get_sports(session))
    pprint(sports_id.sports)

    for sport in sports_id.sports:
        schedule = get_sport_schedule(sport.id, session)
        pprint(schedule)
