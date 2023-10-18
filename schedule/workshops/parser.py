import datetime
import re
from typing import Optional

import pandas as pd

from schedule.processors.regex import symbol_translation
from schedule.workshops.config import bootcamp_config as config
from schedule.workshops.models import WorkshopEvent


class HashableDict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


class WorkshopParser:
    df: pd.DataFrame

    def __init__(self):
        with config.SPREADSHEET_PATH.open("rb") as f:
            self.df = pd.read_excel(f, engine="openpyxl", header=0)
        # self.df.iloc[:, 0].ffil(, axis=0, inplace=True)
        # self.df.set_index(self.df.columns[0], inplace=True)

        WorkshopParser.process_header_with_dates(self.df)
        self.df = WorkshopParser.remove_trailing_spaces(self.df)

    def get_events(self) -> list[WorkshopEvent]:
        events = []
        for _, event_cells in self.df.iterrows():
            for date, value in event_cells.items():
                cell_event = WorkshopParser.process_event_cell(value)
                if cell_event is None:
                    continue
                start_time = cell_event.timeslots[0][0]
                end_time = cell_event.timeslots[-1][1]
                cell_event.set_datetime(start_time, end_time, date)
                events.append(cell_event)
        return events

    @staticmethod
    def remove_trailing_spaces(df: pd.DataFrame):
        return df.map(
            lambda x: x.strip() if isinstance(x, str) else x,
        )

    @staticmethod
    def process_header_with_dates(df: pd.DataFrame):
        new_columns = dict()
        for column in df.columns:
            if not pd.isna(column):
                new_columns[column] = WorkshopParser.strptime_for_header(column)
        df.rename(columns=new_columns, errors="raise", inplace=True)

    @staticmethod
    def strptime_for_header(date_text: str) -> datetime.date:
        # Friday18.08 -> 18.08
        date_text = date_text[-5:]
        # 16.08, 18.08
        month_and_day = datetime.datetime.strptime(date_text, "%d.%m")
        return datetime.date(
            year=config.YEAR_OF_BOOTCAMP,
            month=month_and_day.month,
            day=month_and_day.day,
        )

    @staticmethod
    def process_event_cell(event_cell: str) -> Optional[WorkshopEvent]:
        if pd.isna(event_cell):
            return None
        # replace multiple \n with one \n to avoid empty lines using regex
        event_cell = re.sub(r"\n+", "\n", event_cell)
        # replace cyrillic symbols with their unicode analogs
        event_cell = event_cell.translate(symbol_translation)
        splitter = event_cell.splitlines()
        splitter = list(map(str.strip, splitter))
        new_splitter = []
        timeslots = []
        # find time
        for line in splitter:
            timeslot = WorkshopParser.process_timeslot(line)
            if timeslot is not None:
                timeslots.append(timeslot)
            else:
                new_splitter.append(line)

        splitter = new_splitter

        match splitter:
            case summary, *comments, speaker, location, capacity:
                return WorkshopEvent(
                    summary=summary,
                    comments=comments,
                    speaker=speaker,
                    location=location,
                    capacity=capacity,
                    timeslots=timeslots,
                )
            case _:
                raise ValueError()

    @staticmethod
    def process_timeslot(
        time_text: str,
    ) -> Optional[tuple[datetime.time, datetime.time]]:
        # 15:00 - 16:00
        # OR
        # 1) 15:00 - 16:00
        # 2) 16:00 - 17:00
        match = re.search(r"(\d{1,2}:\d{2}) - (\d{1,2}:\d{2})", time_text)
        if match:
            start_text, end_text = match.groups()
        else:
            return None
        start_time = datetime.datetime.strptime(start_text, "%H:%M").time()
        end_time = datetime.datetime.strptime(end_text, "%H:%M").time()
        return start_time, end_time
