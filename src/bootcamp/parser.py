import datetime
from itertools import groupby

import pandas as pd

from src.bootcamp.config import bootcamp_config as config
from src.bootcamp.models import BootcampEvent


class HashableDict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


class BootcampParser:
    df: pd.DataFrame

    def __init__(self):
        with config.SPREADSHEET_PATH.open("rb") as f:
            self.df = pd.read_excel(f, engine="openpyxl", header=0, index_col=[0])
        # self.df.iloc[:, 0].ffil(, axis=0, inplace=True)
        # self.df.set_index(self.df.columns[0], inplace=True)
        BootcampParser.process_header_with_dates(self.df)
        BootcampParser.process_index_with_time(self.df)
        self.df = BootcampParser.remove_trailing_spaces(self.df)

    def get_events(self):
        events = []
        for (start_time, end_time), event_cells in self.df.iterrows():
            for date, value in event_cells.items():
                cell_events = BootcampParser.process_event_cell(value)
                for event in cell_events:
                    event.set_datetime(start_time, end_time, date)
                    events.append(event)
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
                new_columns[column] = BootcampParser.strptime_for_header(column)
        df.rename(columns=new_columns, errors="raise", inplace=True)

    @staticmethod
    def process_index_with_time(df: pd.DataFrame):
        new_index = dict()
        for index in df.index:
            if not pd.isna(index):
                new_index[index] = BootcampParser.strptime_for_index(index)
        df.rename(index=new_index, errors="raise", inplace=True)

    @staticmethod
    def strptime_for_header(date_text: str) -> datetime.date:
        date_text = date_text.splitlines()[0]
        month_and_day = datetime.datetime.strptime(date_text, "%B, %d")
        return datetime.date(
            year=config.YEAR_OF_BOOTCAMP,
            month=month_and_day.month,
            day=month_and_day.day,
        )

    @staticmethod
    def strptime_for_index(time_text: str) -> tuple[datetime.time, datetime.time]:
        start_text, end_text = time_text.split("-")
        start_time = datetime.datetime.strptime(start_text, "%H:%M").time()
        end_time = datetime.datetime.strptime(end_text, "%H:%M").time()
        return start_time, end_time

    @staticmethod
    def process_event_cell(event_cell: str) -> list[BootcampEvent]:
        if pd.isna(event_cell):
            return []

        splitter = event_cell.splitlines()
        splitter = list(map(str.strip, splitter))
        events = []
        match splitter:
            case ["X"]:
                pass
            case [only_one_line]:
                events.append(BootcampEvent(summary=only_one_line))
            case "English Practice" | "Skills Lab" as lesson_type, *lesson_lines:
                is_empty = lambda elem: elem == ""
                lessons = [list(grouper) for empty, grouper in groupby(lesson_lines, is_empty) if not empty]

                for lesson in lessons:
                    academic_group, location = lesson[0].split(", ")
                    teacher = lesson[1] if len(lesson) == 2 else None
                    description = f"{academic_group}\n{teacher}" if teacher else academic_group
                    events.append(
                        BootcampEvent(
                            summary=lesson_type,
                            description=description,
                            group=academic_group,
                            location=location,
                        )
                    )
            case "Lecture:", description, teacher, location:
                events.append(BootcampEvent(summary=description, description=f"{teacher}", location=location))
            case "Final Test", subject:
                events.append(BootcampEvent(summary=f"Final Test: {subject}"))
            case "Workshops /", "English speaking test":
                events.append(BootcampEvent(summary="Workshops OR English speaking test"))
            case _:
                raise ValueError()
        return events
