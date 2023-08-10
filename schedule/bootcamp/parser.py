import datetime
import json
from itertools import groupby

import icalendar
import pandas as pd

from schedule.bootcamp.config import bootcamp_config as config
from schedule.bootcamp.models import BootcampEvent


class BootcampParser:
    df: pd.DataFrame

    def __init__(self):
        with config.SPREADSHEET_PATH.open("rb") as f:
            self.df = pd.read_excel(f, engine="openpyxl", header=0, index_col=[0])
        # self.df.iloc[:, 0].fillna(method="ffill", axis=0, inplace=True)
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
        return df.applymap(
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
                lessons = [
                    list(grouper)
                    for empty, grouper in groupby(lesson_lines, is_empty)
                    if not empty
                ]

                for lesson in lessons:
                    academic_group, location = lesson[0].split(", ")
                    teacher = lesson[1] if len(lesson) == 2 else None
                    events.append(
                        BootcampEvent(
                            summary=lesson_type,
                            description=f"{academic_group}\n{teacher}",
                            group=academic_group,
                            location=location,
                        )
                    )
            case "Lecture:", description, teacher, location:
                events.append(
                    BootcampEvent(
                        summary=description, description=f"{teacher}", location=location
                    )
                )
            case "Final Test", subject:
                events.append(BootcampEvent(summary=f"Final Test: {subject}"))
            case "Workshops /", "English speaking test":
                events.append(
                    BootcampEvent(summary="Workshops OR English speaking test")
                )
            case _:
                raise ValueError()
        return events


if __name__ == "__main__":
    parser = BootcampParser()
    events = parser.get_events()
    common_events = list(filter(lambda e: e.group is None, events))
    specific_events = list(filter(lambda e: e.group is not None, events))
    specific_events.sort(key=lambda e: e.group)
    directory = config.SAVE_ICS_PATH
    json_file = config.SAVE_JSON_PATH
    json_data = {"calendars": []}
    year_path = directory / str(config.YEAR_OF_BOOTCAMP)
    bootcamp_alias = f"bootcamp{config.YEAR_OF_BOOTCAMP}"
    for group_name, grouper in groupby(specific_events, key=lambda e: e.group):
        group_events = list(grouper) + common_events
        group_calendar = icalendar.Calendar(
            prodid="-//one-zero-eight//InNoHassle Schedule",
            version="2.0",
            method="PUBLISH",
        )

        group_calendar["x-wr-calname"] = f"Bootcamp 2023 {group_name}"
        group_calendar["x-wr-timezone"] = config.TIMEZONE
        group_calendar["x-wr-caldesc"] = "Generated by InNoHassle Schedule"

        for group_event in group_events:
            group_event: BootcampEvent
            group_calendar.add_component(group_event.get_vevent())
        group_slug = group_name.lower().replace(" ", "")
        file_name = f"{group_slug}.ics"
        file_path = year_path / file_name
        json_data["calendars"].append(
            {
                "path": file_path.relative_to(json_file.parent).as_posix(),
                "tags": [
                    {"alias": bootcamp_alias, "type": "category"},
                    {"alias": group_slug, "type": bootcamp_alias},
                ],
                "alias": f"{bootcamp_alias}-{group_slug}",
            }
        )

        with open(file_path, "wb") as f:
            f.write(group_calendar.to_ical())
        # create a new .json file with information about calendars
    with open(json_file, "w") as f:
        json.dump(json_data, f, indent=4, sort_keys=True)
