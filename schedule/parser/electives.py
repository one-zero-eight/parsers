import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from itertools import pairwise, groupby
from pathlib import Path
from typing import Optional, Collection
from zlib import crc32

import googleapiclient.discovery
import icalendar
import numpy as np
import pandas as pd
from google.oauth2.credentials import Credentials
from pydantic import BaseModel

from parser.config import PARSER_PATH, electives_config as config, Elective
from parser.utils import *

BRACKETS_PATTERN = re.compile(r"\((.*?)\)")


class ElectiveEvent(BaseModel):
    elective: Elective
    start: datetime
    end: datetime
    location: Optional[str]
    description: Optional[str]
    event_type: Optional[str]
    group: Optional[str] = None

    def __hash__(self):
        string_to_hash = str(
            (
                self.elective.alias,
                self.start.isoformat(),
                self.end.isoformat(),
                self.location,
                self.event_type,
                self.group,
            )
        )

        return crc32(string_to_hash.encode("utf-8"))

    def get_uid(self) -> str:
        return "%x@innohassle.ru" % abs(hash(self))

    @property
    def description(self: "ElectiveEvent") -> str:
        """
        Description of the event

        :return: description of the event
        :rtype: str
        """

        r = {
            "Location": self.location,
            "Instructor": self.elective.instructor,
            "Type": self.event_type,
            "Group": self.group,
            "Subject": self.elective.name,
            "Time": f"{self.start.strftime('%H:%M')} - {self.end.strftime('%H:%M')} {self.start.strftime('%d.%m.%Y')}",
        }

        r = {k: v for k, v in r.items() if v}
        return "\n".join([f"{k}: {v}" for k, v in r.items()])

    def get_vevent(self) -> icalendar.Event:
        vevent = icalendar.Event()
        vevent["summary"] = self.elective.name
        if self.event_type is not None:
            vevent["summary"] += f" ({self.event_type})"
        vevent["dtstart"] = self.start.strftime("%Y%m%dT%H%M%S")
        vevent["dtend"] = self.end.strftime("%Y%m%dT%H%M%S")
        vevent["uid"] = self.get_uid()
        vevent["categories"] = self.elective.name
        vevent["description"] = self.description

        if self.location is not None:
            vevent["location"] = self.location

        return vevent


class ElectiveParser:
    spreadsheets: googleapiclient.discovery.Resource
    credentials: Credentials
    logger = logging.getLogger(__name__ + "." + "Parser")

    def __init__(self):
        self.credentials = get_credentials(
            Path(config.CREDENTIALS_PATH),
            PARSER_PATH / "token.json",
            scopes=config.API_SCOPES,
        )
        self.spreadsheets = connect_spreadsheets(self.credentials)

    def get_clear_df(
        self, spreadsheet_id: str, target_range: str, target_title: str
    ) -> pd.DataFrame:
        """Get data from Google Sheets and return it as a DataFrame with merged
        cells and empty cells in the course row filled by left value."""

        self.logger.debug("Getting dataframe from Google Sheets...")
        self.logger.info(
            f"Retrieving data: {spreadsheet_id}/{target_title}-{target_range}"
        )

        values = (
            self.spreadsheets.values()
            .get(spreadsheetId=spreadsheet_id, range=target_range)  # type : ignore
            .execute()["values"]
        )

        df = pd.DataFrame(data=values)
        # strip all values
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        max_x, max_y = df.shape

        self.logger.info(f"Data retrieved: {max_x}x{max_y}")
        spreadsheet = self.spreadsheets.get(
            spreadsheetId=config.SPREADSHEET_ID,
            ranges=[target_range],
            includeGridData=False,  # values already fetched
        ).execute()
        self.logger.info(
            f"Spreadsheet {spreadsheet['properties']['title']} retrieved:"
            + f"Sheets({len(spreadsheet['sheets'])}): "
            + f"> {sheet['properties']['title']}"
            for sheet in spreadsheet["sheets"]
        )

        # get target sheet
        target_sheet = None
        for sheet in spreadsheet["sheets"]:
            if sheet["properties"]["title"] == target_title:
                target_sheet = sheet
                break

        if target_sheet is None:
            raise ValueError(f"Target sheet {target_title} not found")

        self.logger.info(
            f"Target sheet: {target_sheet['properties']['title']}"
            + f"> Sheet index: ({target_sheet['properties']['index']})"
        )

        df.fillna("", inplace=True)

        self.logger.info("Cells merged")
        self.logger.info("Dataframe ready")
        return df

    @staticmethod
    def parse_week_df(
        df: pd.DataFrame, electives: Collection[Elective]
    ) -> list[ElectiveEvent]:
        # for each cell in day column
        def process_cell(cell: str):
            #           "BDLD(lec) 312" ->
            #           {"ele"BDLD", "lec", "312"
            #           "PP(lab/group2)303" -> "PP", "lab/group2", "303"

            result: list[dict] = []

            if not cell:
                return result

            occurences = cell.strip().split("\n")

            for line in occurences:
                dct = {}
                # find event_type in brackets "BDLD(lec) 312"
                r = re.search(BRACKETS_PATTERN, line)
                if r:
                    event_type = r.group(1)
                    event_type = event_type.replace(" ", "")
                    line = line.replace(r.group(0), " ")
                    if event_type.startswith("lab"):
                        if "/" in event_type:
                            dct["group"] = event_type.split("/")[1]
                        event_type = "lab"
                    elif not event_type.startswith(("lec", "tut")):
                        dct["group"] = event_type
                        event_type = None

                    dct["event_type"] = event_type

                parts = line.split()

                if len(parts) == 2:
                    event_name, event_location = parts
                else:
                    event_name = parts[0]
                    event_location = None

                elective = next((e for e in electives if e.alias == event_name), None)

                if elective is None:
                    raise ValueError(f"Course not found: {event_name}")

                dct["elective"] = elective
                dct["location"] = event_location

                result.append(dct)
            return result

        # first column is time
        # first row is day in format 'Month Day'
        # first cell is week number

        events = []

        # get first column
        time_column = df.iloc[:, 0]

        # set time column as index and drop it
        df = df.set_index(time_column)
        df = df.drop(time_column.name, axis=1)

        # get first row
        day_row = df.iloc[0]

        # set day row as columns and drop it
        df.columns = day_row
        df = df.drop(day_row.name, axis=0)

        days = [day for day in df.columns if day != ""]

        for day in days:
            # copy column
            day_df = df[day].copy()
            dtstart = datetime.strptime(day, "%B %d")
            dtstart = dtstart.replace(year=datetime.now().year)
            # drop rows with empty cells
            day_df = day_df.dropna()
            day_df = day_df[day_df != ""]
            # for each cell in day column
            for timeslot, cell in day_df.items():
                start_delta, end_delta = map(
                    lambda t: datetime.strptime(t, "%H:%M").time(), timeslot.split("-")
                )
                cell_events = process_cell(cell)
                event_start = datetime.combine(dtstart, start_delta)
                event_end = datetime.combine(dtstart, end_delta)

                for cell_event in cell_events:
                    event = ElectiveEvent(
                        start=event_start, end=event_end, **cell_event
                    )
                    events.append(event)

        return events

    def parse_df(
        self, df: pd.DataFrame, electives: list[Elective]
    ) -> list[ElectiveEvent]:
        """Parse DataFrame to dict with separation by groups."""

        self.logger.debug("Parsing dataframe to separation by days|groups...")
        self.logger.info("Get 'week' indexes...")
        week_column = df.iloc[:, 0]
        week_mask = week_column.str.startswith("Week").values
        week_indexes = np.argwhere(week_mask).flatten().tolist()
        self.logger.info(f"> Found {len(week_indexes)} indexes:")

        self.logger.debug("Separating by days...")
        max_x, max_y = df.shape

        events = []

        for start_y, end_y in pairwise(week_indexes + [max_x]):
            week = df.iloc[start_y, 0]
            self.logger.info(f"> Week: {week}")
            week_df = df.iloc[start_y:end_y, :]
            week_events = self.parse_week_df(week_df, electives)
            events.extend(week_events)

        return events


def convert_separation(
    events: list[ElectiveEvent],
) -> dict[str, icalendar.Calendar]:
    output = defaultdict(lambda: icalendar.Calendar())
    # group events by Elective and group
    grouping = groupby(events, lambda e: (e.elective, e.group))

    for (elective, group), events in grouping:
        elective: Elective
        if group is None:
            cal = output[elective.alias]
        else:
            cal = output[f"{elective.alias}-{group}"]

        for event in events:
            event: ElectiveEvent
            vevent = event.get_vevent()
            cal.add_component(vevent)

    return dict(output)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = ElectiveParser()

    calendars = {
        "filters": [
            {
                "title": "Elective type",
                "alias": "elective_type",
            }
        ],
        "title": "Electives",
        "calendars": [],
    }

    for i in range(len(config.TARGET_SHEET_TITLES)):
        sheet_title = config.TARGET_SHEET_TITLES[i]

        df = parser.get_clear_df(
            spreadsheet_id=config.SPREADSHEET_ID,
            target_title=sheet_title,
            target_range=config.TARGET_RANGES[i],
        )

        parsed = parser.parse_df(df, config.ELECTIVES)
        converted = convert_separation(parsed)

        directory = (
            PARSER_PATH
            / config.SAVE_ICS_PATH
            / sheet_title.replace("/", "-").replace(" ", "-")
        )

        directory.mkdir(parents=True, exist_ok=True)

        for elective_name, calendar in converted.items():
            # print
            calendar["prodid"] = "-//one-zero-eight//InNoHassle Calendar"
            calendar["version"] = "2.0"
            calendar["x-wr-calname"] = elective_name
            calendar["x-wr-caldesc"] = "Generated by InNoHassle Calendar"
            calendar["x-wr-timezone"] = config.TIMEZONE

            file_path = directory / f"{elective_name}.ics"
            relative_directory = file_path.relative_to(
                (PARSER_PATH / config.SAVE_JSON_PATH).parent
            )
            calendars["calendars"].append(
                {
                    "name": elective_name,
                    "elective_type": config.TARGET_SHEET_TITLES[i],
                    "file": relative_directory.as_posix(),
                }
            )

            with open(file_path, "wb") as f:
                f.write(calendar.to_ical())

    # create a new .json file with information about calendar
    with open(PARSER_PATH / config.SAVE_JSON_PATH, "w") as f:
        json.dump(calendars, f, indent=4, sort_keys=True)
