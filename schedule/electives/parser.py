import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from itertools import pairwise, groupby
from pathlib import Path
from typing import Collection

import googleapiclient.discovery
import icalendar
import numpy as np
import pandas as pd
from google.oauth2.credentials import Credentials

from schedule.electives.models import Elective, ElectiveEvent
from config import electives_config as config
from utils import *

BRACKETS_PATTERN = re.compile(r"\((.*?)\)")


class ElectiveParser:
    """
    Elective parser class
    """

    spreadsheets: googleapiclient.discovery.Resource
    """ Google Sheets API object """
    credentials: Credentials
    """ Google API credentials object """
    logger = logging.getLogger(__name__ + "." + "Parser")
    """ Logger object """

    def __init__(self):
        self.credentials = get_credentials(
            credentials_path=config.CREDENTIALS_PATH,
            token_path=config.TOKEN_PATH,
            scopes=config.API_SCOPES,
        )
        self.spreadsheets = connect_spreadsheets(self.credentials)

    def get_clear_df(
        self, spreadsheet_id: str, target_range: str, target_title: str
    ) -> pd.DataFrame:
        """
        Get data from Google Sheets and return it as a DataFrame with merged
        cells and empty cells in the course row filled by left value.

        :param spreadsheet_id: ID of the spreadsheet to get data from
        :type spreadsheet_id: str
        :param target_range: range of the spreadsheet to get data from
        :type target_range: str
        :param target_title: title of the sheet to get data from
        :type target_title: str
        :return: dataframe with merged cells and empty cells filled
        :rtype: pd.DataFrame
        """

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
        """
        Parse dataframe with week schedule

        :param df: dataframe with week schedule
        :type df: pd.DataFrame
        :param electives: list of electives to parse
        :type electives: Collection[parser.models.Elective]
        :return: list of parsed events
        :rtype: list[ElectiveEvent]
        """

        # for each cell in day column
        def process_cell(cell: str) -> list[dict[str, str]]:
            """
            Process cell, find events in it and return list of parsed events

            :param cell: cell to process
            :type cell: str
            :return: list of parsed events
            :rtype: list[dict[str, str]]
            """
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
                # if brackets in the end of the line, just add to the description

                if end_brackets := re.search(r"\((.*?)\)$", line):
                    dct["notes"] = end_brackets.group(1)
                    line = line.replace(end_brackets.group(0), " ")
                elif middle_brackets := re.search(r"\((.*?)\)", line):
                    event_type = middle_brackets.group(1)
                    event_type = event_type.replace(" ", "")
                    line = line.replace(middle_brackets.group(0), " ")
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
        """
        Parse DataFrame to dict with separation by groups.

        :param df: dataframe to parse
        :type df: pd.DataFrame
        :param electives: list of electives to parse
        :type electives: list[parser.models.Elective]
        :return: list of parsed events
        :rtype: list[ElectiveEvent]
        """

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
) -> dict[str, list[ElectiveEvent]]:
    """
    Convert list of events to dict with separation by Elective and group.

    :param events: list of events to convert
    :type events: list[ElectiveEvent]
    :return: dict with separation by Elective and group
    :rtype: dict[str, list[ElectiveEvent]]
    """
    output = defaultdict(list)
    # group events by Elective and group
    grouping = groupby(events, lambda e: (e.elective, e.group))

    for (elective, group), events in grouping:
        elective: Elective
        if group is None:
            cal = output[elective.alias]
        else:
            cal = output[f"{elective.alias}-{group}"]
        cal: list[ElectiveEvent]
        cal.extend(events)

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

        directory = config.SAVE_ICS_PATH / sheet_title.replace("/", "-").replace(
            " ", "-"
        )

        directory.mkdir(parents=True, exist_ok=True)

        for calendar_name, events in converted.items():
            calendar = icalendar.Calendar()

            elective = None

            if events:
                elective = events[0].elective

            for event in events:
                calendar.add_component(event.get_vevent())

            # print
            calendar["prodid"] = "-//one-zero-eight//InNoHassle Schedule"
            calendar["version"] = "2.0"
            calendar["x-wr-calname"] = calendar_name
            calendar["x-wr-caldesc"] = "Generated by InNoHassle Schedule"
            calendar["x-wr-timezone"] = config.TIMEZONE
            calendar["method"] = "PUBLISH"

            file_path = directory / f"{calendar_name}.ics"
            relative_directory = file_path.relative_to(config.SAVE_JSON_PATH.parent)

            # change 'gr1' to 'group1' through re
            calendar_name = re.sub(r"gr(oup)?", "group", calendar_name)
            calendar_name = calendar_name.replace("-", " ")

            calendars["calendars"].append(
                {
                    "name": calendar_name,
                    "elective_type": config.TARGET_SHEET_TITLES[i],
                    "file": relative_directory.as_posix(),
                    "description": elective.name if elective else "",
                }
            )

            with open(file_path, "wb") as f:
                f.write(calendar.to_ical())

    # create a new .json file with information about calendar
    with open(config.SAVE_JSON_PATH, "w") as f:
        json.dump(calendars, f, indent=4, sort_keys=True)
