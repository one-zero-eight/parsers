import json
import logging
import os.path
from collections import defaultdict
from datetime import datetime
from itertools import pairwise, groupby
from pathlib import Path
from typing import Optional, Collection

import googleapiclient.discovery
import icalendar
import numpy as np
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from schedule.parser.config import PARSER_PATH, electives_config as config, Elective

from pydantic import BaseModel


class ElectiveEvent(BaseModel):
    elective: Elective
    start: datetime
    end: datetime
    location: Optional[str]
    description: Optional[str]

    group: Optional[str] = None


class ElectiveParser:
    spreadsheets: googleapiclient.discovery.Resource
    credentials: Credentials
    logger = logging.getLogger(__name__ + "." + "Parser")

    def __init__(
            self,
    ):

        self.credentials = self.init_api(
            Path(config.CREDENTIALS_PATH),
            scopes=config.API_SCOPES
        )
        self.spreadsheets = self.connect_spreadsheets()

    @staticmethod
    def init_api(credentials: Path, scopes: list[str]) -> Credentials:
        """
        Initialize API credentials.
        @param credentials: Path to credentials file.
        @type credentials: Path
        @param scopes: List of scopes to authorize.
        @type scopes: list[str]
        @return Current Credentials object.
        :rtype: Credentials
        """
        creds = None
        token_path = PARSER_PATH / "token.json"
        # The file token.json stores the user's access and refresh tokens, and
        # is created automatically when the authorization flow completes for
        # the first time.
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(
                token_path, scopes
            )

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(credentials), scopes
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_path, 'w') as token:
                token.write(creds.to_json())

        return creds

    def connect_spreadsheets(self):
        """
        Connect to Google Sheets API.
        @return service.spreadsheets()
        :rtype: googleapiclient.discovery.Resource
        """

        service = googleapiclient.discovery.build(
            'sheets',
            'v4',
            credentials=self.credentials
        )
        # Call the Sheets API
        return service.spreadsheets()

    def get_clear_df(
            self,
            spreadsheet_id: str,
            target_range: str,
            target_title: str
    ) -> pd.DataFrame:
        """
        Get data from Google Sheets and return it as a DataFrame with merged
        cells and empty cells in the course row filled by left value.

        Args:
            spreadsheet_id (str): ID of the spreadsheet to get data from.
            target_range (str): A1 notation of the values to retrieve. Or
             named range.
            target_title (str): Title of the sheet to retrieve data from.

        Returns:
            DataFrame with merged cells and empty cells in the course row
        """

        self.logger.debug("Getting dataframe from Google Sheets...")
        self.logger.info(
            f"Retrieving data: {spreadsheet_id}/{target_title}-{target_range}"
        )

        values = self.spreadsheets.values().get(
            spreadsheetId=spreadsheet_id,
            range=target_range
        ).execute()["values"]

        df = pd.DataFrame(data=values)
        # strip all values
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        max_x, max_y = df.shape

        self.logger.info(f"Data retrieved: {max_x}x{max_y}")
        spreadsheet = self.spreadsheets.get(
            spreadsheetId=config.SPREADSHEET_ID,
            ranges=[target_range],
            includeGridData=False  # values already fetched
        ).execute()
        self.logger.info(
            f"Spreadsheet {spreadsheet['properties']['title']} retrieved:" +
            f"Sheets({len(spreadsheet['sheets'])}): " +
            f"> {sheet['properties']['title']}"
            for sheet in spreadsheet['sheets']
        )

        # get target sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == target_title:
                target_sheet = sheet
                break

        if target_sheet is None:
            raise ValueError(f"Target sheet {target_title} not found")

        self.logger.info(
            f"Target sheet: {target_sheet['properties']['title']}" +
            f"> Sheet index: ({target_sheet['properties']['index']})"
        )

        if "merges" in target_sheet:
            self.logger.info(f"> Sheet merges: ({len(target_sheet['merges'])})")
            self.logger.info("Merging cells")

            for merge in target_sheet["merges"]:
                x0 = merge["startRowIndex"]
                y0 = merge["startColumnIndex"]
                x1 = merge["endRowIndex"]
                y1 = merge["endColumnIndex"]

                if x0 < max_x and y0 < max_y:
                    df.iloc[x0: x1, y0: y1] = df.iloc[x0][y0]

        df.fillna('', inplace=True)

        self.logger.info("Cells merged")
        self.logger.info("Dataframe ready")
        return df

    @staticmethod
    def parse_week_df(df: pd.DataFrame, electives: Collection[Elective]) -> list[ElectiveEvent]:
        # for each cell in day column
        def process_cell(cell: str):
            #           "BDLD(lec) 312" ->
            #           {"ele"BDLD", "lec", "312"
            #           "PP(lab/group2)303" -> "PP", "lab/group2", "303"

            result: list[dict] = []

            if not cell:
                return result

            occurences = (cell
                          .replace('(', ' ')
                          .replace(')', ' ')
                          .strip()
                          .split('\n'))

            for line in occurences:
                dct = {}
                parts = line.split()
                if len(parts) == 3:
                    event_name, event_type, event_location = parts
                elif len(parts) == 2:
                    event_name, event_location = parts
                    event_type = None
                else:
                    event_name = parts[0]
                    event_type = None
                    event_location = None

                elective = next(
                    (e for e in electives
                     if e.alias == event_name),
                    None
                )

                if elective is None:
                    raise ValueError(f"Course not found: {event_name}")

                dct['elective'] = elective
                dct['location'] = event_location

                if event_type:
                    dct['type'] = event_type

                    if 'lab' in event_type and '/' in event_type:
                        event_type, group = event_type.split('/')
                        dct['group'] = group
                        dct['type'] = event_type

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

        days = [day for day in df.columns if day != '']

        for day in days:
            # copy column
            day_df = df[day].copy()
            dtstart = datetime.strptime(day, "%B %d")
            dtstart = dtstart.replace(year=datetime.now().year)
            # drop rows with empty cells
            day_df = day_df.dropna()
            day_df = day_df[day_df != '']
            # for each cell in day column
            for timeslot, cell in day_df.items():
                start_delta, end_delta = map(
                    lambda t: datetime.strptime(t, '%H:%M').time(),
                    timeslot.split("-")
                )
                cell_events = process_cell(cell)
                event_start = datetime.combine(dtstart, start_delta)
                event_end = datetime.combine(dtstart, end_delta)

                for cell_event in cell_events:
                    event = ElectiveEvent(
                        start=event_start,
                        end=event_end,
                        **cell_event
                    )
                    events.append(event)

        return events

    def parse_df(self, df: pd.DataFrame, electives: list[Elective]) -> list[ElectiveEvent]:
        """
        Parse DataFrame to dict with separation by groups.
        @param df: DataFrame to parse.
        @type df: pd.DataFrame
        @return Dict with groups and their lessons. Day of week is a first
        key. Group is a second key.
        :rtype: dict
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

        for (start_y, end_y) in pairwise(week_indexes + [max_x]):
            week = df.iloc[start_y, 0]
            self.logger.info(f"> Week: {week}")
            week_df = df.iloc[start_y: end_y, :]
            week_events = self.parse_week_df(week_df, electives)
            events.extend(week_events)

        return events


def convert_separation(
        events: list[ElectiveEvent],
):
    output = defaultdict(lambda: {"calendar": icalendar.Calendar()})
    # group events by Elective and group
    grouping = groupby(events, lambda e: (e.elective, e.group))

    for (elective, group), events in grouping:
        elective: Elective
        if group is None:
            cal = output[elective.alias]["calendar"]
        else:
            cal = output[f"{elective.alias}-{group}"]["calendar"]

        for event in events:
            event: ElectiveEvent
            vevent = icalendar.Event()
            vevent['summary'] = elective.name
            vevent['dtstart'] = event.start.strftime("%Y%m%dT%H%M%S")
            vevent['dtend'] = event.end.strftime("%Y%m%dT%H%M%S")
            vevent['location'] = event.location
            desc = f"{elective.name}"

            if group is not None:
                desc += f"\n{group}"

            if elective.instructor:
                desc += f"\n{elective.instructor}"

            if elective.type:
                desc += f"\n{elective.type}"

            vevent['description'] = desc
            cal.add_component(vevent)
            # print(vevent)
    return dict(output)


if __name__ == '__main__':
    parser = ElectiveParser()

    calendars = {
        "filters": [{
            "title": "Elective type",
            "alias": "elective_type",
        }],
        "title": "Electives",
        "calendars": []
    }

    for i in range(len(config.TARGET_SHEET_TITLES)):
        df = parser.get_clear_df(
            spreadsheet_id=config.SPREADSHEET_ID,
            target_title=config.TARGET_SHEET_TITLES[i],
            target_range=config.TARGET_RANGES[i]
        )

        parsed = parser.parse_df(df, config.ELECTIVES[i])
        converted = convert_separation(parsed)

        for course_name, calendar_dict in converted.items():
            calendar = calendar_dict["calendar"]
            # print
            calendar['prodid'] = '-//one-zero-eight//InNoHassle Calendar'
            calendar['version'] = '2.0'
            calendar['x-wr-calname'] = course_name
            calendar['x-wr-caldesc'] = 'Generated by InNoHassle Calendar'
            calendar['x-wr-timezone'] = config.TIMEZONE

            file_name = f"{course_name}.ics"
            calendars["calendars"].append(
                {
                    "name": course_name,
                    "elective_type": config.TARGET_SHEET_TITLES[i],
                    "file": "electives/" + file_name
                }
            )

            with open(
                    PARSER_PATH / config.SAVE_PATH / file_name,
                    'wb'
            ) as f:
                f.write(calendar.to_ical())

    # create a new .json file with information about calendar
    with open(
            PARSER_PATH / config.SAVE_JSON_PATH,
            "w"
    ) as f:
        json.dump(calendars, f, indent=4)
