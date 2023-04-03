import json
import logging
import os.path
from collections import defaultdict
from datetime import datetime
from itertools import pairwise
from pathlib import Path

import googleapiclient.discovery
import icalendar
import numpy as np
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from schedule.parser.config import PARSER_PATH, electives_config as config


class ElectivesParser:
    spreadsheets: googleapiclient.discovery.Resource
    credentials: Credentials
    logger = logging.getLogger(__name__ + "." + "Parser")

    def __init__(
        self,
    ):

        self.credentials = self.init_api(
            config.CREDENTIALS_PATH,
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
                    credentials, scopes
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
            f"> Sheet index: ({target_sheet['properties']['index']})" +
            f"> Sheet merges: ({len(target_sheet['merges'])})"
        )

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

    def parse_week_df(self, df: pd.DataFrame, course_names: list[str]) -> dict:
        # first column is time
        # first row is day in format 'Month Day'
        # first cell is week number

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

        separation_by_course = defaultdict(list)
        days = [day for day in df.columns if day != '']

        for day in days:
            dtstart = datetime.strptime(day, "%B %d")
            dtstart = dtstart.replace(year=datetime.now().year)

            for course in course_names:
                course_mask = df[day].str.contains(course, regex=False).values
                course_indexes = np.argwhere(course_mask).flatten().tolist()
                for index in course_indexes:
                    timeslot = df.index[index]

                    start_delta, end_delta = map(
                        lambda t: datetime.strptime(t, '%H:%M').time(),
                        timeslot.split("-")
                    )

                    event_start = datetime.combine(dtstart, start_delta)
                    event_end = datetime.combine(dtstart, end_delta)

                    # split cell by newlines
                    lines = df[day].iloc[index].splitlines()
                    # find needed line which contains course name
                    current = next((x for x in lines if course in x), None)
                    desc, _, location = current.rpartition(" ")

                    # add to dict
                    separation_by_course[course].append(
                        {
                            "dtstart"    : event_start.strftime(
                                "%Y%m%dT%H%M%S"
                            ),
                            "dtend"      : event_end.strftime("%Y%m%dT%H%M%S"),
                            "description": desc + "\n" + location,
                            "location"   : location
                        }
                    )
        return separation_by_course

    def parse_df(self, df: pd.DataFrame, course_names: list[str]) -> dict:
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
        separation_by_days = defaultdict(list)

        for (start_y, end_y) in pairwise(week_indexes + [max_x]):
            week = df.iloc[start_y, 0]
            self.logger.info(f"> Week: {week}")
            week_df = df.iloc[start_y: end_y, :]
            week_separation = self.parse_week_df(week_df, course_names)

            for course, lessons in week_separation.items():
                separation_by_days[course].extend(lessons)

        return separation_by_days


def convert_separation(
    separation_by_courses: list[dict],
    ELECTIVES: list[dict]
):
    # create map from short course name to dict with course info
    electives_map = {
        elective["Short name"]: elective
        for elective in ELECTIVES
    }

    output = defaultdict(lambda: {"calendar": icalendar.Calendar()})

    for course, lessons in separation_by_courses.items():
        cal = output[course]["calendar"]

        name = electives_map[course].get("Name of the course", None)
        instructors = electives_map[course].get("Instructors", None)

        extra_desc = [name, instructors]
        filtered = filter(None, extra_desc)
        extra_desc = list(filtered)

        for lesson in lessons:
            event = icalendar.Event()
            event['summary'] = course
            event['dtstart'] = lesson["dtstart"]
            event['dtend'] = lesson["dtend"]

            event['description'] = "\n".join(
                [lesson["description"]] + extra_desc
            )
            event['location'] = lesson["location"]
            cal.add_component(event)

    return output


if __name__ == '__main__':
    parser = ElectivesParser()

    calendars = {
        "filters"  : [{
            "title": "Elective type",
            "alias": "elective_type",
        }],
        "title"    : "Electives",
        "calendars": []
    }

    for i in range(len(config.TARGET_SHEET_TITLES)):
        df = parser.get_clear_df(
            spreadsheet_id=config.SPREADSHEET_ID,
            target_title=config.TARGET_SHEET_TITLES[i],
            target_range=config.TARGET_RANGES[i]
        )

        course_names = list(
            map(lambda elective: elective["Short name"], config.ELECTIVES[i])
        )
        parsed = parser.parse_df(df, course_names)
        converted = convert_separation(parsed, config.ELECTIVES[i])

        for course, calendar_dict in converted.items():
            calendar = calendar_dict["calendar"]
            calendar['prodid'] = '-//one-zero-eight//InNoHassle Calendar'
            calendar['version'] = '2.0'
            calendar['x-wr-calname'] = course
            calendar['x-wr-caldesc'] = 'Generated by InNoHassle Calendar'
            calendar['x-wr-timezone'] = config.TIMEZONE

            file_name = f"{course}.ics"
            calendars["calendars"].append(
                {
                    "name"         : course,
                    "elective_type": config.TARGET_SHEET_TITLES[i],
                    "file"         : "electives/" + file_name
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
