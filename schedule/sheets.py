import logging
from collections import defaultdict
from pathlib import Path
from typing import List
import os.path

import googleapiclient.discovery
import numpy as np
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from schedule.config import config, PARSER_PATH
from itertools import pairwise
import re
from datetime import datetime, timedelta
from datetime import datetime as dt

from icalendar import Calendar, Event


class Parser:
    spreadsheets: googleapiclient.discovery.Resource
    credentials: Credentials
    logger = logging.getLogger(
        __name__ + "." + "Parser"
    )

    def __init__(
        self,
    ):

        self.credentials = self.init_api(
            config.CREDENTIALS_PATH,
            scopes=config.API_SCOPES
        )
        self.connect_spreadsheets()

    @staticmethod
    def init_api(credentials: Path, scopes: List[str]) -> Credentials:
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
        try:
            service = build('sheets', 'v4', credentials=self.credentials)
            # Call the Sheets API
            self.spreadsheets = service.spreadsheets()
        except HttpError as err:
            raise err

    def get_clear_df(
        self,
        spreadsheet_id: str,
        target_range: str,
        target_title: str
    ) -> pd.DataFrame:
        """
        Get data from Google Sheets and return it as a DataFrame with merged
        cells and empty cells in the course row filled by left value.
        :param spreadsheet_id: ID of the spreadsheet to retrieve data from.
        :type spreadsheet_id: str
        :param target_range: A1 notation of the values to retrieve. Or named
        range.
        :type target_range: str
        :param target_title: Title of the sheet to retrieve data from.
        :type target_title: str
        :return: DataFrame with merged cells and empty cells in the course row
        :rtype: pd.DataFrame
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

        target_sheet = next(
            (
                sheet for sheet in spreadsheet["sheets"] if
                sheet['properties']['title'] == target_title)
            , None
        )

        if target_sheet is None:
            raise Exception("Target sheet not found")

        self.logger.info(
            f"Target sheet: {target_sheet['properties']['title']}" +
            f"> Sheet index: ({target_sheet['properties']['index']})" +
            f"> Sheet merges: ({len(target_sheet['merges'])})"
        )

        self.logger.info("Merging cells")

        for merge in target_sheet["merges"]:
            x0, x1, y0, y1 = (
                merge["startRowIndex"],
                merge["endRowIndex"],
                merge["startColumnIndex"],
                merge["endColumnIndex"]
            )

            if x0 < max_x and y0 < max_y:
                df.iloc[x0: x1, y0: y1] = df.iloc[x0][y0]

        df.fillna('', inplace=True)

        self.logger.info("Cells merged")
        self.logger.info("Filling empty cells")
        for y in range(1, max_y):
            course_name = df.iloc[0, y]
            if course_name == '':
                df.iloc[0, y] = df.iloc[0, y - 1]
                self.logger.info(f"> Filled empty cell in courses line: {y}")
        self.logger.info("Empty cells filled")
        self.logger.info("Dataframe ready")
        return df

    def parse_df(self, df: pd.DataFrame) -> dict:
        self.logger.debug("Parsing dataframe to separation by days|groups...")
        self.logger.info("Get 'week' indexes...")
        week_column = df.iloc[:, 0]
        week_mask = week_column.isin(config.DAYS).values
        week_indexes = np.argwhere(week_mask).flatten().tolist()
        self.logger.info(f"> Found {len(week_indexes)} indexes:")

        self.logger.debug("Separating by days...")
        max_x, max_y = df.shape
        separation_by_days = {}
        courses_line = df.iloc[0, 1:]
        groups_line = df.iloc[1, 1:]

        for start_x, end_x in pairwise(week_indexes + [max_x]):
            day_name = week_column[start_x]
            self.logger.info(f"> Separating day {day_name}")
            # until the next entrance of this day
            week_df = df.iloc[start_x: end_x]
            day_row = week_df.iloc[0]
            day_mask = day_row.isin(config.DAYS).values
            day_indexes = np.argwhere(day_mask).flatten().tolist()

            separation_by_days[day_name] = {}

            for start_y, end_y in pairwise(day_indexes + [max_y]):
                course_name = courses_line.iloc[start_y]
                self.logger.info(f">> Separating course {course_name}")

                course_df = week_df.iloc[1:, start_y: end_y]
                columns = ["Time"]
                for group_name in groups_line.iloc[start_y: end_y]:
                    if group_name != '':
                        columns.append(group_name)
                course_df.columns = columns
                course_df.set_index("Time", inplace=True)
                separation_by_days[day_name][course_name] = course_df

        return separation_by_days


def convert_separation(
    separation_by_days: dict,
    very_first_date: datetime.date,
    very_last_date: datetime.date
) -> defaultdict[Calendar]:
    print("Parsing into ics...")
    calendars = defaultdict(Calendar)

    for day_name, separation_by_courses in separation_by_days.items():
        print(f"> Parsing day {day_name}")
        weekday_dtstart = nearest_weekday(
            very_first_date,
            weekday_converter[day_name]
        )
        for course_name, course_df in separation_by_courses.items():
            # print(f">> Course {course_name}")
            # merge cells at the same time and same group
            course_df = course_df.groupby("Time").agg(
                lambda x: "\n".join(x)  #### HERE
            )
            course_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
            course_df.fillna('', inplace=True)

            for time, groups in course_df.iterrows():
                # print(f">>> Time {time}")
                for group_name, value in groups.items():
                    if value != '':
                        calendar = calendars[group_name]
                        event = Event()
                        by_parts = [v for v in value.split("\n") if
                                    v != '']
                        parts = len(by_parts)
                        summary = by_parts[0] if parts > 0 else ''
                        description = by_parts[1] if parts > 1 else ''
                        description += f"\n{group_name}"
                        location = by_parts[2] if parts > 2 else ''

                        start_delta, end_delta = map(
                            lambda t: dt.strptime(t, '%H:%M').time(),
                            time.split("-")
                        )

                        dtstart = dt.combine(weekday_dtstart, start_delta)
                        dtend = dt.combine(weekday_dtstart, end_delta)

                        event['summary'] = summary
                        event['description'] = description
                        event['location'] = location
                        # convert to datetime format for ics
                        event['dtstart'] = dtstart.strftime(
                            "%Y%m%dT%H%M%S"
                        )
                        event['dtend'] = dtend.strftime("%Y%m%dT%H%M%S")
                        event.add(
                            'rrule',
                            get_weekday_rrule(day_name, very_last_date)
                        )
                        calendar.add_component(event)
    return calendars


def nearest_weekday(date, day):
    """
    Returns the date of the next given weekday after
    the given date. For example, the date of next Monday.

    NB: if it IS the day we're looking for, this returns 0.
    consider then doing onDay(foo, day + 1).
    """
    days = (day - date.weekday() + 7) % 7
    return date + timedelta(days=days)


weekday_converter = {
    'MONDAY'   : 0,
    'TUESDAY'  : 1,
    'WEDNESDAY': 2,
    'THURSDAY' : 3,
    'FRIDAY'   : 4,
    'SATURDAY' : 5,
    'SUNDAY'   : 6
}


def get_weekday_rrule(day_name, end_date):
    day = weekday_converter[day_name]
    _ = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]

    return {
        'FREQ'    : 'WEEKLY',
        'INTERVAL': 1,
        'UNTIL'   : end_date,
    }


if __name__ == '__main__':
    parser = Parser()
    target_id = config.TARGET_ID

    df = parser.get_clear_df(
        spreadsheet_id=config.SPREADSHEET_ID,
        target_range=config.TARGET_RANGES[target_id],
        target_title=config.TARGET_SHEET_TITLES[target_id]
    )
    separation_by_days = parser.parse_df(df)

    from_date = dt.fromisoformat(
        config.RECURRENCE[target_id]["start"]
    ).date()
    until_date = dt.fromisoformat(
        config.RECURRENCE[target_id]["end"]
    ).date()

    calendars = convert_separation(
        separation_by_days,
        from_date,
        until_date
    )

    remove_pattern = re.compile(r"\(.*\)")

    for group_name, calendar in calendars.items():
        print(f"Writing {group_name}...")
        calendar['prodid'] = '-//one-zero-eight//InNoHassle Calendar'
        calendar['version'] = '1.0'
        calendar['x-wr-calname'] = group_name
        calendar['x-wr-caldesc'] = 'Generated by InNoHassle Calendar'
        calendar['x-wr-timezone'] = config.TIMEZONE
        formatted_group_name = remove_pattern.sub(
            '',
            group_name
        ).upper().replace(' ', '')
        file_name = f"{formatted_group_name}.ics"

        with open(
            PARSER_PATH / config.SAVE_PATH / file_name,
            'wb'
        ) as f:
            f.write(calendar.to_ical())
