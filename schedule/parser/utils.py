import datetime
import os
import re
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import googleapiclient.discovery


def nearest_weekday(date: datetime.date, day: int) -> datetime.date:
    """ Returns the date of the next given weekday after
    the given date. For example, the date of next Monday. """
    days = (day - date.weekday() + 7) % 7
    return date + datetime.timedelta(days=days)


weekday_converter = {
    'MONDAY': 0,
    'TUESDAY': 1,
    'WEDNESDAY': 2,
    'THURSDAY': 3,
    'FRIDAY': 4,
    'SATURDAY': 5,
    'SUNDAY': 6
}

pattern_multiple_spaces = re.compile(r'\s{2,}')

CURRENT_YEAR = datetime.datetime.now().year

symbol_translation = str.maketrans(
    "АВЕКМНОРСТУХаср",
    "ABEKMHOPCTYXacp",
    #  ' \n\t'
)


def remove_trailing_spaces(s: str):
    return pattern_multiple_spaces.sub(" ", s).strip()


def beautify_string(v: str | None):
    if v:
        v = remove_trailing_spaces(v)
        v = v.translate(symbol_translation)
    return v


def get_credentials(credentials: Path, token_path: Path, scopes: list[str]) -> Credentials:
    """ Initialize API credentials. """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and
    # is created automatically when the authorization flow completes for
    # the first time.
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(str(token_path), scopes)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials), scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return creds


def connect_spreadsheets(credentials: Credentials) -> googleapiclient.discovery.Resource:
    """ Connect to Google Sheets API. """

    service = googleapiclient.discovery.build(
        'sheets',
        'v4',
        credentials=credentials
    )

    # Call the Sheets API
    return service.spreadsheets()


__all__ = [
    'CURRENT_YEAR',
    'weekday_converter',
    'nearest_weekday',
    'remove_trailing_spaces',
    'beautify_string',
    'get_credentials',
    'connect_spreadsheets',
    'symbol_translation'
]
