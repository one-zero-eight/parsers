import datetime
import os
import re
from pathlib import Path

import googleapiclient.discovery
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


def nearest_weekday(date: datetime.date, day: int) -> datetime.date:
    """
    Returns the date of the next given weekday after
    the given date. For example, the date of next Monday.

    :param date: date to start from
    :type date: datetime.date
    :param day: weekday to find
    :type day: int
    :return: date of the next given weekday
    :rtype: datetime.date
    """
    days = (day - date.weekday() + 7) % 7
    return date + datetime.timedelta(days=days)


weekday_converter = {
    "MONDAY": 0,
    "TUESDAY": 1,
    "WEDNESDAY": 2,
    "THURSDAY": 3,
    "FRIDAY": 4,
    "SATURDAY": 5,
    "SUNDAY": 6,
}

pattern_multiple_spaces = re.compile(r"\s{2,}")

CURRENT_YEAR = datetime.datetime.now().year

symbol_translation = str.maketrans(
    "АВЕКМНОРСТУХаср",
    "ABEKMHOPCTYXacp",
    #  ' \n\t'
)


def remove_trailing_spaces(s: str) -> str:
    """
    Remove multiple spaces and trailing spaces.

    :param s: string to remove spaces from
    :type s: str
    :return: string without multiple spaces and trailing spaces
    :rtype: str
    """
    return pattern_multiple_spaces.sub(" ", s).strip()


def beautify_string(string: str | None) -> str | None:
    """
    Remove trailing spaces and translate cyrillic symbols to latin ??.
    #TODO

    :param string: string to beautify
    :type string: str
    :return: beautified string
    :rtype: str
    """
    if string is not None:
        string = remove_trailing_spaces(string)
        string = string.translate(symbol_translation)
    return string


def get_credentials(
    credentials: Path, token_path: Path, scopes: list[str]
) -> Credentials:
    """
    Initialize API credentials.

    :param credentials: path to credentials
    :type credentials: Path
    :param token_path: path to token
    :type token_path: Path
    :param scopes: scopes to use
    :type scopes: list[str]
    :return: credentials
    :rtype: Credentials
    """
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
        with open(token_path, "w") as token:
            token.write(creds.to_json())

    return creds


def connect_spreadsheets(
    credentials: Credentials,
) -> googleapiclient.discovery.Resource:
    """
    Connect to Google Sheets API.

    :param credentials: OAuth2 credentials
    :type credentials: Credentials
    :return: Google Sheets API service
    :rtype: googleapiclient.discovery.Resource
    """

    service = googleapiclient.discovery.build("sheets", "v4", credentials=credentials)

    # Call the Sheets API
    return service.spreadsheets()


__all__ = [
    "CURRENT_YEAR",
    "weekday_converter",
    "nearest_weekday",
    "remove_trailing_spaces",
    "beautify_string",
    "get_credentials",
    "connect_spreadsheets",
    "symbol_translation",
]
