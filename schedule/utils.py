import datetime
import os
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


def get_credentials(
    credentials_path: Path, token_path: Path, scopes: list[str]
) -> Credentials:
    """
    Initialize API credentials.

    :param credentials_path: path to credentials
    :type credentials_path: Path
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
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_path), scopes
            )
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


def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent


def get_current_year() -> int:
    """Returns current year."""
    return datetime.datetime.now().year


DAYS = [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
]
"""Days of week"""

__all__ = [
    "get_current_year",
    "weekday_converter",
    "nearest_weekday",
    "get_credentials",
    "connect_spreadsheets",
    "get_project_root",
    "DAYS",
]
