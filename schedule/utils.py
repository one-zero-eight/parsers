__all__ = [
    "get_current_year",
    "nearest_weekday",
    "get_weekday_rrule",
    "get_credentials",
    "connect_spreadsheets",
    "get_project_root",
    "get_sheets",
    "get_sheet_by_id",
    "get_namespace",
    "get_merged_ranges",
    "split_range_to_xy",
]

import datetime
import os
import re
from pathlib import Path

# noinspection StandardLibraryXml
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import googleapiclient.discovery
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from openpyxl.utils import coordinate_to_tuple


def nearest_weekday(date: datetime.date, day: int) -> datetime.date:
    """
    Returns the date of the next given weekday after
    the given date. For example, the date of next Monday.

    :param date: date to start from
    :type date: datetime.date
    :param day: weekday to find (0 is Monday, 6 is Sunday)
    :type day: int
    :return: date of the next given weekday
    :rtype: datetime.date
    """
    days = (day - date.weekday() + 7) % 7
    return date + datetime.timedelta(days=days)


def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent


def get_current_year() -> int:
    """Returns current year."""
    return datetime.datetime.now().year


def get_weekday_rrule(end_date: datetime.date) -> dict:
    """
    Get RRULE for recurrence with weekly interval and end date.

    :param end_date: end date
    :type end_date: datetime.date
    :return: RRULE dictionary with weekly interval and end date.
        See `here <https://icalendar.org/iCalendar-RFC-5545/3-8-5-3-recurrence-rule.html>`__
    :rtype: dict

    >>> get_weekday_rrule(datetime.date(2021, 1, 1))
    {'FREQ': 'WEEKLY', 'INTERVAL': 1, 'UNTIL': datetime.date(2021, 1, 1)}
    """
    return {
        "FREQ": "WEEKLY",
        "INTERVAL": 1,
        "UNTIL": end_date,
    }


# ----------------- Google Sheets -----------------
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


# ----------------- Excel -----------------
def get_sheets(xlsx_zipfile: ZipFile) -> dict[str, str]:
    """
    Read xl/workbook.xml and return dict of sheet_id: sheet_name

    :param xlsx_zipfile: .xlsx file as ZipFile
    :return: dict of sheet_id: sheet_name
    """
    with xlsx_zipfile.open("xl/workbook.xml") as f:
        xml_struct = ET.parse(f)
    root = xml_struct.getroot()
    sheets = dict()
    for child in root:
        _, _, tag = child.tag.rpartition("}")
        if tag == "sheets":
            for sheet in child:
                sheet_id = sheet.attrib["sheetId"]
                sheet_name = sheet.attrib["name"]
                sheets[sheet_id] = sheet_name
            break
    return sheets


def get_sheet_by_id(xlsx_zipfile: ZipFile, sheet_id: str) -> ET.Element:
    """
    Read xl/worksheets/sheet{sheet_id}.xml and return root element

    :param xlsx_zipfile: .xlsx file as ZipFile
    :param sheet_id: id of sheet to read
    :return: root element of sheet
    """
    with xlsx_zipfile.open(f"xl/worksheets/sheet{str(sheet_id)}.xml") as f:
        xml_struct = ET.parse(f)
    root = xml_struct.getroot()
    return root


def get_namespace(element: ET.Element):
    """
    Get namespace from element tag

    :param element: element to get namespace from
    :return: namespace
    """
    m = re.match(r"{.*}", element.tag)
    return m.group(0) if m else ""


def get_merged_ranges(xlsx_sheet: ET.Element) -> list[str]:
    """
    Get list of merged ranges from sheet element

    :param xlsx_sheet: sheet element
    :return: list of merged ranges (e.g. ['A1:B2', 'C3:D4'])
    """
    namespace = get_namespace(xlsx_sheet)
    merged_cells = xlsx_sheet.find(f"{namespace}mergeCells")
    merged_ranges = []
    for merged_cell in merged_cells:
        merged_ranges.append(merged_cell.attrib["ref"])
    return merged_ranges


def split_range_to_xy(target_range: str):
    """
    Split range to x, y coordinates starting from 0

    :param target_range: range to split e.g. "A1:B2"
    :return: two points (x1, y1), (x2, y2)
    """
    start, end = target_range.split(":")
    start_row, start_col = coordinate_to_tuple(start)
    start_row, start_col = start_row - 1, start_col - 1
    end_row, end_col = coordinate_to_tuple(end)
    end_row, end_col = end_row - 1, end_col - 1
    return (start_row, start_col), (end_row, end_col)
