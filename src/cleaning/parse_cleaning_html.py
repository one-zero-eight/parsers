import io
import re
from collections import defaultdict
from datetime import date, datetime

import httpx
import numpy as np
import pandas as pd
from dateutil import relativedelta

from src.logging_ import logger


def process_dataframe(df: pd.DataFrame, entries: dict[str, list[date]]) -> None:
    # drop columns that contain only NaN
    df = df.dropna(axis="columns", how="all")

    # Drop trailing empty rows without removing empty rows in the middle (e.g. weeks w/o cleaning scheduled)
    valid_rows = df.dropna(axis="index", how="all")
    if not valid_rows.empty:
        last_valid_index = valid_rows.index[-1]
        df = df.loc[:last_valid_index]

    # now second row should be [Monday ПОНЕДЕЛЬНИК,Tuesday ВТОРНИК,Wednesday СРЕДА,Thursday ЧЕТВЕРГ,
    # Friday ПЯТНИЦА,Saturday СУББОТА,Sunday ВОСКРЕСЕНЬЕ]
    assert (
        df.iloc[1]
        == [
            "Monday ПОНЕДЕЛЬНИК",
            "Tuesday ВТОРНИК",
            "Wednesday СРЕДА",
            "Thursday ЧЕТВЕРГ",
            "Friday ПЯТНИЦА",
            "Saturday СУББОТА",
            "Sunday ВОСКРЕСЕНЬЕ",
        ]
    ).all(), "Second row should be days of week"

    # first cell should be year
    # if year is missing in the first cell, assume it's current one
    year = df.iloc[0, 0]
    if not np.isnan(year):
        year = int(year)
    else:
        year = date.today().year

    # Parse month(s) from header cell - can be single "Сентябрь/September"
    # or multi-month "Сентябрь/September-Октябрь/October"
    month_header = df.iloc[0, 1]
    # Split by '-' to handle multi-month headers, then extract English month name after '/'
    month_parts = month_header.split("-")
    months = []
    for part in month_parts:
        english_name = part.strip().split("/")[-1].strip()
        months.append(datetime.strptime(english_name, "%B").month)

    # 3, 5, 7, 9, 11... rows should be days of month (be careful at the end of month and start of month)
    days = df.iloc[2::2, :]
    # drop first, second and days rows
    df = df.drop(df.index[[0, 1, *range(2, len(df), 2)]])
    days = days.astype(int).values.flatten()
    first_day_large_than_15 = days[0] > 15
    # Split the list based on monotonic increasing by one
    split_lists = list(np.split(days, np.where(np.diff(days) != 1)[0] + 1))

    # Determine which splits are prev month trailing, main months, and next month leading
    # If first split starts with a high day number (>15), it's from the previous month
    has_prev = first_day_large_than_15
    # Expected number of main month splits equals number of months in the header
    expected_main = len(months)
    expected_total = expected_main + (1 if has_prev else 0)
    has_next = len(split_lists) > expected_total

    prev_splits = []
    main_splits = []
    next_splits = []

    idx = 0
    if has_prev:
        prev_splits.append(split_lists[idx])
        idx += 1
    for _ in range(expected_main):
        if idx < len(split_lists):
            main_splits.append(split_lists[idx])
            idx += 1
    while idx < len(split_lists):
        next_splits.append(split_lists[idx])
        idx += 1

    first_month_date = date(year, months[0], 1)
    previous_month_date = first_month_date - relativedelta.relativedelta(months=1)

    days_as_dates = []

    # Previous month trailing days
    for s in prev_splits:
        days_as_dates.extend([previous_month_date.replace(day=int(day)) for day in s])

    # Main months
    for i, s in enumerate(main_splits):
        month_date = date(year, months[i], 1)
        days_as_dates.extend([month_date.replace(day=int(day)) for day in s])

    # Next month leading days
    last_month_date = date(year, months[-1], 1)
    next_month_date = last_month_date + relativedelta.relativedelta(months=1)
    for s in next_splits:
        days_as_dates.extend([next_month_date.replace(day=int(day)) for day in s])

    # flatten df
    df = df.values.flatten()
    # create new Series with dates as index
    series = pd.Series(df, index=pd.to_datetime(days_as_dates))
    # drop Nans
    series = series.dropna()
    for index, value in series.items():
        # 7 корпус 1-7 этажи 7 building 1-7 floors
        # 2 корпус 3-4 этаж 2 building 3-4 floor
        # 3 корпус 3 building
        matches = re.finditer(r"(?P<building>\d)\s+building(\s+(?P<floors>(\d+|\d+-\d+))\s+floors?)?", value)
        for m in matches:
            building = m.group("building")
            floors = m.group("floors")
            if floors:
                key = f"{building} building {floors} floors"
            else:
                key = f"{building} building"
            entries[key].append(index.date())
            logger.debug(f"{key}, {index.date()}")


def parse(dfs: dict[str, pd.DataFrame]) -> dict[str, list[date]]:
    entries: dict[str, list[date]] = defaultdict(list)
    for sheet in dfs:
        df = dfs[sheet]
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        process_dataframe(df, entries)
    return entries


def _extract_sheet_gids(pubhtml_url: str) -> list[str]:
    """
    Fetch the pubhtml page and extract sheet GIDs from the embedded JavaScript.

    The pubhtml page contains JS like:
        items.push({name: "...", ..., gid: "1174170404", ...});
    """
    logger.debug(f"Fetching pubhtml page: {pubhtml_url}")
    response = httpx.get(pubhtml_url, follow_redirects=True)
    response.raise_for_status()
    html = response.text

    gids = re.findall(r'gid:\s*"(\d+)"', html)
    logger.debug(f"Found {len(gids)} sheet GIDs: {gids}")
    return gids


def _pubhtml_to_base_url(pubhtml_url: str) -> str:
    """
    Convert a pubhtml URL to the base URL for CSV export.

    E.g. '.../pubhtml' -> '.../pub'
         '.../pubhtml?...' -> '.../pub'
    """
    # Strip query params and /pubhtml suffix
    base = pubhtml_url.split("?")[0]
    if base.endswith("/pubhtml"):
        base = base[: -len("/pubhtml")] + "/pub"
    return base


def parse_from_url(url: str) -> dict[str, list[date]]:
    """
    Parse cleaning schedule from a Google Sheets pubhtml URL.

    Extracts sheet GIDs from the pubhtml page, then fetches each sheet
    as CSV and parses it.

    :param url: Google Sheets pubhtml URL
    :return: dict mapping location to list of cleaning dates
    """
    gids = _extract_sheet_gids(url)
    if not gids:
        logger.warning("No sheet GIDs found in pubhtml page")
        return {}

    base_url = _pubhtml_to_base_url(url)
    entries: dict[str, list[date]] = defaultdict(list)

    for gid in gids:
        csv_url = f"{base_url}?gid={gid}&single=true&output=csv"
        logger.debug(f"Fetching CSV: {csv_url}")
        response = httpx.get(csv_url, follow_redirects=True)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text), header=None)
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        process_dataframe(df, entries)

    return entries


if __name__ == "__main__":
    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRll5tP8-fP0mJ7jtfrDp71297dFn7dFtIki7gJ0H6i_7QXM5HSSGRo2FR_vo8XKv8MKMSIzyiJqIKQ/pubhtml"
    print(parse_from_url(url))

