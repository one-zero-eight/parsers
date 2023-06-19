import datetime
import re

pattern_multiple_spaces = re.compile(r"\s{2,}")
symbol_translation = str.maketrans(
    "АВЕКМНОРСТУХаср",
    "ABEKMHOPCTYXacp",
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
    Remove trailing spaces and translate cyrillic symbols to latin.

    :param string: string to beautify
    :type string: str
    :return: beautified string
    :rtype: str
    """
    if string is not None:
        string = remove_trailing_spaces(string)
        string = string.translate(symbol_translation)
    return string


def process_only_on(input_str: str) -> tuple[str, list[datetime.date]] | None:
    """
    Process string with "ONLY ON" information. Returns tuple of formatted string and list of dates.

    :param input_str: string with "ONLY ON" information (e.g. "108 (ONLY ON 14/06, 18/06)") or just string
    :return: None if no "ONLY ON" information found, tuple of formatted string and list of dates otherwise

    >>> process_only_on("108 (ONLY ON 14/06, 18/06)")
    ("108", [datetime.date(month=6, day=14), datetime.date(month=6, day=18)])
    """

    if match := re.search(r"\(ONLY ON (.+)\)", input_str, re.IGNORECASE):
        only_on_str = match.group(1)
        only_on_str = re.sub(r"\s+", "", only_on_str)
        only_on = []
        for date_str in only_on_str.split(","):
            date = datetime.datetime.strptime(date_str, "%d/%m").date()
            only_on.append(date)
        formatted_str = re.sub(r"\s+\(ONLY ON .+\)", "", input_str)
        return formatted_str, only_on


def process_desc_in_parentheses(input_str: str) -> tuple[str, str] | None:
    """
    Process string with parentheses description. Returns tuple of formatted string(without desc) and description.

    :param input_str: string with parentheses description (e.g. "Software Project (lec)")
    :return: None if no parentheses description found, tuple of formatted string and description otherwise
    """
    if match := re.search(r"\((.+)\)", input_str):
        parentheses_desc = match.group(1)
        # remove spaces
        parentheses_desc = re.sub(r"\s+", "", parentheses_desc)
        formatted_str = re.sub(r"\s+\(.+\)", "", input_str)
        return formatted_str, parentheses_desc
