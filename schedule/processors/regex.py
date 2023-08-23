import datetime
import re

symbol_translation = str.maketrans(
    "АВЕКМНОРСТУХаср",
    "ABEKMHOPCTYXacp",
)


def sluggify(s: str) -> str:
    """
    Sluggify string.

    :param s: string to sluggify
    :type s: str
    :return: sluggified string
    :rtype: str
    """
    s = s.lower()
    s = s.translate(symbol_translation)
    # also translates special symbols, brackets, commas, etc.
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", "-", s)
    # remove multiple dashes
    s = re.sub(r"-{2,}", "-", s)

    return s


def process_spaces(s: str) -> str:
    """
    Remove multiple spaces and trailing spaces.
    """
    return re.sub(r"\s{2,}", " ", s).strip()


def process_brackets(s: str) -> str:
    """
    Prettify string with brackets.

    :param s: string to prettify
    :type s: str
    :return: prettified string
    :rtype: str
    """
    # remove multiple brackets in a row
    s = re.sub(r"(\(\s*)+\(", "(", s)
    s = re.sub(r"(\)\s*)+\)", ")", s)

    # set only one space after and before brackets except for brackets in the end of string
    s = re.sub(r"\s*\(\s*", " (", s)
    s = re.sub(r"\s*\)\s*", ") ", s)
    s = s.strip()
    return s


def process_commas(s: str) -> str:
    """
    Prettify string with commas.

    :param s: string to prettify
    :type s: str
    :return: prettified string
    :rtype: str
    """
    # remove multiple commas in a row
    s = re.sub(r"(\,\s*)+\,", ",", s)
    # set only one space after and before commas except for commas in the end of string
    s = re.sub(r"\s*\,\s*", ", ", s)
    s = s.strip()
    return s


def prettify_string(string: str | None) -> str | None:
    """
    Remove repeating spaces. Strip string.
    Translate cyrillic symbols to latin alternatives.
    Set only one whitespace before and after brackets. Remove repeating brackets.

    :param string: string to beautify
    :type string: str
    :return: beautified string
    :rtype: str
    """
    if isinstance(string, str):
        # translate cyrillic symbols to latin
        string = string.translate(symbol_translation)
        # set only one space between brackets and remove repeating brackets
        string = process_brackets(string)
        # set only one space after commas and remove repeating commas
        string = process_commas(string)
        # remove repeating spaces and trailing spaces
        string = process_spaces(string)
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
