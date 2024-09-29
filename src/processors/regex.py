import re


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
    s = re.sub(r"\s*\([ \t]*", " (", s)
    s = re.sub(r"\s*\)[ \t]+", ") ", s)
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
    Set only one whitespace before and after brackets. Remove repeating brackets.

    :param string: string to beautify
    :type string: str
    :return: beautified string
    :rtype: str
    """
    if isinstance(string, str):
        # set only one space between brackets and remove repeating brackets
        string = process_brackets(string)
        # set only one space after commas and remove repeating commas
        string = process_commas(string)
        # remove repeating spaces and trailing spaces
        string = process_spaces(string)
    return string


