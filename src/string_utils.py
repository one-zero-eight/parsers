import re


def remove_repeating_spaces_and_trailing_spaces(s: str) -> str:
    return re.sub(r"\s{2,}", " ", s).strip()


def set_one_space_around_brackets_and_remove_repeating_brackets(s: str) -> str:
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


def set_one_space_after_comma_and_remove_repeating_commas(s: str) -> str:
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
    Set only one whitespace before "(" and after ")". Remove repeating brackets.
    Set only one whitespace after ",". Remove repeating commas.
    Remove repeating spaces and trailing spaces. Strip string.

    :param string: string to beautify
    :type string: str
    :return: beautified string
    :rtype: str
    """
    if isinstance(string, str):
        # set only one space between brackets and remove repeating brackets
        string = set_one_space_around_brackets_and_remove_repeating_brackets(string)
        # set only one space after commas and remove repeating commas
        string = set_one_space_after_comma_and_remove_repeating_commas(string)
        # remove repeating spaces and trailing spaces
        string = remove_repeating_spaces_and_trailing_spaces(string)
    return string

