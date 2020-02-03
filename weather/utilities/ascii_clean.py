import re

NOT_CHAR_PATTERN = re.compile('[^a-zA-Z0-9]')


def create_ascii_str_from_str(string: str, replace_char='-') -> str:
    """Take an arbitrary string and create a similar, lower_case string containing only ascii characters and numbers."""
    char_only = NOT_CHAR_PATTERN.sub(replace_char, string)
    ascii_char_only = ''.join((c for c in char_only if 0 < ord(c) < 127))
    return ascii_char_only.lower()