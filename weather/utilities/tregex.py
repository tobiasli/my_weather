r"""
 Name:        tregex
 Purpose:     Wrapper that makes my everyday use of regex much smoother.

 >>> from statkraft.aqua.domain.utilities import tregex
 >>> string = 'jens.stoltenberg@norge.no; barack.obama@usa.com'
 >>> pattern = r'(?P<first_name>\w+)\.(?P<last_name>\w+)@(?P<hostname>\w+)\.(?P<domain>\w+)'
 >>>
 >>> match = tregex.to_object(pattern, string)
 >>> assert match
 >>> jens = match[0]
 >>> obama = match[1]
 >>> assert jens.first_name == 'jens'
 >>> assert obama.last_name == 'obama'
 >>> assert jens.hostname == 'norge'
"""

__author__ = "Tobias Litherland"
__email__ = 'tobias.litherland@statkraft.com'

import re
import difflib
from typing import Union, List, Dict, Tuple, Iterable

NUMBER = Union[int, float]
DEFAULT_FLAG = re.UNICODE | re.DOTALL
NAMED_GROUP_DETECTION = r'(\(\?P<\w+>)'
NAMED_GROUP_REFERENCE_DETECTION = r'\(\?\(\w+\)'
DEFAULT_SEARCH_SCORE_CUTOFF = 0.6
CONTENT_MATCH_DEFAULT_SCORE = 0.01


class TregexUnknownMethodError(Exception):
    pass


class TregexMismatchError(Exception):
    pass


class GenericPropertyClass:
    """Class for containing a dictionary and presenting its contents as properties of the class."""

    def __init__(self, **kwargs) -> None:
        self.dict = kwargs

    def __getattr__(self, item) -> object:
        if item not in self.dict:
            raise AttributeError(f'Attribute {item} not present in GenericPropertyClass.')
        return self.dict[item]

    def __bool__(self) -> bool:
        return bool(self.dict)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        arg_pairs = [f"{k}={repr(v)}" for k, v in self.dict.items()]
        return f'{GenericPropertyClass.__name__}({", ".join(arg_pairs)})'

    def __dir__(self) -> Iterable:
        return list(self.dict.keys())

    def __eq__(self, other) -> bool:
        return self.dict == other.dict


def _process(pattern: str, string: str, output='smart', flags: re.RegexFlag = DEFAULT_FLAG) \
        -> List[Union[Tuple[str], Dict[str, str], str]]:
    """Compile and run a regular expression and posprocess the results depending of the [output].

    Args:
        pattern: The pattern string for the regex.
        string: The string being matched with the pattern.
        output: Which post procesing to run on the output, se Below.
        flags: The input arguments for the regex parse. Defaults to re.UNICODE | re.DOTALL.

    Returns:
        '': Assumes simple match, and returns a list of matching pattern in string.
        'group': Assumes capture groups, and returns a list of tuples for each pattern group for each pattern match.
        'name': Assumes named capture groups in pattern, and returns a list of dictionaries matching the named groups.
        'smart': Scans the pattern for named groups, groups or nothing, and returns the appropriate structure.

    Raises:
        TregexUnknownMethodError when arg 'output' does not match any output type.
    """

    # Todo: Precompile the pattern so that we don't have to compile at every call.

    r = re.compile(pattern, flags)
    result = [found for found in r.finditer(string)]
    if result:
        return_list = []

        if output == 'smart':
            if result[0].groupdict():
                output = 'name'
            elif result[0].groups():
                output = 'group'
            else:
                output = ''

        if not output:
            for m in result:
                return_list += [m.group()]
        elif output == 'name':
            for m in result:
                return_list += [m.groupdict()]
        elif output == 'group':
            for m in result:
                return_list += [m.groups()]
        else:
            raise TregexUnknownMethodError("Unknown method {} for argument 'output'.".format(output))

        return return_list
    else:
        return []


def to_dict(pattern: str, string: str, flags: re.RegexFlag = DEFAULT_FLAG) -> List[Dict]:
    """Identifies named capture groups in pattern, and outputs matches as a list of dictionaries with named capture
    groups as keys."""

    return _process(pattern, string, output='name', flags=flags)


def to_object(pattern: str, string: str, flags: re.RegexFlag = DEFAULT_FLAG) -> List[GenericPropertyClass]:
    """Identifies named capture groups in pattern, and outputs matches as a list of objects with named capture groups as
    properties."""
    result = _process(pattern, string, output='name', flags=flags)
    return [GenericPropertyClass(**dictionary) for dictionary in result]


def match(pattern: str, string: str, flags: re.RegexFlag = DEFAULT_FLAG) -> List[str]:
    """Returns the string if it matches. will remove any named groups from pattern
    before compiling."""

    pattern = re.sub(NAMED_GROUP_DETECTION, '(', pattern)  # Remove regular groups.
    pattern = re.sub(NAMED_GROUP_REFERENCE_DETECTION, '(', pattern)  # Remove named groups.
    return _process(pattern, string, output='', flags=flags)


def to_tuple(pattern: str, string: str, flags: re.RegexFlag = DEFAULT_FLAG) -> List[Tuple[str]]:
    """Identifies capture groups in pattern, and outputs matches as a list of tuples of strings matching the pattern."""

    pattern = re.sub(NAMED_GROUP_DETECTION, '(', pattern)  # Remove named groups.
    return _process(pattern, string, output='group', flags=flags)


def smart(pattern: str, string: str, flags: re.RegexFlag = DEFAULT_FLAG)\
        -> List[Union[Tuple[str], Dict[str, str], str]]:
    """Identifies properties of the pattern, and outputs matches as a list of
    objects based on the properties of the pattern:
        capture groups => List of tuples.
        named capture groups  => List of dictionaries.
        no groups => List of strings
    """

    return _process(pattern, string, output='smart', flags=flags)


def similarity(string1: str, string2: str) -> float:
    """Returns a score based on the degree of match between string1 and string2. Is vulnerable for string length, as it
    will punish a difference in length harshly even if string1 is a subset of string2 or vice versa."""

    return difflib.SequenceMatcher(None, string1, string2).ratio()


def find_best(search_string, search_list, score_cutoff=0, case_sensitive=False):
    """Fuzzy name search from a list of strings using tregex.similarity().

    Args:
        search_string: The string used to find a sufficient match in the search_list.
        search_list: A list of strings where we search for one or more matches.
        score_cutoff: The score cutoff of the results
        case_sensitive: Specify if search is case sensitive or not.


    Returns:
        Returns the best match from the search_list
    """

    result = find(search_string=search_string,
                  search_list=search_list,
                  score_cutoff=score_cutoff,
                  case_sensitive=case_sensitive
                  )
    if result:
        return result[0]
    else:
        return None


def find(search_string: str, search_list: Iterable[str], score_cutoff: NUMBER = DEFAULT_SEARCH_SCORE_CUTOFF,
         case_sensitive: bool = False) -> List[str]:
    """Fuzzy name search from a list of strings using tregex.similarity().

    Args:
        search_string: The string used to find a sufficient match in the search_list.
        search_list: A list of strings where we search for one or more matches.
        score_cutoff: The score cutoff of the results
        case_sensitive: Specify if search is case sensitive or not.


    Returns:
        Returns a list of matching items for each item in search list with a score above score_cutoff, sorted by score.
    """

    result = find_scores(search_string=search_string, search_list=search_list, score_cutoff=score_cutoff,
                         case_sensitive=case_sensitive)

    return [item[1] for item in result]


def find_scores(search_string: str, search_list: Iterable[str], score_cutoff: NUMBER = DEFAULT_SEARCH_SCORE_CUTOFF,
                case_sensitive: bool = False) -> List[Tuple[float, str]]:
    """Fuzzy name search from a list of strings using tregex.similarity().

    Args:
        search_string: The string used to find a sufficient match in the search_list.
        search_list: A list of strings where we search for one or more matches.
        score_cutoff: The score cutoff of the results
        case_sensitive: Specify if search is case sensitive or not.


    Returns:
        Returns a list of tuples containing matching items (score, match) for each item in search list with a score
        above score_cutoff, sorted by score.
    """

    if not case_sensitive:
        search_string_p = search_string.lower()
        search_list_p = [s.lower() for s in search_list]
    else:
        search_string_p = search_string
        search_list_p = search_list

    similarity_scores = [similarity(search_string_p, item) for item in search_list_p]
    scores_cutoff = [(score, item) for score, item in zip(similarity_scores, search_list) if score >= score_cutoff]
    scores_sorted = sorted(scores_cutoff, key=lambda x: x[0], reverse=True)

    # Get the items which have a content match even if the score is too low:
    # Keep the length of the strings, so that the shortest strings get the highest match:
    contains_match = [orig for orig, case in zip(search_list, search_list_p)
                      if search_string_p in case
                      and orig not in [item[1] for item in scores_sorted]]
    contains_sorted = sorted(contains_match, key=lambda x: len(x), reverse=False)

    result = scores_sorted + [(CONTENT_MATCH_DEFAULT_SCORE, item) for item in contains_sorted]

    if not result:
        result = []

    return result
