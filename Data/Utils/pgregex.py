import re

from typing import List

def pg_string_escaping(string: str) -> str:
    return string.translate(str.maketrans({"-": r"\-",
                                              "]": r"\]",
                                              "\\": r"\\",
                                              "^": r"\^",
                                              "$": r"\$",
                                              "*": r"\*",
                                              ".": r"\."}))


def pg_separate_num_string(string: str) -> List:
    return re.split('(\d+)', string)


def pg_count_overlapping(pattern, string_to_search):
    return len(re.findall(pattern, str(string_to_search)))


if __name__ == "__main__":
    print(pg_separate_num_string("ass1112sss12*"))
    print(pg_separate_num_string("42"))
    print(pg_separate_num_string("0-2147483647"))