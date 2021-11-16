import re
"""
find_all_digits: extract all digits from the string

find_all_digits_and_signs: extract all digits and signs (+, -, *, /) from the string

"""

_PG_REGEX_lIB = {'find_all_digits': "\d+",
                 'find_all_digits_and_signs': "\d+|\+|\*|\-|\/"
}

_PG_TEST_STRING = {'find_all_digits': ["20+11* ", "3+2*2   ", "3/2  ", " 3222+5 / 20 "],
                   'find_all_digits_and_signs': ["20+11* ", "3+2*2   ", "3/2  ", " 3222+5 / 20 "]
}


def parse_string(regex_name: str) -> list:
    """

    """
    return list(map(lambda x: re.findall(_PG_REGEX_lIB.get(regex_name), x), _PG_TEST_STRING.get(regex_name)))


if __name__ == "__main__":
    print(parse_string("find_all_digits"))
    print(parse_string("find_all_digits_and_signs"))
