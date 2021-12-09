import re
"""
find_all_digits: extract all digits from the string

find_all_digits_and_signs: extract all digits and signs (+, -, *, /) from the string

"""

_PG_REGEX_lIB = {'find_all_digits': "\d+",
                 'find_all_digits_and_signs': "\d+|\+|\*|\-|\/",
                 'clean_column_name': "\w+",
}

_PG_TEST_STRING = {'find_all_digits': ["20+11* ", "3+2*2   ", "3/2  ", " 3222+5 / 20 "],
                   'find_all_digits_and_signs': ["20+11* ", "3+2*2   ", "3/2  ", " 3222+5 / 20 "],
                    'clean_column_name':        ["Image.999", "ATHLETE", "SERIALNO", "PRICE", "CARDSET", "BUYERNAME", "SELLERNAME", "DATE_TIME"],
}


def parse_string(regex_name: str, dataset) -> list:
    """

    """
    #print(regex_name, dataset)
    return list(map(lambda x: ''.join(re.findall(_PG_REGEX_lIB.get(regex_name), x)), dataset))


class Test:
    def __init__(self):
        pass


if __name__ == "__main__":
    print(parse_string("clean_column_name", _PG_TEST_STRING.get("clean_column_name")))

    exit(0)
    print(parse_string("find_all_digits", _PG_TEST_STRING.get("clean_column_name")))
    print(parse_string("find_all_digits_and_signs", _PG_TEST_STRING.get("clean_column_name")))

    print(dir(Test()))
    print(*dir(Test), sep='\n')
