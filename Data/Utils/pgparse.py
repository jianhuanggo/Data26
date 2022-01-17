import re
import json
import inspect
from collections import abc
from types import SimpleNamespace
from typing import Generator, Tuple
from Meta import pggenericfunc
from functools import wraps
import contextlib


def firstint(string: str) -> Tuple[int, str]:
    _first_number = ""
    for i in range(len(string)):
        if string[i].isdigit():
            _first_number += string[i]
        elif _first_number != "":
            return int(_first_number), string[i:]
    if _first_number != "":
        return int(_first_number), ""
    else:
        return None, None

def pg_nested_object_print(native_object: object):
    if native_object:
        if isinstance(native_object, (tuple, list)):
            for item in native_object:
                if isinstance(item, (tuple, list, dict)):
                    pg_nested_object_print(item)
                else:
                    print(item)
        elif isinstance(native_object, dict):
            for key, value in native_object.items():
                if isinstance(value, dict):
                    pg_nested_object_print(value)
                else:
                    print(f"{key}:{value}")
        else:
            print(native_object)


def pg_flatten_object(nested: object, logger=None) -> Generator[object, None, None]:
    try:
        if nested is not None:
            if isinstance(nested, abc.Mapping):
                for key, value in nested.items():
                    yield key
                    if isinstance(value, (abc.Mapping, list, tuple)):
                        yield from pg_flatten_object(value)
                    else:
                        #yield f"{key}:{value}"
                        yield value
            elif isinstance(nested, (list, tuple)):
                for value in nested:
                    if isinstance(value, (abc.Mapping, list, tuple)):
                        yield from pg_flatten_object(value)
                    else:
                        yield value
            else:
                yield nested

    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)


def pg_common_text_extract(input_text, parameter: dict, logger=None):

    extract_method = {'0': {'search': f"{parameter.get('prefix','')}(.*){parameter.get('surfix','')}"},
                      '1': {'output': str(input_text)}
                     }

    for key, val in extract_method.items():
        try:
            for cmd, context in val.items():
                if cmd == "search":
                    return re.search(val, input_text).group(1)
                else:
                    return val['output']
        except:
            if key == sorted(extract_method.keys())[-1]:
                print(f"extract method {key} failed")
            continue


def parse_argument(argument_string):
    dict = {item.split(':')[0]: item.split(':')[1] for item in argument_string.split(';')}
    return SimpleNamespace(**dict)


def convert_to_parameter_str(sns):
    text = ''
    for key, value in sns.__dict__.items():
        text += ''.join(key + ':' + value + ';')
    return text[:-1]


def ns_to_json(ns_object):
    return json.dumps(ns_object.__dict__)


def json_to_ns(json_object):
    return SimpleNamespace(**json.loads(json_object))


if __name__ == '__main__':
    test = SimpleNamespace(a='test1', b=5)
    print(test)
    out = ns_to_json(test)
    print("this is fine" + str(out))

    exit(0)
    input = json_to_ns(out)
    print(input)
    print(hasattr(input, "a"))
    print(hasattr(input, "c"))
    print("c" in input.__dict__)
    exit(0)

    argu_string = "source_system:rds;source_object:batteries;target_system:redshift;target_object:test_batteries;highwatermark:;" \
                  "timestamp_col:created_at,updated_at"

    x = parse_argument(argu_string)

    print(convert_to_parameter_str(x))

    y = convert_to_parameter_str(x)
    print(parse_argument(y))

