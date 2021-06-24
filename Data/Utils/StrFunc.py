#!/usr/bin/python
import re


def wordreplace(str, original, new):
    return str.replace(original, new)


def wordtransform(data_in: dict) -> dict:
    # define desired replacements here
    rep = {"none": "NONE",
           "[]": "NONE",
           "/": ""}

    rep = dict((re.escape(k), v) for k, v in rep.items())
    pattern = re.compile("|".join(rep.keys()))
    return {k: pattern.sub(lambda m: rep[re.escape(m.group(0))], str(v)) for k, v in data_in.items()}

