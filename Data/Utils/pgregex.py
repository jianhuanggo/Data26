import re


def pg_count_overlapping(pattern, string_to_search):
    return len(re.findall(pattern, str(string_to_search)))