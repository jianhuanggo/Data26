
#https://stackoverflow.com/questions/3368969/find-string-between-two-substrings

def partition_find(string, start, end):
    return string.partition(start)[2].rpartition(end)[0]


def re_find(string, start, end):
    # applying re.escape to start and end would be safer
    return search(start + '(.*)' + end, string, DOTALL).group(1)


def index_find(string, start, end):
    return string[string.find(start) + len(start):string.rfind(end)]

