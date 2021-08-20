import os
import re

import requests
import json
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from collections import Counter
from pathlib import Path
from transformers import pipeline, AutoModelForTokenClassification, AutoTokenizer


#https://gist.github.com/Fluidbyte/2973986#file-common-currency-json

def render_JS(URL):
    session = HTMLSession()
    r = session.get(URL)
    r.html.render()
    return r.html.text


headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }

def data_acquisition(filepath: str, top_data_num: int = 1):
    with open(filepath) as file:
        x = file.read()
        soup = BeautifulSoup(x, 'html.parser')
        y = soup.find_all("div")

        ### get 5 layer depth
        a = sorted([(len(x), num) for num, x in enumerate(y)], key=lambda x: x[0], reverse=True)
        return y, [x[1] for x in a[:top_data_num]]

#url = "https://wax.atomichub.io/market?collection_name=mlb.topps&order=asc&sort=price&symbol=WAX"
#req = requests.get(url, headers)
#a = render_JS(url)

# '/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/stealth.txt'
# '/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/selenium.txt'
def extract(label, elements):
    #print({f"{label}_{_ind}": _val.text for _ind, _val in enumerate(elements) if _val.text})
    #print(elements)
    #exit(0)
    return {f"{label}_{_ind}": _val.text for _ind, _val in enumerate(elements) if _val.text}

"""
def extract_v2(label, elements):
    #print(label, elements)
    _total = []
    #print(len(elements))
    #print(type(elements))
    if isinstance(elements, str):
        #print(elements)
        return [elements]
    elif len(elements) == 0:
        if label == "img":
            _elem = elements['src'] or elements.find_all('img')['src']
            return [_elem]
        return [elements.text]
    elif len(elements) == 1:
        #print(label, elements)
        if label == "img":
            _elem = elements[0]['src'] or elements.find_all('img')['src']
            return [_elem]
        return [elements.text]
    else:
        #return list(map(lambda x: extract_v2(label, x), elements))
        
        for sub_item in elements:
            #print(type(elements))
            #print(type(sub_item))
            _total += extract_v2(label, sub_item)
        return _total
    # pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
    # for sub_item in elements.find_all(label):
    #    _total += extract_v2(label, sub_item)
    # return _total
"""

### depth extractor
def extract_v2(label, elements):
    _total = []
    while True:
        try:
            if isinstance(elements, str):
                return [(label, elements)]
            elif len(elements) == 0:
                if label == "img":
                    _elem = elements['src'] or elements.find_all('img')['src']
                    return [(label, _elem)]
                return [(label, elements.text)]
            elif len(elements) == 1:
                if label == "img":
                    _elem = elements[0]['src'] or elements.find_all('img')['src']
                    return [(label, _elem)]
                return [(label, elements.text)]
            else:
                for sub_item in elements:
                    _total += extract_v2(label, sub_item)
                return _total
        except Exception as err:
            continue


### link extractor
def extract_3(label, elements):
    _total = []
    while True:
        try:
            for _a in elements.find_all(label, href=True):
                if _a.text:
                    _total.append((label, _a['href']))
                    #print(_a['href'])
            return _total
        except Exception as err:
            continue



def extract_v1__(label, elements):
    _total = []
    if isinstance(elements, str):
        return [elements]
    elif len(elements) == 1:
        return [elements.text]
    else:
        for sub_item in elements:
            _total += extract_v1__(label, sub_item)
        return _total






"""
def _extract_data(pg_tag_list, pg_data):
    _data_ext_dict = {'0': pg_data.find_all('img')

                      }
    for x in set(tag.name for tag in item.find_all()):
        for sub_item in item.find_all(x):
            _data += extract_v2(x, item)
"""



def f7(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def pg_to_str(pg_data):
    if isinstance(pg_data, (list, tuple)):
        return '_'.join(str(pg_data))
    else:
        return str(pg_data)


"""
        if _pg_data and list(filter(_pg_data.startswith, _removal_character)) != []:
            _pg_data = _pg_data[1:]
        if _pg_data and list(filter(_pg_data.endswith, _removal_character)) != []:
            _pg_data = _pg_data[:-1]

"""

def pg_detect_price(pg_data):
    #_price_regx = "(?<=\s\$)(\d+)(?=\s)"
    #_price_regx = f"\$\d*[.,]?\d*"
    #_removal_character = ['(', ')']
    _pg_data = pg_data if isinstance(pg_data, str) else pg_to_str(pg_data)
    #print(_pg_data)
    with open('/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/currency.json', 'r') as file:
        _pg_currency = json.load(file)
        for _curr in [_val["symbol"] for _, _val in _pg_currency.items()]:
            _price_regx = f"{_curr}\d*[.,]?\d*"
            _result = ''.join(re.findall(_price_regx, _pg_data))
            #print(_result)
            #print(len(_result))
            #print(len(pg_data) * 0.5)
            if len(_result) > len(pg_data) * 0.5:
                return True
            #print(_pg_data)
            #print(re.findall(_price_regx, _pg_data))
            #print(re.search(r'(?<=\s\$)(\d+)(?=\s)', _pg_data, re.IGNORECASE))
            #print(re.match(_price_regx, _pg_data, re.IGNORECASE))

            # if _pg_data and {_val["symbol"]: _ind for _ind, _val in enumerate(_pg_currency.values())}.get()
            # print({_val["symbol"]: _ind for _ind, _val in enumerate(_pg_currency.values())})
    return False

#print(pg_detect_price("($0.28)"))
#exit(0)

def pg_detect_default(pg_data):
    return True


def pg_generate_label(pg_index, pg_data, pg_num):
    _pg_label_func = {'0': (pg_detect_price, f"price_{pg_num}"),
                      '1': (pg_detect_default, f"text_{pg_num}")
                      }
    for _, func in _pg_label_func.items():
        if func[0](pg_data):
            return func[1]

    #list(filter(_pg_data.startswith, [_val["symbol"] for _, _val in _pg_currency.items()])) != []:
    # #pg_generate_label("a", "($0.29)", 1)
    # ##print(pg_detect_price("AAA VVVV"))


def f8(seq):
    seen = set()
    seen_add = seen.add
    return {pg_generate_label(x[0], x[1], ind): pg_to_str(x[1]) for ind, x in enumerate(seq) if x[1] and not (x[1] in seen or seen_add(x[1]))}


def extract_data(pg_data):
    _summary = []
    for index, item in enumerate(pg_data):
        _data = []
        #print(item)
        #print(f7(sorted(tag.name for tag in item.find_all())))
        for x in f7(sorted((tag.name for tag in item.find_all()))):
            for sub_item in item.find_all(x):
                _data += extract_v2(x, sub_item)

        for x in ('a', 'div'):
            _result = extract_3(x, item)
            _data += _result if _result else []

        #print(set(_data + extract_v3("div", item)))

        print(f8(_data))
        #exit(0)
        #print({f"attrib_{_ind}": _val for _ind, _val in enumerate(set(_data)) if _val})
    print(_summary)


if __name__ == "__main__":
    summarizer = pipeline("summarization")
    summarizer("Sam Shleifer writes the best docstring examples in the whole world", min_length=5, max_length=20)

    exit(0)


    _pg_file_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/"
    Path(os.path.join(_pg_file_dir, "selenium.txt"))
    if Path.exists:
        _pg_data, _pg_data_index = data_acquisition('/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/selenium.txt')
        for item in _pg_data_index:
            extract_data(_pg_data[item])
    exit(0)



##############

    ###table
    ###div
    ###li
    ###scripts
    ###a

    #.find_all("option")
    #y = soup.find_all("select", class_ = "form-control mb-2")[2]