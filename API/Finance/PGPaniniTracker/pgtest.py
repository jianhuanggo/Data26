import os
import re

import requests
import json
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from collections import Counter
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from transformers import AutoModelForTokenClassification, AutoTokenizer


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
        print(a)
        return y, [x[1] for x in a[:top_data_num]]

def data_acquisition1(filepath: str, top_data_num: int = 1):
    result = []
    with open(filepath) as file:
        x = file.read()
        soup = BeautifulSoup(x, 'html.parser')
        y = soup.find_all("div")

        for _index, item in enumerate(y):
            result.append((item, 1, _index,  len(item)))

        print([x[1:] for x in result])

        _result1 = [x[0] for x in result]
        #print(_result1)
        print(len(result))

        for _index1, item1 in enumerate(_result1):
            result.append((item1, 2, _index1,  len(item)))

        print([x[1:] for x in result])
        print(len(result))

        a = sorted([x for x in result], key=lambda x: x[3], reverse=True)
        print([x[1:] for x in a])
        print(len(a))
        exit(0)


        ### get 5 layer depth
        a = sorted([(len(x), num) for num, x in enumerate(y)], key=lambda x: x[0], reverse=True)
        print(a)
        return y, [x[1] for x in a[:top_data_num]]


def data_acquisition2(filepath: str, top_data_num: int = 1):
    with open(filepath) as file:
        x = file.read()
        soup = BeautifulSoup(x, 'html.parser')
        y = soup.find_all("div")

        ### get 5 layer depth
        a = sorted([(get_count(str(x), 'div'), num) for num, x in enumerate(y)], key=lambda x: x[0], reverse=True)
        print(a)
        #print(y[241])
        #b = a[0][1]
        #print(y[b])
        #exit(0)
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
            #print(type(elements))
            #print(len(elements))
            if isinstance(elements, str):
                return [(label, elements)]
            elif len(elements) == 0:
                try:
                    if label == "img":
                        _elem = elements['src'] or elements.find_all('img')['src']
                        return [(label, _elem)]
                except Exception as err:
                    return []
                return [(label, elements.text)]
            elif len(elements) == 1:
                try:
                    if label == "img":
                        _elem = elements[0]['src'] or elements.find_all('img')['src']
                        return [(label, _elem)]
                except Exception as err:
                    return []
                return [(label, elements.text)]
            else:
                #print(len(str(elements)))
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


def assign_label(data: str):
    tokenizer = AutoTokenizer.from_pretrained("dslim/bert-base-NER")
    model = AutoModelForTokenClassification.from_pretrained("dslim/bert-base-NER")

    nlp = pipeline("ner", model=model, tokenizer=tokenizer)
    example = "My name is Wolfgang and I live in Berlin"

    ner_results = nlp(example)
    print(ner_results)

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
    #a = [x for x in seq if x[1] and not (x[1] in seen or seen_add(x[1]))]
    #print(a)
    #exit(0)


def extract_data(pg_data):

    #print("aaaaa")

    _summary = []
    for index, item in enumerate(pg_data):
        try:
            #print(f"index: {index}")
            #print(f"length: {len(item)}")
            _data = []
            #print(item)
            #print(f"item: {item}")
            #print(f"tags: {f7(sorted(tag.name for tag in item.find_all()))}")
            for x in f7(sorted((tag.name for tag in item.find_all()))):
                for sub_item in item.find_all(x):
                    #print(f"x: {x}")
                    _data += extract_v2(x, sub_item)

            for x in ('a', 'div'):
                _result = extract_3(x, item)
                _data += _result if _result else []

            #print(set(_data + extract_v3("div", item)))

            print(f8(_data))
        except Exception as err:
            continue
        #exit(0)
        #print({f"attrib_{_ind}": _val for _ind, _val in enumerate(set(_data)) if _val})
    print(_summary)

    #summarizer = pipeline("summarization")
    #print(summarizer("Sam Shleifer writes the best docstring examples in the whole world", min_length=5, max_length=20))




def extract_data2(pg_data):
    _summary = []
    #print(type(pg_data))
    #print(len(pg_data))
    while len(pg_data) > 3:
        _sorted = sorted([(len(str(x[1])), x[0], x[1]) for x in enumerate(pg_data)], reverse=True)
        #print(_sorted[0][0])
        #print(sum([x[0] for x in _sorted]))
        #print(_sorted[0][0]/sum([x[0] for x in _sorted]))
        #print([(x[0], x[1]) for x in _sorted])
        #if len(str(_sorted[0][2])) == 156683:
        #    break
        if _sorted[0][0]/sum([x[0] for x in _sorted]) < 0.5:
            return pg_data
        pg_data = _sorted[0][2]

    #print(str(_sorted[0][2]))
    return _sorted[0][2]


def extract_data1(pg_data):
    from xml.dom.minidom import parse
    print("bbbb")

    _summary = []
    for index, item in enumerate(pg_data):
        #rint(item)
        print(index, len(str(item)))
        if index == 15:
            print(type(item))
            for x, y in enumerate(item):
                if x == 1:
                    print("aaaa")
                    #print(str(y))
                    for w, z in enumerate(y):
                        if x == 1:
                            print(str(z))
                        print(w, len(z))
                #print(x, len(str(y)))

            exit(0)
            #with open('/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/test.txt', 'w') as file:
            #    file.write(str(item))
            _domtree = parse('/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/test.txt')
            _group = _domtree.documentElement

            from xml.etree import ElementTree as ET
            from collections import defaultdict

            xml_tree = ET.fromstring(str(item))
            project = defaultdict(list)
            for item in xml_tree:
                for t in item:
                    # here t is s tag under item. You can have multiple tags
                    project[t.tag].append(t.text)

            print(project)
            print(dir(_group))
            print(_group)
            exit(0)
    """
    _data = []
    #print(item)
    #print(f"item: {item}")
    print(f"tags: {f7(sorted(tag.name for tag in item.find_all()))}")
    for x in f7(sorted((tag.name for tag in item.find_all()))):
        for sub_item in item.find_all(x):
            _data += extract_v2(x, sub_item
    for x in ('a', 'div'):
        _result = extract_3(x, item)
        _data += _result if _result else [
    #print(set(_data + extract_v3("div", item))
    print(f8(_data))

        #exit(0)
        #print({f"attrib_{_ind}": _val for _ind, _val in enumerate(set(_data)) if _val})
    print(_summary)
    """

def get_count(pg_data, pg_tag):
    return pg_data.count(f"<{pg_tag}")


def test_case(dirpath: str, filename: str):
    Path(os.path.join(dirpath, filename))

    if Path.exists:
        _pg_data, _pg_data_index = data_acquisition(os.path.join(dirpath, filename))
        print(_pg_data_index)
        for item in _pg_data_index:
            _cleaned_data = extract_data2(_pg_data[item])
            extract_data(_cleaned_data)


if __name__ == "__main__":
    #assign_label()
    #exit(0)


    _pg_file_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/"
    #Path(os.path.join(_pg_file_dir, "selenium.txt"))
    test_case(_pg_file_dir, "selenium.txt")
    test_case(_pg_file_dir, "selenium.txt.backup.08192021")
    test_case(_pg_file_dir, "ad8ef04b_selenium.txt")
    #test_case(_pg_file_dir, "44b2f6f3_selenium.txt")
    test_case(_pg_file_dir, "1d9beaeb_selenium.txt")
    test_case(_pg_file_dir, "930587e7_selenium.txt")
    test_case(_pg_file_dir, "def6e85e_selenium.txt")



    exit(0)


    Path(os.path.join(_pg_file_dir, "ad8ef04b_selenium.txt"))

    if Path.exists:
        _pg_data, _pg_data_index = data_acquisition2(os.path.join(_pg_file_dir, "ad8ef04b_selenium.txt"))
        print(_pg_data_index)
        for item in _pg_data_index:
            #print(_pg_data[item])

            #print(sorted([(get_count(str(y), 'div'), x) for x, y in enumerate(_pg_data[item])], key=lambda x: x[0], reverse=True))
            print(sorted([(get_count(str(y), 'div'), x) for x, y in enumerate(_pg_data[241])], key=lambda x: x[0], reverse=True))
            #for _ind1, _itm1 in enumerate(_pg_data[item]):
            #for _ind1, _itm1 in enumerate(_pg_data[241]):
                #if _ind1 == 15:
                #    print(_itm1)
                #    print(f"len: {get_count(str(_itm1), 'div')}")
                #    exit(0)
            #for index, item1 in enumerate(_pg_data[item]):
            #    print(len(item1), index)
            #exit(0)




            extract_data(_pg_data[item])
            #extract_data(_pg_data[241])
    exit(0)



##############

    ###table
    ###div
    ###li
    ###scripts
    ###a

    #.find_all("option")
    #y = soup.find_all("select", class_ = "form-control mb-2")[2]