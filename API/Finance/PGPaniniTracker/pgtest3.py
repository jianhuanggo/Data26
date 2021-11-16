import os
import re
import requests
import json
from bs4 import BeautifulSoup
import bs4
from requests_html import HTMLSession
from collections import Counter
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from transformers import AutoModelForTokenClassification, AutoTokenizer

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
        #soup = BeautifulSoup(x, 'html5lib')
        y = soup.find_all("div")

        ### get 5 layer depth
        #bs4.element.Comment

        a = sorted([(len(x), num) for num, x in enumerate(y) if not isinstance(x, bs4.element.Comment)], key=lambda x: x[0], reverse=True)
        print(a)
        return y, [x[1] for x in a[:top_data_num]]


def extract(label, elements):
    # print({f"{label}_{_ind}": _val.text for _ind, _val in enumerate(elements) if _val.text})
    # print(elements)
    # exit(0)
    return {f"{label}_{_ind}": _val.text for _ind, _val in enumerate(elements) if _val.text}


### depth extractor
def extract_v2(label, elements):
    _total = []
    while True:
        try:
            # print(type(elements))
            # print(len(elements))
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
                # print(len(str(elements)))
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
                    # print(_a['href'])
            return _total
        except Exception as err:
            continue

def f7(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def pg_to_str(pg_data):
    if isinstance(pg_data, (list, tuple)):
        return '_'.join(str(pg_data))
    else:
        return str(pg_data)

def pg_detect_price(pg_data):
    # _price_regx = "(?<=\s\$)(\d+)(?=\s)"
    # _price_regx = f"\$\d*[.,]?\d*"
    # _removal_character = ['(', ')']
    _pg_data = pg_data if isinstance(pg_data, str) else pg_to_str(pg_data)
    # print(_pg_data)
    with open('/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/currency.json',
              'r') as file:
        _pg_currency = json.load(file)
        for _curr in [_val["symbol"] for _, _val in _pg_currency.items()]:
            _price_regx = f"{_curr}\d*[.,]?\d*"
            _result = ''.join(re.findall(_price_regx, _pg_data))
            # print(_result)
            # print(len(_result))
            # print(len(pg_data) * 0.5)
            if len(_result) > len(pg_data) * 0.5:
                return True
            # print(_pg_data)
            # print(re.findall(_price_regx, _pg_data))
            # print(re.search(r'(?<=\s\$)(\d+)(?=\s)', _pg_data, re.IGNORECASE))
            # print(re.match(_price_regx, _pg_data, re.IGNORECASE))

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

    # print(pg_detect_price("($0.28)"))
    # exit(0)


def pg_detect_default(pg_data):
    return True


def pg_generate_label(pg_index, pg_data, pg_num):
    _pg_label_func = {'0': (pg_detect_price, f"price_{pg_num}"),
                      '1': (pg_detect_default, f"text_{pg_num}")
                      }
    for _, func in _pg_label_func.items():
        if func[0](pg_data):
            return func[1]

    # list(filter(_pg_data.startswith, [_val["symbol"] for _, _val in _pg_currency.items()])) != []:
    # #pg_generate_label("a", "($0.29)", 1)
    # ##print(pg_detect_price("AAA VVVV"))


def f8(seq):
    seen = set()
    seen_add = seen.add
    return {pg_generate_label(x[0], x[1], ind): pg_to_str(x[1]) for ind, x in enumerate(seq) if
            x[1] and not (x[1] in seen or seen_add(x[1]))}
    # a = [x for x in seq if x[1] and not (x[1] in seen or seen_add(x[1]))]
    # print(a)
    # exit(0)


def extract_v4(label, elements):
    _total = []
    while True:
        try:
            # print(type(elements))
            # print(len(elements))
            if isinstance(elements, str):
                print(f"in extract_v4: str element: {elements}")
                print(type(elements))
                return [(label, elements)]
            elif len(elements) == 0:
                print(f"in extract_v4: 0 element: {str(elements)}")
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
                print(f"in extract_v4: only element attribute: {elements.attrs}")
                print(f"in extract_v4: only element: {str(elements)}")
                return [(label, elements.text)]
            else:
                # print(len(str(elements)))
                for sub_item in elements:
                    _total += extract_v4(label, sub_item)
                return _total
        except Exception as err:
            continue


def extract_v5(label, elements):
    _total = []
    while True:
        try:
            if isinstance(elements, (bs4.element.Comment, bs4.element.NavigableString)):
                return []
            elif isinstance(elements, str):
                print(f"in extract_v5: str element: {elements}")
                print(type(elements))
                return [(label, elements)]
            elif len(elements) == 0:
                print(f"in extract_v5: 0 element: {str(elements)}")
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
                print(f"in extract_v5: only element attribute: {elements.attrs}")
                print(f"in extract_v5: only element: {str(elements)}")
                return [(label, elements.text)]
            else:
                # print(len(str(elements)))
                for sub_item in elements:
                    _total += extract_v5(label, sub_item)
                return _total
        except Exception as err:
            continue



def extract_types(label, elements):
    _total = []
    while True:
        try:
            if isinstance(elements, str):
                return [type(elements)]
            elif len(elements) == 0:
                try:
                    if label == "img":
                        _elem = elements['src'] or elements.find_all('img')['src']
                        return [type(_elem)]
                except Exception as err:
                    return []
                return [type(elements)]
            elif len(elements) == 1:
                try:
                    if label == "img":
                        _elem = elements[0]['src'] or elements.find_all('img')['src']
                        return [type(_elem)]
                except Exception as err:
                    return []
                return [type(elements)]
            else:
                for sub_item in elements:
                    _total += extract_types(label, sub_item)
                return list(set(_total))
        except Exception as err:
            continue



"""
def interested_data(tag: str, element):
    _pg_valuable_label = {"div": (["class", "title", "price"],
                          "span": ["class", "title", "price"],
                          "li": ["class", "title", "price"]
                          "other": []

    }
"""

def extract_data(pg_data):
    # print("aaaaa")

    _summary = []
    _count = 0
    ### bs4.element.Comment
    ### bs4.element.Tag
    _data = []

    for _test in [x for x in pg_data if not isinstance(x, bs4.element.Comment)]:
        _data += extract_v5('div', _test)
        #_data += extract_types('div', _test)
    print(_data)
    exit(0)



    for index, item in enumerate(pg_data):
        try:
            # print(f"index: {index}")
            # print(f"length: {len(item)}")
            print(type(item))
            if not isinstance(item, bs4.element.Comment):
                print("i am here")
                _data += extract_v4('div', item)
            print(_data)
            exit(0)
            for sub_item in item.find_all('div'):
                _data += extract_v4('div', sub_item)
                #print(sub_item)
                print(_data)
                exit(0)


            print(f"tags: {f7(sorted(tag.name for tag in item.find_all()))}")
            for x in f7(sorted((tag.name for tag in item.find_all()))):
                for sub_item in item.find_all(x):
                    # print(f"x: {x}")
                    #print(x)
                    #print(type(x))
                    #print(item)
                    print(type(sub_item))
                    print(f"attributes: {sub_item.attrs}")
                    print(f"sub_item: {sub_item}")
                    _data += extract_v4(x, sub_item)
                    print(f"data: {_data}")
                    if _count > 3:
                        exit(0)
                    else:
                        _count += 1
            for x in ('a', 'div'):
                _result = extract_3(x, item)
                _data += _result if _result else []

            # print(set(_data + extract_v3("div", item)))

            print(f8(_data))
        except Exception as err:
            continue
        # exit(0)
        # print({f"attrib_{_ind}": _val for _ind, _val in enumerate(set(_data)) if _val})
    print(_summary)

    # summarizer = pipeline("summarization")
    # print(summarizer("Sam Shleifer writes the best docstring examples in the whole world", min_length=5, max_length=20))


def extract_data2(pg_data):
    _summary = []
    # print(type(pg_data))
    # print(len(pg_data))
    while len(pg_data) > 3:
        _sorted = sorted([(len(str(x[1])), x[0], x[1]) for x in enumerate(pg_data)], reverse=True)
        # print(_sorted[0][0])
        # print(sum([x[0] for x in _sorted]))
        # print(_sorted[0][0]/sum([x[0] for x in _sorted]))
        # print([(x[0], x[1]) for x in _sorted])
        # if len(str(_sorted[0][2])) == 156683:
        #    break
        if _sorted[0][0] / sum([x[0] for x in _sorted]) < 0.5:
            return pg_data
        pg_data = _sorted[0][2]

    # print(str(_sorted[0][2]))
    return _sorted[0][2]

def get_count(pg_data, pg_tag):
    return pg_data.count(f"<{pg_tag}")


def test_case(dirpath: str, filename: str):
    Path(os.path.join(dirpath, filename))

    if Path.exists:
        _pg_data, _pg_data_index = data_acquisition(os.path.join(dirpath, filename))
        #print(_pg_data_index)
        #print(_pg_data[_pg_data_index[0]])
        #exit(0)
        for item in _pg_data_index:
            _cleaned_data = extract_data2(_pg_data[item])
            extract_data(_cleaned_data)


if __name__ == "__main__":
    # assign_label()
    # exit(0)

    _pg_file_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/"
    # Path(os.path.join(_pg_file_dir, "selenium.txt"))
    #test_case(_pg_file_dir, "selenium.txt")
    #test_case(_pg_file_dir, "selenium.txt.backup.08192021")
    #test_case(_pg_file_dir, "ad8ef04b_selenium.txt")
    #test_case(_pg_file_dir, "44b2f6f3_selenium.txt")


    #test_case(_pg_file_dir, "1d9beaeb_selenium.txt")
    #test_case(_pg_file_dir, "1bd2ec38_selenium.txt")

    test_case(_pg_file_dir, "def6e85e_selenium.txt")

    #test_case(_pg_file_dir, "930587e7_selenium.txt")
