
import random
import pandas as pd
from typing import List


def pg_generate_data():
    _data = {"name": [chr(random.randint(1, 10) + ord("a")) * 10 for x in range(1, 10)], "price": [random.randint(1, 20) for x in range(1, 10)], "url": [f"https://{''.join([chr(random.randint(1, 10) + ord('a')) for x in range(6)])}" for x in range(1, 10)]}
    print(_data)
    return pd.DataFrame.from_dict(_data)


def pg_low_price(pg_data, name_filter: List, price_filter: List):
    print(pg_data[pg_data["name"].str.contains(name_filter[0])])


if __name__ == "__main__":
    print(pg_generate_data())
    pg_low_price(pg_generate_data(), ["dddddddddd"], [10])


