### youtube video: CSS Selectors: Web Scraping Tutorial
from API.RealEstate import pgrealestate

### everything has a type = submit
# sel = '[type=submit]'

### image immediate follow an element
# sel = 'a img[src]'
import time
import inspect
from requests_html import HTMLSession
from pprint import pprint
import re
from Data.Utils import pgdirectory
from Data.Utils import pgfile
from API.RealEstate import pgrefin
from typing import Callable
import Data.Connect.pgmysql as pgmysql
from Meta import pggenericfunc
from Data.Connect import pgdatabase
from typing import Union

s = HTMLSession()

r = s.get('https://www.realtor.com/soldhomeprices/Duluth_GA')

# sel = '.js-srp-listing-photos.main-photo'

# sel = 'ul li img'
# sel = 'a[href]'
## tag.class


sel = 'img.js-srp-listing-photos.main-photo'
# prefix = "Photo of \d{2,4} "
prefix = "Photo of "
surfix = "' class=\('js-srp-listing-photos'"

# pprint(r.html.find(sel))

"""
temp = []
for item in r.html.find(sel):
    temp.append(str(item))
for item in temp:
    print(re.search(f"{prefix}(.*){surfix}", item).group(1))
"""
output_dir = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Real_estate_research/data"


def page_number():
    page_num = 38 + 1
    return page_num


def step1(city: str, state_abbr: str, page_num_func: Callable):
    url_base = "https://www.realtor.com/soldhomeprices/"

    for pn in range(1, page_num_func()):
        page_num = f"/pg-{pn}" if pn >= 2 else ""
        uri = f"{city}_{state_abbr}{page_num}"
        print(f"{url_base}{uri}")

        r = s.get(f"{url_base}{uri}")
        print(r.text)

        exit(0)
        for item in a:
            re.search(f"Photo of (.*)' class=\('js-srp-listing-photos", str(item)).group(1)
        print(list(map(lambda x: re.search(f"{prefix}(.*){surfix}", str(x)).group(1), r.html.find(sel))))
        filename = f"addr_{city}_{state_abbr}_{pn}_{pgfile.get_random_string()}.txt"

        with open(f"{pgdirectory.add_splash_2_dir(output_dir)}{filename}", 'w') as file:
            for addr in list(map(lambda x: re.search(f"{prefix}(.*){surfix}", str(x)).group(1), r.html.find(sel))):
                file.write(f"{addr}\n")


@pgrealestate.api_real_estate("redfin", "myredfin")
def process_addr(city_name: str, state_abbr: str, _pg_action=None):
    addr_file_cnt = 0
    total_addr_cannot_process = 0
    file_stats = {}
    _address = None
    try:
        for filename in pgdirectory.files_in_dir(f"{pgdirectory.add_splash_2_dir(output_dir)}{city_name}"):
            addr_cnt = 0
            with open(f"{pgdirectory.add_splash_2_dir(output_dir)}{city_name}/{filename}", 'r') as file:
                for line in file.readlines():
                    try:
                        method_cnt = 0
                        print(line)
                        while True:
                            _address = _pg_action.redfin.myredfin.methods(input_string=line,
                                                             city_name=city_name,
                                                             state_abbr=state_abbr,
                                                             method_cnt=str(method_cnt)).strip()
                            print(_address)
                            _pg_action.redfin.myredfin.create_client(full_addr=_address)
                            print(_pg_action.redfin.myredfin.property_id)
                            print(_pg_action.redfin.myredfin.listing_id)

                            if (_pg_action.redfin.myredfin.property_id and _pg_action.redfin.myredfin.listing_id) or _address == "no more methods":
                                break
                            method_cnt += 1

                        if not _pg_action.redfin.myredfin.property_id:
                            with open(
                                    f"{pgdirectory.add_splash_2_dir(output_dir)}exception/{city_name}_exception_4_get_detail_info.txt",
                                    'a') as except_file_1:
                                except_file_1.write(f"{str(line)}\n")
                            total_addr_cannot_process += 1
                        if _pg_action.redfin.myredfin.property_id:
                            house({'property_id': _pg_action.redfin.myredfin.property_id}, {'address': str(line)})
                            house_detail({'property_id': _pg_action.redfin.myredfin.property_id}, _pg_action.redfin.myredfin.get_property_txn_history())
                    except Exception as err:
                        print(err)
                        with open(
                                f"{pgdirectory.add_splash_2_dir(output_dir)}exception/{city_name}_exception_4_get_detail_info.txt",
                                'a') as except_file2:
                            except_file2.write(f"{str(line)}\n")
                        continue
                    addr_cnt += 1
                file_stats[filename] = addr_cnt
        print(f"total address file processed: \n {file_stats}")
        print(f"total address didn't process: {total_addr_cannot_process}")
    except Exception as err:
        print(err)


def house(property_id: dict, addr_data: dict):
    # print(type(property_id))
    # print(type(addr_data))
    # print({**property_id, **addr_data})

    save_to_db("re_house", {**property_id, **addr_data})


def house_detail(property_id: dict, detail_data: list):
    # print(type(property_id))
    # print(type(detail_data[0]))
    if property_id and detail_data:
        temp = []
        for detail in detail_data:
            temp.append({**property_id, **detail})
        # print(temp)
        save_to_db("re_house_detail", temp)


@pgdatabase.db_session('mysql')
def save_to_db(table_name: str, data: Union[list, dict], logger=None, db_session=None) -> bool:
    # print(db_session)
    # print(f" Start insert data for ...")
    # mysql.simple_query(f"update stock_queue set status = 'WIP' where stock_symbol = '{stock_symbol}'")
    try:
        if isinstance(data, dict):
            if not db_session.populate_data(table_name=table_name, mode="simple", data_in=data):
                with open(f"{pgdirectory.add_splash_2_dir(output_dir)}exception/exception_data_4_db.txt",
                          'a') as exception_file:
                    exception_file.write(f"{data}\n")
        elif isinstance(data, list):
            for _data_item in data:
                print(_data_item)
                if not db_session.populate_data(table_name=table_name, mode="simple", data_in=_data_item):
                    with open(f"{pgdirectory.add_splash_2_dir(output_dir)}exception/exception_data_4_db.txt",
                              'a') as exception_file:
                        exception_file.write(f"{data}\n")

        print(f"Data is successfully loaded to {table_name}\n\n")
        return True

    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)
        return False


if __name__ == '__main__':
    step1("Alpharetta", "GA", page_number)
    #process_addr("Johns-Creek", "GA")
    #exit(0)

    """
    # step2("Alpharetta")
    # step2("Suwanee")
    # step2("Johns-Creek")

    # "601 Astley Dr, 30097"
    # 510 Calmwater Ln, 30022
    # 135 Bellacree Rd, 30097
    # 400 Sandwedge Ln, 30022
    # 3003 Clipstone Ct, 30022
    # 390 Pelton Ct, 30022
    # 10413 Park Walk Pt Unit 24, 30022
    # 5375 Cottage Farm Rd, 30022
    # 9796 Preswicke Pt Unit 73, 30022
    # 5280 Bannergate Dr, 30022

    _addr = "5280 Bannergate Dr, 30022"
    client = pgrefin.PGRedfin(prop_address=_addr)
    print(client.property_id)
    print(client.listing_id)

    exit(0)
    """




