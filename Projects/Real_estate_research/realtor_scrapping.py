### youtube video: CSS Selectors: Web Scraping Tutorial

### everything has a type = submit
#sel = '[type=submit]'

### image immediate follow an element
#sel = 'a img[src]'
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

#sel = '.js-srp-listing-photos.main-photo'

#sel = 'ul li img'
#sel = 'a[href]'
## tag.class


sel = 'img.js-srp-listing-photos.main-photo'
#prefix = "Photo of \d{2,4} "
prefix = "Photo of "
surfix = "' class=\('js-srp-listing-photos'"



#pprint(r.html.find(sel))

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

    for pn in range(38, page_num_func()):
        page_num = f"/pg-{pn}" if pn >= 2 else ""
        uri = f"{city}_{state_abbr}{page_num}"
        r = s.get(f"{url_base}{uri}")
        print(list(map(lambda x: re.search(f"{prefix}(.*){surfix}", str(x)).group(1), r.html.find(sel))))
        filename = f"addr_{city}_{state_abbr}_{pn}_{pgfile.get_random_string()}.txt"

        with open(f"{pgdirectory.add_splash_2_dir(output_dir)}{filename}", 'w') as file:
            for addr in list(map(lambda x: re.search(f"{prefix}(.*){surfix}", str(x)).group(1), r.html.find(sel))):
                file.write(f"{addr}\n")


prefix2 = ""
surfix2 = " \d{5}"

string_list = ["Muirfield Sq, Duluth, GA 30096"]


def methods(input_string: str, city_name: str, state_abbr: str, method_cnt: str, logger=None) -> str:
    method_dict = {'0': input_string.replace(f"{city_name.replace('-',' ')}, {state_abbr} ", ""),
                   '1': re.search(f"{prefix2}(.*){surfix2}", input_string).group(1),
                   '2': input_string.replace("Unit ", "#")
    }
    try:
        return method_dict.get(method_cnt, "no more methods")
    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)


def step2(city_name: str, state_abbr: str):
    _client = None
    addr11 = None
    #client = pgrefin.PGRedfin(prop_address="2581 Doral Dr, Duluth, GA ")

    addr_file_cnt = 0
    total_addr_cannot_process = 0
    file_stats = {}
    try:
        for filename in pgdirectory.files_in_dir(f"{pgdirectory.add_splash_2_dir(output_dir)}{city_name}"):
            addr_cnt = 0
            with open(f"{pgdirectory.add_splash_2_dir(output_dir)}{city_name}/{filename}", 'r') as file:
                for line in file.readlines():
                    try:
                        method_cnt = 0
                        print(line)
                        while True:
                            addr11 = methods(input_string=line, city_name=city_name, state_abbr=state_abbr, method_cnt=str(method_cnt)).strip()
                            print(addr11)
                            _client = pgrefin.PGRedfin(prop_address=addr11)
                            print(_client.property_id)
                            print(_client.listing_id)


                            if (_client.property_id and _client.listing_id) or addr11 == "no more methods":
                                break
                            method_cnt += 1
                        if not _client.property_id and not _client.listing_id:
                            with open(f"{pgdirectory.add_splash_2_dir(output_dir)}exception/{city_name}_exception_4_get_detail_info.txt", 'a') as except_file1:
                                except_file1.write(f"{str(line)}\n")
                            total_addr_cannot_process += 1

                        house({'property_id': _client.property_id}, {'address': str(line)})
                        house_detail({'property_id': _client.property_id}, _client.get_property_txn_history())
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




                #client = pgrefin.PGRedfin(prop_address="lines")

def house(property_id: dict, addr_data: dict):
    #print(type(property_id))
    #print(type(addr_data))
    #print({**property_id, **addr_data})

        save_to_db("re_house", {**property_id, **addr_data})


def house_detail(property_id: dict, detail_data: list):
    #print(type(property_id))
    #print(type(detail_data[0]))
    if property_id and detail_data:
        temp = []
        for detail in detail_data:
            temp.append({**property_id, **detail})
        #print(temp)
        save_to_db("re_house_detail", temp)


@pgdatabase.db_session('mysql')
def save_to_db(table_name: str, data: Union[list, dict], logger=None, db_session=None) -> bool:


    #print(db_session)
    #print(f" Start insert data for ...")
    #mysql.simple_query(f"update stock_queue set status = 'WIP' where stock_symbol = '{stock_symbol}'")
    try:
        if isinstance(data, dict):
            if not db_session.populate_data(table_name=table_name, mode="simple", data_in=data):
                with open(f"{pgdirectory.add_splash_2_dir(output_dir)}exception/exception_data_4_db.txt", 'a') as exception_file:
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
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name,err)
        return False




"""
    print(client.property_id)
    print(client.listing_id)

    client.get_property_txn_history()

    client.get_school_info()
    # print(list(mls_data['payload']['propertyHistoryInfo']['priceEstimates'].keys()))
    # print(client.get_detail_info()['payload']['propertyHistoryInfo']['priceEstimates'])
    print(client.get_detail_info()['payload']['propertyHistoryInfo'])

    # pprint(client.get_detail_info()['payload']['propertyHistoryInfo'])

    client.get_property_txn_history()
"""

#address = re.search("Photo of(.*)class=\('js-srp-listing-photos'", temp[0])

#address = re.search(f"{prefix}(.*){surfix}", temp[0]).group(1)

if __name__ == '__main__':
    """
    _addr = "11280 Bramshill Dr, Johns Creek, GA 30022"
    method_dict = {'0': _addr.replace(f"Johns Creek, GA", ""),
                   '1': re.search(f"{prefix2}(.*){surfix2}", _addr).group(1),
    }
    method_cnt = str(2)
    print (method_dict.get(method_cnt, "other"))

    exit(0)

    _addr = "7380 Jamestown Dr, Alpharetta, GA"
    
    client = pgrefin.PGRedfin(prop_address="7380 Jamestown Dr, Alpharetta, GA")
    print(client.property_id)
    print(client.listing_id)
    house({'property_id': client.property_id}, {'address': _addr})
    #house_detail({'property_id': client.property_id}, client.get_property_txn_history())


    exit(0)
    """
    step2("Johns-Creek", "GA")

    exit(0)



    #step2("Alpharetta")
    #step2("Suwanee")
    #step2("Johns-Creek")

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
    _addr = "11280 Bramshill Dr, Johns Creek, GA 30022"
    print(_addr.replace("Johns Creek, GA", ""))
    client = pgrefin.PGRedfin(prop_address=_addr.replace("Johns Creek, GA", ""))
    print(client.property_id)
    print(client.listing_id)
    print(client.get_property_txn_history())
    exit(0)
    """


    mysql = pgmysql.PGMysql("/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Real_estate_research/mysql8.yml")

    #print(list(map(lambda x: re.search(f"{prefix2}(.*){surfix2}", x).group(1), string_list)))

    #print(re.search(f"{prefix2}(.*){surfix2}", string).group(1))
    #step1("Duluth", "GA", page_number)
    #step1("Suwanee", "GA", page_number)
    #step1("Johns-Creek", "GA", page_number)
    #step1("Alpharetta", "GA", page_number)
    #step2()
    client = pgrefin.PGRedfin(prop_address="7380 Jamestown Dr, Alpharetta, GA")
    print(client.property_id)
    print(client.listing_id)
    #client.get_school_info()
    #print(client.get_detail_info()['payload']['propertyHistoryInfo'])
    #print(client.get_detail_info()['payload']['propertyHistoryInfo'])
    print(client.get_property_txn_history())




