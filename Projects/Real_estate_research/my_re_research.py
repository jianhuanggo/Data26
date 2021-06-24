import os
from datetime import datetime
from API.RealEstate import pgzillow
from Data.StorageFormat import pgstorageformat
import Data.Utils.pgdirectory as directory
from Data.Utils.pgfile import filename_remove_space


from Data.StorageFormat import pgcsv


#@pgstorageformat.storageformat('json')
def get_zillow_data(*, city, state, filename_prefix, storage_format=None):
    out_dir = '/'.join(filename_prefix.split('/')[:-1])
    if not directory.isdirectoryexist(os.path.join(out_dir)):
        print(f"Missing directory {os.path.join(out_dir)}")
        try:
            directory.createdirectory(f"{os.path.join(out_dir)}")
        except Exception as err:
            raise err
        finally:
            print(f"{os.path.join(out_dir)} is created")

    _counter = 1
    city_no_space = filename_remove_space(city)
    while True:
        data = pgzillow.PGZillow().get_info(city, state, _counter)
        print(data)
        print(type(data))
        if not data or not data['properties']:
            break
        storage_format.save(filename=f"{filename_prefix}_{city_no_space}_{state}_{_counter}.json", data=data)
        _counter += 1


@pgstorageformat.storageformat('json')
def get_zillow_data1(input_directory, storage_format=None):

    data =[]
    for filename in directory.files_in_dir(path=input_directory):
        data += storage_format.load(filename=f"{input_directory}/{filename}")['properties']
    return data


@pgstorageformat.storageformat('excel')
def get_zillow_data2(input_directory, output_filepath, storage_format=None):
    out_dir = '/'.join(output_filepath.split('/')[:-1])
    if not directory.isdirectoryexist(os.path.join(out_dir)):
        print(f"Missing directory {os.path.join(out_dir)}")
        try:
            directory.createdirectory(f"{os.path.join(out_dir)}")
        except Exception as err:
            raise err
        finally:
            print(f"{os.path.join(out_dir)} is created")
    data = list(get_zillow_data1(input_directory)[0].keys())
    print(data)
    storage_format.save_row(filename=output_filepath, data=get_zillow_data1(input_directory))
"""
    for property_item in get_zillow_data1():
        print(property_item)
        storage_format.save(filename="/Users/jianhuang/test100/New/temp/real_estate_johns_creek.csv",
                            data=property_item)
"""

if __name__ == '__main__':
    today = datetime.now().strftime("%m%d%Y")
    input_city = "suwanee"
    cleaned_city_name = filename_remove_space(input_city)
    input_state = "ga"
    raw_data_location = f"/Users/jianhuang/data/real_estate/{cleaned_city_name}"
    input_filename_prefix = raw_data_location + "/raw_on_market_re"

    #get_zillow_data(city=input_city,
    #                state=input_state,
    #                filename_prefix=input_filename_prefix)


    get_zillow_data2(raw_data_location,
                     f"/Users/jianhuang/data/real_estate/summary/on_market_{cleaned_city_name}_{input_state}_{today}.csv")



