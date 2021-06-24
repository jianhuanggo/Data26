import io
import pandas as pd
from Data.StorageFormat.Excel import pgexcel
from io import BytesIO
from Data.Storage import pgstorage
from Data.StorageFormat import pgstorageformat
from Data.Utils import pgdirectory
__version__ = "1.5"

data1 = "client 0, the result of 1 square is 1"
data = "col1, col2\nclient 0, the result of 0 square is 0"
_df = pd.read_csv(io.StringIO(data), sep=",", header=None)
#print(_df)
#location = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/Localdisk/Test_Localdisk/temp/client99.xlsx"
directory = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/Localdisk/Test_Localdisk/temp/"
#with pd.ExcelWriter(location) as writer:
#    _df.to_excel(writer)


@pgstorage.pgstorage(object_type='localdisk', object_name="mycomputer")
@pgstorage.pgstorage(object_type='s3', object_name="account2")
@pgstorage.pgstorage(object_type='s3', object_name="account1")
@pgstorageformat.pgstorageformat(object_type='excel', object_name="default")
@pgstorageformat.pgstorageformat(object_type='blockchain', object_name="default")
def test1(storage_s3_account1=None,
          storage_s3_account2=None,
          sformat_excel_default=None,
          sformat_blockchain_default=None,
          _pg_action=None,
          *args, **kwargs):

    print("ok")
    print(_pg_action)

    #storage_s3_account1.set_param(storage_parameter={**storage_s3_account1.storage_parameter, **{'mode': "direct", 'object_key': "test1"}})
    _pg_action['storage_s3_account1'].set_param(storage_parameter={**_pg_action['storage_s3_account1'].storage_parameter, **{'mode': "direct", 'object_key': "test1"}})


    #sformat_excel_default.set_param(storage_parameter={**sformat_excel_default.storage_parameter, **{'header': False}})
    _pg_action['storage_format_excel_default'].set_param(storage_parameter={**_pg_action['storage_format_excel_default'].storage_parameter, **{'header': False}})

    dataset = []
    #_pg_action['storage_s3_account1'].save("This is a test, 2", 'storage_format_excel_default')

    #_d = _pg_action['storage_s3_account1'].load("s3://testba8a4bda", 'storage_format_excel_default')

    #for _data in _d:
    #    for lines in _data['StreamingBody'].read().decode('utf-8').split('\n'):
    #        dataset.append(lines)


    print(f"data from s3: {dataset}")

    print(_pg_action['storage_localdisk_mycomputer']._storage_format_instance, _pg_action['storage_localdisk_mycomputer']._name, sep=" ** ")
    print(_pg_action['storage_s3_account1']._storage_format_instance, _pg_action['storage_s3_account1']._name, sep=" ** ")
    print(_pg_action['storage_s3_account2']._storage_format_instance, _pg_action['storage_s3_account2']._name, sep=" ** ")
    print(_pg_action['storage_format_excel_default']._storage_instance, _pg_action['storage_format_excel_default']._name, sep=" ** ")
    print(_pg_action['storage_format_blockchain_default']._storage_instance, _pg_action['storage_format_blockchain_default']._name, sep=" ** ")
    #df = pd.read_csv(BytesIO(bytes_data))


@pgstorage.pgstorage(object_type='s3', object_name="account1")
@pgstorageformat.pgstorageformat(object_type='excel')
def test2(storage_s3_account1=None, storage_format=None):
    storage_s3_account1.set_param(storage_parameter={**storage_s3_account1.storage_parameter, **{'mode': "direct", 'object_key': "test1"}})
    storage_format.set_param(storage_parameter={**storage_format.storage_parameter, **{'header': False}})

    storage_s3_account1.save("This is a test, 2", storage_format="excel")
    _d = storage_s3_account1.load("s3://testba8a4bda", storage_format="excel")

    print(f"data from s3: {_d}")
    for _data in _d:
        for lines in _data['StreamingBody'].read().decode('utf-8').split('\n'):
            print(lines)

    #df = pd.read_csv(BytesIO(bytes_data))


if __name__ == '__main__':
    test1()

    #excel = pgexcel.PGExcel()
    #for filename in pgdirectory.files_in_dir(directory):
    #    excel.load_file(f"{pgdirectory.add_splash_2_dir(directory)}{filename}")
    #print(excel.df)


