from DataValidator.Core import table_column_type
from Data.Utils import db
from DataValidator.Model import base, Table_Count_Compare, Table_MetaData

@db.connect('rds')
def get_table_list(db_instance=None):
    return db_instance.get_table_list()

@db.connect('meta')
def create_all_table(db_instance=None):
    try:
        base.Base.metadata.create_all(db_instance.engine)
        db_instance.session.commit()
    except Exception as err:
        raise (f"Can't create all tables. {err}")

def run(table_list):
    for item in table_list:
        print(item[0])
        col_list = table_column_type.column_type_rds(item[0])
        print(col_list)
        table_column_type.populate_metadata('RDS', item[0], col_list)
        col_list = table_column_type.column_type_redshift(item[0])
        print(col_list)
        table_column_type.populate_metadata('Redshift', item[0], col_list)

if __name__ == '__main__':
    create_all_table()
    run(get_table_list().fetchall())



