from Data.Utils import db

def parse_len(str):
    return str.split('(')[0], str.split('(')[-1].split(')')[0]

@db.connect('rds')
def column_type_rds(table_name, db_instance=None):
    return db_instance.get_db_metadata(table_name)


@db.connect('redshift')
def column_type_redshift(table_name, db_instance=None):
    return db_instance.get_db_metadata(table_name)

@db.connect('meta')
def populate_metadata(database_name, table_name, column_list, db_instance=None):

    for item in column_list:
        if database_name == 'RDS':
            if item[2] == 'integer':
                print(f"insert into metaschema.scd_table_metadata (tbl_id, database_name, tbl_name, col_name, col_type, col_length, col_scale) "
                      f"values (nextval('metaschema.table_meta_seq'), '{database_name}', '{table_name}', '{item[1]}', '{item[2]}', '{item[4]}', '{item[5]}')")
                db_instance.execute(f"insert into metaschema.scd_table_metadata (tbl_id, database_name, tbl_name, col_name, col_type, "
                                    f"col_length, col_scale) values (nextval('metaschema.table_meta_seq'),'{database_name}', '{table_name}', '{item[1]}', "
                                    f"'{item[2]}', '{item[4]}', '{item[5]}')")
            else:
                db_instance.execute(f"insert into metaschema.scd_table_metadata (tbl_id, database_name, tbl_name, col_name, col_type, "
                                    f"col_length, col_scale) values (nextval('metaschema.table_meta_seq'),'{database_name}', '{table_name}', "
                                    f"'{item[1]}', '{item[2]}', '{item[3]}', '{item[5]}')")
        elif database_name == 'Redshift':
            if '_sdc_' not in item[1]:
                if item[2] in ('bigint', 'boolean', 'timestamp without time zone', 'double precison'):
                    col_length = col_scale = "None"
                    print(f"insert into metaschema.scd_table_metadata (tbl_id, database_name, tbl_name, col_name, col_type, col_length, "
                          f"col_scale) values (nextval('metaschema.table_meta_seq'),'{database_name}', '{table_name}', '{item[1]}', '{item[2]}', "
                          f"'{col_length}', '{col_scale}')")
                    db_instance.execute(f"insert into metaschema.scd_table_metadata (tbl_id, database_name, tbl_name, col_name, col_type, "
                                        f"col_length, col_scale) values (nextval('metaschema.table_meta_seq'),'{database_name}', '{table_name}', "
                                        f"'{item[1]}', '{item[2]}', '{col_length}', '{col_scale}')")
                else:
                    col_type, col_length = parse_len(item[2])
                    if ',' in col_length:
                        col_length, col_scale = col_length.split(',')[0], col_length.split(',')[1]
                    else:
                        col_scale = "None"
                    print(f"insert into metaschema.scd_table_metadata (tbl_id, database_name, tbl_name, col_name, col_type, col_length, "
                          f"col_scale) values (nextval('metaschema.table_meta_seq'), '{database_name}', '{table_name}', '{item[1]}', '{col_type}', "
                          f"'{col_length}', '{col_scale}')")
                    db_instance.execute(f"insert into metaschema.scd_table_metadata (tbl_id, database_name, tbl_name, col_name, "
                                        f"col_type, col_length, col_scale) values (nextval('metaschema.table_meta_seq'), '{database_name}', '{table_name}', "
                                        f"'{item[1]}', '{col_type}', '{col_length}', '{col_scale}')")

    db_instance.execute('commit')

if __name__ == '__main__':
    col_list = column_type_rds('ride_updates')
    print(col_list)
    populate_metadata('RDS', 'ride_updates', col_list)
    col_list = column_type_redshift('ride_updates')
    print(col_list)
    populate_metadata('Redshift', 'ride_updates', col_list)
