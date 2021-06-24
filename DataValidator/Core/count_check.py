# !/usr/bin/env python3

from Data.Connect import postgresql
from Data.Config import pgconfig
from sqlalchemy.orm.exc import NoResultFound
from Data.Utils import db
from collections import namedtuple


class RecordCountValidation:

    def __init__(self, args):
        self._table_list = []
        self.result = {}



        self._conf = pgconfig.Config()

        self._meta_post_host = getattr(self._conf, 'SCOOT_META_POST_HOST')
        self._meta_post_username = getattr(self._conf, 'SCOOT_META_POST_USERNAME')
        self._meta_post_userpass = getattr(self._conf, 'SCOOT_META_POST_PASS')
        self._meta_post_port = getattr(self._conf, 'SCOOT_META_POST_PORT')
        self._meta_post_db = getattr(self._conf, 'SCOOT_META_POST_DB')

        meta_post_url = f"postgresql://{self._meta_post_username}:{self._meta_post_userpass}@{self._meta_post_host}/{self._meta_post_db}"

        Postgresql = namedtuple('Postgresql', ['post_host',
                                               'post_username',
                                               'post_userpass',
                                               'post_port',
                                               'post_db',
                                               'post_url'])

        args = Postgresql(post_host=self._meta_post_host,
                          post_username=self._meta_post_username,
                          post_userpass=self._meta_post_userpass,
                          post_port=self._meta_post_port,
                          post_db=self._meta_post_db,
                          post_url=meta_post_url)

        self.metadata = postgresql.ConnectPostgresql1(args)


    @property
    def table_list(self):
        return self._table_list

    def table_list_clean(self):
        self._table_list = []

    def result_clean(self):
        self.result = {}

    @db.connect('rds')
    def run(self, table_list, db_instance=None):
        if not table_list:
            return

        for item in table_list:
            print(f"Getting count for {item}")
            tabresult = db_instance.session.execute(f"select count(1) from {item}")
            self.result[item] = tabresult.fetchall()


    @db.connect('redshift')
    def run_redshift(self, table_list, db_instance=None):
        if not table_list:
            return

        for item in table_list:
            print(f"Getting count for {item}")
            tabresult = db_instance.session.execute(f"select count(1) from {item}")
            self.result[item] = tabresult.fetchall()

    def getTableListfromfile(self, filename):
        with open(filename, 'r') as file:
            for line in file:
                self._table_list.append(line.strip())

    @db.connect('rds')
    def getTableListfromDB(self, exclusion_list=None, db_instance=None):
        for item in db_instance.get_table_list().fetchall():
            if exclusion_list:
                if item[0] not in exclusion_list:
                    self._table_list.append(item[0])
            else:
                self._table_list.append(item[0])
        return self._table_list

    @db.connect('redshift')
    def getTableListfromRedshift(self, exclusion_list=None, db_instance=None):
        for item in db_instance.get_table_list().fetchall():
            if exclusion_list:
                if item[0] not in exclusion_list:
                    self._table_list.append(item[0])
            else:
                self._table_list.append(item[0])
        return self._table_list

    def print(self):
        for index, value in self.result.items():
            print(f"table {index} has {value[0][0]} records")


    def save2DBfromfile(self, is_source, filename):
        with open(filename, 'r') as f:
            line = f.readlines()

        for item in line:
            index, value = item.split(',')
            if is_source:

                self.metadata.execute(f"insert into metaschema.scd_table_count_compare (tbl_id, system_name, object_name, object_count) values (nextval('table_count_seq'), 'RDS', '{index.strip()}', '{value.strip()}')")
            else:
                self.metadata.execute(f"insert into metaschema.scd_table_count_compare (tbl_id, system_name, object_name, object_count) values (nextval('table_count_seq'), 'Redshift', '{index.strip()}', '{value.strip()}')")

            print(f"inserted to table table_count_compare with value ('{index.strip()}', '{value.strip()}')")

        self.metadata.session.commit()

    @db.connect('redshift')
    def save2DB(self, is_source, db_instance=None):
        for index, value in self.result.items():
            if is_source:
                print(f"insert into metaschema.table_count_compare (source_table, source_count) values ('{index}', '{value[0][0]}')")
                self.metadata.execute(f"insert into metaschema.table_count_compare (source_table, source_count) values ('{index}', '{value[0][0]}')")
            else:
                self.metadata.execute(f"insert into metaschema.table_count_compare (target_table, target_count) values ({index}, {value})")

        self.metadata.session.commit()


    def save2file(self, filename):
        with open(filename, 'w') as f:
            for index, value in self.result.items():
                f.write(f"{index}, {value[0][0]}\n")


    def savetometaDB(self, is_source):
        for index, value in self.result.items():
            if is_source:
                print(f"insert into metaschema.scd_table_count_compare (source_table, source_count) values ('{index}', '{value[0][0]}')")
                self.metadata.execute(f"insert into metaschema.scd_table_count_compare (tbl_id, system_name, object_name, object_count) values ('RDS', '{index}', '{value[0][0]}')")
            else:
                self.metadata.execute(f"insert into metaschema.scd_table_count_compare (tbl_id, system_name, object_name, object_count) values ('Redshift', '{index}', '{value[0][0]}')")

        self.metadata.session.commit()

    def targetmatch(self):
        for index, value in self.result.items():
            print(f"select id from table_compare where source_table = '{index.split('.')[-1]}'")
            try:
                tbl_id = self.metadata.execute(f"select id from metaschema.table_compare where source_table = '{index.split('.')[-1]}'").first()
            except NoResultFound:
                tbl_id = []  # or however you need to handle it
            #print(tbl_id.fetchone())
            if tbl_id:
                self.metadata.execute(f"update metaschema.table_compare set target_table = '{index}', target_count = '{value[0][0]}' where id = {tbl_id[0]}")

            self.metadata.session.commit()

    def __repr__(self):
        return (f"({self.__class__.__name__}")


if __name__ == '__main__':
    args = db.setup_rds()
    record = RecordCountValidation(args)
    #source_table_list = record.getTableListfromDB(['ride_updates'])
    source_table_list = record.getTableListfromDB()
    record.run(source_table_list)
    record.print()
    record.save2file("table_list.txt")
    record_keep = RecordCountValidation(args)
    record_keep.save2DBfromfile(True, "table_list.txt")
    record_keep.table_list_clean()
    record_keep.result_clean()

    record_keep = RecordCountValidation(args)
    record_keep.getTableListfromRedshift()
    record_keep.run_redshift(record_keep.table_list)
    record_keep.save2file("table_list_target.txt")
    record_target = RecordCountValidation(args)
    record_target.save2DBfromfile(False, "table_list_target.txt")




