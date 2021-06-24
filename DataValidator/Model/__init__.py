from Daemon.Model.base import Base
from sqlalchemy import Column, String, Integer, Sequence, ForeignKey
import datetime


class Table_Count_Compare(Base):

    __tablename__ = 'scd_count_compare'

    tbl_id = Column(String, Sequence('table_count_seq'), primary_key=True)
    system_name = Column(String)
    object_name = Column(String)
    object_count = Column(String)

    def __init__(self, system_name, object_name, object_count):
        self.system_name = system_name
        self.object_name = object_name
        self.object_count = object_count

    def get_id(self):
        return str(self.tbl_id)

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"


class Table_MetaData(Base):

    __tablename__ = 'scd_table_metadata'

    tbl_id = Column(String, Sequence('table_meta_seq'), primary_key=True)
    database_name = Column(String)
    tbl_name = Column(String)
    col_name = Column(String)
    col_type = Column(String)
    col_length = Column(String)
    col_scale = Column(String)

    def __init__(self, database_name, tbl_name, col_name, col_type, col_length, col_scale):
        self.database_name = database_name
        self.tbl_name = tbl_name
        self.col_name = col_name
        self.col_type = col_type
        self.col_length = col_length
        self.col_scale = col_scale

    def get_id(self):
        return str(self.tbl_id)

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"


class Table_Active_Replica(Base):

    __tablename__ = 'scmd_table_active_replica'

    tbl_id = Column(String, Sequence('table_active_replica_seq'), primary_key=True)
    source_db_name = Column(String)
    target_db_name = Column(String)
    source_tbl_name = Column(String)
    target_tbl_name = Column(String)
    active_flg = Column(String)
    key_col = Column(String)

    def __init__(self, source_db_name, target_db_name, source_tbl_name, target_tbl_name, active_flg, key_col):
        self.source_db_name = source_db_name
        self.target_db_name = target_db_name
        self.source_tbl_name = source_tbl_name
        self.target_tbl_name = target_tbl_name
        self.active_flg = active_flg
        self.key_col = key_col

    def get_id(self):
        return str(self.tbl_id)

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"

"""
create table metaschema.scmd_table_active_replica (
tbl_id              SERIAL,
source_db_name      varchar,
target_db_name      varchar,
source_tbl_name     varchar,
target_tbl_name     varchar,
active_flg          char(1)
)

create sequence metaschema.table_active_replica_seq
"""