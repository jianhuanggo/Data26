import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session
from Data.Connect import base
from Data.Config import pgconfig
from Data.Security import encrypt


class ConnectRedshift(base.Base):

    def __init__(self, args):
        if not args:
            conf = pgconfig.Config()
            self._redshift_host = getattr(conf, 'SCOOT_REDSHIFT_HOST')
            self._redshift_username = getattr(conf, 'SCOOT_REDSHIFT_USERNAME')
            self._redshift_keypass = getattr(conf, 'SCOOT_REDSHIFT_KEYPASS')
            self._redshift_secret = getattr(conf, 'SCOOT_REDSHIFT_SECRET')

            sp = encrypt.SecurityPass(db_system='redshift', postfix='')
            self._redshift_userpass = sp.gen_decrypt(entity='redshift')
            print(self._redshift_userpass)



            self._redshift_userpass = getattr(conf, 'SCOOT_REDSHIFT_PASS')
            self._redshift_port = getattr(conf, 'SCOOT_REDSHIFT_PORT')
            self._redshift_db = getattr(conf, 'SCOOT_REDSHIFT_DB')
        else:
            self._redshift_host = args.redshift_host
            self._redshift_username = args.redshift_username
            self._redshift_userpass = args.redshift_userpass
            self._redshift_port = args.redshift_port
            self._redshift_db = args.redshift_db

        try:
            self._redshift_url = f"postgresql://{self._redshift_username}:{self._redshift_userpass}@{self._redshift_host}" \
                                 f":{self._redshift_port}/{self._redshift_db}"
            self._engine_args = dict()
            self._engine_args['pool_size'] = 5
            self._engine_args['pool_recycle'] = 3600
            self._engine_args['encoding'] = 'utf-8'
            #self._engine_args['connect_timeout'] = 3600

            self._engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{self._redshift_username}:"
                                                    f"{self._redshift_userpass}@{self._redshift_host}:"
                                                    f"{self._redshift_port}/{self._redshift_db}", **self._engine_args)

            # Construct a sessionmaker object
            Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self._engine,
                                                  expire_on_commit=False))

            # Base.metadata.create_all(engine)
            self._session = Session()
            self._session.execute("select count(1) from pg_database")

        except:
            print('Exception: Error connect to database.')
            # self.logger.critical('Exception: Error connecting metadata database.')
            raise
        else:
            print(f"Connected to {self._redshift_db} on {self._redshift_host} with user: {self._redshift_username}")
            # self.logger.info('Connected to metadata Database with user:' + 'postgres')

    @property
    def url(self):
        return self._redshift_url

    @property
    def session(self):
        return self._session

    @property
    def engine(self):
        return self._engine

    def execute(self, query):
        try:
            self._session.execute(query)

        except Exception as e:
            raise (f"Encounter error in running SQL {query} error: {e}")

    def close(self):
        return self._session.close()

    def get_table_list(self):
        return self._session.execute(f"select distinct trim(nspname) ||'.'|| trim(relname) table_name from stv_tbl_perm "
                                     f"join pg_class on pg_class.oid = stv_tbl_perm.id "
                                     f"join pg_namespace on pg_namespace.oid = relnamespace "
                                     f"join pg_database on pg_database.oid = stv_tbl_perm.db_id "
                                     f"and trim(nspname) = 'prod' order by 1;")

    def get_table_list_by_schema(self, *, schema_name):
        return self._session.execute(f"select table_name from INFORMATION_SCHEMA.tables "
                                     f"where table_schema = '{schema_name}' order by 1;")

    def get_column_list_by_table(self, *, table_catalog='scootdw', schema_name, table_name):
        return self._session.execute(f"select column_name, ordinal_position FROM INFORMATION_SCHEMA.COLUMNS where "
                                     f"table_catalog = '{table_catalog}' and table_schema = '{schema_name}' "
                                     f"and table_name = '{table_name}' ")

    def get_db_metadata(self, table_name):
        return self._session.execute(f"select table_name, column_name, data_type, character_maximum_length, numeric_precision, "
                                     f"numeric_scale FROM INFORMATION_SCHEMA.COLUMNS WHERE table_catalog = 'scootdw' "
                                     f"AND table_schema = 'prod' AND table_name = '{table_name}' order by 1;").fetchall()

    def check_duplicate(self, primary_key, table_name):
        return self._session.execute(f"select {primary_key}, count(1) from {table_name} "
                                     f"group by {primary_key} having count(1) > 1;").fetchall()

    def __repr__(self):
        return f"{self.__class__.__name__}({self._redshift_username}@{self._redshift_host})"


if __name__ == '__main__':
    cr = ConnectRedshift(args=None)


