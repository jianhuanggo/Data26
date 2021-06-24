import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session
from Data.Security import encryption
from Data.Config import pgconfig
from Data.Connect import base
from sqlalchemy import exc
from Data.Security import encrypt
from Daemon.Model import Base


class ConnectPostgresql(base.Base):
    def __init__(self, args=None):

        if not args:
            self._conf = pgconfig.Config()
            self._host = getattr(self._conf, 'POST_HOST')
            self._username = getattr(self._conf, 'POST_USERNAME')
            self._userpass = getattr(self._conf, 'POST_PASS')
            print(self._post_userpass)
            self._port = getattr(self._conf, 'POST_PORT')
            self._db = getattr(self._conf, 'POST_DB')
            #self._post_url = getattr(conf, 'SCOOT_RDS_POST_URL')
        else:
            self._host = args.host
            self._username = args.username
            self._userpass = args.userpass
            self._port = args.port
            self._db = args.db
            #self._post_url = args.post_url

        try:
            self._postgresql_url = f"postgresql://{self._username}:" \
                                   f"{self._userpass}@{self._host}/{self._db}"

            print(self._postgresql_url)
            self._engine_args = dict()
            self._engine_args['pool_size'] = 5
            self._engine_args['pool_recycle'] = 3600
            self._engine_args['encoding'] = 'utf-8'
            #self._engine_args['connect_timeout'] = 3600


            self._engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{self._username}:"
                                                    f"{self._userpass}@{self._host}:"
                                                    f"{self._port}/{self._db}", **self._engine_args)


            # Construct a sessionmaker object
            self._Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self._engine,
                                                        expire_on_commit=False))

            self._session = self._Session()


        except:
            print('Exception: Error connecting database.')
            raise
        else:
            print(f"Connected to {self._db} on {self._host} with user: {self._username}")

    @property
    def url(self):
        return self._postgresql_url

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

    def commit(self):
        try:
            self._session.commit()

        except exc.IntegrityError:
            self._session.rollback()
            return False

        #except exc.DatabaseError as err:
            #print(f"Something wrong with committing data {err}")

        return True

    def close(self):
        self._session.close()
        print(f"Closed connection to {self._db} on {self._host} with user: {self._username}")

    def get_table_list(self):
        return self._session.execute(f"SELECT table_name FROM information_schema.tables "
                                     f"WHERE table_schema='public' and table_type != 'VIEW'")

    def get_db_metadata(self, table_name):
        return self._session.execute(f"select table_name, column_name, data_type, character_maximum_length, "
                                     f"numeric_precision, numeric_scale FROM INFORMATION_SCHEMA.COLUMNS WHERE "
                                     f"table_catalog = 'ddh3j8703l2puv' AND table_name = '{table_name}' order by 1;").fetchall()

    def __repr__(self):
        return f"{self.__class__.__name__}({self._username}@{self._host})"


if __name__ == '__main__':
    Conn = ConnectPostgresql()

