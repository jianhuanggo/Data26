from select import select
import json
import time
from datetime import datetime
import logging
import argparse
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import sys
import json
import uuid
import subprocess

_version_ = 0.1


def get_random_filename(file_name):
    random_file_name = '_'.join([str(uuid.uuid4().hex[:6]), file_name])
    return random_file_name


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='Postgresql Write-Ahead Logging (WAL) JSON Parser')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-d', '--dbname', action='store', dest='database_name',
                               required=True, help='The name of the database')
        argparser.add_argument('-o', '--host', action='store', dest='host_name', required=True,
                               help='The host name of the database')
        argparser.add_argument('-u', '--username', action='store', dest='user_name', required=True,
                               help='The user name to login to the database and have access to WAL plug-in')
        argparser.add_argument('-p', '--password', action='store', dest='password', required=True,
                               help='The password to login to the database')
        argparser.add_argument('-r', '--port', action='store', dest='port_string', default="5432",
                               help='The database port number')
        argparser.add_argument('-t', '--testflag', action='store', type=bool, dest='test_flg',
                               default=False,
                               help='seconds to sleep between polling attempts')
        argparser.add_argument('-w', '--walslot', action='store', dest='wal_slot', required=True,
                               help='seconds to sleep between polling attempts')
        logging.debug('parsing argparser arguments')
        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    return args


def telnet(*, servername: str, port: str):
    print("Testing network connection... ")
    output, result = subprocess.getstatusoutput(f"telnet {servername} {port}")
    if output == 0:
        print("Done")
        return True
    else:
        print("Failed")
        return False


def nmap(*, servername: str, port: str):
    print("Testing network connection... ")
    output, result = subprocess.getstatusoutput(f"nmap  -Pn -p {port} {servername} ")
    if output == 0:
        print("Done")
        return True
    else:
        print("Failed")
        return False


class PostgresLogParse:

    def __init__(self, args):

        self.args = args

        try:
            self._my_connection = psycopg2.connect(dsn=f"dbname='{self.args.database_name}' "
                                                       f"host='{self.args.host_name}' "
                                                       f"user='{self.args.user_name}' "
                                                       f"password='{self.args.password}'",
                                                   connection_factory=LogicalReplicationConnection)
            self._cur = self._my_connection.cursor()
            #self._cur.start_replication(slot_name='walslot', options={'pretty-print': 1}, decode=True)
            self._cur.start_replication(slot_name=self.args.wal_slot, options={'pretty-print': 1}, decode=True)

        except Exception as err:
            raise err

        self.count = 0
        self.initial_filename = get_random_filename('replication.log')
        self.filename = None
        self.last_lsn = None
        self.time_sleep = 0
        self.current_time = datetime.now()
        self.msg = None
        self.keepalive_interval = 10
        self.list_files = []
        if self.args.test_flg:
            self.lines_per_file = 100
        else:
            self.lines_per_file = 100000
        self.string_text = None
        self.test_list = []
        self.result_count = 0



    @property
    def cur(self):
        return self._cur

    def consume(self, msg):
        temp = dict(json.loads(msg.payload))
        if not self.args.test_flg:
            json.dump(msg.payload, self.filename)
        else:
            for item in temp.values():
                try:
                    self.test_list.append(dict(item[0]))
                except Exception as err:
                    pass

    def persist_data(self):
        while True:
            try:
                self.msg = self._cur.read_message()
            except psycopg2.DatabaseError:
                self._cur = self._my_connection.cursor()

            if self.msg:
                assert self.msg.cursor == self.cur
                self.consume(self.msg)
                self.count += 1
                print(f"Reading record #{self.count}")
                if self.args.test_flg and self.count == 10:
                    break

                if self.count > self.lines_per_file:
                    try:
                        self.filename.close()
                        if self.args.test_flg:
                            break
                        self.filename = open(get_random_filename('replication.log'), 'a')
                        self.count = 0
                    except Exception as err:
                        raise err

                self.msg.cursor.send_feedback(
                    flush_lsn=self.msg.data_start,
                    reply=True
                )

                self.last_lsn = self.msg.data_start

            else:
                now = datetime.now()
                timeout = self.keepalive_interval - (now - self.cur.io_timestamp).total_seconds()
                try:
                    sel = select([self.cur], [], [], max(0, timeout))
                    if not any(sel):
                        self.cur.send_feedback()  # timed out, send keepalive message
                except InterruptedError:
                    pass  # recalculate timeout and continue

            now = datetime.now()
            time_diff = now - self.current_time
            if time_diff.total_seconds() > 20:
                print("send feedback again")
                self._cur.send_feedback(
                    flush_lsn=self.last_lsn,
                    reply=True
                )
                self.current_time = now

    def test(self):
        for item in self.test_list:
            if item['kind'] in ('insert', 'update', 'delete'):
                self.result_count += 1

        if self.result_count >= 7:
            print("This test is successful")


if __name__ == '__main__':
    args = get_parser()
    wat_plugin = PostgresLogParse(args)
    wat_plugin.persist_data()
    wat_plugin.test()


