from Data.Connect import meta
from Data.Connect import postgresql
from multiprocessing import Queue
from subprocess import Popen
from Data.Utils import pgfile
import multiprocessing
from Data.Connect import redshift
from Data.Connect import s3
from pathos.multiprocessing import ProcessingPool as Pool
from Data.Utils import pgfile, pgdirectory
from Data.Config import pgconfig
import os, sys
import shutil
from Data.Utils import pgprocess as pr


class Extract:
    def __init__(self, source, target):
        self._source = source
        self._target = target
        self._source_obj = meta.meta_lookup(source)
        self._target_obj = meta.meta_lookup(target)
        self._task_list = []
        conf = pgconfig.Config()

        self._db_client_dbshell = getattr(conf, 'SCOOT_CLIENT_DBSHELL')
        self._post_host = getattr(conf, 'SCOOT_RDS_POST_HOST')
        self._post_username = getattr(conf, 'SCOOT_RDS_POST_USERNAME')
        self._post_userpass = getattr(conf, 'SCOOT_RDS_POST_PASS')
        self._post_port = getattr(conf, 'SCOOT_RDS_POST_PORT')
        self._post_db = getattr(conf, 'SCOOT_RDS_POST_DB')
        self._post_url = getattr(conf, 'SCOOT_RDS_POST_URL')


        self._sess = postgresql.ConnectPostgresql(self._post_host, self._post_port, self._post_db,
                                                  self._post_username, self._post_userpass)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._source}{self._target}"

    @property
    def source_obj(self):
        return self._source_obj

    @property
    def target_obj(self):
        return self._target_obj

    @property
    def task_list(self):
        return self._task_list

    def schedule_multi_extract(self, data_path, post_url, tablename, key, try_num=0):

        # Common variables
        limit = ''
        quote = ''
        task_list = []

        if try_num == 0:
            query_result = self._sess.execute(f"select max({key}), min({key}) from {tablename}")
            item = query_result.fetchall()
            min_val = item[0][1]
            max_val = item[0][0]
            i = min_val

            interval = 200000

            #print(directory.workingdirectory())
            data_path = f"{pgdirectory.workingdirectory()}/data/{tablename}"
            #print(data_path)

            try:
                if not pgdirectory.isdirectoryexist(data_path):
                    pgdirectory.createdirectory(data_path)
                else:
                    new_dir_name = pgdirectory.get_random_directory(data_path)
                    pgdirectory.rename_directory(data_path, new_dir_name)
                    pgdirectory.createdirectory(data_path)
            except Exception as e:
                raise ("Can not create the directory for storing data")


            while min_val < max_val:
                query_list =[]
                if min_val + interval > max_val:
                    upper_bound = max_val
                else:
                    upper_bound = min_val + interval - 1

                validation_query = f"select count(1) from {tablename} where id between {min_val} and {upper_bound} {limit}"
                print(validation_query)
                query = f"COPY (select * from {tablename} where id between {min_val} and {upper_bound}) {limit} TO stdout DELIMITER ',' CSV {quote}"
                query_list.append(tablename)
                query_list.append(validation_query)
                query_list.append(query)
                query_list.append(min_val)
                query_list.append(upper_bound)
                query_list.append(data_path)
                task_list.append(query_list)
                min_val += interval
        else:
            if not data_path:
                raise (f"data path for {tablename} does not exist!")

            for item in pgfile.get_notmatch_file(data_path):
                _, tablename, lower_bound, upper_bound, _ = item.split('_')
                query_list = []
                validation_query = f"select count(1) from {tablename} where id between {lower_bound} and {upper_bound} {limit}"
                query = f"COPY (select * from {tablename} where id between {lower_bound} and {upper_bound}) {limit} TO stdout DELIMITER ',' CSV {quote}"
                query_list.append(tablename)
                query_list.append(validation_query)
                query_list.append(query)
                query_list.append(lower_bound)
                query_list.append(upper_bound)
                query_list.append(data_path)
                task_list.append(query_list)
                os.rename(data_path + '/' + item, data_path + '/' + item + ".retry." + str(try_num))

        return task_list, data_path


    @classmethod
    def run_multi_extract(cls, query_list):
        print(f"Processing {query_list[0]} for id between {query_list[3]} and {query_list[4]}...")

        conf_run = pgconfig.Config()

        post_host = getattr(conf_run, 'SCOOT_RDS_POST_HOST')
        post_username = getattr(conf_run, 'SCOOT_RDS_POST_USERNAME')
        post_userpass = getattr(conf_run, 'SCOOT_RDS_POST_PASS')
        post_port = getattr(conf_run, 'SCOOT_RDS_POST_PORT')
        post_db = getattr(conf_run, 'SCOOT_RDS_POST_DB')
        post_url = getattr(conf_run, 'SCOOT_RDS_POST_URL')

        sess = postgresql.ConnectPostgresql(post_host, post_port, post_db, post_username, post_userpass)
        query_result = sess.execute(query_list[1])
        result = query_result.fetchall()
        table_count = result[0][0]

        db_client_dbshell = getattr(conf_run, 'SCOOT_CLIENT_DBSHELL')
        loadConf = [db_client_dbshell, post_url, "-c", query_list[2]]

        try:
            file_name_partial = pgfile.get_random_filename(query_list[0])
            file_name = f"{query_list[5]}/{file_name_partial}_{query_list[3]}_{query_list[4]}_.csv"

            f = open(file_name, "w")
            p2 = Popen(loadConf, stdout=f)
            p2.wait()

        except Exception as e:
            raise (f"something is wrong {e}\n")

        finally:
            f.close()

        file_count = pgfile.file_len(file_name)

        if table_count == file_count:
            os.rename(file_name, file_name + ".match")
            print(f"Validation of {query_list[0]} for id between {query_list[3]} and {query_list[4]} passed!")
            return True
        else:
            os.rename(file_name, file_name + ".notmatch")
            print(f"Validation of {query_list[0]} for id between {query_list[3]} and {query_list[4]} failed!")
            return False

    def schedule_retry(self, try_num, data_path):

        limit = ''
        quote = ''

        task_list = []

        for item in pgfile.get_notmatch_file(data_path):
            _, tablename, lower_bound, upper_bound, _ = item.split('_')
            query_list = []
            validation_query = f"select count(1) from {tablename} where id between {lower_bound} and {upper_bound} {limit}"
            query = f"COPY (select * from {tablename} where id between {lower_bound} and {upper_bound}) {limit} TO stdout DELIMITER ',' CSV {quote}"
            query_list.append(tablename)
            query_list.append(validation_query)
            query_list.append(query)
            query_list.append(lower_bound)
            query_list.append(upper_bound)
            query_list.append(data_path)
            task_list.append(query_list)
            os.rename(data_path + '/' + item, data_path + '/' + item + ".retry." + str(try_num))
        return task_list, data_path


#@pr.multiprocess
def run(task_list):
    return print(task_list)


my_func = pr.my_decorator(run)


if __name__ == '__main__':
    ext = Extract('postgresql', 's3')
    #print(ext.source_obj)
    #print(ext.target_obj)

    
    cpu_count = multiprocessing.cpu_count()
    p = Pool(cpu_count)


    post_url = ""
    #tablename = "ride_updates"
    tablename = 'ride_updates'
    key = "id"
    task_list, path = ext.schedule_multi_extract("", post_url, tablename, key, 0)
    result = p.map(ext.run_multi_extract, task_list)
    p.close()
    p.join()
    #print(result)
    #try_num = 1
    #while len(file.get_notmatch_file(path)) > 0:
    #    task_list, _ = ext.schedule_multi_extract(path, post_url, tablename, key, try_num)
    #    result = p.map(ext.run_multi_extract, task_list)
    #    try_num += 1

    

    #tablename = "ride_updates"
    #key = "id"
    #task_list, _ = ext.schedule_multi_extract("", "", tablename, key, 0)
    #print(run(task_list))

    #cpu_count = multiprocessing.cpu_count()
    #p = Pool(cpu_count)
    # print(sys._getframe().f_code.co_name)
    #result = p.map(run1, task_list)
    #p.close()
    #p.join()

    #print(path)
    #data_path = "/Users/jianhuang/PycharmProjects/Data/DataMover/Core/data/admins"
    #task_list, path = ext.schedule_retry(1, data_path)
    #result = p.map(ext.run_multi_extract, task_list)
    # p.close()
    # p.join()

    #candidates = [[random.randint(0, 9) for _ in range(5)] for _ in range(10)]
    #print(candidates)
    #pool = multiprocessing.Pool(processes=4)
    #results = [pool.apply_async(my_func, ([c], {})) for c in candidates]
    #results = [pool.apply_async(my_func, task_list)]
    #print(results)
    #pool.close()
    #for item in results:
        #print(item.get())
    #f = [r.get()[0] for r in results]
    #print(f)
