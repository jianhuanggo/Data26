import os
from types import SimpleNamespace
import subprocess
import time

def job_type_system(current_task, *args, **kwargs):
    loadConf = []

    #logger.info(args)
    # if "PYTHONPATH" not in os.environ and "PYTHONPATH" in args[2]:
    #     os.environ['PYTHONPATH'] = args[2]['PYTHONPATH']

    loadConf.append(current_task.job_command)
    try:
        for argument in current_task.job_argument.strip().split():
            loadConf.append(argument)
    except Exception as err:
        print(err)




    #loadConf = ['python', 'pgdaemon.py', "-i", str(daemon_id), "-t", "60", "-y", daemon_type, "start"]
    # print(loadConf)
    #self.args.logger.debug(loadConf)

    p2 = subprocess.Popen(loadConf, cwd=current_task.cwd)
    p2.wait()
    if p2.returncode != 0:
        return False
    time.sleep(2)
    print("ending debugging!!!!!!!")
    return True


if __name__ == "__main__":
    a = SimpleNamespace()
    a.job_command = "python"
    a.job_argument = '/data/mydata/envs/Data35/Data26/WebScraping2/pgscrapy/pgspiderrun2.py -i /data/mydata/envs/Data35/Data26/WebScraping2/pgscrapy/pgscrapy/pg_spider_request/panini_public_activity.yml -d /data/mydata/pg_data/panini/activity'
    a.cwd = "/data/mydata/envs/Data35/Data26/WebScraping2/pgscrapy"
    job_type_system(a)
