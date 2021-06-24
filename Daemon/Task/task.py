from Daemon.Task.Tasks import table_mover
from Daemon.Task.Tasks import history_load
from Daemon.Task.Tasks import data_profile

Selector_dic = {'data_mover': table_mover.table_mover,
                'history_load': history_load.history_load,
                'data_profiler': data_profile.table_profile}


def selector(package_name):
    #print(Selector_dic.get(package_name))
    return Selector_dic.get(package_name)


class Task:

    def __init__(self):
        self._task_list = {}

    def get_task(self, task_name):
        if task_name in self._task_list:
            return self._task_list[task_name]
        else:
            print(f"task {task_name} is not found")

    def add_task(self, task_name, task_command):
        if task_name not in self._task_list:
            self._task_list[task_name] = task_command




