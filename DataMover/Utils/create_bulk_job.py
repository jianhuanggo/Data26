import subprocess
import time


class JobManager:

    def __init__(self):
        self.table_list = []

    def print_comand(self, file_name):
        with open(file_name, 'r') as f:
            for line in f.readlines():
                print(f"python job_generation.py -t {str(line).strip()} -b 10000 -s normal -y normal -e prod")

    def get_table(self, file_name):
        with open(file_name, 'r') as f:
            for line in f.readlines():
                self.table_list.append(f"python job_generation.py -t {str(line).strip()} -b 10000 -s normal -y normal -e prod")

    def execute_cmd(self):
        for item in self.table_list:
            try:
                command = item.split()

                proc = subprocess.Popen(command)
                proc.wait()
                reader = proc.stdout
                print(reader)
                time.sleep(5)

            except Exception as e:
                raise e


if __name__ == '__main__':
    job_creation = JobManager()
    job_creation.get_table('table_list.txt')
    job_creation.execute_cmd()
