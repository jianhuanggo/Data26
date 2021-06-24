from Daemon.Model.base import Base
from sqlalchemy import Column, String, Integer, Sequence, ForeignKey
import datetime


class Threshold(Base):

    __tablename__ = 'scd_threshold'

    threshold_id = Column(String(100), primary_key=True)
    threshold_type = Column(String(100))
    threshold_value = Column(String(100))
    threshold_status = Column(String(100))

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"


    def get_id(self):
        return str(self.threshold_id)

    def __init__(self, threshold_id, threshold_name, threshold_value, threshold_status):
        self.threshold_id = threshold_id
        self.threshold_name = threshold_name
        self.threshold_value = threshold_value
        self.threshold_status = threshold_status


class Register(Base):

    __tablename__ = 'scd_register'

    daemon_id = Column(Integer, primary_key=True)
    daemon_name = Column(String(100))
    daemon_type = Column(String(100))
    pid = Column(String(100))
    status = Column(String(100))

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"

    def get_id(self):
        return str(self.daemon_id)

    def __init__(self, daemon_id, daemon_name, daemon_type, pid, status):
        self.daemon_id = daemon_id
        self.daemon_name = daemon_name
        self.daemon_type = daemon_type
        self.pid = pid
        self.status = status


class Job(Base):

    __tablename__ = 'scd_job'

    job_id = Column(String(100), primary_key=True)
    job_name = Column(String(100))
    schedule_id = Column(String(100))
    priority_value = Column(String(100))
    server_node = Column(String(100))
    hold_flag = Column(String(100))
    job_type = Column(String(100))
    job_command = Column(String(100))
    job_argument = Column(String(1000))
    system_parameter = Column(String(1000))
    time_created = Column(String(100))
    time_updated = Column(String(100))

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"

    def get_id(self):
        return str(self.job_id)

    def __init__(self, job_id, job_name, schedule_id, priority_value, server_node, hold_flag, job_type, job_command,
                 job_argument, system_parameter):
        self.job_id = job_id
        self.job_name = job_name
        self.schedule_id = schedule_id
        self.priority_value = priority_value
        self.server_node = server_node
        self.hold_flag = hold_flag
        self.job_type = job_type
        self.job_command = job_command
        self.job_argument = job_argument
        self.system_parameter = system_parameter
        self.time_created = str(datetime.datetime.now())
        self.time_updated = str(datetime.datetime.now())


class Job_Instance(Base):

    __tablename__ = 'scd_job_instance'

    job_instance_id = Column(String(100), Sequence('job_inst_id_seq'), primary_key=True)
    #job_id = Column(String, ForeignKey("Job.job_id"))
    job_id = Column(String(100))
    job_name = Column(String(100))
    schedule_id = Column(String(100))
    priority_value = Column(String(100))
    server_node = Column(String(100))
    hold_flag = Column(String(100))
    ignore_flag = Column(String(100))
    start_time = Column(String(100))
    end_time = Column(String(100))
    status = Column(String(100))
    job_comment = Column(String(500))
    job_type = Column(String(100))
    job_command = Column(String(100))
    job_argument = Column(String(1000))
    system_parameter = Column(String(1000))
    affected_row_count = Column(String(100))
    try_num = Column(Integer)
    time_created = Column(String(100))
    time_updated = Column(String(100))

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"

    def get_id(self):
        return str(self.job_instance_id)

    def __init__(self, job_id, job_name, schedule_id, priority_value, server_node, hold_flag, ignore_flag,
                 job_type, job_command, job_argument, system_parameter):
        self.job_id = job_id
        self.job_name = job_name
        self.schedule_id = schedule_id
        self.priority_value = priority_value
        self.server_node = server_node
        self.hold_flag = hold_flag
        self.ignore_flag = ignore_flag
        self.start_time = str(datetime.datetime.now())
        self.end_time = 'NONE'
        self.status = 'NEW'
        self.job_comment = 'NONE'
        self.job_type = job_type
        self.job_command = job_command
        self.job_argument = job_argument
        self.system_parameter = system_parameter
        self.affected_row_count = '0'
        self.try_num = 0
        self.time_created = str(datetime.datetime.now())
        self.time_updated = str(datetime.datetime.now())


class Job_Instance_Archive(Base):

    __tablename__ = 'scd_job_instance_archive'

    job_instance_id = Column(String(100), primary_key=True)
    job_id = Column(String(100))
    job_name = Column(String(100))
    schedule_id = Column(String(100))
    priority_value = Column(String(100))
    server_node = Column(String(100))
    hold_flag = Column(String(100))
    ignore_flag = Column(String(100))
    start_time = Column(String(100))
    end_time = Column(String(100))
    status = Column(String(100))
    job_comment = Column(String(500))
    job_type = Column(String(100))
    job_command = Column(String(100))
    job_argument = Column(String(1000))
    system_parameter = Column(String(1000))
    affected_row_count = Column(String(100))
    try_num = Column(Integer)
    time_created = Column(String(100))
    time_updated = Column(String(100))

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"

    def get_id(self):
        return str(self.job_instance_id)

    def __init__(self, job_instance_id, job_id, job_name, schedule_id, priority_value, server_node, hold_flag,
                 ignore_flag, start_time, end_time, status, job_comment, job_type, job_command, job_argument,
                 system_parameter, affected_row_count, try_num, time_created, time_updated):
        self.job_instance_id = job_instance_id
        self.job_id = job_id
        self.job_name = job_name
        self.schedule_id = schedule_id
        self.priority_value = priority_value
        self.server_node = server_node
        self.hold_flag = hold_flag
        self.ignore_flag = ignore_flag
        self.start_time = start_time
        self.end_time = end_time
        self.status = status
        self.job_comment = job_comment
        self.job_type = job_type
        self.job_command = job_command
        self.job_argument = job_argument
        self.system_parameter = system_parameter
        self.affected_row_count = affected_row_count
        self.try_num = try_num
        self.time_created = time_created
        self.time_updated = time_updated


class Job_Task_Lock(Base):

    __tablename__ = 'scd_job_task_lock'

    job_instance_id = Column(String(100), primary_key=True)
    job_id = Column(String(100))
    job_name = Column(String(100))
    worker_name = Column(String(100))
    time_created = Column(String(100))
    time_updated = Column(String(100))

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})>"

    def get_id(self):
        return str(self.job_instance_id)

    def __init__(self, job_instance_id, job_id, job_name, worker_name):
        self.job_instance_id = job_instance_id
        self.job_id = job_id
        self.job_name = job_name
        self.worker_name = worker_name
        self.time_created = str(datetime.datetime.now())
        self.time_updated = str(datetime.datetime.now())


class Job_Task_Check(Base):

    __tablename__ = 'scd_job_task_check'

    job_check_id = Column(String(100), Sequence('job_check_id_seq'), primary_key=True)
    job_instance_id = Column(String(100))
    job_id = Column(String(100))
    job_name = Column(String(100))
    worker_name = Column(String(100))
    time_created = Column(String(100))
    time_updated = Column(String(100))

    def __init__(self, job_instance_id, job_id, job_name, worker_name):
        self.job_instance_id = job_instance_id
        self.job_id = job_id
        self.job_name = job_name
        self.worker_name = worker_name
        self.time_created = str(datetime.datetime.now())
        self.time_updated = str(datetime.datetime.now())

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})"


class Job_CDC(Base):

    __tablename__ = 'scd_job_cdc'

    job_cdc_id = Column(String(100), Sequence('job_cdc_id_seq'), primary_key=True)
    job_id = Column(String(100))
    cdc_type = Column(String(100))
    last_extracted_val = Column(String(100))
    time_created = Column(String(100))
    time_updated = Column(String(100))

    def __init__(self, job_instance_id, job_id, job_name, worker_name):
        self.job_instance_id = job_instance_id
        self.job_id = job_id
        self.job_name = job_name
        self.worker_name = worker_name
        self.time_created = str(datetime.datetime.now())
        self.time_updated = str(datetime.datetime.now())

    def __repr__(self):
        return f"<Info(table_name={self.__tablename__})"


class System_Parameter(Base):

    __tablename__ = 'scd_system_param'

    system_id = Column(String(100), primary_key=True)
    system_name = Column(String(100))
    system_health_status = Column(String(100))
    system_connection_allowed = Column(String(100))
    time_created = Column(String(100))
    time_updated = Column(String(100))

    def __init__(self, system_id, system_name, system_health_status, system_connection_allowed, time_created, time_updated):
        self.system_id = system_id
        self.system_name = system_name
        self.system_health_status = system_health_status
        self.system_connection_allowed = system_connection_allowed
        self.time_created = time_created
        self.time_updated = time_updated


    def __repr_(self):
        return f"<Info(table_name={self.__tablename__})"






