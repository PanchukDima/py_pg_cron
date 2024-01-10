import time

import psycopg2
import psycopg2.extras

from task import Task
from threading import Thread

import configparser
import logging
from logging.handlers import RotatingFileHandler


def create_rotating_log(path, level, maxBytes, backupCount):
    """
    Creates a rotating log
    """
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                                  "%Y-%m-%d %H:%M:%S")

    # add a rotating handler
    handler = RotatingFileHandler(path, maxBytes=maxBytes,
                                  backupCount=backupCount)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class Manager():
    def __init__(self, **kwargs):
        self.task_list = []
        self.connection_str = kwargs.get("conn_str")
        self.db = psycopg2.connect(self.connection_str)
        self.state = "sleep"
    def run(self):
        # start init
        logger = logging.getLogger("Rotating Log")
        logger.info(f"Start worker:")
        while(1):
            time.sleep(1)
            cron_list = self.getDbCronTasks()
            self.taskChecker(cron_list)
            self.updateCronDateRun()


    def updateCronDateRun(self):
        for job in self.task_list:
            job["Task"].nextRunDate = job["Task"].getNextStart()
            job["Task"].checkStart()
    def statusWorker(self):
        list_result = ""
        for job in self.task_list:
            list_result += f"\n Id: {job['Task'].id} State: {job['Task'].state} Next Run: {job['Task'].nextRunDate} Counter: {job['Task'].run_clock}"
        return list_result
    def jobIdExists(self, id):
        for job in self.task_list:
            if job["Task"].id == id:
                return True
        return False

    def cronIdExists(self, id, cron_list):
        for cron in cron_list:
            if cron["jobid"] == id:
                return True
        return False

    def taskChecker(self, cron_list):
        for cron in cron_list:
            if not self.jobIdExists(cron["jobid"]):
                self.registrationTask(cron)
        for job in self.task_list:
            if not self.cronIdExists(job["Task"].id, cron_list):
                self.unregistrationTask(job["Task"].id)
        # update data job
        for job in self.task_list:
            for cron in cron_list:
                if cron["jobid"] == job["_id"]:
                    job["Task"].updateParam(cron)
    def registrationTask(self, cron):
        task = Task(id=cron["jobid"],
                    name=cron["jobname"],
                    command=cron["command"],
                    schedule=cron["schedule"],
                    active=cron["active"],
                    conn_str=self.connection_str)
        thread = Thread(target=task.run, daemon=True)

        result = {
            "_id": cron["jobid"],
            "Task": task,
            "Thread": thread
        }
        self.task_list.append(result)
        thread.start()
    def unregistrationTask(self, jobid):
        for job in self.task_list:
            if job["Task"].id == jobid:
                job["Task"].state = "remove"

    def getDbCronTasks(self):
        cur = self.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM cron.job")
        list = cur.fetchall()
        cur.close()
        return list

    def taskRun(self, task):
        pass


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('/etc/pyPgCron/config.ini')
    db = config['DATABASE']
    log = config['LOG']
    log_file = f"{log['path']}/test.log"
    create_rotating_log(log_file, log['level'], int(log['rotation_max_Bytes']), int(log['backupCount']))
    logger = logging.getLogger("Rotating Log")
    manager = Manager(conn_str=f"{db['connStr']}")
    manager.run()



