import croniter
import datetime
import os, time
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError, errorcodes, errors
import re

import logging
from logging.handlers import RotatingFileHandler

class Task():
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.command = kwargs.get("command")
        self.schedule = kwargs.get("schedule")
        self.active = kwargs.get("active")
        self.connection_str = kwargs.get("conn_str")
        self.state = "sleep"
        self.run_clock = 0
        self.nextRunDate = self.getNextStart()
        self.lastRunDate = None
        self.schedule_type = None
        self.interval = None

    def getNextStart(self):
        if croniter.croniter.is_valid(self.schedule):
            self.schedule_type = "cron"
            now = datetime.datetime.now()
            cron = croniter.croniter(self.schedule, now)
            return cron.get_next(datetime.datetime)
        else:
            data = re.findall(r'(\d*)\s(\w*)', self.schedule)
            if data[0][1].lower() in ["seconds", "second", "s"]:
                self.schedule_type = "seconds"
                self.interval = int(data[0][0])
                return None
    def checkStart(self):
        if self.schedule_type == "cron":
            if self.nextRunDate < datetime.datetime.now():
                self.run_clock += 1
        if self.lastRunDate is None:
            self.run_clock = 1

    def updateParam(self, cron :dict):
        self.name = cron["jobname"]
        self.command = cron["command"]
        self.schedule = cron["schedule"]
        self.active = cron["active"]
    def run(self):
        logger = logging.getLogger("Rotating Log")
        logger.info(f"Start work job id: {self.id} name: {self.name}")
        while(1):
            if self.active:
                if self.state == "remove":
                    self.state = "delete"
                    break
                if self.nextRunDate is not None and self.schedule_type == "cron":
                    if self.nextRunDate < datetime.datetime.now() and self.run_clock > 0:
                        self.run_clock -= 1
                        self.action()
                elif self.schedule_type == "seconds":
                    self.run_clock -= 1
                    self.action()
                    time.sleep(self.interval)

    def registrationLogRunJob(self):
        try:
            db = psycopg2.connect(self.connection_str)
            cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(f"INSERT INTO cron.job_run_details(jobid, command, start_time) VALUES({self.id}, '{self.command}', now()) RETURNING runid;")
            db.commit()
            return cur.fetchone()['runid']
        except Exception as err:
            print("Oops! An exception has occured:", err)
            print("Exception TYPE:", type(err))
            logger = logging.getLogger("Rotating Log")
            logger.critical(f"Error Insert job_run_details job id: {self.id} error: {err}")
            return 0

    def succesSaveLogJob(self, runid):
        try:
            db = psycopg2.connect(self.connection_str)
            cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(f"UPDATE cron.job_run_details SET status = 'succesed', return_message = '', end_time = now() WHERE runid = {runid}")
            db.commit()
        except Exception as err:
            print("Oops! An exception has occured:", err)
            print("Exception TYPE:", type(err))
            logger = logging.getLogger("Rotating Log")
            logger.critical(f"errror update job_run_details job id: {self.id} error: {err}")

    def errorLogSave(self, runid, err):
        try:
            db = psycopg2.connect(self.connection_str)
            cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(
                f"UPDATE cron.job_run_details SET status = 'error', return_message = '{err}', end_time = now() WHERE runid = {runid}")
            db.commit()
        except Exception as err:
            print("Oops! An exception has occured:", err)
            print("Exception TYPE:", type(err))
            logger = logging.getLogger("Rotating Log")
            logger.critical(f"Error update job_run_details job id: {self.id} error: {err}")
    def action(self):
        self.state = "work"
        db = psycopg2.connect(self.connection_str)
        cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        runid = self.registrationLogRunJob()
        try:
            cur.execute(self.command)
            print(cur.fetchall())
            db.commit()
        except Exception as err:
            print("Oops! An exception has occured:", err)
            print("Exception TYPE:", type(err))
            self.errorLogSave(runid, err)
            logger = logging.getLogger("Rotating Log")
            logger.warning(f"SQL error job id: {self.id} error: {err}")
            self.state = "wait"
            return
        self.succesSaveLogJob(runid)
        self.state = "wait"
