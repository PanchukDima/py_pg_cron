import psycopg2
import time


#db = psycopg2.connect(dsn="dbname=ETALON host=10.0.0.71 user=SOLUTION_MED password=elsoft")
#cur = db.cursor()
#cur.execute("SELECT * from cron.job");
#for row in cur.fetchall():
#    print(row)

class Manager():
    def __init__(self, **kwargs):
        self.task_list = kwargs.get("list_task")

    def run(self):
        for task in self.task_list:
            print(task.action_clock)
            time.sleep(2)


