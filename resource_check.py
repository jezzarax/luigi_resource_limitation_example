import pandas as pd
import luigi
import time
from datetime import datetime

class Task1(luigi.Task):

    id = luigi.Parameter()

    resources = {"some_resource": 1}

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(f"./tmp_output/task1_{self.id}.csv")

    def run(self):
#        self.decrease_running_resources({"some_resource": 1})
        time.sleep(5)
        df = pd.DataFrame([[self.id, "a", datetime.now()]], columns = ["id", "title", "tstamp"])
        self.output().makedirs()
        df.to_csv(self.output().path, index = False)


class Task2(luigi.Task):

    id = luigi.Parameter()

    resources = {"some_resource": 1}

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(f"./tmp_output/task2_{self.id}.csv")

    def run(self):
#        self.decrease_running_resources({"some_resource": 1})
        time.sleep(5)
        df = pd.DataFrame([[self.id, "b", datetime.now()]], columns = ["id", "title", "tstamp"])
        self.output().makedirs()
        df.to_csv(self.output().path, index = False)


class UeberTask(luigi.Task):

    def requires(self):
        ids = range(5)
        tasks = []
        for id in ids:
            tasks.append(Task1(id=id))
            tasks.append(Task2(id=id))
        return tasks

    def output(self):
        return luigi.LocalTarget("./tmp_output/full_set.csv")

    def run(self):
        full_set = pd.concat([pd.read_csv(x.path) for x in self.input()])
        self.output().makedirs()
        print(full_set)
        full_set.to_csv(self.output().path, index=False)

