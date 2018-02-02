

import luigi, numpy, pandas, time

class MyFirstTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget("/home/user/output1.csv")
    def run(self):
        df = pandas.DataFrame(numpy.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
        time.sleep(15)
        df.to_csv(self.output().path, index=False)

class MyDependentTask(luigi.Task):
    constant = luigi.IntParameter()
    def output(self):
        return luigi.LocalTarget("/home/user/output2.csv")
    def requires(self):
        return {"first_task": MyFirstTask()}
    def run(self):
        df1 = pandas.DataFrame(numpy.random.randint(0,100,size=(100, 4)), columns=list('EFGH'))
        df2 = pandas.read_csv(self.input()["first_task"].path)
        df_final = pandas.DataFrame(data=df1.as_matrix() + df2.as_matrix() + self.constant, columns=["A+E","B+F","C+G","D+H"])
        time.sleep(15)
        df_final.to_csv(self.output().path, index=False)


tasks = []
tasks.append(MyFirstTask())
tasks.append(MyDependentTask(5))
luigi.build(tasks, local_scheduler=False, workers=1)