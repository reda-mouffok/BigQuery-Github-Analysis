import os
from pyspark.sql.functions import expr, lag, to_date
from pyspark.sql.window import Window
from google.cloud import bigquery
import matplotlib.pyplot as plt


class BigquerySparkProcessing:

    def __init__(self, logger):
        self.logger = logger

    def read_public_data(self, spark, project_name, database, table):

        credential_file = "credentials/" + os.listdir("credentials")[0]
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_file
        client = bigquery.Client()
        table_path = project_name + "." + database + "." + table
        query = ('SELECT * FROM `' + table_path + '` LIMIT 20')
        query_job = client.query(query)
        query_result = query_job.result()
        df = query_result.to_dataframe()
        return spark.createDataFrame(df)

    def process_data(self, commit_table, languages_table):
        self.logger.info("processing credentials")

        window_spec = Window.partitionBy("language").orderBy("time_sec")

        processed_df = commit_table \
            .join(languages_table, commit_table.repo_name == languages_table.repo_name, "inner") \
            .select(commit_table["committer.time_sec"].alias("time_sec"),
                    languages_table["language.name"].alias("language"),
                    commit_table["committer.date"].alias("date")) \
            .withColumn("previous_time_sec", lag("time_sec", 1).over(window_spec)) \
            .withColumn("delta_time_sec", expr("time_sec - previous_time_sec")) \
            .withColumn("date_commit", to_date("date", 'MM-dd-yyyy')) \
            .groupBy("language", "date_commit") \
            .avg("delta_time_sec") \
            .withColumnRenamed("avg(delta_time_sec)", "avg_time") \
            .na.fill(0, subset=["avg_time"])

        return processed_df

    def display_data(self, processed_df):
        self.logger.info("displaying credentials")
        lines = processed_df.select("language").distinct().toPandas()
        df = processed_df.toPandas()
        for lang in lines["language"]:
            p = df[df["language"] == lang]
            plt.plot(p["date_commit"], p["avg_time"], label = lang)
        plt.legend()
        plt.savefig('github_analysis.png')

