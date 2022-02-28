import os
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import expr, lag, date_format
from pyspark.sql.window import Window
from google.cloud import bigquery
from pandas.plotting import register_matplotlib_converters


class BigQuerySparkProcessing:

    # Provide a SparkSession and the Logger
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    """
        This method aims to read data from BigQuery and return a DataFrame
        
        @spark :      The SparkSession.
        @project :    The project name on GCP.
        @database :   The database name containing the public data.
        @table :      The table name the returned by this method.
        @columns :    The list of the selected columns ( by default select all ).
        @condition :  The condition added to the query ( by default no condition is provided ).
        
        @return : A DataFrame of the requested table on BigQuery
    """

    def read_public_data(self, project_name, database, table, columns="*", condition=" "):
        self.logger.info("Read public data from : " + database)

        credential_file = "credentials/" + os.listdir("credentials")[0]
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_file
        client = bigquery.Client()
        table_path = project_name + "." + database + "." + table
        columns_names = str(columns).replace("[", "").replace("]", "").replace("'", "")
        query = ('SELECT ' + columns_names + ' FROM ' + table_path + " " + condition + ' LIMIT 4000000')
        query_job = client.query(query)
        query_result = query_job.result()
        df = query_result.to_dataframe()
        return self.spark.createDataFrame(df)


    """
        This method aims to process data by providing a temporal distribution
        of GitHub commits per language

        @commit_table :      The table containing GitHub commits.
        @languages_table :   The table containing the GitHub repositories 
                             names and their languages.

        @return : DataFrame with temporal distribution 
    """

    def process_data(self, commit_table, languages_table):
        self.logger.info("Processing data")

        window_spec = Window.partitionBy("language").orderBy("time_sec")

        processed_df = commit_table \
            .join(languages_table, commit_table.repo_name == languages_table.repo_name, "inner") \
            .select(commit_table["time_sec"],
                    languages_table["name"].alias("language"),
                    commit_table["date"], commit_table["repo_name"]) \
            .withColumn("previous_time_sec", lag("time_sec").over(window_spec)) \
            .withColumn("delta_time_sec", expr("time_sec - previous_time_sec")) \
            .na.fill(0, subset=["delta_time_sec"]) \
            .withColumn("date_commit", date_format("date", 'yyyy-MM-dd')) \
            .groupBy("language", "date_commit") \
            .avg("delta_time_sec") \
            .withColumnRenamed("avg(delta_time_sec)", "avg_time") \
            .sort("date_commit")

        return processed_df


    """
        This method aims to display data by generating an image

        @processed_df :      The table containing GitHub commits temporal distribution.
        @lang :              A list of chosen languages to display ( by default take all languages ).
        @format :            The format of date on the temporal distribution.
        @min_date :          A filter on the minimum date of the dataset. 
        @max_date :          A filter on the maximum date of the dataset. 
        
    """

    def display_data(self,
                     processed_df,
                     lang="('') or true",
                     format="yyy-MM-dd",
                     min_date="1970-01-01",
                     max_date="3000-01-01"):

        self.logger.info("Displaying data")

        # Convert the Spark Dataframe to Pandas dataframe
        languages = processed_df \
            .select("language") \
            .distinct() \
            .filter("lower(language) in " + str(lang).lower()) \
            .toPandas()

        # Apply user filters
        df = processed_df. \
            withColumn("date_commit", date_format("date_commit", format)) \
            .filter("date_commit >'" + min_date + "'") \
            .filter("date_commit <'" + max_date + "'") \
            .toPandas()
        register_matplotlib_converters()

        # Initiating the plot
        plt.title("Github : Temporal distribution of commits per language", fontsize=10)

        # Setting the x and y labels
        plt.xlabel("Date", fontsize=8)
        plt.ylabel("Delta Minutes ", fontsize=8)
        plt.xticks(rotation=45, fontsize=5)
        plt.yticks(fontsize=5)

        # Setting a plot for every language
        for language in languages["language"]:
            p = df[df["language"] == language]
            plt.plot(pd.to_datetime(p["date_commit"]), (p["avg_time"] / 60), label=language)
            plt.legend(bbox_to_anchor=(1.15, 1), fontsize=5)

        # Saving the result
        plt.savefig('result/bigquery_github_analysis.png', bbox_inches='tight', dpi=150)
        self.logger.info("Image created on the following location : result/bigquery_github_analysis.png ")