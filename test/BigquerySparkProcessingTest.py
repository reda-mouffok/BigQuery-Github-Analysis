from unittest import TestCase

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from src.builders.sparkBuilder import get_spark_session
from src.builders.sparkLogger import Log4j
from main import BigquerySparkProcessing


class SparkExampleTest(TestCase):
    spark = get_spark_session()
    logger = Log4j(spark)


    def test_process_data(self):

        commit_table_schema = StructType([
            StructField("repo_name", StringType(), True),
            StructField("committer", StructType([
                StructField("time_sec", StringType(), True),
                StructField("date", TimestampType(), True)
            ]), True),
        ])

        languages_table_schema = StructType([
            StructField("repo_name", StringType(), True),
            StructField("language", StructType([
                StructField("name", StringType(), True)
            ]), True),
        ])

        commit_table = self.spark.read.schema(commit_table_schema) \
            .option("multiline", "true") \
            .format("json") \
            .load("data/commit_table.json")

        languages_table = self.spark.read.schema(languages_table_schema) \
            .option("multiline", "true") \
            .format("json") \
            .load("data/languages_table.json")

        bigquery_processor = BigquerySparkProcessing(self.logger)
        df = bigquery_processor.process_data(commit_table, languages_table)
        bigquery_processor.display_data(df)
        print(self.spark.sparkContext.getConf().getAll())

        df.printSchema()
        df.show()




