from unittest import TestCase

from pyspark.sql.functions import date_format
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, DoubleType
from src.Builders.SparkBuilder import create_spark_session
from src.Builders.SparkLogger import Log4j
from src.Processing.BigQuerySparkProcessing import BigQuerySparkProcessing


class BigQuerySparkProcessingTest(TestCase):
    spark = create_spark_session()
    logger = Log4j(spark)

    def test_process_data(self):
        commit_table_schema = StructType([
            StructField("repo_name", StringType(), True),
            StructField("time_sec", StringType(), True),
            StructField("date", TimestampType(), True)
        ])

        languages_table_schema = StructType([
            StructField("repo_name", StringType(), True),
            StructField("name", StringType(), True)
        ])

        expected_df_schema = StructType([
            StructField("language", StringType(), True),
            StructField("date_commit", DateType(), True),
            StructField("avg_time", DoubleType(), True)
        ])

        commit_table = self.spark.read.schema(commit_table_schema) \
            .option("multiline", "true") \
            .format("json") \
            .load("data/commit_table.json")

        languages_table = self.spark.read.schema(languages_table_schema) \
            .option("multiline", "true") \
            .format("json") \
            .load("data/languages_table.json")

        expected_df = self.spark.read.schema(expected_df_schema) \
            .option("multiline", "true") \
            .format("json") \
            .load("data/expected_df.json") \
            .withColumn("date_commit", date_format("date_commit", 'yyyy-MM-dd'))  # fix the date type

        bigquery_processor = BigQuerySparkProcessing(self.spark, self.logger)
        result_df = bigquery_processor.process_data(commit_table, languages_table)

        self.assertEqual(result_df.dtypes, expected_df.dtypes,
                         "Expected Dataframe should contains the same (columns,types) as those of the result Dataframe")

        self.assertEqual(result_df.collect(), expected_df.collect(),
                         "Expected Dataframe should be equal to result Dataframe")
