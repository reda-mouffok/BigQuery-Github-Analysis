from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser


def get_spark_session():
    spark_config = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/app.properties")

    for config_name, config_value in config.items("CONFIGS"):
        spark_config.set(config_name, config_value)

    try:
        spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
        return spark
    except Exception as spark_error:
        print(spark_error)