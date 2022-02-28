import configparser
from pyspark.sql import SparkSession
from pyspark import SparkConf



def create_spark_session():

    # Set conf variables
    spark_config = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/app.properties")

    for config_name, config_value in config.items("CONFIGS"):
        spark_config.set(config_name, config_value)

    try:
        # Create the Spark Session
        spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
        return spark
    except Exception as spark_error:
        print(spark_error)