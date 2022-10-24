import yaml
from pyspark.sql import SparkSession
from pyspark import SparkConf



def create_spark_session():

    # Set conf variables
    spark_config = SparkConf()


    with open('conf/app.yaml') as file:
        try:
            data = yaml.safe_load(file)
            for key, value in data.items():
                spark_config.set(key, value)
        except yaml.YAMLError as exception:
            print(exception)

    try:
        # Create the Spark Session
        spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
        return spark
    except Exception as spark_error:
        print(spark_error)