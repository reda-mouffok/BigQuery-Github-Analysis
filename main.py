import os
from src.Builders.SparkBuilder import create_spark_session
from src.Builders.SparkLogger import Log4j
from src.Processing.BigQuerySparkProcessing import BigQuerySparkProcessing


def main():
    # Init job variables
    # In this job, we took only sample data.
    project_name = "`bigquery-public-data"
    database = "github_repos"
    commits_table_name = "sample_commits`"
    commits_columns = ["committer.time_sec", "committer.date", "repo_name"]
    languages_table_name = "languages` as l cross join UNNEST(l.language) as language_exploded"
    # Select only sample data from languages table
    languages_condition = """
                            WHERE repo_name IN (
                                'tensorflow/tensorflow',
                                'facebook/react',
                                'twbs/bootstrap',
                                'apple/swift',
                                'Microsoft/vscode',
                                'torvalds/linux') 
                            """
    languages_columns = ["repo_name", "language_exploded.name"]

    # Init the Spark Session
    spark = create_spark_session()
    logger = Log4j(spark)
    logger.info("Pyspark Github Analysis Started")

    # Read public data from BigQuery
    bigquery_processor = BigQuerySparkProcessing(spark, logger)

    # Due to the BigQuery costs, the tables is readen ones and saved.
    if not os.path.exists("data/commits"):
        commits_table = bigquery_processor\
            .read_public_data(project_name,
                              database,
                              commits_table_name,
                              commits_columns)
        commits_table.write.format("orc").mode("overwrite").save("data/commits")
    else:
        commits_table = spark.read.format("orc").load("data/commits")

    if not os.path.exists("data/languages"):
        languages_table = bigquery_processor\
            .read_public_data(project_name,
                              database,
                              languages_table_name,
                              languages_columns,
                              languages_condition)
        languages_table.write.format("orc").mode("overwrite").save("data/languages")
    else:
        languages_table = spark.read.format("orc").load("data/languages")

    # Run the processing of data
    processed_df = bigquery_processor.process_data(commits_table, languages_table)

    # Init config params and save the result into an image
    max_date = "2015-12-15"
    min_date = "2014-01-01"
    format = "YYYY-MM-dd"
    language_list = (
        'C', 'C++', 'shell', 'python', 'go', 'java', 'reat', 'html', 'css', 'visual basic', 'javascript', 'dockerfile')

    bigquery_processor.display_data(processed_df,
                                    lang=language_list,
                                    format=format,
                                    min_date=min_date,
                                    max_date=max_date)

    # Stop the Spark Session
    logger.info("Pyspark Github Analysis execution completed")
    spark.stop()


if __name__ == "__main__":
    main()
