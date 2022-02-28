
from src.builders.sparkBuilder import get_spark_session
from src.builders.sparkLogger import Log4j
from src.processors.BigquerySparkProcessing import BigquerySparkProcessing


def main():
    # Mettre les variables
    project_name = "bigquery-public-credentials"
    database = "github_repos"
    commit_table_name = "sample_commits"
    languages_table_name = "languages"

    # Initialiser la session
    spark = get_spark_session()
    logger = Log4j(spark)
    logger.info("Pyspark Github Analysis Started")

    # Executer le job
    bigquery_processor = BigquerySparkProcessing(logger)
    commit_table = bigquery_processor.read_public_data(spark, project_name, database, commit_table_name)
    language_table = bigquery_processor.read_public_data(spark, project_name, database, languages_table_name)
    processed_df = bigquery_processor.process_data(commit_table, language_table)

    # Adisplay
    bigquery_processor.display_data(processed_df)

    # Arret de la session
    logger.info("Pyspark Github Analysis execution completed")
    spark.stop()


if __name__ == "__main__":
    main()
