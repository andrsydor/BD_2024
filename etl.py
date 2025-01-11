# import findspark
#
#
# findspark.init()
# findspark.find()
#

from logging import getLogger, basicConfig

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession

from core.csv_data_manager import CSVDataManager
from core.delta_lake_manager import DeltaLakeManager
from core.environment import Environment
from core.processing import prepare_posts, prepare_comments, questions_from_posts, answers_from_posts
from core.loading import write_initial_deltalake, upsert_datalake

from metadata.schemas import posts_schema, comments_schema, tag_synonyms_schema
from metadata.columns import StackExchangeColumns as columns
from metadata import deltalake


def etl(logger, posts_csv_files, comments_csv_files, tags_synonyms_csv_files, incremental):
    environment = Environment()

    logger.info("Creating spark session")
    builder = (
        SparkSession.builder
        .master(environment.spark_master())
        .appName(environment.app_name())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session created")

    csv_data_manager = CSVDataManager(spark_session, environment.data_directory_path())

    # Extract
    posts = csv_data_manager.read(posts_csv_files, schema=posts_schema)
    comments = csv_data_manager.read(comments_csv_files, schema=comments_schema)
    tag_synonyms = csv_data_manager.read(tags_synonyms_csv_files, schema=tag_synonyms_schema)
    logger.info("Dataframes loaded")

    # Transform
    posts_dataframe = prepare_posts(posts)
    questions_with_synonyms = questions_from_posts(posts_dataframe, tag_synonyms)
    answers_dataframe = answers_from_posts(posts_dataframe)
    comments_dataframe = prepare_comments(comments)

    dataframes_by_name = {
        deltalake.PROCESSED_QUESTIONS: questions_with_synonyms,
        deltalake.PROCESSED_ANSWERS: answers_dataframe,
        deltalake.PROCESSED_COMMENTS: comments_dataframe
    }

    # Load
    deltalake_manager = DeltaLakeManager(spark_session, environment.data_directory_path(), deltalake.DELTALAKE_NAME)
    logger.info("Triggering the computation by requesting write to the output")
    if incremental:
        logger.info("Incremental upsert in progress")
        upsert_datalake(deltalake_manager, dataframes_by_name)
    else:
        logger.warn("Overwrite in progress")
        write_initial_deltalake(deltalake_manager, dataframes_by_name, columns.partition_date)
    logger.info("Result successfully saved")


if __name__ == "__main__":

    # TODO: add args parsing

    main_logger = getLogger(__name__)
    basicConfig(level="INFO")
    etl(
        logger=main_logger,
        posts_csv_files=["/Posts_1.csv"],
        comments_csv_files=["/Comments_1.csv", "/Comments_2.csv"],
        tags_synonyms_csv_files=["/TagSynonyms.csv"],
        incremental=True
    )
