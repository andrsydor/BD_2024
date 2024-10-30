from logging import getLogger, basicConfig

from pyspark import SparkConf
from pyspark.sql import SparkSession

from core.csv_data_manager import CSVDataManager
from core.environment import Environment
from core.processing import tag_usage_per_day

from metadata.schemas import posts_schema, comments_schema, tag_synonyms_schema, tags_schema


def etl(logger, posts_csv_files, comments_csv_files, tags_synonyms_csv_files, tags_csv_files, output_name):
    environment = Environment()
    spark_config = SparkConf()

    logger.info("Creating spark session")
    spark_session = (SparkSession.builder
                     .master(environment.spark_master())
                     .appName(environment.app_name())
                     .config(conf=spark_config)
                     .getOrCreate())
    logger.info("Spark session created")

    csv_data_manager = CSVDataManager(spark_session, environment.data_directory_path())

    posts_dataframe = csv_data_manager.read(posts_csv_files, schema=posts_schema)
    comments_dataframe = csv_data_manager.read(comments_csv_files, schema=comments_schema)
    tag_synonyms_dataframe = csv_data_manager.read(tags_synonyms_csv_files, schema=tag_synonyms_schema)
    tags_dataframe = csv_data_manager.read(tags_csv_files, schema=tags_schema)
    logger.info("Dataframes loaded")
    logger.info("Starting processing")

    tag_usage_per_day_df = tag_usage_per_day(posts_dataframe, comments_dataframe, tag_synonyms_dataframe,
                                             tags_dataframe)
    logger.info("Triggering the computation by requesting write to the output")
    csv_data_manager.write(tag_usage_per_day_df, output_name)
    logger.info("Result successfully saved")


if __name__ == "__main__":

    # TODO: add args parsing

    logger = getLogger(__name__)
    basicConfig(level="INFO")
    etl(
        logger=logger,
        posts_csv_files=["/Posts_1.csv"],
        comments_csv_files=["/Comments_1.csv", "/Comments_2.csv"],
        tags_synonyms_csv_files=["/TagSynonyms.csv"],
        tags_csv_files=["/Tags_1.csv", "/Tags_2.csv"],
        output_name="/Processed10"
    )
