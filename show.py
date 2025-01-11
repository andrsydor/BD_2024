# import findspark
# findspark.init()
# findspark.find()


from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession

from core.delta_lake_manager import DeltaLakeManager
from core.environment import Environment

from metadata import deltalake


def main():
    environment = Environment()

    builder = (
        SparkSession.builder
            .master(environment.spark_master())
            .appName(environment.app_name())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    deltalake_manages = DeltaLakeManager(spark_session, environment.data_directory_path(), deltalake.DELTALAKE_NAME)

    deltalake_manages.read(deltalake.PROCESSED_QUESTIONS).show(100, False)
    deltalake_manages.read(deltalake.PROCESSED_ANSWERS).show(100, False)
    deltalake_manages.read(deltalake.PROCESSED_COMMENTS).show(100, False)


if __name__ == "__main__":
    main()
