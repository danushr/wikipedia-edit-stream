import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.5"


def get_spark(app="wikipedia-edit-stream"):
    b = (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(b, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
