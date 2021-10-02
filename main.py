"""
Script to execute car crash case study
"""
import logging
from pyspark.sql import SparkSession

from car_crash.analysis import Analysis
logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder.enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("OFF")

logger.info("Case Study Solution...")

Analysis(spark).start()

logger.info("#############COMPLETED#########################")




