import logging
import logging.config
import json
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])
radio_code_schema = StructType([
        StructField("disposition_code", StringType(), True),
        StructField("description", StringType(), True)
    ])

def run_spark_job(spark, conf):

    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf.get("producer", "bootstrap.servers")) \
        .option("subscribe", conf.get("producer", "topic_name")) \
        .option("startingOffsets", conf.get("consumer", "auto.offset.reset")) \
        .option("maxOffsetsPerTrigger", conf.get("spark", "max_offsets_per_trigger")) \
        .option("maxRatePerPartition", conf.get("spark", "max_rate_per_partition")) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    logger.info("Schema of Input data")
    df.printSchema()
    
    # when we print schema, we will see the following output
    """
    key(binary)
    value(binary)
    topic(string)
    partition(int)
    offset(long)
    timestamp(long)
    timestampType(int)
    """

    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table \
        .select("original_crime_type_name", "disposition", "city", "call_date_time") \
        .withWatermark("call_date_time", "60 minutes")

    # count the number of original crime type
    crime_type_count_agg_df = distinct_table.\
        groupBy("original_crime_type_name", psf.window("call_date_time", "60 minutes")).\
        count(). \
        sort("count", ascending=False)

    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    # write output stream
    logger.info("Streaming Count per Crime Type")
    crime_type_count_query = crime_type_count_agg_df \
        .writeStream \
        .trigger(processingTime="15 seconds") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    # attach a ProgressReporter
    crime_type_count_query.awaitTermination()

    crime_per_location_agg_df = distinct_table.\
        groupBy("city", psf.window("call_date_time", "60 minutes")).\
        count(). \
        sort("count", ascending=False)

    logger.info("Streaming Count per Location")
    crime_per_location_query = crime_per_location_agg_df \
        .writeStream \
        .trigger(processingTime="15 seconds") \
        .outputMode("complete") \
        .format("console") \
        .start()
    # attach a ProgressReporter
    crime_per_location_query.awaitTermination()
    
     
    # get the right radio code json path
    radio_code_json_filepath = conf.get("spark", "radio_code_file")
    radio_code_df = spark.\
        read.\
        option("multiline", "true").\
        json(radio_code_json_filepath, schema=radio_code_schema)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    logger.info("Joinging agg data and radio codes")
    join_df = distinct_table \
        .join(radio_code_df, "disposition") \
        .select("original_crime_type_name", "description")

    logger.info("Streaming crime type with their description")
    join_query = join_df \
        .writeStream \
        .trigger(processingTime="15 seconds") \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    join_query.awaitTermination()

if __name__ == "__main__":
    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)
    config = ConfigParser()
    config.read("app.cfg")

    spark = SparkSession \
            .builder \
            .master(config.get("spark", "master")) \
            .appName("sf-crime-statistics") \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Starting Spark Session!!")
    run_spark_job(spark, config)
    logger.info("Closing Spark Session!!")
    spark.stop()
