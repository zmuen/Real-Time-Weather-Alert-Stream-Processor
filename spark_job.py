from spark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("WeatherAlertStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", StringType()),
    StructField("areaDesc", StringType()),
    StructField("severity", StringType()),
    StructField("event", StringType()),
    StructField("effective", TimestampType()),
    StructField("expires", TimestampType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_alerts") \
    .load()

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

split_df = json_df.withColumn("area", split(col("areaDesc"), "; "))

agg = split_df \
    .withWatermark("effective", "15 minutes") \
    .groupBy(
        window(col("effective"), "1 hour", "10 minutes"),
        col("event")
    ) \
    .count() \
    .orderBy("window")

query = agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()