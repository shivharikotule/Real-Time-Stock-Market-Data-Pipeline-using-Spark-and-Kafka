import os
import findspark

# Set Spark Home
os.environ["SPARK_HOME"] = "/home/talentum/spark"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

# Manually set Spark Submit Arguments
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages " \
    "com.databricks:spark-xml_2.11:0.6.0," \
    "org.apache.spark:spark-avro_2.11:2.4.3," \
    "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5," \
    "org.apache.hadoop:hadoop-aws:3.2.0 pyspark-shell"

# Initialize findspark
findspark.init()

# Import Spark Modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col

# Create Spark Session with Kafka and AWS S3 JARs
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

print("Spark Session Created Successfully!")

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock-data"

# Define schema for stock data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True)
])

print("Starting Kafka Streaming Query...")

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka messages
df_parsed = df.selectExpr("CAST(value AS STRING)")

# Apply schema and transformations
df_final = df_parsed \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.timestamp"),
        col("data.open").cast(DoubleType()),
        col("data.high").cast(DoubleType()),
        col("data.low").cast(DoubleType()),
        col("data.close").cast(DoubleType()),
        col("data.volume").cast(DoubleType())
    ) \
    .dropna()  # Drop rows with null values

# Console Output (For Debugging)
query_console = df_final \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Store Transformed Data to S3 as Parquet
S3_BUCKET_PATH = "s3a://your-bucket-name/kafka-data/"

#query_s3 = df_final \
 #   .writeStream \
  #  .format("parquet") \
  #  .option("checkpointLocation", "s3a://your-bucket-name/kafka-checkpoint/") \
  #  .option("path", S3_BUCKET_PATH) \
  #  .outputMode("append") \
  #  .start()

query_console.awaitTermination()
#query_s3.awaitTermination()