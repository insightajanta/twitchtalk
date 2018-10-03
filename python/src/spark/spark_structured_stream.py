from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

brokers = "ec2-52-206-31-163.compute-1.amazonaws.com:9092,ec2-52-87-92-193.compute-1.amazonaws.com:9092,ec2-18-215-104-101.compute-1.amazonaws.com:9092"

# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts
ds1 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", "chatmessage_new") \
    .option("startingOffsets", "earliest")\
    .load()
#     .option("startingOffsets", "latest")
ds1.writeStream.format("parquet").option("path", "/tmp/chat_new").option("checkpointLocation", "/tmp/chat_new.checkpoint/").start()
val ds2 = spark.read.parquet("/tmp/chat_new")

ds2 = ds1.selectExpr("CAST(value AS STRING)")
ds2.writeStream.format("console").start()
ds1.writeStream.format("console").start()

import org.apache.spark.sql.types.{DataTypes, StructType}

val ds1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "chatmessage_new").option("startingOffsets", "earliest").load()
val personJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

val struct = new StructType().add("username", DataTypes.StringType).add("channel", DataTypes.StringType).add("message", DataTypes.StringType)

val ds3 = ds2.selectExpr("CAST(value AS STRING)")
val ds4 = ds3.select(from_json($"value", struct).as("m"))