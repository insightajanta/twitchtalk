package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


class TwitchChatProcessorSave(brokers: String) {

  def process(): Unit = {

    val spark = SparkSession.builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.conf
    import spark.implicits._

//    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
//    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
//    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
//    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)

    val brokers = "localhost:9092"
    val inputDf = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", brokers).
      option("subscribe", "chatmessage_new").
      option("startingOffsets", "earliest").
      load()

    val chatDf = inputDf.selectExpr(
      "CAST(value AS STRING)",
      "timestamp as ts",
      "minute(timestamp) as minutes",
      "hour(timestamp) as hours",
      "to_date(timestamp) as dt")

    import org.apache.spark.sql.types.{DataTypes, StructType}

    val struct = new StructType().
      add("username", DataTypes.StringType).
      add("channel", DataTypes.StringType).
      add("message", DataTypes.StringType)

    val chatNestedDf = chatDf.select(from_json($"value", struct).alias("chatstruct"), $"ts", $"minutes", $"hours", $"dt")
    val chatfinal = chatNestedDf.select($"chatstruct.username", $"chatstruct.channel", $"chatstruct.message", $"ts", $"minutes", $"hours", $"dt")
//    val chatNestedDf = chatDf.select(from_json($"value", struct),
//      minute(col("ts")).alias("minute"),
//      col("ts"))
//
//    chatNestedDf.withWatermark("ts", "1 minute").
//      groupBy(col("minute"), window(col("ts"), "1 minute", "1 minute")).count().
//      writeStream.format("console").option("checkpointLocation", "/tmp/chat_new.checkpoint/").start()

//    val kafkaOutput = inputDf.writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", brokers)
//      .option("topic", "ages")
//      .option("checkpointLocation", "/tmp/checkpoints")
//      .start()

//    chatfinal.writeStream
//      .format("parquet")
//      .option("path", "/tmp/chat_new")
//      .option("checkpointLocation", "/tmp/checkpoints")
//      .start()

    chatfinal.writeStream.
      format("parquet").
      option("path", "s3n://com.ajanta/data/chatfinal").
      option("checkpointLocation", "/tmp/checkpoints/chatfinal/").
      start()

    spark.streams.awaitAnyTermination()
  }

  /*
  def writeToCassandra() = {
    val host = "<ip address>"
    val clusterName = "<cluster name>"
    val keyspace = "<keyspace>"
    val tableName = "chats_by_minute"

    spark.setCassandraConf(clusterName, CassandraConnectorConf.ConnectionHostParam.option(host))
    spark.readStream.format("rate").load()
      .selectExpr("value % 10 as key")
      .groupBy("key")
      .count()
      .toDF("key", "value")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        batchDF.write       // Use Cassandra batch data source to write streaming out
          .cassandraFormat(tableName, keyspace)
          .option("cluster", clusterName)
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()
  }
  */
}

