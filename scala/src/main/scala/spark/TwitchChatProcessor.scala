package spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object TwitchChatProcessor {
  def setupDataframe(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Config.kafkaBrokers)
      .option("subscribe", Config.kafkaTopic)
      .option("startingOffsets", "latest")
      .load()

    val chatDf = inputDf
      .withWatermark("timestamp", "1 minute")
      .selectExpr(
      "CAST(value AS STRING)",
      "timestamp as ts",
      "minute(timestamp) as minutes",
      "hour(timestamp) as hours",
      "date_format(timestamp, 'yyyy-MM-dd') as dt")

    import org.apache.spark.sql.types.{DataTypes, StructType}

    val struct = new StructType().
      add("username", DataTypes.StringType).
      add("channel", DataTypes.StringType).
      add("message", DataTypes.StringType)

    val chatNestedDf = chatDf
      .select(from_json($"value", struct)
        .alias("chatstruct"), $"ts", $"minutes", $"hours", $"dt")

    chatNestedDf.select($"chatstruct.username", $"chatstruct.channel", $"chatstruct.message", $"ts", $"minutes", $"hours", $"dt")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ChatProcessor")
      .config("spark.cassandra.connection.host", Config.cassandraHost)
      .getOrCreate()

    println("master::::::::" + spark.conf.get("spark.master"))
    val chatFinal = setupDataframe(spark)

    startS3ChatStream(chatFinal, spark)
    startCassandraChatChannelStream(chatFinal, spark)
    startCassandraChatUsernameStream(chatFinal, spark)

    spark.streams.awaitAnyTermination()
  }

  def startS3ChatStream(cf: DataFrame, spark: SparkSession) = {
    cf.writeStream
      .trigger(Trigger.ProcessingTime(60000))
      .format("parquet")
      .option("path", Config.chatMessagesS3Location)
      .option("checkpointLocation", s"hdfs://ec2-18-213-94-80.compute-1.amazonaws.com:9000/tmp/checkpointschatfinal/")
      .partitionBy("dt", "hours")
      .start()

  }

  def startCassandraChatChannelStream(chatFinal: DataFrame, spark: SparkSession) = {
    val cf = chatFinal.groupBy("dt", "hours", "minutes", "channel").count()
    import org.apache.spark.sql.streaming.OutputMode
    val sink = new CassandraSink(spark.sparkContext, new ChatChannelByMinuteWriter())

    cf.writeStream
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(60000))
      .option("checkpointLocation", s"hdfs://ec2-18-213-94-80.compute-1.amazonaws.com:9000/tmp/checkpoint/channel/")
      .foreach(sink)
      .start()
  }

  def startCassandraChatUsernameStream(chatFinal: DataFrame, spark: SparkSession) = {
    val cf = chatFinal.groupBy("dt", "hours", "minutes", "username").count()
    import org.apache.spark.sql.streaming.OutputMode
    val sink = new CassandraSink(spark.sparkContext, new ChatUsernameByMinuteWriter())

    cf.writeStream
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(60000))
      .option("checkpointLocation", s"hdfs://ec2-18-213-94-80.compute-1.amazonaws.com:9000/tmp/checkpoint/username/")
      .foreach(sink)
      .start()
  }
}

