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

    val chatDf = inputDf.selectExpr(
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

    val chatFinal = setupDataframe(spark)

    startS3ChatStream(chatFinal)
    startCassandraChatChannelStream(chatFinal, spark)
    startCassandraChatUsernameStream(chatFinal, spark)

    spark.streams.awaitAnyTermination()
  }

  def startS3ChatStream(cf: DataFrame) = {
    cf.writeStream
      .trigger(Trigger.ProcessingTime(60000))
      .format("parquet")
      .option("path", Config.chatMessagesS3Location)
      .option("checkpointLocation", "/tmp/checkpointschatfinal/")
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
      .option("checkpointLocation", "/tmp/checkpoint/channel")
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
      .option("checkpointLocation", "/tmp/checkpoint/username")
      .foreach(sink)
      .start()
  }
}

