package spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TwitchChatProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ChatProcessor")
      .getOrCreate()

    import spark.implicits._

    val inputDf = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", Config.kafkaBrokers).
      option("subscribe", Config.kafkaTopic).
      option("startingOffsets", "latest").
      load()

    val chatDf = inputDf.selectExpr(
      "CAST(value AS STRING)",
      "timestamp as ts",
      "minute(timestamp) as minutes",
      "hour(timestamp) as hours",
      "date_format(timestamp, 'yyyy-MM-dd') as dt")

    import org.apache.spark.sql.types.{DataTypes, StructType}
    import org.apache.spark.sql.streaming.Trigger

    val struct = new StructType().
      add("username", DataTypes.StringType).
      add("channel", DataTypes.StringType).
      add("message", DataTypes.StringType)

    val chatNestedDf = chatDf.select(from_json($"value", struct).alias("chatstruct"), $"ts", $"minutes", $"hours", $"dt")
    val chatfinal = chatNestedDf.select($"chatstruct.username", $"chatstruct.channel", $"chatstruct.message", $"ts", $"minutes", $"hours", $"dt")
    chatfinal.writeStream.trigger(Trigger.ProcessingTime(60000)).
      format("parquet").
      option("path", Config.chatMessagesS3Location).
      option("checkpointLocation", "/tmp/checkpoints/chatfinal/").
      partitionBy("dt", "hours").
      start()

    //https://stackoverflow.com/questions/45642904/spark-structured-streaming-multiple-sinks
    //https://stackoverflow.com/questions/47228309/how-to-write-stream-to-s3-with-year-month-and-day-of-the-day-when-records-were
      spark.streams.awaitAnyTermination()
  }
}

