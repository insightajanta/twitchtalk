package spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TwitchChatProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ChatProcessor")
      .getOrCreate()

//    val sc = spark.conf
    import spark.implicits._

//    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Config.accessKeyId)
//    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Config.secretAccessKey)

    val inputDf = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", Config.kafkaBrokers).
      option("subscribe", Config.kafkaTopic).
      option("startingOffsets", "earliest").
      load()

    val chatDf = inputDf.selectExpr(
      "CAST(value AS STRING)",
      "timestamp as ts",
      "minute(timestamp) as minutes",
      "hour(timestamp) as hours",
      "to_date(timestamp) as dt")

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
      start()

    spark.streams.awaitAnyTermination()
  }
}

