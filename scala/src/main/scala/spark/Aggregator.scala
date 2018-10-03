package spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object Aggregator {
  val minute = 60
  val hour = 60 * 60
  val day = hour * 24
  val week = day * 7

  val timeMap = Map("hour" -> hour, "day" -> day, "week" -> week)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Aggregator")
      .config("spark.cassandra.connection.host", Config.cassandraHost)
      .config("hadoop.fs.s3n.awsAccessKeyId", Config.accessKeyId)
      .config("hadoop.fs.s3n.awsSecretAccessKey", Config.secretAccessKey)
      .getOrCreate()

    val sc = spark.conf

    val chatdf = spark.read.parquet(Config.chatMessagesS3Location)
    val lcdf = spark.read.parquet(Config.chatMessagesS3Location)
    if (args != null && args.head == "day")
      dayAggregator(chatdf, "chat")
    else if (args != null && args.head == "hour")
      hourAggregator(chatdf, "chat")
    else
      minuteAggregator(chatdf, "chat")
  }

  def byMinuteDF(inDF: DataFrame) : DataFrame = {
    val lastMinuteTS = DateTime.now().minusMinutes(5)
    val lastMinute = lastMinuteTS.getMinuteOfHour()
    val lastMinuteHour = lastMinuteTS.getHourOfDay()
    val lastMinuteDate = lastMinuteTS.toString("YYYY-MM-dd")
    println (lastMinuteTS)

    inDF.filter(s"minutes = $lastMinute and hours = $lastMinuteHour and dt = '$lastMinuteDate'")
  }

  def byHourDF(inDF: DataFrame) : DataFrame = {
    val lastHourTS = DateTime.now().minusHours(1)
    val lastHour = lastHourTS.getHourOfDay()
    val lastHourDate = lastHourTS.toString("YYYY-MM-dd")
    println (lastHourTS)
    inDF.filter(s"hours = $lastHour and dt = '$lastHourDate'")
  }

  def byDayDF(inDF: DataFrame) : DataFrame = {
    val lastDay = DateTime.now().minusDays(1).toString("YYYY-MM-dd")
    inDF.filter(s"dt = '$lastDay'")
  }

  def saveToCassandra(df: DataFrame, table: String) = {
    df.write.format("org.apache.spark.sql.cassandra").
      options(Map("table" -> table, "keyspace" -> Config.cassandraKeySpace)).
      mode(SaveMode.Append).
      save()
  }

  def minuteAggregator(df: DataFrame, metric: String) = {
    val minuteDF = byMinuteDF(df)
    minuteDF.cache()

    val channelByMinute = minuteDF.groupBy("dt", "hours", "minutes", "channel").count()
    saveToCassandra(channelByMinute, s"${metric}_channel_by_minute")

    val userByMinute = minuteDF.groupBy("dt", "hours", "minutes", "username").count()
    saveToCassandra(userByMinute, s"${metric}_user_by_minute")
  }

  def hourAggregator(df: DataFrame, metric: String) = {
    val hourDF = byHourDF(df)
    hourDF.cache()

    val channelByHour = hourDF.groupBy("dt", "hours", "channel").count()
    saveToCassandra(channelByHour, s"${metric}_channel_by_hour")

    val userByHour = hourDF.groupBy("dt", "hours", "username").count()
    saveToCassandra(userByHour, s"${metric}_user_by_hour")
  }

  def dayAggregator(df: DataFrame, metric: String) = {
    val dayDF = byDayDF(df)
    dayDF.cache()

    val channelByDay = dayDF.groupBy("dt", "channel").count()
    saveToCassandra(channelByDay, s"${metric}_channel_by_day")

    val userByDay = dayDF.groupBy("dt", "username").count()
    saveToCassandra(userByDay, s"${metric}_user_by_day")
  }
}