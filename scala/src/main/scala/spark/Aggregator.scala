package spark

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SaveMode, SparkSession}
import org.joda.time.DateTime


object Aggregator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Aggregator")
      .config("spark.cassandra.connection.host", Config.cassandraHost)
      .config("hadoop.fs.s3n.awsAccessKeyId", Config.accessKeyId)
      .config("hadoop.fs.s3n.awsSecretAccessKey", Config.secretAccessKey)
      .getOrCreate()

    val chatdf = spark.read.parquet(Config.chatMessagesS3Location).
      selectExpr("username", "channel", "ts", "minutes", "hours", "cast(dt as String)" )
    val lcdf = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "live_channel", "keyspace" -> Config.cassandraKeySpace)).load()

    if (!args.isEmpty && args.head == "day") {
      dayAggregator(chatdf, "chat", "channel")
      dayAggregator(chatdf, "chat", "username")
      dayAggregator(lcdf, "live", "channel")
    }
    else if (!args.isEmpty && args.head == "hour") {
      hourAggregator(chatdf, "chat", "channel")
      hourAggregator(chatdf, "chat", "username")
      hourAggregator(lcdf, "live", "channel")
    }
    else {
      minuteAggregator(chatdf, "chat", "channel")
      minuteAggregator(chatdf, "chat", "username")
      minuteAggregator(lcdf, "live", "channel")
    }
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
    df.printSchema()
    df.write.format("org.apache.spark.sql.cassandra").
      options(Map("table" -> table, "keyspace" -> Config.cassandraKeySpace)).
      mode(SaveMode.Append).
      save()
  }

  def aggregateAndSave(groupedDataset: RelationalGroupedDataset, tablePrefix: String, table: String) = {
    if (tablePrefix == "chat")
      saveToCassandra(groupedDataset.count(), table)
    else if (tablePrefix == "live")
      saveToCassandra(groupedDataset.avg("viewers").withColumnRenamed("avg(viewers)", "count"), table)
  }

  def minuteAggregator(df: DataFrame, tablePrefix: String, metric: String) = {
    val minuteDF = byMinuteDF(df)

    val groupedByMinute = minuteDF.groupBy("dt", "hours", "minutes", metric)
    aggregateAndSave(groupedByMinute, tablePrefix, s"${tablePrefix}_${metric}_by_minute")
  }

  def hourAggregator(df: DataFrame, tablePrefix: String, metric: String) = {
    val hourDF = byHourDF(df)

    val groupedByHour = hourDF.groupBy("dt", "hours", metric)
    aggregateAndSave(groupedByHour, tablePrefix, s"${tablePrefix}_${metric}_by_hour")
  }

  def dayAggregator(df: DataFrame, tablePrefix: String, metric: String) = {
    val dayDF = byDayDF(df)

    val groupedByDay = dayDF.groupBy("dt", metric)
    aggregateAndSave(groupedByDay, tablePrefix, s"${tablePrefix}_${metric}_by_day")
  }
}