$ spark-shell --jars ~/spark-cassandra-connector-assembly-2.3.2.jar --conf spark.cassandra.connection.host="10.0.0.7"
$ pyspark --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 --conf spark.cassandra.connection.host="10.0.0.7"


val cmdf = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "chatmessage", "keyspace" -> "twitchspace")).load()
val lcdf = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "livechannel", "keyspace" -> "twitchspace")).load()

-- python
cmdf = spark.read.format("org.apache.spark.sql.cassandra").options(table = "chatmessage", keyspace = "twitchspace").load()



-- scala
--List of channels with the most engagement - current hour
cmdf.filter("hours = round(unix_timestamp()/3600 - .5)").groupBy("hours", "channel").count().selectExpr("from_unixtime(hours * 3600) as hour", "channel", "count").orderBy(desc("count")).show
--List of channels with the most engagement - last hour
cmdf.filter("hours = round(unix_timestamp()/3600 - 1.5)").groupBy("hours", "channel").count().selectExpr("from_unixtime(hours * 3600) as hour", "channel", "count").orderBy(desc("count")).show
--List of channels with the most engagement - yesterday same hour
cmdf.filter("hours = round(unix_timestamp()/3600 - 24.5)").groupBy("hours", "channel").count().selectExpr("from_unixtime(hours * 3600) as hour", "channel", "count").orderBy(desc("count")).show

--List of channels with the most average viewers - current hour
lcdf.filter("hours = round(unix_timestamp()/3600 - .5)").groupBy("hours", "channel").agg(mean("viewers").alias("average_viewers")).selectExpr("from_unixtime(hours * 3600) as hour", "channel", "rint(average_viewers)").orderBy(desc("average_viewers")).show
--List of channels with the most average viewers - last hour
lcdf.filter("hours = round(unix_timestamp()/3600 - 1.5)").groupBy("hours", "channel").agg(mean("viewers").alias("average_viewers")).selectExpr("from_unixtime(hours * 3600) as hour", "channel", "rint(average_viewers)").orderBy(desc("average_viewers")).show
--List of channels with the most average viewers - yesterday same hour
lcdf.filter("hours = round(unix_timestamp()/3600 - 24.5)").groupBy("hours", "channel").agg(mean("viewers").alias("average_viewers")).selectExpr("from_unixtime(hours * 3600) as hour", "channel", "rint(average_viewers)").orderBy(desc("average_viewers")).show

--List of most engaged users - with most chat messages - current hour
cmdf.filter("hours = round(unix_timestamp()/3600 - .5)").groupBy("hours", "username").count().selectExpr("from_unixtime(hours * 3600) as hour", "username", "count").orderBy(desc("count")).show
--List of most engaged users - with most chat messages - last hour
cmdf.filter("hours = round(unix_timestamp()/3600 - 1.5)").groupBy("hours", "username").count().selectExpr("from_unixtime(hours * 3600) as hour", "username", "count").orderBy(desc("count")).show
--List of most engaged users - with most chat messages - yesterday same hour
cmdf.filter("hours = round(unix_timestamp()/3600 - 24.5)").groupBy("hours", "username").count().selectExpr("from_unixtime(hours * 3600) as hour", "username", "count").orderBy(desc("count")).show


-- write to cassandra
val engagedChannels = cmdf.groupBy("hours", "channel").count().selectExpr("from_unixtime(hours * 3600) as hour", "channel", "count as num_messages")
engagedChannels.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "engagedchannel", "keyspace" -> "twitchspace")).save()

val engagedUsers = cmdf.groupBy("hours", "username").count().selectExpr("from_unixtime(hours * 3600) as hour", "username", "count as num_messages")
engagedUsers.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "engageduser", "keyspace" -> "twitchspace")).save()

val popularChannels = lcdf.groupBy("hours", "channel").agg(mean("viewers").alias("average_viewers")).selectExpr("from_unixtime(hours * 3600) as hour", "channel", "rint(average_viewers) as avg_viewers")
popularChannels.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "popularchannel", "keyspace" -> "twitchspace")).save()
