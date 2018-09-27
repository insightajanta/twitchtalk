
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.config.config import *

class Aggregator:
    def engagement_aggregator(spark):
        chatdf = spark.read.format("org.apache.spark.sql.cassandra")\
            .options(table="chatmessage", keyspace="twitchspace").load()

        engaged_channel = chatdf\
            .filter("hours = round(unix_timestamp()/3600 - .5)")\
            .groupBy("hours", "channel").count()\
            .selectExpr("from_unixtime(hours * 3600) as hour", "channel", "count as num_messages")\
            .orderBy(desc("num_messages"))

        engaged_channel.write.format("org.apache.spark.sql.cassandra").mode("append")\
            .options(table="engagedchannel", keyspace="twitchspace").save()

        engaged_user = chatdf\
            .filter("hours = round(unix_timestamp()/3600 - .5)")\
            .groupBy("hours", "username").count()\
            .selectExpr("from_unixtime(hours * 3600) as hour", "username", "count as num_messages")\
            .orderBy(desc("num_messages"))

        engaged_user.write.format("org.apache.spark.sql.cassandra").mode("append")\
            .options(table="engageduser", keyspace="twitchspace").save()

    def livechannel_aggregator(spark):
        lcdf = spark.read.format("org.apache.spark.sql.cassandra")\
            .options(table="livechannel", keyspace="twitchspace").load()

        popular_channel = lcdf\
            .filter("hours = round(unix_timestamp()/3600 - .5)")\
            .groupBy("hours", "channel")\
            .agg(mean("viewers").alias("avg_viewers"))\
            .selectExpr("from_unixtime(hours * 3600) as hour", "channel", "rint(avg_viewers) as avg_viewers")\
            .orderBy(desc("avg_viewers"))

        popular_channel.write.format("org.apache.spark.sql.cassandra").mode("append")\
            .options(table="popularchannel", keyspace="twitchspace").save()

    if __name__ == "__main__":
        spark = SparkSession.builder.appName("Twitch Aggregator")\
            .config("spark.cassandra.connection.host", config['cassandra_host'][0]).getOrCreate()
