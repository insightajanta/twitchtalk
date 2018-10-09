#Spark Submit

#####nohup /usr/local/spark/bin/spark-submit --class spark.TwitchChatProcessor --master spark://ec2-18-213-94-80.compute-1.amazonaws.com:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --num-executors 1 --executor-cores 2 --executor-memory 2G /home/ubuntu/jars/twitchchat-assembly-1.0.jar &> logs/spark.TwitchChatProcessor.log &

## crontab on spark master
##### * * * * * /bin/bash /home/ubuntu/scripts/cronjob.sh minute
 
##### 0 * * * * /bin/bash /home/ubuntu/scripts/cronjob.sh hour

##### 0 0 * * * /bin/bash /home/ubuntu/scripts/cronjob.sh day

##spark-shell with kafka and cassandra
#####spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2,datastax:spark-cassandra-connector:2.3.1-s_2.11 --conf spark.cassandra.connection.host=localhost
