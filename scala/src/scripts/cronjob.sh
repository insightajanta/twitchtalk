#/bin/bash
export AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}

# start the aggregator this way. It is currently in cron
/usr/local/spark/bin/spark-submit --conf spark.max.cores=8 --class spark.Aggregator --master spark://ec2-18-213-94-80.compute-1.amazonaws.com:7077 --num-executors 1 --executor-cores 1 --executor-memory 3G /home/ubuntu/jars/twitchchat-assembly-1.0.jar $1 >> /home/ubuntu/logs/spark.Aggregator-$1-$(date +-\%Y-\%m-\%d).log 2>&1

# start the Chat processor like so from the command line
#/usr/local/spark/bin/spark-submit --master spark://ec2-18-213-94-80.compute-1.amazonaws.com:7077 --deploy-mode client --conf spark.cores.max=6 --class spark.TwitchChatProcessor --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --num-executors 1 --executor-memory 2G /home/ubuntu/jars/twitchchat-assembly-1.0.jar