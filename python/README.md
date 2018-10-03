# twitchchat

## Python packages to install

#####To use kafka from Python install this package
sudo pip install kafka-python
#####To use json like format - here used for kafka ingestion
sudo pip install msgpack
#####To use python-twitch-client
sudo pip install python-twitch-client
- note you may need to install additional dependencies here.
I had to pip install the following on my macbook
    - six
    - requests
    - configparser
#####To use python cassandra
sudo pip install cassandra-driver
    

## Kafka installation
download kafka and unzip:

download from: https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz

follow instructions: https://kafka.apache.org/quickstart

## Cassandra installation (local) version 3.11.3
download from: http://www.apache.org/dyn/closer.lua/cassandra/3.11.3/apache-cassandra-3.11.3-bin.tar.gz

Follow instructions for Installing from binary tarball here: http://cassandra.apache.org/doc/latest/getting_started/installing.html

In cqlsh:
- create keyspace twitchspace;
- create keyspace twitchspace with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
- create table livechannels(ts text primary key, value text)

For cassandra-spark connector:

Following instructions from: https://www.codementor.io/sheena/installing-cassandra-spark-linux-debian-ubuntu-14-du107vbhx

- git clone https://github.com/datastax/spark-cassandra-connector.git
- cd spark-cassandra-connector/
- git ch v2.3.2
- ./sbt/sbt assembly
- cp spark-cassandra-connector/target/full/scala-2.11/spark-cassandra-connector-assembly-2.3.2.jar ~
- spark-shell --jars ~/spark-cassandra-connector-assembly-2.3.2.jar --conf spark.cassandra.connection.host="10.0.0.7"
- val cm = sc.cassandraTable("twitchspace", "chatmessage2")
- cm.take(10).foreach(println)
- Or to read as a dataframe: val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "livechannel2", "keyspace" -> "twitchspace")).load()
- df.show

##Redis
brew install redis (installed redis-4.0.11.high_sierra.bottle.tar.gz)

brew services start redis

sudo pip install redis (Successfully installed redis-2.10.6)

#Docker steps
##Cassandra
(full instructions: https://docs.docker.com/samples/library/cassandra/)

docker run --name cassandra -d cassandra:3.11.3

To connect using cqlsh, do the following

docker run -it --link cassandra:cassandra --rm cassandra cqlsh cassandra

#Dash
- pip install dash==0.27.0
- pip install dash-html-components==0.13.2
- pip install dash-core-components==0.30.2

#crontab entry
0 * * * * spark-submit --master spark://ec2-18-213-94-80.compute-1.amazonaws.com:7077 --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 /home/ubuntu/twitchchat/src/spark/aggregation.py &> /home/ubuntu/twitchchat/logs/aggregator`date +"-%Y-%m-%d-%H-%M-%S"`.log

