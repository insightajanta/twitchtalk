import json
import msgpack
import redis
import uuid

from kafka import KafkaConsumer
# from src.datastore import PythonCassandraExample
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
# from src.config import config


# class ChatMessageConsumer:
#     def __init__(self, config):
#         self.config = config
#
#         # live channel consumer
#         self.chat_consumer = KafkaConsumer(bootstrap_servers=self.config['kafka_config'],
#                                            value_deserializer=msgpack.loads, api_version=(0, 10, 1))
#         self.chat_consumer.subscribe(['chatmessage'])
#         self.redis = redis.Redis(host=self.config['redis_host'], port=6379)
#
#         # setup cassandra below
#         self.cass = PythonCassandraExample.PythonCassandraExample(self.config)
#         self.cass.createsession()
#         self.cass.setlogger()
#         self.cass.session.set_keyspace(self.config['cassandra_keyspace'])
#
#     def insert_chat_data(self):
#         print "came in insert_chat_data"
#         insert_sql = self.cass.session.prepare("INSERT INTO chatmessage ("
#                                                "ts,"
#                                                "hours,"
#                                                "uuid,"
#                                                "channel,"
#                                                "username,"
#                                                "message) VALUES (?,?,?,?,?,?)")
#
#         for msg in self.chat_consumer:
#             self.cass.session.execute(insert_sql,
#                                       [msg.timestamp,
#                                        msg.timestamp/3600000,
#                                        uuid.uuid4(),
#                                        msg.value['channel'],
#                                        msg.value['username'],
#                                        msg.value['message']])
#             self.cass.log.info('Insert Completed: chatmessage')

def utf8_decoder_with_ignore(s):
    s
    """ Decode the unicode as UTF-8 """
    # if s is None:
    #     return None
    # return s.decode('utf-8', "ignore")


def create_context():
    sc = SparkContext(appName="Chat message processor")
    sc.setLogLevel("WARN")
    ssc2 = StreamingContext(sc, 500)

    # Define Kafka Consumer
    brokers = "ec2-52-206-31-163.compute-1.amazonaws.com:9092,ec2-52-87-92-193.compute-1.amazonaws.com:9092,ec2-18-215-104-101.compute-1.amazonaws.com:9092"
    kstream = KafkaUtils.createDirectStream(ssc2, topics=['chatmessage'],
                                            kafkaParams={"metadata.broker.list": brokers},
                                            valueDecoder=utf8_decoder_with_ignore,
                                            keyDecoder=utf8_decoder_with_ignore)

    ## --- Processing
    # Extract messages
    # parsed = kstream.map(lambda v: json.loads(v[1].decode("utf-8", "ignore")))
    # parsed.count()

    # Count number of messages in the batch
    count_this_batch = kstream.count().map(lambda x: ('Messages this batch: %s' % x))

    # Count by windowed time period
    count_windowed = kstream.countByWindow(60, 5).map(lambda x: ('Messages total (One minute rolling count): %s' % x))

    # # Get users
    # users_dstream = parsed.map(lambda msg: msg['username'])
    #
    # # Count each value and number of occurences
    # count_values_this_batch = users_dstream.countByValue() \
    #     .transform(lambda rdd: rdd \
    #                .sortBy(lambda x: -x[1])) \
    #     .map(lambda x: "User counts this batch:\tValue %s\tCount %s" % (x[0], x[1]))
    #
    # # Count each value and number of occurences in the batch windowed
    # count_values_windowed = users_dstream.countByValueAndWindow(60, 5) \
    #     .transform(lambda rdd: rdd \
    #                .sortBy(lambda x: -x[1])) \
    #     .map(lambda x: "User counts (One minute rolling):\tValue %s\tCount %s" % (x[0], x[1]))
    #
    # # Write total tweet counts to stdout
    # # Done with a union here instead of two separate pprint statements just to make it cleaner to display
    count_this_batch.union(count_windowed).pprint()
    #
    # # Write tweet author counts to stdout
    # count_values_this_batch.pprint(5)
    # count_values_windowed.pprint(5)

    return ssc2


if __name__ == '__main__':
    ssc = StreamingContext.getOrCreate('hdfs://ec2-18-213-94-80.compute-1.amazonaws.com:9000/tmp/checkpoint_v01/', lambda: create_context())
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopGraceFully = True)

    # sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    # sc.setLogLevel("WARN")
    # ssc = StreamingContext(sc, 60)
    # kafkaStream = KafkaUtils.createStream(ssc, config['kafka_config'], 'spark-streaming', {'twitter':1})
