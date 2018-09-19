from kafka import KafkaConsumer
from kafka.errors import KafkaError
import msgpack
import PythonCassandraExample
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

import json
import redis

class LiveChannelConsumer:
    def __init__(self, config):
        self.config = config

        #live channel consumer
        self.live_channel_consumer = KafkaConsumer(value_deserializer=msgpack.loads, api_version=(0, 10, 1))
        self.live_channel_consumer.subscribe(['livechannels'])

        #setup cassandra below
        self.cass = PythonCassandraExample.PythonCassandraExample()
        self.cass.createsession()
        self.cass.setlogger()
        self.cass.session.set_keyspace('testkeyspace')

    def insert_data(self, key, value):
        for msg in self.live_channel_consumer:
            key = str(msg.timestamp)
            value = json.dumps(msg.value)

            insert_sql = self.cass.session.prepare("INSERT INTO livechannels (key, value) VALUES (?,?)")
            batch = BatchStatement()
            batch.add(insert_sql, (key, value))
            self.cass.session.execute(batch)
            self.cass.log.info('Batch Insert Completed')


class ChatMessageConsumer:
    def __init__(self, config):
        self.config = config

        #live channel consumer
        self.chat_consumer = KafkaConsumer(value_deserializer=msgpack.loads, api_version=(0, 10, 1))
        self.chat_consumer.subscribe(['chatmessage'])
        self.redis = redis.Redis(host='localhost', port=6379)

    def run(self):
        # ConsumerRecord(topic=u'chatmessage', partition=0, offset=157, timestamp=1537320670585, timestamp_type=0, key=None, value={' channel ': '#nickmercs', ' username ': 'jpking715', ' message ': 'this is live'}, checksum=1363374979, serialized_key_size=-1, serialized_value_size=66)
        for msg in self.chat_consumer:
            # print(msg.timestamp, ":", msg.value[' username '])
            username = msg.value[' username ']
            message = msg.value[' message ']
            if self.redis.get(message):
                print "Found duplicate message: " + message
            value = self.redis.get(username)
            if value is None:
                self.redis.set(username, str(msg.timestamp) + ':' + str(1))
            else:
                # print "value: " + value
                words = value.split(':')
                # print "words: "
                # print words
                # if old timestamp - new timestamp < 60 secs
                oldtime = long(words[0])
                oldcount = int(words[1])
                if msg.timestamp - oldtime < 6000:
                    if oldcount < 2:
                        self.redis.set(username, words[0] + ':' + str(oldcount+1))
                    else:
                        print "found an offender: " + username
                else:
                    self.redis.set(username, str(msg.timestamp) + ':' + str(1))
