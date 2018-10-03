import json
import msgpack
import redis
import uuid

from kafka import KafkaConsumer
from src.datastore import PythonCassandraExample


class ChatMessageConsumer:
    def __init__(self, config):
        self.config = config

        # live channel consumer
        self.chat_consumer = KafkaConsumer(bootstrap_servers=self.config['kafka_config'],
                                           value_deserializer=msgpack.loads, api_version=(0, 10, 1))
        self.chat_consumer.subscribe(['chatmessage'])
        self.redis = redis.Redis(host=self.config['redis_host'], port=6379)

        # setup cassandra below
        self.cass = PythonCassandraExample.PythonCassandraExample(self.config)
        self.cass.createsession()
        self.cass.setlogger()
        self.cass.session.set_keyspace(self.config['cassandra_keyspace'])

    def run(self):
        # ConsumerRecord(topic=u'chatmessage', partition=0, offset=157, timestamp=1537320670585, timestamp_type=0, key=None,
        # value={' channel ': '#nickmercs', ' username ': 'jpking715', ' message ': 'this is live'}, checksum=1363374979, serialized_key_size=-1, serialized_value_size=66)
        for msg in self.chat_consumer:
            # print ("in for loop")
            username = msg.value['username']
            message = msg.value['message']
            if self.redis.get(message):
                print "Found duplicate message: " + message
            value = self.redis.get(username)
            if value is None:
                self.redis.set(username, str(msg.timestamp) + ':' + str(1))
            else:
                words = value.split(':')
                oldtime = long(words[0])
                oldcount = int(words[1])
                if msg.timestamp - oldtime < self.config['time_chunk']:
                    if oldcount < self.config['max_frequency']:
                        self.redis.set(username, words[0] + ':' + str(oldcount + 1))
                    else:
                        print "found an offender: " + username
                else:
                    self.redis.set(username, str(msg.timestamp) + ':' + str(1))

    def insert_chat_data(self):
        print "came in insert_chat_data"
        insert_sql = self.cass.session.prepare("INSERT INTO chatmessage ("
                                               "ts,"
                                               "hours,"
                                               "uuid,"
                                               "channel,"
                                               "username,"
                                               "message) VALUES (?,?,?,?,?,?)")

        for msg in self.chat_consumer:
            self.cass.session.execute(insert_sql,
                                      [msg.timestamp,
                                       msg.timestamp/3600000,
                                       uuid.uuid4(),
                                       msg.value['channel'],
                                       msg.value['username'],
                                       msg.value['message']])
            self.cass.log.info('Insert Completed: chatmessage')