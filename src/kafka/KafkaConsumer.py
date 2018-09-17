from kafka import KafkaConsumer
from kafka.errors import KafkaError
import msgpack
import PythonCassandraExample
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

import json

consumer = KafkaConsumer(value_deserializer=msgpack.loads)
consumer.subscribe(['chatmessage'])

cass = PythonCassandraExample.PythonCassandraExample()
cass.createsession()
cass.setlogger()
cass.session.set_keyspace('testkeyspace')


def insert_data(key, value):
    insert_sql = cass.session.prepare("INSERT INTO  chatmessage (key, value) VALUES (?,?)")
    batch = BatchStatement()
    batch.add(insert_sql, (key, value))
    cass.session.execute(batch)
    cass.log.info('Batch Insert Completed')


for msg in consumer:
    print(msg.timestamp)
    print (msg.value)
    insert_data(str(msg.timestamp), json.dumps(msg.value))