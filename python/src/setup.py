from datastore import cassandra_store
from config.config import *

cass = cassandra_store.PythonCassandraExample(config)
cass.createsession()
cass.setlogger()
cass.create_chat_channels("twitchspace")
cass.create_live_channels("twitchspace")
cass.create_tables()
