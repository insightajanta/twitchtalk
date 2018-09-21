from datastore import PythonCassandraExample
from config.config import *

cass = PythonCassandraExample.PythonCassandraExample(config)
cass.createsession()
cass.setlogger()
cass.session.create_keyspace('twitchspace')
cass.create_tables()