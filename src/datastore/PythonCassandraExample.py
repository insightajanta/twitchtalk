"""
Python  by Techfossguru
Copyright (C) 2017  Satish Prasad

"""
import logging
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement


class PythonCassandraExample:

    def __init__(self, config):
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.log = None
        self.config = config

    def __del__(self):
        self.cluster.shutdown()

    def createsession(self):
        self.cluster = Cluster(self.config['cassandra_host'])
        self.session = self.cluster.connect(self.keyspace)

    def getsession(self):
        return self.session

    # How about Adding some log info to see what went wrong
    def setlogger(self):
        log = logging.getLogger()
        log.setLevel('INFO')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        log.addHandler(handler)
        self.log = log

    # Create Keyspace based on Given Name
    def createkeyspace(self, keyspace, drop=False):
        """
        :param keyspace:  The Name of Keyspace to be created
        :return:
        """
        # Before we create new lets check if exiting keyspace; we will drop that and create new
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            if drop:
                self.log.info("dropping existing keyspace...")
                self.session.execute("DROP KEYSPACE " + keyspace)
            else:
                self.log.info("existing keyspace, not doing anything...")
                return


        self.log.info("creating keyspace...")
        self.session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                """ % keyspace)

        self.log.info("setting keyspace...")
        self.session.set_keyspace(keyspace)

    def create_tables(self):
        c_sql = """
                CREATE TABLE IF NOT EXISTS livechannel (
                ts Timestamp,
                hours int,
                uuid UUID,
                broadcast_platform text,
                created_at text,
                game text,
                status text,
                updated_at text,
                privacy_options_enabled Boolean,
                logo text,
                partner Boolean,
                display_name text,
                followers int,
                broadcaster_language text,
                private_video Boolean,
                description text,
                views int,
                channel text,
                language text,
                mature Boolean,
                average_fps double,
                id text,
                viewers int,
                PRIMARY KEY (hours, ts, uuid))
                with clustering order by (ts desc);
                 """
        self.session.execute(c_sql)
        self.log.info("livechannel Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS chatmessage (
                hours int,
                ts Timestamp,
                uuid UUID,
                channel text,
                username text,
                message text,
                PRIMARY KEY (hours, ts, uuid))
                with clustering order by (ts desc);
                 """
        self.session.execute(c_sql)
        self.log.info("chatmessage Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS popularChannel (
                hour Timestamp,
                channel text,
                avg_viewers int,
                PRIMARY KEY (hour, avg_viewers, channel))
                with clustering order by (avg_viewers desc);
                 """
        self.session.execute(c_sql)
        self.log.info("engagedChannel Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS engagedUser (
                hour Timestamp,
                username text,
                num_messages int,
                PRIMARY KEY (hour, num_messages, username))
                with clustering order by (num_messages desc);
                 """
        self.session.execute(c_sql)
        self.log.info("engagedChannel Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS engagedChannel (
                hour Timestamp,
                channel text,
                num_messages int,
                PRIMARY KEY (hour, num_messages, channel))
                with clustering order by (num_messages desc);
                 """
        self.session.execute(c_sql)
        self.log.info("engagedChannel Table Created !!!")
