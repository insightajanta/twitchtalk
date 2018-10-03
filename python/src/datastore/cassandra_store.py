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
                CREATE TABLE IF NOT EXISTS live_channel (
                hours int,
                minutes int,
                ts Timestamp,
                dt text,
                broadcast_platform text,
                created_at Timestamp,
                game text,
                status text,
                updated_at Timestamp,
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
                PRIMARY KEY (dt, hours, minutes, ts, id))
                with clustering order by (hours desc, minutes desc);
                 """
        self.session.execute(c_sql)
        self.log.info("livechannel Table Created !!!")

    # chat aggregate tables - by minute, by hour, by day
    def create_chat_channels(self, keyspace):
        c_sql = "use " + keyspace
        self.session.execute(c_sql)
        self.log.info("using keyspace " + keyspace)

        c_sql = """
                CREATE TABLE IF NOT EXISTS chat_channel_by_minute (
                hours int,
                minutes int,
                dt text,
                channel text,
                count int,
                PRIMARY KEY (dt, hours, minutes, count, channel))
                with clustering order by (hours desc, minutes desc, count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("chat_channel_by_minute Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS chat_user_by_minute (
                hours int,
                minutes int,
                dt text,
                username text,
                count int,
                PRIMARY KEY (dt, hours, minutes, count, username))
                with clustering order by (hours desc, minutes desc, count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("chat_user_by_minute Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS chat_channel_by_hour (
                hours int,
                dt text,
                channel text,
                count int,
                PRIMARY KEY (dt, hours, count, channel))
                with clustering order by (hours desc, count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("chat_channel_by_hour Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS chat_user_by_hour (
                hours int,
                dt text,
                username text,
                count int,
                PRIMARY KEY (dt, hours, count, username))
                with clustering order by (hours desc, count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("chat_user_by_minute Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS chat_channel_by_day (
                dt text,
                channel text,
                count int,
                PRIMARY KEY (dt, count, channel))
                with clustering order by (count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("chat_channel_by_day Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS chat_user_by_day (
                dt text,
                username text,
                count int,
                PRIMARY KEY (dt, count, username))
                with clustering order by (count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("chat_user_by_day Table Created !!!")

    # live channel aggregate tables - by minute, by hour, by day
    def create_live_channels(self, keyspace):
        c_sql = "use " + keyspace
        self.session.execute(c_sql)
        self.log.info("using keyspace " + keyspace)

        c_sql = """
                CREATE TABLE IF NOT EXISTS live_channel_by_minute (
                hours int,
                minutes int,
                dt text,
                channel text,
                count int,
                PRIMARY KEY (dt, hours, minutes, count, channel))
                with clustering order by (hours desc, minutes desc, count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("live_channel_by_minute Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS live_channel_by_hour (
                hours int,
                dt text,
                channel text,
                count int,
                PRIMARY KEY (dt, hours, count, channel))
                with clustering order by (hours desc, count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("live_channel_by_hour Table Created !!!")

        c_sql = """
                CREATE TABLE IF NOT EXISTS live_channel_by_day (
                dt text,
                channel text,
                count int,
                PRIMARY KEY (dt, count, channel))
                with clustering order by (count desc);
                 """
        self.session.execute(c_sql)
        self.log.info("live_channel_by_day Table Created !!!")
