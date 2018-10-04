
from src.datastore.cassandra_store import PythonCassandraExample
from datetime import datetime
from datetime import timedelta
from src.config.config import config
from collections import OrderedDict


def add_rows_to_map(rows, minute_map):
    for row in rows:
        minute_map[row.metric] = row.count


def map_to_list(minute_map, n):
    return_list = []
    for i in range(n):
        return_list.append(minute_map.get(i, -1))
    return return_list


class DataUtil:
    def __init__(self, config):
        self.config = config
        self.cass = PythonCassandraExample(self.config)
        self.cass.createsession()
        self.cass.setlogger()
        self.cass.session.set_keyspace(self.config['cassandra_keyspace'])

    def get_daily_data(self, table):
        query = "select dt, sum(count) as count from " + table + " group by dt limit 7"
        rows = self.cass.session.execute(query)
        row_map = {}
        for row in rows:
            row_map[row.dt] = row.count
        return row_map

    def get_hourly_data(self, table):
        now = datetime.utcnow()
        earlier = now - timedelta(days=1)
        query1 = "select hours as metric, sum(count) as count from " + table + \
                 " where dt='" + earlier.strftime("%Y-%m-%d") + \
                 "' and hours >" + str(earlier.hour) + \
                 " group by hours"
        query2 = "select hours as metric, sum(count) as count from " + table + \
                 " where dt='" + now.strftime("%Y-%m-%d") + \
                 "' and hours <=" + str(now.hour) + \
                 " group by hours"

        rows = self.cass.session.execute(query1)
        hour_map = {}
        add_rows_to_map(rows, hour_map)
        rows = self.cass.session.execute(query2)
        add_rows_to_map(rows, hour_map)
        return map_to_list(hour_map, 23)

    def get_per_minute_data(self, table):
        now = datetime.utcnow()
        earlier = now - timedelta(hours=1, minutes=10)
        query1 = "select minutes as metric, sum(count) as count from " + table + \
                 " where dt='" + earlier.strftime("%Y-%m-%d") + \
                 "' and hours =" + str(earlier.hour) + \
                 " and minutes > " + str(earlier.minute) + " group by minutes"
        query2 = "select minutes as metric, sum(count) as count from " + table + \
                 " where dt='" + now.strftime("%Y-%m-%d") + \
                 "' and hours =" + str(now.hour) \
                 + " and minutes <= " + str(now.minute) + " group by minutes"

        rows = self.cass.session.execute(query1)
        minute_map = {}
        add_rows_to_map(rows, minute_map)
        rows = self.cass.session.execute(query2)
        add_rows_to_map(rows, minute_map)
        return map_to_list(minute_map, 59)

    def get_current_live_channel(self, n):
        query = "select * from live_channel_by_hour limit " + str(n)
        rows = self.cass.session.execute(query)
        row_map = {}
        for row in rows:
            row_map[row.channel] = row.count
        ordered = OrderedDict(sorted(row_map.items(), key=lambda t: t[1]))
        return ordered

    def get_current_chat_user(self, n):
        query = "select * from chat_user_by_hour limit " + str(n)
        rows = self.cass.session.execute(query)
        row_map = {}
        for row in rows:
            row_map[row.username] = row.count
        ordered = OrderedDict(sorted(row_map.items(), key=lambda t: t[1]))
        return ordered

    def get_current_chat_channel(self, n):
        query = "select * from chat_channel_by_hour limit " + str(n)
        rows = self.cass.session.execute(query)
        row_map = {}
        for row in rows:
            row_map[row.channel] = row.count
        ordered = OrderedDict(sorted(row_map.items(), key=lambda t: t[1]))
        return ordered


if __name__ == '__main__':
    # print(DataUtil(config).get_daily_data('live_channel_by_day'))
    chans = DataUtil(config).get_current_chat_channel(10)
    print(chans.keys())
    keys = map(lambda x: x[1:], chans.keys())
    print(keys)
