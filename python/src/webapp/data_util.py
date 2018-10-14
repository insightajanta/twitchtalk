
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
        now = datetime.utcnow()
        earlier = now - timedelta(days=7)

        query = "select dt, sum(count) as count from " + table \
                + " where dt > '" + earlier.strftime("%Y-%m-%d") + "' group by dt limit 7 allow filtering"
        if config['debug']:
            print query

        rows = self.cass.session.execute(query)
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        row_map = {}
        for row in rows:
            row_map[row.dt] = row.count

        new_map = dict((days[datetime.strptime(key, '%Y-%m-%d').weekday()], value) for (key, value) in row_map.items())
        ret_map = OrderedDict()
        for day in days:
            ret_map[day] = new_map.pop(day, -1)
        return ret_map

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

    def get_per_minute_chat_data(self):
        now = datetime.utcnow()
        earlier = now - timedelta(hours=1, minutes=10)
        query1 = "select minutes as metric, sum(count) as count from chat_channel_minute" \
                 " where dt='" + earlier.strftime("%Y-%m-%d") + \
                 "' and hours =" + str(earlier.hour) + \
                 " and minutes > " + str(now.minute) + " group by minutes"
        query2 = "select minutes as metric, sum(count) as count from chat_channel_minute" \
                 " where dt='" + now.strftime("%Y-%m-%d") + \
                 "' and hours =" + str(now.hour) \
                 + " and minutes <= " + str(now.minute) + " group by minutes"

        if self.config['debug']:
            print(query1)
            print(query2)

        rows = self.cass.session.execute(query1)
        minute_map = {}
        add_rows_to_map(rows, minute_map)
        rows = self.cass.session.execute(query2)
        add_rows_to_map(rows, minute_map)
        return map_to_list(minute_map, 59)

    def get_per_minute_live_data(self):
        now = datetime.utcnow()
        earlier = now - timedelta(hours=1, minutes=10)
        query1 = "select minutes as metric, sum(viewers) as count from live_channel" \
                 " where dt='" + earlier.strftime("%Y-%m-%d") + \
                 "' and hours =" + str(earlier.hour) + \
                 " and minutes > " + str(now.minute) + " group by minutes"
        query2 = "select minutes as metric, sum(viewers) as count from live_channel" \
                 " where dt='" + now.strftime("%Y-%m-%d") + \
                 "' and hours =" + str(now.hour) \
                 + " and minutes <= " + str(now.minute) + " group by minutes"

        if self.config['debug']:
            print(query1)
            print(query2)

        rows = self.cass.session.execute(query1)
        minute_map = {}
        add_rows_to_map(rows, minute_map)
        rows = self.cass.session.execute(query2)
        add_rows_to_map(rows, minute_map)
        return map_to_list(minute_map, 59)

    def get_top_n_last_hour(self, n, table, column):
        now = datetime.utcnow()
        earlier = now - timedelta(hours=1)

        query = "select " \
                + column + " as metric, count from " + table \
                + " where dt='" + earlier.strftime("%Y-%m-%d") + "' limit " + str(n)
        if config['debug']:
            print(query)
        rows = self.cass.session.execute(query)
        row_map = {}
        for row in rows:
            row_map[row.metric] = row.count
        ordered = OrderedDict(sorted(row_map.items(), key=lambda t: t[1]))
        if config['debug']:
            print(ordered)
        return ordered

    def get_current_live_channel(self, n):
        return self.get_top_n_last_hour(n, "live_channel_by_hour", "channel")

    def get_current_chat_user(self, n):
        return self.get_top_n_last_hour(n, "chat_username_by_hour", "username")

    def get_current_chat_channel(self, n):
        return self.get_top_n_last_hour(n, "chat_channel_by_hour", "channel")


if __name__ == '__main__':
    print(DataUtil(config).get_daily_data("chat_channel_by_day"))