import dash
import dash_core_components as dcc
import dash_html_components as html
from src.config.config import config
from data_util import DataUtil
from datetime import datetime

app = dash.Dash()
minutes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
           10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
           20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
           30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
           40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
           50, 51, 52, 53, 54, 55, 56, 57, 58, 59]
hours = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
query_helper = DataUtil(config)


def get_hour_live_graph():
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='hour-graph-live',
                            figure={
                                'data': [
                                    {'x': minutes,
                                     'y': query_helper.get_per_minute_live_data(),
                                     'type': 'line', 'name': 'Popular'}
                                ],
                                'layout': {
                                    'title': 'Popular - avg viewership per minute'
                                }
                            }
                        )])


def get_hour_chat_graph():
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='hour-graph-chat',
                            figure={
                                'data': [
                                    {'x': minutes,
                                     'y': query_helper.get_per_minute_chat_data(),
                                     'type': 'line', 'name': 'Engaged'}
                                ],
                                'layout': {
                                    'title': 'Engaged - chat messages per minute'
                                }
                            }
                        )])


def get_day_graph(name, table_prefix):
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='day-graph-' + name,
                            figure={
                                'data': [
                                    {'x': hours,
                                     'y': query_helper.get_hourly_data(table_prefix + "_channel_by_hour"),
                                     'type': 'line', 'name': name}
                                ],
                                'layout': {
                                    'title': name
                                }
                            }
                        )])


def get_week_graph(name, table_prefix):
    row_map = query_helper.get_daily_data(table_prefix + '_channel_by_day')
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='week-graph-' + name,
                            figure={
                                'data': [
                                    {'x': row_map.keys(), #map(lambda x: days[datetime.strptime(x, '%Y-%m-%d').weekday()],
                                              #row_map.keys()),
                                     'y': row_map.values(),
                                     'type': 'line', 'name': name}
                                ],
                                'layout': {
                                    'title': name
                                }
                            }
                        )])


def get_current_popular_channels():
    row_map = query_helper.get_current_live_channel(10)
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='popular-channels',
                            figure={
                                'data': [
                                    {'y': row_map.keys(),
                                     'x': row_map.values(),
                                     'type': 'bar', 'name': 'popular', 'orientation': 'h'}
                                ],
                                'layout': {
                                    'title': 'Top 10 Most Popular Channels'
                                }
                            }
                        )])


def get_current_engaged_channels():
    row_map = query_helper.get_current_chat_channel(10)

    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='engaged-channels',
                            figure={
                                'data': [
                                    {'y': map(lambda x: x[1:], row_map.keys()),
                                     'x': row_map.values(),
                                     'type': 'bar', 'name': 'engaged', 'orientation': 'h'}
                                ],
                                'layout': {
                                    'title': 'Top 10 Most Engaged Channels'
                                }
                            }
                        )])


def get_current_engaged_users():
    row_map = query_helper.get_current_chat_user(10)
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='engaged-users',
                            figure={
                                'data': [
                                    {'x': row_map.values(),
                                     'y': row_map.keys(),
                                     'type': 'bar', 'name': 'engaged', 'orientation': 'h'}
                                ],
                                'layout': {
                                    'title': 'Top 10 Most Engaged Users'
                                }
                            }
                        )])


def serve_layout():
    return html.Div(children=[
        html.H1(children='Twitch Talk'),

        html.Div(children=['''
    Tracking Popularity and Engagement of Live channels
    ''']),

        html.Div(className='flex-grid',
                 children=[
                     get_current_popular_channels(),
                     get_current_engaged_channels(),
                     get_current_engaged_users()
                 ]),
        html.Div(className='flex-grid',
                 children=[
                     get_hour_chat_graph(),
                     get_day_graph("Engaged - chat messages per hour", "chat"),
                     get_week_graph("Engaged - chat messages per day", "chat")
                 ]),
        html.Div(className='flex-grid',
                 children=[
                     get_hour_live_graph(),
                     get_day_graph("Popular - avg viewership per hour", "live"),
                     get_week_graph("Popular - avg viewership per day", "live")
                 ])
    ])


app.layout = serve_layout

if __name__ == '__main__':
    if config['debug']:
        print(config['webapp_host'] + ":" + config['webapp_port'])
    app.run_server(debug=True, host=config['webapp_host'], port=config['webapp_port'])
