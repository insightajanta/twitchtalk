# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app = dash.Dash()
minutes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
           10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
           20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
           30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
           40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
           50, 51, 52, 53, 54, 55, 56, 57, 58, 59]

hours = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def get_hour_graph():
    return html.Div(className='col',
             children=[
                 dcc.Graph(
                     id='hour-graph',
                     figure={
                         'data': [
                             {'x': minutes,
                              'y': [15, 14, 11, 17, 28, 11, 7],
                              'type': 'line', 'name': 'popular'},
                             {'x': minutes,
                              'y': [11, 10, 13, 10, 15, 20, 11],
                              'type': 'line', 'name': 'engaged'},
                         ],
                         'layout': {
                             'title': 'by hour, per minute'
                         }
                     }
                 )])


def get_day_graph():
    return html.Div(className='col',
             children=[
                 dcc.Graph(
                     id='day-graph',
                     figure={
                         'data': [
                             {'x': hours,
                              'y': [15, 14, 11, 17, 28, 11, 7, 21, 32, 14, 4, 8, 8, 17, 10, 2, 6, 12,
                                    5, 30, 14],
                              'type': 'line', 'name': 'popular'},
                             {'x': hours,
                              'y': [11, 10, 13, 10, 15, 20, 11, 7, 22, 24, 13, 3, 11, 9, 7, 16, 10,
                                    3, 7, 12, 5, 21,
                                    11],
                              'type': 'line', 'name': 'engaged'},
                         ],
                         'layout': {
                             'title': 'by day, per hour'
                         }
                     }
                 )])


def get_week_graph():
    return html.Div(className='col',
             children=[
                 dcc.Graph(
                     id='week-graph',
                     figure={
                         'data': [
                             {'x': days,
                              'y': [15, 14, 11, 17, 28, 11, 7],
                              'type': 'line', 'name': 'popular'},
                             {'x': days,
                              'y': [11, 10, 13, 10, 15, 20, 11],
                              'type': 'line', 'name': 'engaged'},
                         ],
                         'layout': {
                             'title': 'by week, per day'
                         }
                     }
                 )])


def get_current_popular_channels():
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='popular-channels',
                            figure={
                                'data': [
                                    {'y': ['ninja', 'timthetatman', 'csruhub', 'loltyler1', 'gdf7885', 'castro_1021',
                                           'drdisrespectlive', 'gamepedia', 'asmongold', 'twitchpresents'],
                                     'x': [99686, 21863, 20891, 15924, 15204, 14643, 14587, 14021, 13677, 12604],
                                     'type': 'bar', 'name': 'popular', 'orientation': 'h'}
                                ],
                                'layout': {
                                    'title': 'Top 10 Most Popular Channels'
                                }
                            }
                        )])


def get_current_engaged_channels():
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='engaged-channels',
                            figure={
                                'data': [
                                    {'y': ['twitchpresents', 'asmongold', 'loltyler1', 'drdisrespectlive', 'csruhub',
                                           'castro_1021', 'ninja', 'cohhcarnage', 'handongsuk', 'timthetatman'],
                                     'x': [17603, 12288, 9168, 4952, 3316, 3108, 1693, 1258, 841, 448],
                                     'type': 'bar', 'name': 'engaged', 'orientation': 'h'}
                                ],
                                'layout': {
                                    'title': 'Top 10 Most Engaged Channels'
                                }
                            }
                        )])


def get_current_engaged_users():
    return html.Div(className='col',
                    children=[
                        dcc.Graph(
                            id='engaged-users',
                            figure={
                                'data': [
                                    {'x': [407, 218, 181, 174, 155, 149, 133, 129, 114, 108],
                                     'y': ['nightbot', 'moobot', 'loryfaye', 'umbreon125', 'nullor0', 'theogravity',
                                              'slobatro', 'lefotheof', 'sflopezz', 'shiningwaterdragon'],
                                     'type': 'bar', 'name': 'engaged', 'orientation': 'h'}
                                ],
                                'layout': {
                                    'title': 'Top 10 Most Engaged Users'
                                }
                            }
                        )])

app.layout = html.Div(children=[
    html.H1(children='Analyze Gamers and their Followers'),

    html.Div(children=['''
        Popular Channels (Average viewers) vs Engaged Channels (Users Chatting) Visualization.
    ''']),

    html.Div(className='flex-grid',
             children=[
                 get_hour_graph(),
                 get_day_graph(),
                 get_week_graph()
             ]),
    html.Div(className='flex-grid',
             children=[
                 get_current_popular_channels(),
                 get_current_engaged_channels(),
                 get_current_engaged_users()
             ])

])


if __name__ == '__main__':
    app.run_server(debug=True)
