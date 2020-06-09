#%%
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash_daq as daq 
import plotly.graph_objects as go
import random
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
from time import sleep

dash_app = dash.Dash(__name__)
app = dash_app.server

DATA_DIR = r'D:\datasets\Vibe_data\data'

df = pq.read_table(DATA_DIR).to_pandas()
df['hashtags'] = df.hashtags.str.split('|')
#%%
COLORS = {
    'negative' : '#ef8a62',
    'neutral' : '#f7f7f7',
    'positive' : '#00acee',
}

#00A1E4
COLORS = {
    'positive' : '#00A1E4',
    'negative' : '#ee6352',
    'neutral' : '#f7f7f7',
}

SIDEBAR = html.Div(className='sidenav', children = [
    html.H1("Vibe", style = {'color':COLORS['positive'],'margin-bottom':'0px'}),
    html.H6("How's Twitter Feeling?", style = {'margin-top':'0px'}),
    html.Hr(),
    html.Div(style = {'text-align':'left'}, children = [
        html.P(['''Investigate daily geographical, hashtag, and sentiment trends in real time. This dashboard is backed by a powerful
                sentiment classifier implemented with Spark and connected to the Twitter sample stream.
                Check it out ''',
                html.A('here.', href = 'https://github.com/AllenWLynch/spark_twitter_sentiment_stream', target = '_blank')
            ]),
        html.Br(),
        html.P('Currently, the stream is classifying:'),
        html.Div(className = 'row', style = {'margin' : '10px'}, children = [
            html.Div(html.H1('9750', id = 'activity_text', style = {'color' : COLORS['positive'], 'margin-bottom' : '0px'}), className = 'column', style = {'text-align' : 'right'}),
            html.Div(html.P('Tweets/min', style = {'margin' : '5px'}), className = 'column'),
        ]),
        html.P('while only interacting with 1% of all Twitter volumne.', style = {'margin-top' : '0px'})
    ]),
    html.Hr(),
])

INTERVAL_SLIDER = dcc.Slider(
    id = 'slider',
    min=1,
    max=60,
    step=None,
    marks={
        1: '1 min',
        10: '10 mins',
        30: '30 mins',
        60: 'Hourly',
    },
    value=1,
    updatemode='mouseup',
) 

timekey = {
    1:'1Min',
    10:'10Min',
    30:'30Min',
    60:'60Min',
}

HASHTAG_PLOT_LAYOUT = dict(margin = {'l': 0, 'b': 0, 't': 0, 'r': 0},
            paper_bgcolor = '#fff',
            plot_bgcolor = '#fff',
            xaxis_gridcolor = '#DCDCDC',
            uirevision = 'true',
)

HASHTAG_PLOT = go.Figure(go.Bar(
        orientation = 'h',
        marker_color = COLORS['positive']
))

HASHTAG_PLOT.update_layout(**HASHTAG_PLOT_LAYOUT)

TOGGLE_SWITCH = html.Div(daq.ToggleSwitch(id = 'toggle', value = False), style = {'width' : '100%', 'display' : 'inline-block'})

FILTERBAR = dcc.Dropdown(id = 'filters', 
                options = [
                    {'label' : option, 'value' : option}
                    for option in np.concatenate(['#' + df.hashtags.explode().unique(), df.place_name.unique()])
                ],
                multi = True,
            )

OPTIONS_BOX = html.Div(children = [
                html.Div(className = 'row', style = {'margin-botton' : '20px', 'margin-top' : '20px', 
                'margin-left' : '17%', 'margin-right':'17%','padding-bottom' : '10px'}, children = [
                    html.Div(style = {'float' : 'left', 'width' : '50%'}, children = [
                        html.Div(className = 'infobox', style = {'margin' : '0px', 'height' : '100px', 'border-right':'none'}, children = [
                            html.Div('Interval', className = 'header', style = {'margin-top' : '1px'}),
                            html.Div(INTERVAL_SLIDER, className = 'body'),
                        ]),
                    ]),
                    html.Div(style = {'float' : 'left', 'width' : '50%'}, children = [
                        html.Div(className = 'infobox', style = {'margin' : '0px', 'height' : '100px'}, children = [
                            html.Div('Filters', className = 'header', style = {'margin-top' : '1px'}),
                            html.Div(FILTERBAR, className = 'body'),
                        ]),
                    ]),
                ])
            ])

GEO_PLOT_LAYOUT = dict(geo_scope = 'usa',
    margin = {'l': 0, 'b': 0, 't': 0, 'r': 0},
    uirevision = 'true',
)

GEO_PLOT = go.Figure(go.Scattergeo())

GEO_PLOT.update_layout(**GEO_PLOT_LAYOUT)

def random_walk(prev, var):
    return max(0, prev + random.normalvariate(0, var))

def generate_random_walk(initial_val, length, var):
    walk = [initial_val]
    for i in range(length - 1):
        walk.append(random_walk(walk[-1], var))
    return walk

random.seed(5000)

time_x = list(range(24))
num_neg = [-1 * r for r in generate_random_walk(24, 24, 4)]
num_pos = generate_random_walk(24, 24, 4)

RIVER_PLOT = go.Figure()
RIVER_TRACE_1 = dict(
    mode = 'lines+markers',
    line = dict(width=0.5, color=COLORS['positive']),
    stackgroup='one',
    name = 'Positive',
)
RIVER_TRACE_2 = dict(
    mode = 'lines+markers',
    line = dict(width=0.5, color=COLORS['negative']),
    stackgroup='two',
    name = 'Negative',
)
RIVER_LAYOUT = dict(
    showlegend = True,
    plot_bgcolor = '#fff',
    margin = {'l': 0, 'b': 0, 't': 0, 'r': 0},
    yaxis = dict(
        title = '# of Tweets',
        linecolor = '#DCDCDC',
        ticks = 'outside',
        tickcolor= '#DCDCDC',
    ),
    xaxis = dict(
        showline = True,
        linecolor = '#DCDCDC',
        ticks = 'outside',
        tickcolor = '#DCDCDC',
        title = 'Time',
    ),
    legend=dict(
        x=0.02,
        y=1,
        traceorder="reversed",
        bgcolor="#fff",
        bordercolor="#DCDCDC",
        borderwidth=1,
        orientation="h",
    ),
    font_family = 'Arial',
    barmode = 'relative',
    uirevision = 'true',
)

RIVER_PLOT.add_trace(go.Scatter(
    x = time_x,
    y = num_pos,
    **RIVER_TRACE_1
))

RIVER_PLOT.add_trace(go.Scatter(
    x = time_x,
    y = num_neg,
    **RIVER_TRACE_2
))

RIVER_PLOT.update_layout(
    **RIVER_LAYOUT
)

dash_app.layout = html.Div(children=[
    SIDEBAR, 
    html.Div(className='main', children = [
        html.Div(className = 'bottom_content', children = [
            dcc.Graph(
                id='river_plot',
                figure = RIVER_PLOT,
                style = {'height' : '50vh', 'margin' : '20px'}
            ),
        ]),
        html.Div(className = 'top_content', children = [
            dcc.Loading(html.Div(id = 'update_msg', style = {'display' : 'none', 'width' : '0px'}), type = 'circle', id = 'loading'),
            dcc.Interval(id = 'interval', interval = 60 * 1000),
        ]),
        OPTIONS_BOX,
        html.Div(className = 'row', children = [
            html.Div(className = 'column', children = [
                html.Div(className = 'infobox', children = [
                    html.Div('Geotags', className = 'header'),
                    html.Div(className = 'body', 
                            children = dcc.Graph(id = 'geo_plot', figure = GEO_PLOT, style = {'height' : '45vh', 'margin-bottom' : '40px'})
                        ),
                ]),
            ]),
            html.Div(className = 'column', children = [
                html.Div(className = 'infobox', children = [
                    html.Div('Hashtags', className = 'header'),
                    html.Div(dcc.Graph(id = 'hashtag_plot', figure = HASHTAG_PLOT, style = {'height' : '45vh', 'margin' : '20px'}), className = 'body'),
                ]),
            ]),
        ]),
    ])
])

@dash_app.callback(
    Output('geo_plot', 'figure'),
    [Input('update_msg', 'children')],
    [State('filters','value')]
)
def update_geoplot(n_intervals, filter_vals):
    
    geo_df = df.groupby('place_name').agg({'lat': 'first', 'lon' : 'first', 
        'num_positive' : 'sum', 'num_negative' : 'sum', 'total' : 'sum'})

    geo_df['marker_color'] = np.where(geo_df.total/2 <= geo_df.num_negative, COLORS['negative'], COLORS['positive'])

    newfig = go.Figure(go.Scattergeo(
        lon = geo_df.lon,
        lat = geo_df.lat,
        text = geo_df.index,
        mode = 'markers',
        marker_size = np.maximum(geo_df.total, 10),
        marker_color = geo_df.marker_color,
    ))
    newfig.update_layout(**GEO_PLOT_LAYOUT)
    return newfig

@dash_app.callback(
    Output('hashtag_plot', 'figure'),
    [Input('update_msg', 'children')]
)
def update_geoplot(n_intervals):
    
    hashtag_df = df.explode('hashtags').groupby('hashtags').agg({'total' : 'sum'})
    hashtag_df = hashtag_df[hashtag_df.index != ""]
    hashtag_df.index = '#' + hashtag_df.index 
    hashtag_df = hashtag_df.nlargest(25, 'total', keep = 'first').sort_values('total')
    newfig = go.Figure(go.Bar(
        y = hashtag_df.index,
        x = hashtag_df.total,
        orientation = 'h',
        marker_color = COLORS['positive']
    ))
    newfig.update_layout(**HASHTAG_PLOT_LAYOUT)
    return newfig

@dash_app.callback(
    Output('filters', 'value'),
    [Input('hashtag_plot', 'clickData'), Input('geo_plot', 'clickData')],
    [State('filters','value')]
)
def update_filtersbar(hashtag_clickdata, geo_clickdata, filter_values):

    if filter_values is None:
        filter_values = []

    ctx = dash.callback_context.triggered

    if ctx[0]['prop_id'] == 'hashtag_plot.clickData' and not hashtag_clickdata is None:
        try:
            hashtag_click = hashtag_clickdata['points'][0]['label']
            if not hashtag_click in filter_values:
                filter_values.append(hashtag_click)
        except KeyError:
            pass
    elif ctx[0]['prop_id'] == 'geo_plot.clickData' and not geo_clickdata is None:
        try:
            geo_click = geo_clickdata['points'][0]['text']
            if not geo_click in filter_values:
                filter_values.append(geo_click)
        except KeyError:
            pass

    return filter_values if len(filter_values) > 0 else None

@dash_app.callback(
    Output('activity_text', 'children'),
    [Input('update_msg', 'children')]
)
def update_activity(num_intervals):
    activity_df = df[df.start_window == df.start_window.values[-1]]
    activity = activity_df.total.sum()
    return str(activity)

def apply_filters(row, filters):

    hashtag_filters = []
    place_name_filters = []
    for f in filters:
        if f[0] == '#':
            hashtag_filters.append(f[1:] in row.hashtags)
        else:
            place_name_filters.append(f == row.place_name)

    return (any(hashtag_filters) or len(hashtag_filters) == 0) and (any(place_name_filters) or len(place_name_filters) == 0)

@dash_app.callback(
    Output('river_plot','figure'),
    [Input('filters','value'), Input('slider','value')],
)
def update_river(filter_vals, slide_val):

    if filter_vals is None or len(filter_vals) == 0:
        river_df = df.copy()
    else:
        river_mask = df.apply(lambda x : apply_filters(x, filter_vals), axis = 1)
        river_df = df[river_mask]
    
    river_df = river_df.groupby([pd.Grouper(key = 'start_window', freq = timekey[slide_val])]).agg({'num_positive':'sum','num_negative':'sum'})
    river_df['num_negative'] = -1 * river_df.num_negative

    newfig = go.Figure()

    newfig.add_trace(go.Scatter(
        x = river_df.index,
        y = river_df.num_positive,
        **RIVER_TRACE_1
    ))

    newfig.add_trace(go.Scatter(
        x = river_df.index,
        y = river_df.num_negative,
        **RIVER_TRACE_2
    ))

    newfig.update_layout(
        **RIVER_LAYOUT
    )

    return newfig

#fix for streaming?
@dash_app.callback(
    Output('update_msg', 'children'),
    [Input('interval','n_intervals')]
)
def update_df(n_intervals):

    df = pq.read_table(DATA_DIR).to_pandas()
    df['hashtags'] = df.hashtags.str.split('|')

    return n_intervals

if __name__ == '__main__':
    dash_app.run_server(debug=True)