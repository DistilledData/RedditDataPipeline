import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table
from datetime import datetime as dt
import pandas as pd
import pandas.io.sql as psql
import flask
import json
import psycopg2
import os
import sys

from dash.dependencies import Input, Output

#add path for util directory to path
#to import from get_postgres_credentials.py
sys.path.append(os.getcwd() + '/../utils')

from get_postgres_credentials import get_secret
from subreddit_list import subreddits

#adding hyperlinks in table is adapted from https://github.com/plotly/dash-recipes/blob/master/dash-html-table-hyperlinks.py
def create_table(dataframe):
    rows = []
    for i in range(len(dataframe)):
        row = []
        for col in dataframe.columns:
            value = dataframe.iloc[i][col]
            # update this depending on which
            # columns you want to show links for
            # and what you want those links to be
            if col == 'permalink':
                cell = html.Td(html.A(href="https://www.reddit.com" + value, children=value))
            else:
                cell = html.Td(children=value)
            row.append(cell)
        rows.append(html.Tr(row))
    return html.Table(id='table',children=
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        rows
    )

#{'username', 'password', 'host', 'port', 'dbname'}
secrets = get_secret()

server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server)#, external_stylesheets=external_stylesheets, server=server) # call flask server

df = pd.read_csv('top_10_daily_posts_by_number_comments_2019_12_01.csv')

#app = dash.Dash()
colors = {
        'background': 'rgba(0, 0, 0, 0)',
        'text': 'rgba( 0, 0, 0, 255)'
    }
   
app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Reddit Data Pipeline',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),
    html.Div(children='Find the most engaging posts for analysis!', style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    html.Div(
        children='Date:', style={
        'textAlign': 'left',
        'color': colors['text']
    }),
    dcc.DatePickerSingle(
        id='Date',
        min_date_allowed=dt(2019, 12, 1),
        max_date_allowed=dt(2019, 12, 31),
        initial_visible_month=dt(2019, 12, 1),
        date = dt(2019, 12, 1)
    ),
    #dcc.DatePickerRange(
    #    id='Date Range',
    #    min_date_allowed=dt(2019, 12, 1),
    #    max_date_allowed=dt(2019, 12, 31),
    #    initial_visible_month=dt(2019, 12, 1),
    #    start_date = dt(2019, 12, 1).date(),
    #    end_date=dt(2019, 12, 31).date()
    #),
    html.Div(
        children='Number of Posts:', style={
        'textAlign': 'left',
        'color': colors['text']
    }),
    dcc.Input(
        id="Number of Posts",
        type="number",
        #placeholder="0",
        min = 1,
        value = 10
    ),
    html.Div(
        children='Subreddit:', style={
        'textAlign': 'left',
        'color': colors['text']
    }),
    dcc.Dropdown(
        id='subreddit',
        options=subreddits,
        placeholder="Select a subreddit"
    ),
    html.Div(
        children="Metric:", style={
            'textAlign': 'left',
        'color': colors['text']
    }),
    dcc.Dropdown(
        id='metric',
        options=[
            { 'label': "Score", 'value': "Score" },
            { 'label': "Number Comments", 'value': "Number Comments"}
        ],
        value="Number Comments"
    ),
    create_table(df),

    # Hidden div inside the app that stores the intermediate value
    html.Div(id='intermediate-value-daily-highest-posts',
             style={'display': 'none'},
             children = json.dumps(
                 { 'date': dt(2019, 12, 1),
                   'number_posts': 10,
                   'subreddit': None,
                   'metric': "Number Comments"}, 
                 default = str
             )
    ),    
    html.Div(id='intermediate-value-date',
             style={'display': 'none'},
             children=json.dumps({ 'date': dt(2019, 12, 1)}, default = str)
    ),
    html.Div(id='intermediate-value-number-posts',
             style={'display': 'none'},
             children=json.dumps({ 'number_posts': 10})
    ),
    html.Div(id='intermediate-value-subreddit',
             style={'display': 'none'},
             children=json.dumps({ 'subreddit': None })
    ),
    html.Div(id='intermediate-value-metric',
             style={'display': 'none'}
    )
])

@app.callback(Output('intermediate-value-date', 'children'),
              [Input('Date', 'date')])
def update_date(date):
    return json.dumps({ 'date': date}, default = str)
    
@app.callback(Output('intermediate-value-number-posts', 'children'),
              [Input("Number of Posts", 'value')])
def update_number_posts(number_posts):
    return json.dumps({ 'number_posts': number_posts})

@app.callback(Output('intermediate-value-subreddit', 'children'),
              [Input('subreddit', 'value')])
def update_subredddit(subreddit):
    return json.dumps({ 'subreddit': subreddit })

@app.callback(Output('intermediate-value-metric', 'children'),
              [Input('metric', 'value')])
def update_metric(metric):
    return json.dumps({ 'metric': metric })

@app.callback(Output('intermediate-value-daily-highest-posts', 'children'),
              [Input('intermediate-value-date', 'children'),
               Input('intermediate-value-number-posts', 'children'),
               Input('intermediate-value-subreddit', 'children'),
               Input('intermediate-value-metric', 'children')])

def update_query_information(date_json, number_posts_json, subreddit_json, metric_json):
    date_dict = json.loads(date_json)
    number_posts_dict = json.loads(number_posts_json)
    subreddit_dict = json.loads(subreddit_json)
    metric_dict = json.loads(metric_json)

    combined_dict = {**date_dict, **number_posts_dict, **subreddit_dict, **metric_dict}
    return json.dumps(combined_dict, default = str)

@app.callback(Output('table', 'children'),
              [Input('intermediate-value-daily-highest-posts', 'children')])
def update_table(query_json):
    query_dict = json.loads(query_json)

    if query_dict['date'].find(" ") != -1:
        date, time = query_dict['date'].split()
    elif query_dict['date'].find('T') != -1:
        date, time = query_dict['date'].split('T')
    else:
        date = query_dict['date']

    year, month, day = date.split('-')
    #post_table_name = 'daily_posts_' + year + '_' + month + '_' + day.zfill(2)

    number_posts = query_dict['number_posts']
    subreddit = query_dict['subreddit']
    metric = query_dict['metric']
    
    query_string = 'SELECT author, subreddit, permalink, score, number_comments, post_date FROM daily_posts WHERE DATE(post_date) = \'' + str(year) + '-' + str(month) + '-' + str(day).zfill(2) + '\''

    if subreddit is not None:
        query_string += " AND subreddit = \'" + subreddit + "\'"

    query_string += " ORDER BY "
    if metric == "Number Comments":
        query_string += "number_comments"
    else:
        query_string += "score"
    query_string += " DESC LIMIT " + str(number_posts) + ";"
    
    records_df = None
    try:
        connection = psycopg2.connect(user = secrets['username'],
                                      password = secrets['password'],
                                      host = secrets['host'],
                                      port = secrets['port'],
                                      database = secrets['dbname'])
        
        cursor = connection.cursor()
            
        # Print PostgreSQL version
        records_df = psql.read_sql(query_string, connection)
        #records = cursor.fetchall()
        #logfile.write("You are connected to - ", record,"\n")
        
    except (Exception, psycopg2.Error) as error :
        pass
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            logfile.write("PostgreSQL connection is closed\n")
        if records_df is None:
            return
        else:
            return create_table(records_df)

if __name__ == '__main__':
    app.run_server(debug=True)
