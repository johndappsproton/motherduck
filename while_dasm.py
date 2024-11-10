# while_dsam.py
# 
from dash import Dash, html, dcc, callback, Output, Input 
import plotly.express as px 
import pandas as pd
from sqlalchemy import create_engine, text

duckdb_engine = create_engine("duckdb:///md:sample_data")
connection = duckdb_engine.connect()

countries = pd.read_sql('SELECT DISTINCT country_name as countries FROM who.ambient_air_quality ORDER BY country_name', connection)

app = Dash()

app.layout = [
    html.H1(children='Air quality by country', style={'textAlign':'center'}),
    dcc.Dropdown(countries['countries'].tolist(), 'Canada', id='dropdown-selection'),
    dcc.Graph(id='graph-content')
]

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
	t = text("SELECT year, avg(pm25_concentration) as avg_pm25 FROM who.ambient_air_quality WHERE country_name=:country GROUP by year ORDER by year")
	result = pd.read_sql(t, connection, params={'country': value})
	return px.line(result, x='year', y='avg_pm25')

if __name__ == '__main__':
    app.run(debug=True)