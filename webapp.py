from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd

app = Dash(__name__)

branded_food = pd.read_csv('data/branded_food_reduced.csv')

fig = px.bar(branded_food, x='category_id', y='serving_size')