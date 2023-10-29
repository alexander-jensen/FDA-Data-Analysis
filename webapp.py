from dash import Dash, html, dcc, dash_table
import plotly.express as px
import pandas as pd

app = Dash(__name__)

branded_food = pd.read_csv('data/branded_food_reduced.csv')
categories = pd.read_csv('data/branded_food_categories.csv')

branded_food = branded_food.merge(categories, on='category_id') 
category_hist = (px.histogram(branded_food,
    y='category').update_xaxes(categoryorder='total descending'))

app.layout = html.Div([
    html.Div(children='FDA Branded Food Data Visualization'),
    dash_table.DataTable(data=branded_food.head(100).to_dict('records'), page_size=10),
    # Create a histogram for different categories
    dcc.Graph(figure=category_hist)
])

if __name__ == '__main__':
    app.run(debug=True)
