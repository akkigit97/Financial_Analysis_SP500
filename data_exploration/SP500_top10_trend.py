import pandas as pd
import numpy as np
from pymongo import MongoClient
from urllib.parse import quote_plus
import plotly.graph_objects as go

# MongoDB connection setup
username = 'akhilamohan24'
password = 'GS2ksl2bQhoiscry'
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

uri = f"mongodb+srv://{encoded_username}:{encoded_password}@sp500.7wtsc.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, ssl=True)
db = client["Sp500"]

# Fetch data from MongoDB
sp500_data = pd.DataFrame(list(db["sp500_data"].find()))
top10_data = pd.DataFrame(list(db["Top10_stocks"].find()))

# Ensure 'Adj_Close' is consistent in top10_data
if 'Adj Close' in top10_data.columns:
    top10_data.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
elif 'adj_close' in top10_data.columns:
    top10_data.rename(columns={'adj_close': 'Adj_Close'}, inplace=True)
else:
    raise KeyError("The 'Adj_Close' column is missing in top10_data. Please verify your dataset.")

# Ensure 'Date' is properly formatted
sp500_data['Date'] = pd.to_datetime(sp500_data['Date'], errors='coerce')
top10_data['Date'] = pd.to_datetime(top10_data['Date'], errors='coerce')

# Drop rows with invalid dates
sp500_data.dropna(subset=['Date'], inplace=True)
top10_data.dropna(subset=['Date'], inplace=True)

# Normalize S&P 500 Adjusted Close Prices
sp500_data['Normalized_SP500'] = (
    (sp500_data['Adj_Close'] - sp500_data['Adj_Close'].min()) /
    (sp500_data['Adj_Close'].max() - sp500_data['Adj_Close'].min())
)

# Debugging: Check S&P 500 normalized data
print("Normalized S&P 500 Data:")
print(sp500_data[['Date', 'Normalized_SP500']].head())

# Prepare the figure
fig = go.Figure()

# Add S&P 500 normalized trendline
fig.add_trace(go.Scatter(
    x=sp500_data['Date'],
    y=sp500_data['Normalized_SP500'],
    mode='lines',
    name='S&P 500 (Normalized)',
    line=dict(color='blue')
))

# Normalize and plot each top 10 stock
for ticker in top10_data['Ticker'].unique():
    stock_data = top10_data[top10_data['Ticker'] == ticker]
    stock_data['Normalized_Adj_Close'] = (
        (stock_data['Adj_Close'] - stock_data['Adj_Close'].min()) /
        (stock_data['Adj_Close'].max() - stock_data['Adj_Close'].min())
    )
    
    # Debugging: Check normalized data for each stock
    print(f"Normalized Data for {ticker}:")
    print(stock_data[['Date', 'Normalized_Adj_Close']].head())
    
    # Add stock trendline to the plot
    fig.add_trace(go.Scatter(
        x=stock_data['Date'],
        y=stock_data['Normalized_Adj_Close'],
        mode='lines',
        name=ticker,
        line=dict(dash='solid') 
    ))

# Update layout for clarity
fig.update_layout(
    title="S&P 500 and Top 10 Individual Stocks (Normalized) Trendlines",
    xaxis_title="Date",
    yaxis_title="Normalized Adjusted Close Price",
    legend=dict(title="Dataset", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
)

fig.show()
