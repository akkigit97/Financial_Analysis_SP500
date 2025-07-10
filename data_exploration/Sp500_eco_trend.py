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
macroeco_data = pd.DataFrame(list(db["macroeco"].find()))

# Ensure 'Date' is properly formatted
sp500_data['Date'] = pd.to_datetime(sp500_data['Date'], errors='coerce')
macroeco_data['Date'] = pd.to_datetime(macroeco_data['Date'], errors='coerce')

# Drop rows with invalid dates
sp500_data.dropna(subset=['Date'], inplace=True)
macroeco_data.dropna(subset=['Date'], inplace=True)

# Filter macroeconomic indicators
inflation_data = macroeco_data[macroeco_data['Indicator'] == 'Inflation (CPI)']
gdp_data = macroeco_data[macroeco_data['Indicator'] == 'Gross Domestic Product (GDP)']
interest_rate_data = macroeco_data[macroeco_data['Indicator'] == 'Interest Rates (Fed Funds Rate)']

# Normalize S&P 500 Adjusted Close Prices
sp500_data['Normalized_SP500'] = (
    (sp500_data['Adj_Close'] - sp500_data['Adj_Close'].min()) /
    (sp500_data['Adj_Close'].max() - sp500_data['Adj_Close'].min())
)

# Normalize inflation, GDP, and interest rates
def normalize_data(df, value_column):
    df['Normalized_Value'] = (
        (df[value_column] - df[value_column].min()) /
        (df[value_column].max() - df[value_column].min())
    )
    return df

inflation_data = normalize_data(inflation_data, 'Value')
gdp_data = normalize_data(gdp_data, 'Value')
interest_rate_data = normalize_data(interest_rate_data, 'Value')

# Debugging: Inspect normalized data
print("Normalized Inflation Data:")
print(inflation_data[['Date', 'Normalized_Value']].head())
print("Normalized GDP Data:")
print(gdp_data[['Date', 'Normalized_Value']].head())
print("Normalized Interest Rate Data:")
print(interest_rate_data[['Date', 'Normalized_Value']].head())

# Merge data with common dates (optional)
start_date = max(
    sp500_data['Date'].min(),
    inflation_data['Date'].min(),
    gdp_data['Date'].min(),
    interest_rate_data['Date'].min()
)
end_date = min(
    sp500_data['Date'].max(),
    inflation_data['Date'].max(),
    gdp_data['Date'].max(),
    interest_rate_data['Date'].max()
)

sp500_data = sp500_data[(sp500_data['Date'] >= start_date) & (sp500_data['Date'] <= end_date)]
inflation_data = inflation_data[(inflation_data['Date'] >= start_date) & (inflation_data['Date'] <= end_date)]
gdp_data = gdp_data[(gdp_data['Date'] >= start_date) & (gdp_data['Date'] <= end_date)]
interest_rate_data = interest_rate_data[(interest_rate_data['Date'] >= start_date) & (interest_rate_data['Date'] <= end_date)]

# Plot trendlines using Plotly
fig = go.Figure()

# Add S&P 500 normalized trendline
fig.add_trace(go.Scatter(
    x=sp500_data['Date'],
    y=sp500_data['Normalized_SP500'],
    mode='lines',
    name='S&P 500 (Normalized)',
    line=dict(color='blue')
))

# Add normalized inflation trendline
fig.add_trace(go.Scatter(
    x=inflation_data['Date'],
    y=inflation_data['Normalized_Value'],
    mode='lines',
    name='Inflation (CPI)',
    line=dict(color='orange', dash='dot')
))

# Add normalized GDP trendline
fig.add_trace(go.Scatter(
    x=gdp_data['Date'],
    y=gdp_data['Normalized_Value'],
    mode='lines',
    name='GDP',
    line=dict(color='green', dash='dot')
))

# Add normalized interest rate trendline
fig.add_trace(go.Scatter(
    x=interest_rate_data['Date'],
    y=interest_rate_data['Normalized_Value'],
    mode='lines',
    name='Interest Rates',
    line=dict(color='red', dash='dot')
))

# Update layout for clarity
fig.update_layout(
    title="S&P 500 and Macroeconomic Indicators (Normalized) Trendlines",
    xaxis_title="Date",
    yaxis_title="Normalized Values",
    legend=dict(title="Dataset", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
)

fig.show()
