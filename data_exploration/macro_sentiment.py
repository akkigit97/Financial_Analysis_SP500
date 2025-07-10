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
macroeco_data = pd.DataFrame(list(db["macroeco"].find()))
news_data = pd.DataFrame(list(db["news_data"].find()))

# Ensure 'Date' is properly formatted
macroeco_data['Date'] = pd.to_datetime(macroeco_data['Date'], errors='coerce')
news_data['Date'] = pd.to_datetime(news_data['Date'], errors='coerce')

# Drop rows with invalid dates
macroeco_data.dropna(subset=['Date'], inplace=True)
news_data.dropna(subset=['Date'], inplace=True)

# Filter macroeconomic indicators
inflation_data = macroeco_data[macroeco_data['Indicator'] == 'Inflation (CPI)']
gdp_data = macroeco_data[macroeco_data['Indicator'] == 'Gross Domestic Product (GDP)']
interest_rate_data = macroeco_data[macroeco_data['Indicator'] == 'Interest Rates (Fed Funds Rate)']

# Normalize macroeconomic indicators
def normalize_data(df, value_column):
    df['Normalized_Value'] = (
        (df[value_column] - df[value_column].min()) /
        (df[value_column].max() - df[value_column].min())
    )
    return df

inflation_data = normalize_data(inflation_data, 'Value')
gdp_data = normalize_data(gdp_data, 'Value')
interest_rate_data = normalize_data(interest_rate_data, 'Value')

# Check for sentiment columns in news_data
print("News Data Columns:", news_data.columns)  # Debugging: Check column names

if 'Sentiment' in news_data.columns:
    # Extract positive and negative components from 'Sentiment' field
    news_data['Avg_Positive_Sentiment'] = news_data['Sentiment'].apply(
        lambda x: x['TitleSentiment']['pos'] if isinstance(x, dict) and 'TitleSentiment' in x else np.nan
    )
    news_data['Avg_Negative_Sentiment'] = news_data['Sentiment'].apply(
        lambda x: x['TitleSentiment']['neg'] if isinstance(x, dict) and 'TitleSentiment' in x else np.nan
    )
else:
    raise KeyError("The 'Sentiment' column does not exist in news_data. Verify your dataset.")

# Aggregate positive and negative sentiment scores by date
news_data = news_data.groupby('Date').agg(
    Avg_Positive_Sentiment=('Avg_Positive_Sentiment', 'mean'),
    Avg_Negative_Sentiment=('Avg_Negative_Sentiment', 'mean')
).reset_index()

# Normalize sentiment scores
def normalize_sentiment(df, column):
    df[f'Normalized_{column}'] = (
        (df[column] - df[column].min()) /
        (df[column].max() - df[column].min())
    )
    return df

news_data = normalize_sentiment(news_data, 'Avg_Positive_Sentiment')
news_data = normalize_sentiment(news_data, 'Avg_Negative_Sentiment')

# Debugging: Check normalized sentiment columns
print("Normalized News Sentiment Data Columns:", news_data.columns)
print(news_data[['Date', 'Normalized_Avg_Positive_Sentiment', 'Normalized_Avg_Negative_Sentiment']].head())

# Merge data with common dates
start_date = max(
    inflation_data['Date'].min(),
    gdp_data['Date'].min(),
    interest_rate_data['Date'].min(),
    news_data['Date'].min()
)
end_date = min(
    inflation_data['Date'].max(),
    gdp_data['Date'].max(),
    interest_rate_data['Date'].max(),
    news_data['Date'].max()
)

inflation_data = inflation_data[(inflation_data['Date'] >= start_date) & (inflation_data['Date'] <= end_date)]
gdp_data = gdp_data[(gdp_data['Date'] >= start_date) & (gdp_data['Date'] <= end_date)]
interest_rate_data = interest_rate_data[(interest_rate_data['Date'] >= start_date) & (interest_rate_data['Date'] <= end_date)]
news_data = news_data[(news_data['Date'] >= start_date) & (news_data['Date'] <= end_date)]

# Plot trendlines using Plotly
fig = go.Figure()

# Add normalized inflation trendline
fig.add_trace(go.Scatter(
    x=inflation_data['Date'],
    y=inflation_data['Normalized_Value'],
    mode='lines',
    name='Inflation (Normalized)',
    line=dict(color='purple', dash='solid')
))

# Add normalized GDP trendline
fig.add_trace(go.Scatter(
    x=gdp_data['Date'],
    y=gdp_data['Normalized_Value'],
    mode='lines',
    name='GDP (Normalized)',
    line=dict(color='blue', dash='solid')
))

# Add normalized interest rate trendline
fig.add_trace(go.Scatter(
    x=interest_rate_data['Date'],
    y=interest_rate_data['Normalized_Value'],
    mode='lines',
    name='Interest Rates (Normalized)',
    line=dict(color='red', dash='solid')
))

# Add normalized positive sentiment trendline
fig.add_trace(go.Scatter(
    x=news_data['Date'],
    y=news_data['Normalized_Avg_Positive_Sentiment'],
    mode='lines',
    name='Positive Sentiment (Normalized)',
    line=dict(color='green', dash='dot')
))

# Add normalized negative sentiment trendline
fig.add_trace(go.Scatter(
    x=news_data['Date'],
    y=news_data['Normalized_Avg_Negative_Sentiment'],
    mode='lines',
    name='Negative Sentiment (Normalized)',
    line=dict(color='red', dash='dot')
))

# Update layout for clarity
fig.update_layout(
    title="Macroeconomic Indicators and Sentiment Trends (Normalized)",
    xaxis_title="Date",
    yaxis_title="Normalized Values",
    legend=dict(title="Dataset", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
)

fig.show()
