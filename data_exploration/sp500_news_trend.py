import pandas as pd
import numpy as np
from pymongo import MongoClient
from urllib.parse import quote_plus
import plotly.graph_objects as go

# MongoDB connection setup
username = 'Add your details'
password = 'Add your details'
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

uri = f"mongodb+srv://{encoded_username}:{encoded_password}@sp500.7wtsc.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, ssl=True)
db = client["Sp500"]

# Fetch data from MongoDB
sp500_data = pd.DataFrame(list(db["sp500_data"].find()))
news_data = pd.DataFrame(list(db["news_data"].find()))

# Ensure 'Date' is properly formatted
sp500_data['Date'] = pd.to_datetime(sp500_data['Date'], errors='coerce')
news_data['Date'] = pd.to_datetime(news_data['Date'], errors='coerce')

# Drop rows with invalid dates
sp500_data.dropna(subset=['Date'], inplace=True)
news_data.dropna(subset=['Date'], inplace=True)

# Check for sentiment columns in news_data
if 'Sentiment' in news_data.columns:
    # Extract positive and negative components from 'Sentiment' field
    news_data['Positive_Title_Sentiment'] = news_data['Sentiment'].apply(
        lambda x: x['TitleSentiment']['pos'] if isinstance(x, dict) and 'TitleSentiment' in x else np.nan
    )
    news_data['Negative_Title_Sentiment'] = news_data['Sentiment'].apply(
        lambda x: x['TitleSentiment']['neg'] if isinstance(x, dict) and 'TitleSentiment' in x else np.nan
    )
    news_data['Positive_Abstract_Sentiment'] = news_data['Sentiment'].apply(
        lambda x: x['AbstractSentiment']['pos'] if isinstance(x, dict) and 'AbstractSentiment' in x else np.nan
    )
    news_data['Negative_Abstract_Sentiment'] = news_data['Sentiment'].apply(
        lambda x: x['AbstractSentiment']['neg'] if isinstance(x, dict) and 'AbstractSentiment' in x else np.nan
    )
else:
    raise KeyError("The 'Sentiment' column does not exist in news_data. Verify your dataset.")

# Normalize S&P 500 Adjusted Close Prices
sp500_data['Normalized_SP500'] = (
    (sp500_data['Adj_Close'] - sp500_data['Adj_Close'].min()) /
    (sp500_data['Adj_Close'].max() - sp500_data['Adj_Close'].min())
)

# Aggregate positive and negative sentiment scores by date
news_data = news_data.groupby('Date').agg(
    Avg_Positive_Sentiment=('Positive_Title_Sentiment', 'mean'),
    Avg_Negative_Sentiment=('Negative_Title_Sentiment', 'mean')
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

# Debugging: Inspect normalized data
print("Normalized News Sentiment Data:")
print(news_data[['Date', 'Normalized_Avg_Positive_Sentiment', 'Normalized_Avg_Negative_Sentiment']].head())

# Merge data with common dates
start_date = max(sp500_data['Date'].min(), news_data['Date'].min())
end_date = min(sp500_data['Date'].max(), news_data['Date'].max())

sp500_data = sp500_data[(sp500_data['Date'] >= start_date) & (sp500_data['Date'] <= end_date)]
news_data = news_data[(news_data['Date'] >= start_date) & (news_data['Date'] <= end_date)]

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
    title="S&P 500 and Positive/Negative Sentiment (Normalized) Trendlines",
    xaxis_title="Date",
    yaxis_title="Normalized Values",
    legend=dict(title="Dataset", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
)

fig.show()
