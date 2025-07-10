import pandas as pd
import numpy as np
from pymongo import MongoClient
from sklearn.preprocessing import StandardScaler
from mongoDB_setup import connect_mongo

# Connect to MongoDB
db = connect_mongo()

# Load Data from MongoDB
sp500_data = pd.DataFrame(list(db["sp500_data"].find()))
macroeco_data = pd.DataFrame(list(db["macroeco"].find()))
news_data = pd.DataFrame(list(db["news_data"].find()))
top10_data = pd.DataFrame(list(db["Top10_stocks"].find()))

# Function to clean datasets
def clean_dataframe(df, name, date_col="Date"):
    """Cleans and removes duplicates from dataframes."""
    if df.empty:
        print(f" {name} data is empty!")
        return df

    
    df.drop(columns=['_id'], errors='ignore', inplace=True)  
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df.dropna(subset=[date_col], inplace=True)
    
    if "Ticker" in df.columns:
        df.drop_duplicates(subset=[date_col, "Ticker"], keep='last', inplace=True)
    else:
        df.drop_duplicates(subset=[date_col], keep='last', inplace=True)

    df.sort_values(by=date_col, inplace=True)
    return df

# Clean all Datasets
sp500_data = clean_dataframe(sp500_data, "S&P 500")
macroeco_data = clean_dataframe(macroeco_data, "Macroeco")
news_data = clean_dataframe(news_data, "News")
top10_data = clean_dataframe(top10_data, "Top 10 Stocks")

# Ensure continous date range
start_date = "2017-04-01"
end_date = "2024-03-31"
date_range = pd.date_range(start=start_date, end=end_date, freq='D')

# Reindex All Datasets to Ensure a Continuous Time Series
def reindex_dataframe(df, name):
    """Reindexes dataframe to include all dates in the range."""
    df = df.set_index("Date").reindex(date_range).reset_index().rename(columns={'index': 'Date'})
    df.ffill(inplace=True)
    df.bfill(inplace=True)
    df.fillna(0, inplace=True)
    print(f" {name} reindexed and missing values filled!")
    return df

sp500_data = reindex_dataframe(sp500_data, "S&P 500")
macroeco_data = reindex_dataframe(macroeco_data, "Macroeco")

# Extract sentiment - +ve or -ve from news data
if "Sentiment" in news_data.columns:
    news_data['Date'] = pd.to_datetime(news_data['Date'])

    def extract_sentiment(sentiment, key):
        """Extracts compound sentiment from Title or Abstract."""
        if isinstance(sentiment, dict):
            return sentiment.get(key, {}).get("compound", 0)
        return 0

    news_data['Title_Sentiment'] = news_data['Sentiment'].apply(lambda x: extract_sentiment(x, "TitleSentiment"))
    news_data['Abstract_Sentiment'] = news_data['Sentiment'].apply(lambda x: extract_sentiment(x, "AbstractSentiment"))

    # Compute Total Sentiment Score - added feature
    news_sentiment = news_data.groupby("Date", as_index=False).agg({
        'Title_Sentiment': 'mean',
        'Abstract_Sentiment': 'mean'
    })
    news_sentiment['Avg_News_Sentiment'] = (news_sentiment['Title_Sentiment'] + news_sentiment['Abstract_Sentiment']) / 2
    news_sentiment = news_sentiment[['Date', 'Avg_News_Sentiment']]
    news_sentiment = reindex_dataframe(news_sentiment, "News Sentiment")
else:
    print("News data missing! Adding default.")
    news_sentiment = pd.DataFrame({"Date": date_range, "Avg_News_Sentiment": 0})

# Fix Top 10 Stocks 'Adj Close' Prices
top10_stock_names = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "GOOG", "TSLA", "BRK-B", "META", "XOM"]

# Extract "Adj Close" Values from MongoDB JSON
def extract_adj_close(value):
    """Extracts Adj Close from MongoDB nested structure"""
    if isinstance(value, dict) and "$numberDouble" in value:
        return float(value["$numberDouble"])
    return value  

top10_data["Adj Close"] = top10_data["Adj Close"].apply(extract_adj_close)

# Pivot operation to get stock prices per ticker
top10_pivot = top10_data.pivot(index="Date", columns="Ticker", values="Adj Close").reset_index()

# Ensure ALL 10 STOCKS ARE PRESENT
missing_stocks = [ticker for ticker in top10_stock_names if ticker not in top10_pivot.columns]
for ticker in missing_stocks:
    print(f" {ticker} is missing! Adding empty column.")
    top10_pivot[ticker] = np.nan  # Add missing tickers as NaN for proper filling

# Rename columns for clarity
top10_pivot.rename(columns={ticker: f"{ticker}_Adj_Close" for ticker in top10_stock_names}, inplace=True)

# Merge with main dataset (Include News & Macro Data)
combined_data = sp500_data.merge(macroeco_data, on="Date", how="left")
combined_data = combined_data.merge(news_sentiment, on="Date", how="left")
combined_data = combined_data.merge(top10_pivot, on="Date", how="left")

# Fill Missing Values
combined_data.ffill(inplace=True)
combined_data.bfill(inplace=True)

# Normalize Data
scaler = StandardScaler()
to_normalize = ["Adj_Close", "GDP", "Inflation", "Interest_Rate", "Avg_News_Sentiment"] + [f"{ticker}_Adj_Close" for ticker in top10_stock_names]

for feature in to_normalize:
    if feature in combined_data.columns:
        combined_data[f"Normalized_{feature}"] = scaler.fit_transform(combined_data[[feature]])

# Add Normalized S&P 500 Adj Close
combined_data["Normalized_SP500_Adj_Close"] = scaler.fit_transform(combined_data[["Adj_Close"]])

# Feature Engineering
print("\ Performing Feature Engineering...")

# Rolling Features
combined_data['Rolling_Mean_7'] = combined_data['Adj_Close'].rolling(window=7).mean().fillna(0)
combined_data['Rolling_Mean_30'] = combined_data['Adj_Close'].rolling(window=30).mean().fillna(0)
combined_data['Rolling_Volatility_30'] = combined_data['Adj_Close'].rolling(window=30).std().fillna(0)

# Lag Features
combined_data['Lag_1'] = combined_data['Adj_Close'].shift(1).fillna(0)
combined_data['Lag_3'] = combined_data['Adj_Close'].shift(3).fillna(0)
combined_data['Lag_7'] = combined_data['Adj_Close'].shift(7).fillna(0)

# Target Features
combined_data['Future_Return_7'] = ((combined_data['Adj_Close'].shift(-7) - combined_data['Adj_Close']) / combined_data['Adj_Close']).fillna(0)
combined_data['Price_Direction'] = (combined_data['Future_Return_7'] > 0).astype(int)

# Split Data into Training & Testing (Test Data: 1st Feb 2024 - 31st Mar 2024)
train_data = combined_data[combined_data["Date"] < "2024-02-01"]
test_data = combined_data[(combined_data["Date"] >= "2024-02-01") & (combined_data["Date"] <= "2024-03-31")]

# Drop `_id` Columns Before Saving
train_data.drop(columns=['_id'], errors='ignore', inplace=True)
test_data.drop(columns=['_id'], errors='ignore', inplace=True)

# Xlxs for verification
#train_data.to_excel("train_data.xlsx", index=False)
#test_data.to_excel("test_data.xlsx", index=False)

# Save Back to MongoDB
db["feature_engineering"].delete_many({})
db["feature_engineering"].insert_many(combined_data.to_dict("records"))


# Save to MongoDB
db["train_data"].delete_many({})
db["test_data"].delete_many({})
db["train_data"].insert_many(train_data.to_dict("records"))
db["test_data"].insert_many(test_data.to_dict("records"))

print("Training & Testing Data Ready! ")
