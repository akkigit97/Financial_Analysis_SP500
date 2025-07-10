import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import seaborn as sns
import logging
from pymongo import MongoClient
from mongoDB_setup import connect_mongo


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def visualize_data():
    """Fetches data from MongoDB, processes it, and generates visualizations."""
    db = connect_mongo()

    # Function to fetch data
    def fetch_data(collection_name):
        """Fetches data from MongoDB and converts it to a DataFrame."""
        return pd.DataFrame(list(db[collection_name].find()))

    logging.info("Fetching data from MongoDB SP500 Database...")
    sp500_data = fetch_data("sp500_data")
    macroeco_data = fetch_data("macroeco")
    news_data = fetch_data("news_data")
    top10_data = fetch_data("Top10_stocks")

    # Convert Date to Proper Format
    for df in [sp500_data, macroeco_data, news_data, top10_data]:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df.dropna(subset=["Date"], inplace=True)

    # Normalize Function
    def normalize_series(series):
        """Normalize a pandas Series between 0 and 1."""
        return (series - series.min()) / (series.max() - series.min())

    # Normalize S&P 500
    sp500_data["Normalized_SP500"] = normalize_series(sp500_data["Adj_Close"])

    # Normalize Top 10 Stocks
    top10_pivot = top10_data.pivot(index="Date", columns="Ticker", values="Adj Close").reset_index()

    # Ensure unique column names (Avoid double normalization)
    normalized_tickers = []
    for ticker in top10_pivot.columns[1:]:  # Skip 'Date'
        normalized_col = f"Normalized_{ticker}"
        top10_pivot[normalized_col] = normalize_series(top10_pivot[ticker])
        normalized_tickers.append(normalized_col)

    # Compute Top 10 Aggregate (Equal-Weighted Mean)
    top10_pivot["Top10_Aggregate"] = top10_pivot[normalized_tickers].mean(axis=1)
    top10_pivot["Normalized_Top10_Aggregate"] = normalize_series(top10_pivot["Top10_Aggregate"])

    # Normalize Macro Economic Data
    for col in ["GDP", "Inflation", "Interest_Rate"]:
        macroeco_data[f"Normalized_{col}"] = normalize_series(macroeco_data[col])

    # Normalize News Sentiment
    if "Sentiment" in news_data.columns:
        news_data["Title_Sentiment"] = news_data["Sentiment"].apply(
            lambda x: x.get("TitleSentiment", {}).get("compound", 0) if isinstance(x, dict) else 0
        )
        news_data["Abstract_Sentiment"] = news_data["Sentiment"].apply(
            lambda x: x.get("AbstractSentiment", {}).get("compound", 0) if isinstance(x, dict) else 0
        )
        news_data["Avg_News_Sentiment"] = (news_data["Title_Sentiment"] + news_data["Abstract_Sentiment"]) / 2
        news_data["Normalized_News_Sentiment"] = normalize_series(news_data["Avg_News_Sentiment"])

    # Merge Data for Visualization
    sp500_merged = sp500_data.merge(top10_pivot, on="Date", how="left")
    sp500_merged = sp500_merged.merge(macroeco_data, on="Date", how="left")
    sp500_merged = sp500_merged.merge(news_data[["Date", "Normalized_News_Sentiment"]], on="Date", how="left")

    # Drop NaN Values (Ensure Consistency)
    sp500_merged.dropna(inplace=True)

    # Plot 1: S&P 500 vs Top 10 Individual Stocks
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=sp500_merged["Date"], y=sp500_merged["Normalized_SP500"],
                            mode="lines", name="S&P 500 (Normalized)", line=dict(color="blue")))

    for ticker in normalized_tickers:
        fig1.add_trace(go.Scatter(x=sp500_merged["Date"], y=sp500_merged[ticker],
                                mode="lines", name=f"{ticker} (Normalized)", line=dict(dash="dot")))

    fig1.update_layout(
        title="S&P 500 and Top 10 Stocks Normalized Trendlines",
        xaxis_title="Date", yaxis_title="Normalized Adjusted Close",
        legend=dict(title="Dataset", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
    )
    fig1.show()

    # Plot 2: S&P 500 vs Macro Economic Indicators
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=sp500_merged["Date"], y=sp500_merged["Normalized_SP500"],
                            mode="lines", name="S&P 500 (Normalized)", line=dict(color="blue")))
    for col, color in zip(["Normalized_GDP", "Normalized_Inflation", "Normalized_Interest_Rate"],
                           ["green", "red", "purple"]):
        fig2.add_trace(go.Scatter(x=sp500_merged["Date"], y=sp500_merged[col],
                                  mode="lines", name=f"{col.split('_')[1]} (Normalized)", line=dict(color=color, dash="dot")))

    fig2.update_layout(
        title="S&P 500 and Macro Economic Indicators Normalized Trendlines",
        xaxis_title="Date", yaxis_title="Normalized Value",
        legend=dict(title="Dataset", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
    )
    fig2.show()

    # Plot 3: S&P 500 vs Top 10 Aggregate
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=sp500_merged["Date"], y=sp500_merged["Normalized_SP500"],
                            mode="lines", name="S&P 500 (Normalized)", line=dict(color="blue")))
    fig3.add_trace(go.Scatter(x=sp500_merged["Date"], y=sp500_merged["Normalized_Top10_Aggregate"],
                            mode="lines", name="Top 10 Aggregate (Normalized)", line=dict(color="orange", dash="dot")))

    fig3.update_layout(
        title="S&P 500 vs Top 10 Aggregate Normalized Trendlines",
        xaxis_title="Date", yaxis_title="Normalized Value",
        legend=dict(title="Dataset", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
    )
    fig3.show()

    # Plot 4: S&P 500 vs News Sentiment
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=sp500_merged["Date"], y=sp500_merged["Normalized_SP500"],
                            mode="lines", name="S&P 500 (Normalized)", line=dict(color="blue")))
    fig4.add_trace(go.Scatter(x=sp500_merged["Date"], y=sp500_merged["Normalized_News_Sentiment"],
                            mode="lines", name="News Sentiment (Normalized)", line=dict(color="red", dash="dot")))

    fig4.update_layout(
        title="S&P 500 vs News Sentiment Normalized Trendlines",
        xaxis_title="Date", yaxis_title="Normalized Value",
        legend=dict(title="Dataset", orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
    )
    fig4.show()

    logging.info("Data Visualization Completed!")


if __name__ == "__main__":
    visualize_data()
