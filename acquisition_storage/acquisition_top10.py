import yfinance as yf
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from urllib.parse import quote_plus
import pymongo

# 🔹 MongoDB Connection Setup
username = 'akhilamohan24'
password = 'GS2ksl2bQhoiscry'
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

uri = f"mongodb+srv://{encoded_username}:{encoded_password}@sp500.7wtsc.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri, ssl=True)
db = client["Sp500"]  # Database name
collection = db["Top10_stocks"]  # Collection name

print("✅ Connected to MongoDB successfully.")

# 🔹 Define the top 10 companies in the S&P 500 by market capitalization
top_10_companies = {
    "Apple": "AAPL",
    "Microsoft": "MSFT",
    "Amazon": "AMZN",
    "NVIDIA": "NVDA",
    "Alphabet Class A": "GOOGL",
    "Alphabet Class C": "GOOG",
    "Tesla": "TSLA",
    "Berkshire Hathaway": "BRK-B",
    "Meta (Facebook)": "META",
    "Exxon Mobil": "XOM",
}

# 🔹 Define the date range
start_date = "2017-04-01"
end_date = "2024-04-01"

# **Ensure Unique Index in MongoDB (Prevents Duplicate Entries)**
collection.create_index([("Ticker", pymongo.ASCENDING), ("Date", pymongo.ASCENDING)], unique=True)

# 🔹 Function to handle missing dates and convert to MongoDB records
def handle_missing_dates_and_convert(df, ticker, start, end):
    """Ensures all dates are present and fills missing stock data."""
    df.columns = df.columns.get_level_values(0)  # Flatten multi-level columns
    df.reset_index(inplace=True)  # Convert index to Date column
    df.columns = [str(col) for col in df.columns]  # Ensure column names are strings
    df["Date"] = pd.to_datetime(df["Date"])  # Convert Date column

    # Create full date range
    full_date_range = pd.date_range(start=start, end=end, freq="D")

    # Reindex DataFrame to include all dates
    df = df.set_index("Date").reindex(full_date_range).rename_axis("Date").reset_index()

    # Fill missing values (backfill first, then forward-fill as a fallback)
    df.fillna(method="bfill", inplace=True)
    df.fillna(method="ffill", inplace=True)

    # Add Ticker column
    df["Ticker"] = ticker

    # Convert Date to string for MongoDB compatibility
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    return df.to_dict("records")

# 🔹 Fetch and Store Data in MongoDB
for company, ticker in top_10_companies.items():
    print(f"📡 Fetching data for {company} ({ticker})...")
    try:
        # Download historical stock data
        data = yf.download(ticker, start=start_date, end=end_date)

        if not data.empty:
            print(f"✅ Data for {company} fetched successfully!")

            # Ensure continuous data with backfilled values
            records = handle_missing_dates_and_convert(data, ticker, start_date, end_date)

            # Insert/update data in MongoDB
            for record in records:
                try:
                    collection.update_one(
                        {"Ticker": record["Ticker"], "Date": record["Date"]},  # Match by Ticker & Date
                        {"$set": record},  # Update or insert the record
                        upsert=True  # Insert if no match is found
                    )
                except Exception as db_error:
                    print(f"⚠️ Error inserting {ticker} for {record['Date']}: {db_error}")

            print(f"✅ Data for {company} ({ticker}) stored in MongoDB.")
        else:
            print(f"⚠️ No data available for {company} ({ticker}).")
    except Exception as e:
        print(f"❌ Error fetching or processing data for {company} ({ticker}): {e}")

# ✅ Close MongoDB Connection
client.close()
print("🎯 Data fetching and storing process completed successfully!")
