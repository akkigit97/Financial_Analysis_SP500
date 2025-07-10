import requests
import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus

# MongoDB connection setup
username = 'akhilamohan24'
password = 'GS2ksl2bQhoiscry'
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

uri = f"mongodb+srv://{encoded_username}:{encoded_password}@sp500.7wtsc.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, ssl=True)
db = client["Sp500"]
collection = db["macroeco"]

print("✅ MongoDB Connection Successful")

# FRED API details
FRED_API_KEY = "07c1fcad2cb862a200ae50f16c289b15"
start_date = "2017-04-01"
end_date = "2024-04-01"

# Define FRED indicators
fred_indicators = {
    "CPIAUCSL": "Inflation (CPI)",
    "GDP": "Gross Domestic Product (GDP)",
    "FEDFUNDS": "Interest Rates (Fed Funds Rate)"
}

# Function to fetch data from FRED
def fetch_fred_data(series_id, start, end, api_key):
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "observation_start": start,
        "observation_end": end,
        "api_key": api_key,
        "file_type": "json"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        df = pd.DataFrame(response.json()["observations"])
        if df.empty:
            print(f"⚠️ No data found for {series_id}")
            return None
        return df
    else:
        print(f"❌ Error fetching {series_id}: {response.status_code}")
        return None

# Fetch and prepare data
all_data = []
for series_id, indicator_name in fred_indicators.items():
    print(f"📡 Fetching data for {indicator_name}...")
    data = fetch_fred_data(series_id, start_date, end_date, FRED_API_KEY)
    if data is not None:
        data["Indicator"] = indicator_name
        data["Value"] = pd.to_numeric(data["value"], errors="coerce")
        data["Date"] = pd.to_datetime(data["date"])
        data = data[["Date", "Value", "Indicator"]].dropna()
        all_data.append(data)

# ✅ Merge all indicators into a single DataFrame
if all_data:
    macroeco_df = pd.concat(all_data)
    print("✅ Macroeconomic data collected successfully!")

    # Pivot data to have columns for GDP, Inflation, and Interest Rates
    macroeco_pivot = macroeco_df.pivot(index="Date", columns="Indicator", values="Value").reset_index()

    # Rename columns for clarity
    macroeco_pivot.rename(columns={
        "Gross Domestic Product (GDP)": "GDP",
        "Inflation (CPI)": "Inflation",
        "Interest Rates (Fed Funds Rate)": "Interest_Rate"
    }, inplace=True)

    # Ensure full date range is covered
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    macroeco_pivot = macroeco_pivot.set_index("Date").reindex(date_range).reset_index().rename(columns={'index': 'Date'})

    # Fill missing values using **backfill first**, then **forward fill** as a fallback
    macroeco_pivot.bfill(inplace=True)
    macroeco_pivot.ffill(inplace=True)

    # Drop any duplicate rows if they exist
    macroeco_pivot.drop_duplicates(subset=["Date"], inplace=True)

    # ✅ Insert into MongoDB
    collection.delete_many({})  # Clear old data
    collection.insert_many(macroeco_pivot.to_dict("records"))

    print("✅ Macroeconomic data successfully stored in MongoDB.")

    # ✅ Save as Excel for backup
    macroeco_pivot.to_excel("macroeco_data.xlsx", index=False)
    print("✅ Data saved as 'macroeco_data.xlsx'")

else:
    print("❌ No macroeconomic data to insert.")
