import yfinance as yf
import pymongo
from urllib.parse import quote_plus
import pandas as pd

'''Part 1: set up the required fixed requirements for the data:
 such as the ticker symbols, the start and end dates, and the database name.
  Use this to access the required data from yfinance. 
 
 Additionally fill the missing dates (weekends, public holidays etc):
 either with the closing value from the previous day or opening value from the next trading day.
 
 This will ensure a continous time series data for the analysis. '''

# GSPC is the ticker symbol for the S&P 500 index on yfinance
ticker_symbol = "^GSPC"

# Define the start and end dates for data range- this is inclusive
start_date = "2017-04-01"
end_date = "2024-04-01"

# Download the requried data from yfinance in a pandas dataframe called sp500_data
sp500_data = yf.download(ticker_symbol, start=start_date, end=end_date)

# Flatten MultiIndex columns
sp500_data.columns = ['_'.join(filter(None, col)) for col in sp500_data.columns]
print("Flattened columns:", sp500_data.columns)

# Rename flattened columns to simpler names
sp500_data.rename(columns={
    'Date_': 'Date',
    'Adj Close_^GSPC': 'Adj_Close',
    'Close_^GSPC': 'Close',
    'High_^GSPC': 'High',
    'Low_^GSPC': 'Low',
    'Open_^GSPC': 'Open',
    'Volume_^GSPC': 'Volume'
}, inplace=True)
print("Renamed columns:", sp500_data.columns)

# Reset the index to make the Date column explicit
sp500_data.reset_index(inplace=True)
print("Columns after reset_index:", sp500_data.columns)

# Rename 'Date_' to 'Date' for simplicity
sp500_data.rename(columns={'Date_': 'Date'}, inplace=True)

# Ensure 'Date' exists
if 'Date' not in sp500_data.columns:
    raise KeyError("The 'Date' column is missing after reset_index.")

sp500_data['Date'] = pd.to_datetime(sp500_data['Date'])

# Ensure the Date column is sorted
sp500_data.sort_values(by='Date', inplace=True)
print("Columns after sorting:", sp500_data.columns)

# Fill missing dates with the previous business/trading day's closing value
sp500_data.set_index('Date', inplace=True)
sp500_data = sp500_data.asfreq('D', method='ffill')
print("Index after asfreq:", sp500_data.index)

'''the very beginning and last dates do not have a previous or next trading day, 
so we need to handle these cases separately'''

# Add the missing dates for April 1st and 2nd 2017
missing_dates_2017 = pd.date_range(start='2017-04-01', end='2017-04-02', freq='D')
missing_data_2017 = sp500_data.loc['2017-04-03'].copy()  # Use data from April 3rd to backfill
missing_data_2017 = pd.DataFrame([missing_data_2017] * len(missing_dates_2017), index=missing_dates_2017)  # Replicate data


# similar, add missing dates for 2024 March 29th, 30th, 31st, and April 1st
missing_dates_2024 = pd.date_range(start='2024-03-29', end='2024-04-01', freq='D')
missing_data_2024 = sp500_data.loc['2024-03-28'].copy()  # Use data from April 2nd to backfill
missing_data_2024 = pd.DataFrame([missing_data_2024] * len(missing_dates_2024), index=missing_dates_2024)

# Ensure all missing data includes a 'Date' column
missing_data_2017.reset_index(inplace=True)
missing_data_2017.rename(columns={'index': 'Date'}, inplace=True)
missing_data_2024.reset_index(inplace=True)
missing_data_2024.rename(columns={'index': 'Date'}, inplace=True)


print("Columns in missing_data_2017:", missing_data_2017.columns)
print("Columns in missing_data_2024:", missing_data_2024.columns)

# Combine all missing data with the main dataset
sp500_data.reset_index(inplace=True)
print("Columns before concatenation:", sp500_data.columns)
sp500_data = pd.concat([missing_data_2017, missing_data_2024, sp500_data])
print("Columns after concatenation:", sp500_data.columns)


# Sort and remove duplicates (in case of index overlaps)
sp500_data.duplicated(subset=['Date']).sum()
sp500_data.drop_duplicates(subset=['Date'], inplace=True)
sp500_data.sort_values(by=['Date'], inplace=True)

# Calculate daily returns as percentage change in adjusted close prices
sp500_data['Return'] = sp500_data['Adj_Close'].pct_change()

# Remove any rows with NaN values (such as the first row after pct_change calculation)
sp500_data.dropna(inplace=True)

# Reset index to prepare data for MongoDB insertion
# Convert the DataFrame to a dictionary format suitable for MongoDB
sp500_data.columns = [str(col) for col in sp500_data.columns]  # Ensure column names are strings
data_dict = sp500_data.to_dict("records")



'''This section handles connecting to MongoDB, a remote online database to store the acquired data'''

# MongoDB connection to URI and database and collection config
username = 'Add your details'  
password = 'Add your details'  
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

# Connect to MongoDB
#replace the URI with the name of your database if its different
uri = f"mongodb+srv://{encoded_username}:{encoded_password}@sp500.7wtsc.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri, ssl=True)
db = client["Sp500"]    #database name
collection = db["sp500_data"]   #collection name
print("Connected to MongoDB successfully.")

# Insert data into the MongoDB collection sp500_data
if data_dict:
    collection.insert_many(data_dict)
    print(f"Data successfully inserted into MongoDB database 'Sp500', collection 'sp500_data'.")
else:
    print("No data to insert into MongoDB.")


# Save the data to an Excel file for easier understanding of what the data looks like
#excel_filename = 'sp500_data.xlsx'
#sp500_data.to_excel(excel_filename, index=False)
#print(f"Data successfully saved to Excel file 'sp500_data_complete'.")

#collection.delete_many({})

# Close the MongoDB connection
client.close()
