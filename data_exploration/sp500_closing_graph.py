import pandas as pd
import matplotlib.pyplot as plt
from urllib.parse import quote_plus
import pymongo

# MongoDB connection to URI and database and collection config
username = 'Add your details'  
password = 'Add your details'  
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

# Connect to MongoDB
uri = f"mongodb+srv://{encoded_username}:{encoded_password}@sp500.7wtsc.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri, ssl=True)
db = client["Sp500"]    #database name

# Load datasets from MongoDB
sp500_data = pd.DataFrame(db["sp500_data"].find())
macroeco = pd.DataFrame(db["macroeco"].find())

# Ensure 'Date' is in datetime format
sp500_data['Date'] = pd.to_datetime(sp500_data['Date'])
macroeco['Date'] = pd.to_datetime(macroeco['Date'])

# Merge datasets for combined visualization
merged_data = sp500_data.merge(macroeco, on="Date", how="inner")

# Sort data by date
merged_data = merged_data.sort_values("Date")

# Visualization function
def plot_trends(data, x_col, y_cols, title, ylabel, colors=None):
    """
    Plot trends for multiple columns over time.
    
    Args:
        data (DataFrame): The dataset containing the columns to plot.
        x_col (str): The column for the x-axis (e.g., 'Date').
        y_cols (list): A list of column names to plot on the y-axis.
        title (str): Title of the plot.
        ylabel (str): Label for the y-axis.
        colors (list, optional): List of colors for the lines.
    """
    plt.figure(figsize=(12, 6))
    for i, col in enumerate(y_cols):
        plt.plot(data[x_col], data[col], label=col, color=colors[i] if colors else None)
    plt.title(title, fontsize=16)
    plt.xlabel(x_col, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# Plot 1: S&P 500 Closing Price Trend
plot_trends(
    data=merged_data,
    x_col="Date",
    y_cols=["Close"],
    title="S&P 500 Index Closing Price Over Time",
    ylabel="S&P 500 Closing Price"
)

# Plot 2: Macroeconomic Indicators
plot_trends(
    data=merged_data,
    x_col="Date",
    y_cols=["Inflation (CPI)", "GDP", "InterestRate"],
    title="Macroeconomic Indicators Over Time",
    ylabel="Value",
    colors=["red", "blue", "green"]
)

# Plot 3: Combined View (S&P 500 and Macroeconomic Indicators)
plot_trends(
    data=merged_data,
    x_col="Date",
    y_cols=["Close", "Inflation (CPI)", "GDP", "InterestRate"],
    title="S&P 500 and Macroeconomic Indicators Over Time",
    ylabel="Value",
    colors=["black", "red", "blue", "green"]
)
