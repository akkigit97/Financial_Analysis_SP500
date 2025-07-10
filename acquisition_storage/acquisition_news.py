import http.client
import urllib.parse
import json
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
from urllib.parse import quote_plus
import pymongo
import requests
import time
#from mongoDB_setup import connect_mongo

# MongoDB connection to URI and database and collection config
username = 'Add your details'  
password = 'Add your details'  
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

# Connect to MongoDB
#replace thr URI with your database if it's a different name. Likewise, change the collection name if it's different.
uri = f"mongodb+srv://{encoded_username}:{encoded_password}@sp500.7wtsc.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri, ssl=True)
db = client["Sp500"]    #database name
collection = db["news_data"]   #collection name
print("Connected to MongoDB successfully.")

collection.delete_many({})

# Initialize sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# NY Times API details
API_KEY = "Add your details"  
BASE_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"

# Define keywords and date range
keywords = ["S&P 500",
    "Apple",
    "Microsoft",
    "Amazon",
    "NVIDIA",
    "Alphabet",
    "Tesla",
    "Berkshire Hathaway",
    "Meta",
    "Exxon Mobil", 
    "President of the USA", 
    "SpaceX", 
    "Covid", 
    "Economy",
    "Wildfires", 
    "Hurricane",
    "Climate Change",
    "War in Ukraine",
    "Russia-Ukraine Conflict",
    "Inflation",
    "Interest Rates",
    "Market Crash",
    "Pandemic",
    "recession",
    "Stock Market",
    "bitcoin", 
    "cryptocurrency"
    ]
start_date = "20170401"  # Format: YYYYMMDD
end_date = "20240401"    # Format: YYYYMMDD


def fetch_nyt_news(keyword, from_date, to_date, page=0):
    """
    Fetch news articles using NY Times Article Search API.

    Args:
        keyword (str): Keyword to search for.
        from_date (str): Start date in YYYYMMDD format.
        to_date (str): End date in YYYYMMDD format.
        page (int): Page number for paginated results.

    Returns:
        list: List of news articles.
    """
    params = {
        "q": keyword,
        "begin_date": from_date,
        "end_date": to_date,
        "page": page,
        "api-key": API_KEY,
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json().get("response", {}).get("docs", [])
    elif response.status_code == 429:
        print("Rate limit exceeded. Retrying after 10 seconds...")
        time.sleep(10)
        return fetch_nyt_news(keyword, from_date, to_date, page=page)
    else:
        print(f"Error fetching data for '{keyword}', page {page}: {response.status_code} - {response.text}")
        return []

def analyze_sentiment(text):
    """
    Perform sentiment analysis using VADER.

    Args:
        text (str): Text to analyze.

    Returns:
        dict: Sentiment scores.
    """
    return analyzer.polarity_scores(text)


def process_and_store_news(keyword, articles):
    """
    Process news articles, perform sentiment analysis, and store in MongoDB.

    Args:
        keyword (str): The keyword used to fetch the articles.
        articles (list): List of news articles.
    """
    processed_articles = []
    for article in articles:
        try:
            # Parse the published date
            pub_date = datetime.strptime(article["pub_date"], "%Y-%m-%dT%H:%M:%S%z")

            # Perform sentiment analysis
            title_sentiment = analyze_sentiment(article.get("headline", {}).get("main", ""))
            abstract_sentiment = analyze_sentiment(article.get("abstract", ""))

            # Prepare the document for MongoDB
            processed_article = {
                "Date": pub_date.strftime("%Y-%m-%d"),
                "Title": article.get("headline", {}).get("main"),
                "Abstract": article.get("abstract"),
                "URL": article.get("web_url"),
                "Source": "New York Times",
                "Section": article.get("section_name"),
                "Sentiment": {
                    "TitleSentiment": title_sentiment,
                    "AbstractSentiment": abstract_sentiment,
                },
                "SearchKeyword": keyword,
            }
            processed_articles.append(processed_article)
        except Exception as e:
            print(f"Error processing article: {e}")

    # Store in MongoDB
    if processed_articles:
        collection.insert_many(processed_articles)
        print(f"Stored {len(processed_articles)} articles for keyword '{keyword}' in MongoDB.")
    else:
        print(f"No articles to store for keyword '{keyword}'.")


# Main script
if __name__ == "__main__":
    for keyword in keywords:
        print(f"Fetching NY Times news for keyword: '{keyword}'")
        for page in range(5):  # Fetch up to 5 pages (adjust as needed)
            articles = fetch_nyt_news(keyword, start_date, end_date, page=page)
            if articles:
                process_and_store_news(keyword, articles)
            else:
                print(f"No articles found for '{keyword}' on page {page}.")
            time.sleep(2)  # Add delay to avoid rate limits
        time.sleep(5)  # Add delay between keywords

    # Close MongoDB connection
    client.close()
    print("NY Times news fetching, processing, and storing completed.")
