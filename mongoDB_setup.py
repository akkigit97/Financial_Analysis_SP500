from pymongo import MongoClient
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def connect_mongo():
    """
    Establish a reusable MongoDB connection.
    """
    try:
        # Get credentials from environment variables
        username = os.getenv("MONGO_USER")
        password = os.getenv("MONGO_PASS")
        cluster = os.getenv("MONGO_CLUSTER")
        database = os.getenv("MONGO_DB")

        if not username or not password or not cluster or not database:
            raise ValueError("Missing MongoDB credentials in environment variables.")

        # Encode the credentials
        encoded_username = quote_plus(username)
        encoded_password = quote_plus(password)

        # Construct MongoDB URI
        uri = f"mongodb+srv://{encoded_username}:{encoded_password}@sp500.7wtsc.mongodb.net/?retryWrites=true&w=majority"

        # Create and return the MongoDB client
        client = MongoClient(uri, ssl=True)
        print("MongoDB connection successful.")
        return client[database]  # Return the specified database
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        raise
