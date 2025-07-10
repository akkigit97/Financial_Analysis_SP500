from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the keys
def api():
    api_key = os.getenv("API_KEY")
    fred_api_key = os.getenv("FRED_API_KEY")
    return None

