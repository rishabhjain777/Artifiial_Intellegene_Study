import os
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load environment variables from .env file located in the script directory
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# Configuration loaded from .env
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
MESSAGE = "Hello from Python to Group Chat!"

def send_telegram_message(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"  # Optional: "HTML" or "MarkdownV2"
    }
    
    response = requests.post(url, json=payload)
    return response.json()

if __name__ == "__main__":
    result = send_telegram_message(BOT_TOKEN, CHAT_ID, MESSAGE)
    print(result)
