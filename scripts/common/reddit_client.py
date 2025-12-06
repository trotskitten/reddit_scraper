import asyncpraw
import os

def get_reddit():
    return asyncpraw.Reddit(
        client_id=os.environ.get("REDDIT_CLIENT_ID"),
        client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
        user_agent="genai-research-bot:v1.0 (by /u/YOUR_USERNAME)"
    )