import asyncpraw
import os


def get_reddit():
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError(
            "Missing Reddit credentials. Please set REDDIT_CLIENT_ID and "
            "REDDIT_CLIENT_SECRET in your environment."
        )

    return asyncpraw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent="genai-research-bot:v1.0 (by u:genai_research_client)",
    )
