import asyncpraw
import pandas as pd
import datetime
import os
import asyncio
import time 
from IPython.display import clear_output
import datetime


BASE_DIR = r"C:\Users\ranik\OneDrive\Desktop\Sociologia digitale e Analisi del Web\Tesi di laurea\CSV"



async def scrape_subreddit(community: str):
    """Scrape newest posts from a subreddit and maintain a cleaned merged CSV."""

    os.makedirs(BASE_DIR, exist_ok=True)

    from common.reddit_client import get_reddit
    reddit = get_reddit()

    subreddit = await reddit.subreddit(community)

    print(f"Scraping newest posts from r/{community} ...")
    posts_generator = subreddit.new(limit=250)

    posts_dict = {
        "Title": [],
        "Text": [],
        "Username": [],
        "ID": [],
        "community": [],
        "Date": [],
        "Time": [],
        "Post URL": []
    }

    timestamps = []
    raw_count = 0

    async for post in posts_generator:
        created_dt = datetime.datetime.fromtimestamp(post.created_utc)

        posts_dict["Date"].append(created_dt.strftime('%d-%m-%Y'))
        posts_dict["Time"].append(created_dt.strftime('%H:%M:%S'))
        posts_dict["Title"].append(post.title)
        posts_dict["Text"].append(post.selftext)
        posts_dict["Username"].append(str(post.author))
        posts_dict["ID"].append(post.id)
        posts_dict["Post URL"].append(post.url)
        posts_dict["community"].append(community)

        timestamps.append(created_dt)
        raw_count += 1

    # Earliest timestamp for this batch
    earliest_str = (
        min(timestamps).strftime("%Y-%m-%d %H:%M:%S")
        if timestamps else "N/A"
    )

    print(f"Retrieved {raw_count} posts from r/{community} (earliest: {earliest_str})")

    # Merged file path (persistent corpus per subreddit)
    merged_file = os.path.join(BASE_DIR, f"{community}_merged.csv")

    initial_merged_count = 0
    # Load existing merged file if it exists
    if os.path.exists(merged_file):
        existing_df = pd.read_csv(merged_file)
        initial_merged_count = len(existing_df)
        df = pd.concat(
            [existing_df, pd.DataFrame(posts_dict)],
            ignore_index=True
        )
    else:
        df = pd.DataFrame(posts_dict)

    # CLEANING & DEDUPLICATION --------------------------------------------------

    from common.cleaning import clean_dataframe

    # CLEANING (refactored)
    df, removed_empty, removed_duplicates = clean_dataframe(df, text_column="Text")

    # AFTER CLEANING
    after_dedupe = len(df)

    # Save updated merged dataset
    df.to_csv(merged_file, index=False)


    # SUMMARY OUTPUT ------------------------------------------------------------
    new_posts_added = after_dedupe - initial_merged_count

    print(f"--- Summary for r/{community} ---")
    print(f"Raw posts retrieved:                {raw_count}")
    print(f"Removed empty text posts:           {removed_empty}")
    print(f"Removed duplicate-text posts:       {removed_duplicates}")
    print(f"Posts in merged file (before update): {initial_merged_count}")
    print(f"Posts in merged file (after update):  {after_dedupe}")
    print(f"New posts added to merged file:     {new_posts_added}")
    print("------------------------------------\n")

    await reddit.close()

    # 🔙 NEW: return stats for this subreddit
    return {
        "community": community,
        "raw_count": raw_count,
        "removed_empty": removed_empty,
        "removed_duplicates": removed_duplicates,
        "initial_merged_count": initial_merged_count,
        "final_merged_count": after_dedupe,
        "new_posts_added": new_posts_added,
    }



async def main():
    """Scrape multiple subreddits independently and return aggregated stats."""

    os.makedirs(BASE_DIR, exist_ok=True)

    communities = [
        "ChatGPT",
        "consulting",
        "AiAssisted",
        "antiai",
        "GeminiAI",
        "GenAI4all",
        "SideProject",
        "ChatGPTcomplaints",
        "ChatGPTPro",
        "ChatGPTJailbreak",
        "AI_Agents",
        "generativeAI"
    ]

    total_raw = 0
    total_new = 0

    for community in communities:
        stats = await scrape_subreddit(community)
        total_raw += stats["raw_count"]
        total_new += stats["new_posts_added"]

    # 🔙 NEW: return global stats for orchestrator
    return {
        "label": "SUBREDDITS",
        "raw_total": total_raw,
        "old_total": None,   # not really meaningful globally here
        "final_total": None,      # one merged file per subreddit
        "new_posts": total_new,
    }


if __name__ == "__main__":
    asyncio.run(main())