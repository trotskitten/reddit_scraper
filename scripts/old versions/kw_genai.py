import asyncpraw
import pandas as pd
import datetime
import os
import asyncio
import time
import sys
from IPython.display import clear_output
import datetime

BASE_DIR = r"C:\Users\ranik\OneDrive\Desktop\Sociologia digitale e Analisi del Web\Tesi di laurea\CSV"



async def scrape_subreddit(community):

    now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')

    # Get Reddit client
    from common.reddit_client import get_reddit
    reddit = get_reddit()

    subreddit = await reddit.subreddit(community)

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

    keywords = [
        "ai bot", "gen ai", "ai tools", "chatgpt", "genai",
        "generative artificial intelligence", "ai chat", "generative ai"
    ]

    total_raw_posts = 0  # count everything Reddit returns

    # Keyword-by-keyword search
    for kw in keywords:

        posts_generator = subreddit.search(kw, sort="new", limit=2000)

        kw_count = 0
        kw_timestamps = []   # store timestamps for this specific keyword

        async for post in posts_generator:
            created_dt = datetime.datetime.fromtimestamp(post.created_utc)

            posts_dict["Date"].append(created_dt.strftime('%d-%m-%Y'))
            posts_dict["Time"].append(created_dt.strftime('%H:%M:%S'))
            posts_dict["Title"].append(post.title)
            posts_dict["Text"].append(post.selftext)
            posts_dict["Username"].append(str(post.author))
            posts_dict["ID"].append(post.id)
            posts_dict["Post URL"].append(post.url)
            posts_dict["community"].append(str(post.subreddit))

            kw_count += 1
            kw_timestamps.append(created_dt)

        # Determine earliest timestamp for this keyword
        if kw_timestamps:
            earliest_dt = min(kw_timestamps)
            earliest_str = earliest_dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            earliest_str = "N/A"

        print(f"Keyword '{kw}' retrieved: {kw_count} posts (earliest: {earliest_str})")
        total_raw_posts += kw_count

        # Sleep to avoid rate limiting
        await asyncio.sleep(10)

    # Save today's RAW scrape — no cleaning, no dedupe
    daily_file = os.path.join(BASE_DIR, f"{community}_{now}.csv")
    pd.DataFrame(posts_dict).to_csv(daily_file, index=False)

    print(f"--- Summary for r/{community} ---")
    print(f"Raw posts retrieved:                {total_raw_posts}")
    print("------------------------------------\n")

    await reddit.close()



async def main():

    os.makedirs(BASE_DIR, exist_ok=True)

    community = "all"

    # 1. Run scraper → produce RAW daily CSV file
    await scrape_subreddit(community)

    # 2. Collect ALL raw daily files for this community
    import glob
    daily_files = glob.glob(os.path.join(BASE_DIR, f"{community}_*.csv"))

    if not daily_files:
        print(f"No raw files found for {community}")
        return {
            "label": "GENAI",
            "raw_total": 0,
            "old_total": 0,
            "final_total": 0,
            "new_posts": 0,
        }

    # 3. Load ALL raw scrapes from today
    raw_dfs = [pd.read_csv(file) for file in daily_files]
    raw_df = pd.concat(raw_dfs, ignore_index=True)

    # 4. Clean once
    from common.cleaning import clean_dataframe
    cleaned_df, removed_empty_text, removed_duplicates = clean_dataframe(raw_df, text_column="Text")

    # 5. Merge + dedupe + log using shared helper
    from common.io_helpers import merge_clean_save

    stats = merge_clean_save(
        df=cleaned_df,
        merged_filename="GENAI_merged.csv",
        log_filename="GENAI_log.csv",
        base_dir=BASE_DIR
    )

    raw_total = stats["raw_total"]
    before_dedupe_total = stats["old_total"]
    final_total = stats["final_total"]
    new_posts = stats["new_posts"]

    print("========== FINAL GENAI SUMMARY ==========")
    print(f"Raw combined posts today:    {raw_total}")
    print(f"previous total:              {before_dedupe_total}")
    print(f"Final unique posts:          {final_total}")
    print(f"New unique posts added:      {new_posts}")
    print("=========================================\n")

    # 6. Now safe to delete daily raw files
    for file in daily_files:
        os.remove(file)

    return {
        "label": "GENAI",
        "raw_total": raw_total,
        "old_total": before_dedupe_total,
        "final_total": final_total,
        "new_posts": new_posts,
    }


            