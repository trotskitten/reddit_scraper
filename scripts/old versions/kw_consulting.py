
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

    keywords = ["consulting", "consultant", "consultancy"]

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

        await asyncio.sleep(5)

    print(f"Total raw posts retrieved from r/{community}: {total_raw_posts}")

    # Write today's scrape
    daily_file = os.path.join(BASE_DIR, f"{community}_{now}.csv")
    pd.DataFrame(posts_dict).to_csv(daily_file, index=False)

    # Merge with cumulative file
    merged_file = os.path.join(BASE_DIR, f"{community}_consultant_merged.csv")

    if os.path.exists(merged_file):
        df = pd.concat(
            map(pd.read_csv, [daily_file, merged_file]),
            ignore_index=True
        )
    else:
        df = pd.read_csv(daily_file)

    # ---------- CLEANING + DAILY REPORT ----------
    from common.cleaning import clean_dataframe

    df, removed_empty_text, removed_duplicates = clean_dataframe(df, text_column="Text")
    after_dedupe = len(df)

    df.to_csv(merged_file, index=False)

    print(f"--- Summary for r/{community} ---")
    print(f"Raw posts retrieved:                {total_raw_posts}")
    print(f"Removed empty text posts:           {removed_empty_text}")
    print(f"Removed duplicate-text posts:       {removed_duplicates}")
    print(f"Final posts in {community}_consultant_merged:  {after_dedupe}")
    print("------------------------------------\n")
    os.remove(daily_file)

    await reddit.close()



async def main():

    os.makedirs(BASE_DIR, exist_ok=True)

    communities = ["all"]
    all_scraped_dataframes = []

    for community in communities:
        await scrape_subreddit(community)
        merged_path = os.path.join(BASE_DIR, f"{community}_consultant_merged.csv")

        if os.path.exists(merged_path) and os.path.getsize(merged_path) > 0:
            all_scraped_dataframes.append(pd.read_csv(merged_path))
        else:
            print(f"Warning: Empty or missing merged file for {community}")

    # ---------- FINAL GLOBAL MERGE ----------


    if all_scraped_dataframes:

        final_df = pd.concat(all_scraped_dataframes, ignore_index=True)

        from common.cleaning import clean_dataframe
        final_df, removed_empty_text, removed_duplicates = clean_dataframe(final_df, text_column="Text")

        # MERGE + LOG (centralized)
        from common.io_helpers import merge_clean_save

        stats = merge_clean_save(
            df=final_df,
            merged_filename="consulting_kw_merged.csv",
            log_filename="consulting_log.csv",
            base_dir=BASE_DIR
        )

        raw_total = stats["raw_total"]
        before_dedupe_total = stats["old_total"]
        final_total = stats["final_total"]
        new_posts = stats["new_posts"]

        print("========== FINAL CONSULTING SUMMARY ==========")
        print(f"Raw combined posts today:    {raw_total}")
        print(f"previous total:           {before_dedupe_total}")
        print(f"Final unique posts:          {final_total}")
        print(f"New unique posts added:      {new_posts}")
        print("=============================================\n")

        return {
            "label": "CONSULTING",
            "raw_total": raw_total,
            "old_total": before_dedupe_total,
            "final_total": final_total,
            "new_posts": new_posts,
        }

        return {
            "label": "CONSULTING",
            "raw_total": 0,
            "old_total": 0,
            "final_total": 0,
            "new_posts": 0,
        }





