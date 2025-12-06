import os
import pandas as pd
import datetime


def merge_clean_save(
    df,
    merged_filename,
    log_filename,
    base_dir
):
    """
    Handles:
    - merging with existing CSV
    - dropping duplicates
    - cleaning already done BEFORE calling this
    - saving merged dataset
    - logging unique count
    - computing new_posts relative to last run

    Returns:
    {
        "final_total": int,
        "new_posts": int,
        "raw_total": int (before cleaning),
        "old_total": int (after cleaning, before dedupe)
    }
    """

    merged_path = os.path.join(base_dir, merged_filename)
    log_path = os.path.join(base_dir, log_filename)

    # raw before cleaning
    raw_total = len(df)

    # If merged file exists, merge with it
    if os.path.exists(merged_path):
        old_df = pd.read_csv(merged_path)
        combined = pd.concat([old_df, df], ignore_index=True)
    else:
        old_df = pd.DataFrame()
        combined = df.copy()

    # After cleaning but before dedupe
    old_total = len(old_df)

    # Final dedupe
    combined.drop_duplicates(subset=["Text"], inplace=True)
    final_total = len(combined)

    # Load last logged unique count
    if os.path.exists(log_path):
        log_df = pd.read_csv(log_path)
        last_logged_total = int(log_df["unique_texts"].iloc[-1])
    else:
        last_logged_total = 0

    # Compute new unique posts added this run
    new_posts = final_total - last_logged_total

    # Save merged dataset
    combined.to_csv(merged_path, index=False)

    # Log new total
    log_entry = pd.DataFrame([{
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "unique_texts": final_total
    }])

    if os.path.exists(log_path):
        log_entry.to_csv(log_path, mode="a", header=False, index=False)
    else:
        log_entry.to_csv(log_path, index=False)

    return {
        "raw_total": raw_total,
        "old_total": old_total,
        "final_total": final_total,
        "new_posts": new_posts
    }
