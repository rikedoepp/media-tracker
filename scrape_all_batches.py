#!/usr/bin/env python3
"""
Scrape all unscraped URLs in batches of 100
"""
from bigquery_client import BigQueryClient
from web_scraper import get_website_text_content
import time
from datetime import datetime
from google.cloud import bigquery

def scrape_one_batch(batch_size=100):
    """Scrape one batch and return (success_count, failure_count, urls_processed)"""
    client = BigQueryClient()
    
    query = f"""
    SELECT id, url, title
    FROM `media-455519.mediatracker.mediatracker`
    WHERE content IS NULL OR content = ""
    ORDER BY id ASC
    LIMIT {batch_size}
    """
    
    results = list(client.client.query(query).result())
    
    if not results:
        return 0, 0, 0
    
    successes = 0
    failures = 0
    
    for row in results:
        try:
            content = get_website_text_content(row.url)
            
            if content and len(content.strip()) > 50:
                update_query = """
                UPDATE `media-455519.mediatracker.mediatracker`
                SET content = @content,
                    text_scraped = true,
                    text_scraped_at = @scraped_at
                WHERE id = @id
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("content", "STRING", content),
                        bigquery.ScalarQueryParameter("scraped_at", "STRING", datetime.now().isoformat()),
                        bigquery.ScalarQueryParameter("id", "INT64", int(row.id))
                    ]
                )
                client.client.query(update_query, job_config=job_config).result()
                successes += 1
            else:
                update_query = """
                UPDATE `media-455519.mediatracker.mediatracker`
                SET text_scraped = false,
                    text_scrape_error = @error,
                    text_scraped_at = @scraped_at
                WHERE id = @id
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("error", "STRING", "No content extracted"),
                        bigquery.ScalarQueryParameter("scraped_at", "STRING", datetime.now().isoformat()),
                        bigquery.ScalarQueryParameter("id", "INT64", int(row.id))
                    ]
                )
                client.client.query(update_query, job_config=job_config).result()
                failures += 1
        except Exception as e:
            failures += 1
        
        time.sleep(0.5)
    
    return successes, failures, len(results)

def main():
    print("\n" + "="*80)
    print("BATCH SCRAPING - ALL UNSCRAPED URLS")
    print("="*80 + "\n")
    
    total_success = 0
    total_failed = 0
    batch_num = 1
    start_time = time.time()
    
    while True:
        print(f"\nBatch {batch_num}:", flush=True)
        print("-" * 40, flush=True)
        
        batch_start = time.time()
        successes, failures, processed = scrape_one_batch(100)
        batch_time = time.time() - batch_start
        
        if processed == 0:
            print("✅ No more URLs to scrape!", flush=True)
            break
        
        total_success += successes
        total_failed += failures
        
        print(f"  Processed: {processed} URLs", flush=True)
        print(f"  Success: {successes}, Failed: {failures}", flush=True)
        print(f"  Time: {batch_time:.1f}s", flush=True)
        print(f"  Total so far: {total_success} ✅, {total_failed} ❌", flush=True)
        
        batch_num += 1
        time.sleep(1)
    
    total_time = time.time() - start_time
    total = total_success + total_failed
    
    print("\n" + "="*80)
    print("ALL BATCHES COMPLETE")
    print("="*80)
    print(f"✅ Successful: {total_success}/{total} ({total_success/total*100:.1f}%)")
    print(f"❌ Failed: {total_failed}/{total} ({total_failed/total*100:.1f}%)")
    print(f"⏱️  Total time: {total_time/60:.1f} minutes")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
