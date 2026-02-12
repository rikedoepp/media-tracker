#!/usr/bin/env python3
"""
Batch scraping script - processes URLs in smaller batches
"""
import sys
from bigquery_client import BigQueryClient
from web_scraper import get_website_text_content
import time
from datetime import datetime
from google.cloud import bigquery

def scrape_batch(batch_size=100):
    """Scrape a batch of unscraped URLs"""
    
    client = BigQueryClient()
    
    # Get batch of unscraped URLs
    query = f"""
    SELECT id, url, title
    FROM `media-455519.mediatracker.mediatracker`
    WHERE content IS NULL OR content = ""
    ORDER BY id ASC
    LIMIT {batch_size}
    """
    
    print(f"📊 Fetching up to {batch_size} unscraped URLs...", flush=True)
    results = list(client.client.query(query).result())
    
    if not results:
        print("✅ No unscraped URLs found!", flush=True)
        return 0, 0
    
    total = len(results)
    print(f"📋 Found {total} URLs to scrape\n", flush=True)
    
    successes = 0
    failures = 0
    
    for i, row in enumerate(results, 1):
        print(f"[{i}/{total}] ID {row.id}", flush=True)
        print(f"  {row.url[:70]}...", flush=True)
        
        try:
            content = get_website_text_content(row.url)
            
            if content and len(content.strip()) > 50:
                print(f"  ✅ {len(content)} chars", flush=True)
                
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
                print(f"  ❌ No content", flush=True)
                
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
            print(f"  ❌ Error: {str(e)[:50]}", flush=True)
            failures += 1
        
        time.sleep(0.5)  # Small delay
    
    return successes, failures

if __name__ == "__main__":
    batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    
    print(f"\n{'='*80}", flush=True)
    print(f"BATCH SCRAPING ({batch_size} URLs)", flush=True)
    print(f"{'='*80}\n", flush=True)
    
    successes, failures = scrape_batch(batch_size)
    
    print(f"\n{'='*80}", flush=True)
    print(f"BATCH COMPLETE", flush=True)
    print(f"{'='*80}", flush=True)
    print(f"✅ Success: {successes}", flush=True)
    print(f"❌ Failed: {failures}", flush=True)
    print(f"📊 Rate: {successes/(successes+failures)*100:.1f}%", flush=True)
