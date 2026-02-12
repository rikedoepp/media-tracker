#!/usr/bin/env python3
"""
Production script to scrape all unscraped URLs from the database
Handles large batches with progress tracking and error handling
"""

from bigquery_client import BigQueryClient
from web_scraper import get_website_text_content
import time
from datetime import datetime
from google.cloud import bigquery

def scrape_all_unscraped():
    """Scrape all URLs that don't have content yet"""
    
    client = BigQueryClient()
    
    print(f"\n{'='*80}")
    print(f"BULK CONTENT SCRAPING")
    print(f"{'='*80}\n")
    
    # Get all unscraped URLs
    query = """
    SELECT id, url, title
    FROM `media-455519.mediatracker.mediatracker`
    WHERE content IS NULL OR content = ""
    ORDER BY id ASC
    """
    
    print("📊 Fetching unscraped URLs from database...")
    results = list(client.client.query(query).result())
    
    if not results:
        print("✅ No unscraped URLs found! All done.")
        return
    
    total = len(results)
    print(f"📋 Found {total} URLs to scrape\n")
    
    # Track statistics
    successes = 0
    failures = 0
    start_time = time.time()
    
    # Process each URL
    for i, row in enumerate(results, 1):
        elapsed = time.time() - start_time
        avg_time = elapsed / i if i > 0 else 0
        remaining = (total - i) * avg_time
        
        print(f"\n[{i}/{total}] ID {row.id} ({successes} ✅, {failures} ❌)")
        print(f"  URL: {row.url[:70]}...")
        print(f"  Time: {elapsed/60:.1f}m elapsed, ~{remaining/60:.1f}m remaining")
        
        # Scrape the content
        try:
            content = get_website_text_content(row.url)
            
            if content and len(content.strip()) > 50:
                # Successfully scraped
                print(f"  ✅ Scraped {len(content)} characters")
                
                # Update the database
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
                job = client.client.query(update_query, job_config=job_config)
                job.result()
                
                successes += 1
                
            else:
                # Failed to scrape
                print(f"  ❌ No content extracted")
                
                # Log the error
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
                job = client.client.query(update_query, job_config=job_config)
                job.result()
                
                failures += 1
        
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:60]}")
            
            # Log the error
            try:
                update_query = """
                UPDATE `media-455519.mediatracker.mediatracker`
                SET text_scraped = false,
                    text_scrape_error = @error,
                    text_scraped_at = @scraped_at
                WHERE id = @id
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("error", "STRING", str(e)[:500]),
                        bigquery.ScalarQueryParameter("scraped_at", "STRING", datetime.now().isoformat()),
                        bigquery.ScalarQueryParameter("id", "INT64", int(row.id))
                    ]
                )
                job = client.client.query(update_query, job_config=job_config)
                job.result()
            except:
                pass
            
            failures += 1
        
        # Polite delay between requests (1 second)
        if i < total:
            time.sleep(1)
        
        # Progress update every 50 URLs
        if i % 50 == 0:
            success_rate = (successes / i) * 100
            print(f"\n📊 Progress Update:")
            print(f"   Processed: {i}/{total} ({i/total*100:.1f}%)")
            print(f"   Success rate: {success_rate:.1f}%")
            print(f"   Time elapsed: {elapsed/60:.1f} minutes")
    
    # Final summary
    total_time = time.time() - start_time
    success_rate = (successes / total) * 100 if total > 0 else 0
    
    print(f"\n{'='*80}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*80}\n")
    print(f"✅ Successful: {successes}/{total} ({success_rate:.1f}%)")
    print(f"❌ Failed: {failures}/{total} ({failures/total*100:.1f}%)")
    print(f"⏱️  Total time: {total_time/60:.1f} minutes")
    print(f"⚡ Average: {total_time/total:.2f} seconds per URL\n")

if __name__ == "__main__":
    scrape_all_unscraped()
