#!/usr/bin/env python3
"""
Test script to scrape unscraped URLs from the database
"""

from bigquery_client import BigQueryClient
from web_scraper import get_website_text_content
import time
from datetime import datetime

def test_scraping(num_urls=5):
    """Test scraping on a few unscraped URLs"""
    
    client = BigQueryClient()
    
    # Get a few unscraped URLs
    query = f"""
    SELECT id, url, title
    FROM `media-455519.mediatracker.mediatracker`
    WHERE content IS NULL OR content = ""
    LIMIT {num_urls}
    """
    
    print(f"\n{'='*80}")
    print(f"TESTING CONTENT SCRAPING")
    print(f"{'='*80}\n")
    
    results = list(client.client.query(query).result())
    
    if not results:
        print("❌ No unscraped URLs found!")
        return
    
    print(f"Found {len(results)} unscraped URLs to test\n")
    
    successes = 0
    failures = 0
    
    for i, row in enumerate(results, 1):
        print(f"\n[{i}/{len(results)}] Processing ID {row.id}")
        print(f"  URL: {row.url[:80]}...")
        print(f"  Title: {row.title}")
        
        # Scrape the content
        print(f"  Status: Scraping...", end=" ")
        content = get_website_text_content(row.url)
        
        if content and len(content.strip()) > 50:
            # Successfully scraped
            print(f"✅ Success ({len(content)} chars)")
            
            # Update the database
            from google.cloud import bigquery
            update_query = f"""
            UPDATE `media-455519.mediatracker.mediatracker`
            SET content = @content,
                text_scraped = true,
                text_scraped_at = @scraped_at
            WHERE id = {row.id}
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("content", "STRING", content),
                    bigquery.ScalarQueryParameter("scraped_at", "STRING", datetime.now().isoformat())
                ]
            )
            job = client.client.query(update_query, job_config=job_config)
            job.result()
            
            print(f"  Database: Updated ✅")
            successes += 1
            
        else:
            # Failed to scrape
            print(f"❌ Failed (empty or error)")
            
            # Log the error
            from google.cloud import bigquery
            update_query = f"""
            UPDATE `media-455519.mediatracker.mediatracker`
            SET text_scraped = false,
                text_scrape_error = 'No content extracted',
                text_scraped_at = @scraped_at
            WHERE id = {row.id}
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("scraped_at", "STRING", datetime.now().isoformat())
                ]
            )
            job = client.client.query(update_query, job_config=job_config)
            job.result()
            
            failures += 1
        
        # Small delay to be polite
        if i < len(results):
            time.sleep(1)
    
    print(f"\n{'='*80}")
    print(f"SCRAPING TEST COMPLETE")
    print(f"{'='*80}\n")
    print(f"✅ Successful: {successes}")
    print(f"❌ Failed: {failures}")
    print(f"📊 Success rate: {successes/len(results)*100:.1f}%")
    print()

if __name__ == "__main__":
    import sys
    
    # Allow specifying number of URLs to test
    num_urls = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    
    test_scraping(num_urls)
