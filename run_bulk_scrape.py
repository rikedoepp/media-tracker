#!/usr/bin/env python3
"""Bulk scrape script - designed to run as a workflow"""
from google.cloud import bigquery
import os
import sys
import time
import warnings
warnings.filterwarnings('ignore')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'attached_assets/media-455519-e05e80608e53.json'
sys.path.insert(0, '/home/runner/workspace')

from web_scraper import scrape_article_data_fast

def main():
    client = bigquery.Client(project='media-455519')
    
    while True:
        # Get batch of articles needing scrape
        result = client.query('''
            SELECT id, url
            FROM `media-455519.mediatracker.mediatracker`
            WHERE data_ingestion = TRUE
            ORDER BY id
            LIMIT 50
        ''').result()
        
        articles = list(result)
        
        if not articles:
            print("All articles scraped! Waiting 60 seconds before checking again...")
            time.sleep(60)
            continue
        
        print(f"\n[{time.strftime('%H:%M:%S')}] Processing batch of {len(articles)} articles...")
        
        success = 0
        failed = 0
        
        for article in articles:
            try:
                scraped = scrape_article_data_fast(article.url)
                
                if scraped and scraped.get('content') and len(scraped.get('content', '')) > 50:
                    client.query(f'''
                        UPDATE `media-455519.mediatracker.mediatracker`
                        SET content = @content,
                            title = COALESCE(NULLIF(title, ''), @title),
                            publish_date = COALESCE(publish_date, SAFE.PARSE_TIMESTAMP("%Y-%m-%d", @pub_date)),
                            data_ingestion = FALSE
                        WHERE id = {article.id}
                    ''', job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter('content', 'STRING', scraped['content']),
                            bigquery.ScalarQueryParameter('title', 'STRING', scraped.get('title', '')),
                            bigquery.ScalarQueryParameter('pub_date', 'STRING', scraped.get('publish_date', '')),
                        ]
                    )).result()
                    success += 1
                else:
                    # Mark as processed even if no content (avoid infinite loop)
                    client.query(f'''
                        UPDATE `media-455519.mediatracker.mediatracker`
                        SET data_ingestion = FALSE
                        WHERE id = {article.id}
                    ''').result()
                    failed += 1
                    
            except Exception as e:
                print(f"  Error on ID {article.id}: {str(e)[:80]}")
                # Mark as processed to avoid infinite loop
                try:
                    client.query(f'''
                        UPDATE `media-455519.mediatracker.mediatracker`
                        SET data_ingestion = FALSE
                        WHERE id = {article.id}
                    ''').result()
                except:
                    pass
                failed += 1
        
        # Get remaining count
        remaining = client.query('''
            SELECT COUNT(*) as cnt FROM `media-455519.mediatracker.mediatracker` WHERE data_ingestion = TRUE
        ''').result()
        for row in remaining:
            print(f"  Batch done: {success} success, {failed} failed. Remaining: {row.cnt}")
        
        time.sleep(2)  # Brief pause between batches

if __name__ == "__main__":
    main()
