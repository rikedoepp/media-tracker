#!/usr/bin/env python3
"""Bulk scrape script - run in background"""
from google.cloud import bigquery
import os
import sys
import warnings
import time
warnings.filterwarnings('ignore')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'attached_assets/media-455519-e05e80608e53.json'

sys.path.insert(0, '/home/runner/workspace')
from web_scraper import scrape_article_data_fast

client = bigquery.Client(project='media-455519')

def main():
    result = client.query('''
        SELECT id, url
        FROM `media-455519.mediatracker.mediatracker`
        WHERE data_ingestion = TRUE
        ORDER BY id
    ''').result()

    articles = list(result)
    total = len(articles)
    print(f"Processing {total} articles...")
    
    success = 0
    failed = 0
    
    for i, article in enumerate(articles):
        if i % 25 == 0:
            print(f"[{time.strftime('%H:%M:%S')}] Progress: {i}/{total} ({round(i/total*100)}%) - Success: {success}, Failed: {failed}")
            sys.stdout.flush()
        
        try:
            scraped = scrape_article_data_fast(article.url)
            
            if scraped and scraped.get('content') and len(scraped.get('content', '')) > 50:
                client.query(f'''
                    UPDATE `media-455519.mediatracker.mediatracker`
                    SET content = @content,
                        title = COALESCE(NULLIF(title, ''), @title),
                        publish_date = COALESCE(publish_date, SAFE.PARSE_TIMESTAMP('%Y-%m-%d', @pub_date)),
                        data_ingestion = FALSE
                    WHERE id = {article.id}
                ''', job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("content", "STRING", scraped['content']),
                        bigquery.ScalarQueryParameter("title", "STRING", scraped.get('title', '')),
                        bigquery.ScalarQueryParameter("pub_date", "STRING", scraped.get('publish_date', '')),
                    ]
                )).result()
                success += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
    
    print(f"\n✅ Done! Success: {success}, Failed: {failed}")

if __name__ == "__main__":
    main()
