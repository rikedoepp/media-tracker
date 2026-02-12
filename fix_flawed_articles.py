#!/usr/bin/env python3
"""
Fix flawed articles by re-scraping content for articles with:
- "needs enrichment" placeholder
- Empty/null content
- Very short content (<100 chars)
"""

import time
from bigquery_client import BigQueryClient
from web_scraper import scrape_article_data_fast

def get_flawed_articles(bq_client, limit=100, skip_ids=None):
    """Get articles that need re-scraping"""
    skip_clause = ""
    if skip_ids:
        skip_clause = f"AND id NOT IN ({','.join(str(i) for i in skip_ids)})"
    
    query = f'''
    SELECT id, url, content
    FROM `{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}`
    WHERE id BETWEEN 56941 AND 80495
      AND (
        content = 'needs enrichment'
        OR content IS NULL 
        OR content = ''
        OR LENGTH(content) < 100
      )
      {skip_clause}
    ORDER BY id
    LIMIT {limit}
    '''
    return list(bq_client.client.query(query).result())

def update_article_content(bq_client, article_id, title, content, domain):
    """Update article with new scraped content"""
    from google.cloud import bigquery
    
    query = f'''
    UPDATE `{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}`
    SET 
        title = @title,
        content = @content,
        domain = @domain,
        updated_at = CURRENT_TIMESTAMP()
    WHERE id = @id
    '''
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("title", "STRING", title),
            bigquery.ScalarQueryParameter("content", "STRING", content),
            bigquery.ScalarQueryParameter("domain", "STRING", domain),
            bigquery.ScalarQueryParameter("id", "INT64", article_id)
        ]
    )
    
    bq_client.client.query(query, job_config=job_config).result()
    return True

def fix_batch(batch_size=50, delay=0.5, skip_ids=None):
    """Fix a batch of flawed articles"""
    bq_client = BigQueryClient()
    
    if skip_ids is None:
        skip_ids = set()
    
    articles = get_flawed_articles(bq_client, limit=batch_size, skip_ids=skip_ids)
    
    if not articles:
        print("No more articles to fix!")
        return 0, 0, set()
    
    print(f"Found {len(articles)} articles to fix")
    
    success_count = 0
    fail_count = 0
    failed_ids = set()
    
    for i, article in enumerate(articles):
        article_id = article.id
        url = article.url
        
        try:
            scraped = scrape_article_data_fast(url)
            
            if scraped and scraped.get('content') and len(scraped.get('content', '')) >= 50:
                update_article_content(
                    bq_client,
                    article_id,
                    scraped.get('title', ''),
                    scraped.get('content', ''),
                    scraped.get('domain', '')
                )
                success_count += 1
                print(f"✓ {article_id}")
            else:
                fail_count += 1
                failed_ids.add(article_id)
                print(f"✗ {article_id} - no content")
                
        except Exception as e:
            fail_count += 1
            failed_ids.add(article_id)
            print(f"✗ {article_id} - {str(e)[:30]}")
        
        if delay > 0:
            time.sleep(delay)
    
    print(f"\n=== Batch Complete ===")
    print(f"Success: {success_count}, Failed: {fail_count}")
    
    return success_count, fail_count, failed_ids

def count_remaining():
    """Count remaining flawed articles"""
    bq_client = BigQueryClient()
    
    query = '''
    SELECT COUNT(*) as cnt
    FROM `media-455519.mediatracker.mediatracker`
    WHERE id BETWEEN 56941 AND 80495
      AND (
        content = 'needs enrichment'
        OR content IS NULL 
        OR content = ''
        OR LENGTH(content) < 100
      )
    '''
    result = list(bq_client.client.query(query).result())[0]
    return result.cnt

if __name__ == "__main__":
    print(f"Starting fix for flawed articles...")
    print(f"Remaining to fix: {count_remaining()}")
    print()
    
    total_success = 0
    total_fail = 0
    batch_num = 1
    all_failed_ids = set()
    
    while True:
        remaining = count_remaining()
        if remaining == 0:
            break
        
        # Check if we've already tried all remaining articles
        if len(all_failed_ids) >= remaining:
            print(f"All remaining {remaining} articles are unscrape-able")
            break
            
        print(f"\n--- Batch {batch_num} ({remaining} remaining, {len(all_failed_ids)} skipped) ---")
        success, fail, failed_ids = fix_batch(batch_size=50, delay=0.3, skip_ids=all_failed_ids)
        
        all_failed_ids.update(failed_ids)
        total_success += success
        total_fail += fail
        batch_num += 1
        
        if success == 0 and fail == 0:
            print("No more articles to process")
            break
    
    print(f"\n=== COMPLETE ===")
    print(f"Total fixed: {total_success}")
    print(f"Total unscrape-able: {len(all_failed_ids)}")
    print(f"Remaining in DB: {count_remaining()}")
