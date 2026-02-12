"""
Batch scraper for articles missing content.
Run this to scrape all articles with empty/missing content.
"""
from google.cloud import bigquery
import os
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'attached_assets/media-455519-e05e80608e53.json'
client = bigquery.Client(project='media-455519')

from web_scraper import scrape_article_data_fast

def get_articles_needing_scrape(limit=100):
    """Get articles that need content scraping"""
    query = f"""
    SELECT id, url, domain
    FROM `media-455519.mediatracker.mediatracker`
    WHERE content IS NULL 
       OR TRIM(content) = '' 
       OR content = 'needs enrichment'
    ORDER BY id DESC
    LIMIT {limit}
    """
    return list(client.query(query).result())

def update_article_content(article_id, content, domain):
    """Update article with scraped content"""
    # Ensure domain exists in media_data
    from bigquery_client import BigQueryClient
    bq = BigQueryClient()
    bq.ensure_domain_in_media_data(domain)
    
    update_query = """
    UPDATE `media-455519.mediatracker.mediatracker`
    SET content = @content,
        text_scraped = TRUE,
        text_scraped_at = CURRENT_TIMESTAMP()
    WHERE id = @id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("content", "STRING", content),
            bigquery.ScalarQueryParameter("id", "INT64", article_id)
        ]
    )
    client.query(update_query, job_config=job_config).result()

def main():
    print("=== BATCH CONTENT SCRAPER ===\n")
    
    # Get count
    count_query = """
    SELECT COUNT(*) FROM `media-455519.mediatracker.mediatracker`
    WHERE content IS NULL OR TRIM(content) = '' OR content = 'needs enrichment'
    """
    total = list(client.query(count_query).result())[0][0]
    print(f"Total articles needing scraping: {total:,}\n")
    
    if total == 0:
        print("✅ All articles have content!")
        return
    
    batch_size = 50
    success_count = 0
    fail_count = 0
    
    while True:
        articles = get_articles_needing_scrape(batch_size)
        if not articles:
            break
            
        print(f"Processing batch of {len(articles)} articles...")
        
        for i, article in enumerate(articles):
            try:
                print(f"  [{i+1}/{len(articles)}] ID {article.id}: {article.domain[:30]}...", end=" ")
                
                result = scrape_article_data_fast(article.url)
                
                if result and result.get('content') and len(result['content'].strip()) > 50:
                    content = result['content']
                    # Truncate if too long
                    if len(content) > 1000000:
                        content = content[:1000000] + "... [truncated]"
                    
                    update_article_content(article.id, content, article.domain)
                    success_count += 1
                    print("✅")
                else:
                    fail_count += 1
                    print("❌ (no content)")
                    
            except Exception as e:
                fail_count += 1
                print(f"❌ ({str(e)[:30]})")
            
            time.sleep(0.5)  # Rate limiting
        
        print(f"\nBatch complete. Success: {success_count}, Failed: {fail_count}")
        print(f"Remaining: ~{total - success_count - fail_count}\n")
        
        # Check if we should continue
        remaining = list(client.query(count_query).result())[0][0]
        if remaining == 0:
            break
    
    print(f"\n=== SCRAPING COMPLETE ===")
    print(f"Success: {success_count}")
    print(f"Failed: {fail_count}")
    
    # Run enrichment procedure
    print("\nRunning bulk enrichment procedure...")
    client.query("CALL `media-455519.mediatracker.process_backlog_bulk`()").result()
    print("✅ Enrichment complete!")

if __name__ == "__main__":
    main()
