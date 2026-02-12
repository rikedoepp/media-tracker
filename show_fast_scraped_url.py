#!/usr/bin/env python3
"""Show an example URL that was scraped in fast mode (metadata only)"""

from bigquery_client import BigQueryClient

def main():
    bq_client = BigQueryClient()
    
    main_table = f"{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}"
    
    # Find a URL that was scraped in fast mode (text_scraped = False)
    query = f"""
    SELECT url, title, domain, publish_date, content, text_scraped
    FROM `{main_table}`
    WHERE text_scraped = False OR content = '' OR content IS NULL
    ORDER BY RAND()
    LIMIT 1
    """
    
    try:
        results = list(bq_client.client.query(query).result())
        
        if results:
            row = results[0]
            print("📄 Example URL scraped in FAST MODE (metadata only):")
            print("=" * 70)
            print(f"\n🔗 URL: {row.url}")
            print(f"📰 Title: {row.title}")
            print(f"🌐 Domain: {row.domain}")
            print(f"📅 Publish Date: {row.publish_date}")
            print(f"📝 Content Length: {len(row.content or '')} characters")
            print(f"✅ Text Scraped: {row.text_scraped}")
            print("\n" + "=" * 70)
            print("\n💡 This URL has metadata (title, domain, date) but NO full article text yet.")
            print("   Full text can be scraped later using the batch text scraper.")
        else:
            print("⚠️  No URLs found with metadata-only (fast mode) scraping")
            print("   All URLs in database have full text content already")
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
