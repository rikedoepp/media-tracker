from bigquery_client import BigQueryClient
from datetime import datetime, timedelta

client = BigQueryClient()

# Check for recent insertions in the last hour
query = """
SELECT id, url, title, domain, updated_at, content
FROM `media-455519.mediatracker.mediatracker` 
WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY updated_at DESC
LIMIT 20
"""

try:
    results = client.client.query(query).result()
    
    print("=" * 80)
    print("RECENT ACTIVITY IN LAST HOUR:")
    print("=" * 80)
    
    count = 0
    for row in results:
        count += 1
        print(f"\n{count}. ID: {row.id}")
        print(f"   URL: {row.url}")
        print(f"   Title: {row.title}")
        print(f"   Domain: {row.domain}")
        print(f"   Updated: {row.updated_at}")
        
        # Check if there's content (which might indicate data ingestion vs scraping)
        if row.content:
            content_preview = row.content[:100] + "..." if len(row.content) > 100 else row.content
            print(f"   Content: {content_preview}")
        else:
            print(f"   Content: None")
    
    if count == 0:
        print("No records inserted in the last hour")
    else:
        print(f"\n{count} total records in last hour")
        
except Exception as e:
    print(f"Error: {e}")
