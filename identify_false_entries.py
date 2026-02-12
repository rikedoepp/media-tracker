from bigquery_client import BigQueryClient
from datetime import datetime, timedelta

client = BigQueryClient()

# Check for potentially false entries
queries = {
    "NULL IDs": """
        SELECT COUNT(*) as count
        FROM `media-455519.mediatracker.mediatracker` 
        WHERE id IS NULL
    """,
    "Test entries (example.com)": """
        SELECT id, url, title, updated_at
        FROM `media-455519.mediatracker.mediatracker` 
        WHERE domain = 'example.com'
        ORDER BY id
    """,
    "Recent data_ingestion entries": """
        SELECT id, url, title, domain, content, updated_at, data_ingestion
        FROM `media-455519.mediatracker.mediatracker` 
        WHERE data_ingestion = true
        ORDER BY id
    """,
    "Entries from last 2 hours": """
        SELECT id, url, domain, content, data_ingestion, updated_at
        FROM `media-455519.mediatracker.mediatracker` 
        WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
        ORDER BY updated_at DESC
    """
}

for query_name, query in queries.items():
    print(f"\n{'='*80}")
    print(f"{query_name}")
    print('='*80)
    
    try:
        results = client.client.query(query).result()
        
        if query_name == "NULL IDs":
            for row in results:
                print(f"Total NULL ID rows: {row.count}")
        else:
            count = 0
            for row in results:
                count += 1
                print(f"\nID: {row.id}")
                if hasattr(row, 'url'):
                    print(f"  URL: {row.url}")
                if hasattr(row, 'title'):
                    print(f"  Title: {row.title}")
                if hasattr(row, 'domain'):
                    print(f"  Domain: {row.domain}")
                if hasattr(row, 'content'):
                    print(f"  Content: {row.content}")
                if hasattr(row, 'data_ingestion'):
                    print(f"  Data Ingestion: {row.data_ingestion}")
                if hasattr(row, 'updated_at'):
                    print(f"  Updated: {row.updated_at}")
            
            if count == 0:
                print("No entries found")
            else:
                print(f"\nTotal: {count} entries")
                
    except Exception as e:
        print(f"Error: {e}")
