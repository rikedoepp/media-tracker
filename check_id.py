from bigquery_client import BigQueryClient

client = BigQueryClient()

query = """
SELECT id, url, title, domain, updated_at 
FROM `media-455519.mediatracker.mediatracker` 
WHERE id = 63476
"""

try:
    results = client.client.query(query).result()
    
    found = False
    for row in results:
        found = True
        print(f"ID: {row.id}")
        print(f"URL: {row.url}")
        print(f"Title: {row.title}")
        print(f"Domain: {row.domain}")
        print(f"Updated: {row.updated_at}")
    
    if not found:
        print("ID 63476 not found in database")
        
except Exception as e:
    print(f"Error: {e}")
