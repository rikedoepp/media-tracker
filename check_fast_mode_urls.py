#!/usr/bin/env python3
from bigquery_client import BigQueryClient

bq = BigQueryClient()
table = f"{bq.project_id}.{bq.dataset_id}.{bq.table_id}"

# Check if text_scraped column exists
query = f"""
SELECT url, title, domain, publish_date, 
       LENGTH(COALESCE(content, '')) as content_len
FROM `{table}`
LIMIT 5
"""

results = list(bq.client.query(query).result())

print("Sample URLs from database:")
print("=" * 70)
for row in results:
    print(f"\nURL: {row.url[:60]}...")
    print(f"Title: {row.title[:50]}...")
    print(f"Domain: {row.domain}")
    print(f"Content length: {row.content_len} chars")
