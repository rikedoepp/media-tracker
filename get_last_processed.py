#!/usr/bin/env python3
"""Get the last processed URL from BigQuery"""

from bigquery_client import BigQueryClient

bq = BigQueryClient()
table = f"{bq.project_id}.{bq.dataset_id}.{bq.table_id}"

# Get the 10 most recently updated URLs
query = f"""
SELECT url, title, domain, updated_at, 
       LENGTH(COALESCE(content, '')) as content_len
FROM `{table}`
ORDER BY updated_at DESC
LIMIT 10
"""

print("🔍 Last 10 Processed URLs (most recent first):")
print("=" * 80)

results = list(bq.client.query(query).result())
for i, row in enumerate(results, 1):
    print(f"\n#{i} - {row.updated_at}")
    print(f"  URL: {row.url[:75]}...")
    print(f"  Title: {row.title[:55] if row.title else 'N/A'}...")
    print(f"  Domain: {row.domain}")
    print(f"  Content: {row.content_len} chars")

if results:
    print("\n" + "=" * 80)
    print(f"\n✅ LAST PROCESSED URL:")
    last = results[0]
    print(f"   {last.url}")
    print(f"   Updated: {last.updated_at}")
