#!/usr/bin/env python3
from bigquery_client import BigQueryClient

bq = BigQueryClient()
table = f"{bq.project_id}.{bq.dataset_id}.{bq.table_id}"

# Count URLs with empty or minimal content
query = f"""
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN LENGTH(COALESCE(content, '')) = 0 THEN 1 ELSE 0 END) as empty_content,
    SUM(CASE WHEN LENGTH(COALESCE(content, '')) < 100 THEN 1 ELSE 0 END) as minimal_content,
    SUM(CASE WHEN LENGTH(COALESCE(content, '')) >= 100 THEN 1 ELSE 0 END) as full_content
FROM `{table}`
"""

result = list(bq.client.query(query).result())[0]

print("📊 Database Content Analysis:")
print("=" * 70)
print(f"Total URLs: {result.total}")
print(f"Empty content (0 chars): {result.empty_content}")
print(f"Minimal content (<100 chars): {result.minimal_content}")
print(f"Full content (≥100 chars): {result.full_content}")
print()

# Show example of minimal content URL if any
if result.minimal_content > 0:
    sample_query = f"""
    SELECT url, title, domain, LENGTH(COALESCE(content, '')) as content_len
    FROM `{table}`
    WHERE LENGTH(COALESCE(content, '')) < 100
    LIMIT 1
    """
    sample = list(bq.client.query(sample_query).result())
    if sample:
        s = sample[0]
        print("Example URL with metadata only:")
        print(f"  URL: {s.url}")
        print(f"  Title: {s.title}")
        print(f"  Content: {s.content_len} characters")
