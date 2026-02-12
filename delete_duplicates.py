from bigquery_client import BigQueryClient
from datetime import datetime, timedelta

client = BigQueryClient()

print("="*80)
print("FINDING AND DELETING DUPLICATE URLs")
print("="*80)

# Step 1: Find all duplicate URLs and their details
query = f"""
WITH ranked_duplicates AS (
  SELECT 
    id,
    url,
    title,
    domain,
    content,
    publish_date,
    matched_portcos,
    matched_spokespeople,
    tagged_antler,
    unbranded_win,
    data_ingestion,
    updated_at,
    -- Count non-null columns (higher score = more data)
    (CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN domain IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN content IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN publish_date IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN matched_portcos IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN matched_spokespeople IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN tagged_antler IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN unbranded_win IS NOT NULL THEN 1 ELSE 0 END) as filled_columns,
    -- Count URLs to find duplicates
    COUNT(*) OVER (PARTITION BY url) as url_count,
    -- Rank by filled columns (highest first), then by most recent updated_at
    ROW_NUMBER() OVER (
      PARTITION BY url 
      ORDER BY 
        (CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN domain IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN content IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN publish_date IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN matched_portcos IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN matched_spokespeople IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN tagged_antler IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN unbranded_win IS NOT NULL THEN 1 ELSE 0 END) DESC,
        updated_at DESC
    ) as rank
  FROM `{client.project_id}.{client.dataset_id}.{client.table_id}`
)
SELECT *
FROM ranked_duplicates
WHERE url_count > 1
ORDER BY url, rank
"""

print("\n1. Querying for duplicate URLs...")
results = list(client.client.query(query).result())

if not results:
    print("\n✅ No duplicates found!")
    exit(0)

print(f"\n2. Found {len(results)} duplicate entries")

# Group by URL
from collections import defaultdict
duplicates_by_url = defaultdict(list)

for row in results:
    duplicates_by_url[row.url].append({
        'id': row.id,
        'url': row.url,
        'title': row.title,
        'domain': row.domain,
        'content': row.content,
        'filled_columns': row.filled_columns,
        'rank': row.rank,
        'updated_at': row.updated_at
    })

print(f"3. Found {len(duplicates_by_url)} unique URLs with duplicates")

# Identify which rows to keep and which to delete
ids_to_keep = []
ids_to_delete = []
recent_inserts = []  # Track IDs that might be in streaming buffer

now = datetime.now(duplicates_by_url[list(duplicates_by_url.keys())[0]][0]['updated_at'].tzinfo)
streaming_buffer_cutoff = now - timedelta(minutes=90)

print(f"\n4. Analysis of duplicates:")
print(f"   (Streaming buffer cutoff: {streaming_buffer_cutoff})")
print(f"   Processing all {len(duplicates_by_url)} URLs with duplicates...")

# Process ALL duplicates
for url, entries in duplicates_by_url.items():
    for entry in entries:
        # Handle NULL updated_at (consider them safe to delete)
        if entry['updated_at'] is None:
            in_buffer = False
        else:
            in_buffer = entry['updated_at'] > streaming_buffer_cutoff
        
        if entry['rank'] == 1:
            ids_to_keep.append(entry['id'])
        else:
            ids_to_delete.append(entry['id'])
            if in_buffer:
                recent_inserts.append(entry['id'])

# Show first 20 for inspection
print(f"\n   Showing first 20 URLs with most duplicates:")
for url, entries in sorted(duplicates_by_url.items(), key=lambda x: len(x[1]), reverse=True)[:20]:
    print(f"\n   URL: {url}")
    print(f"   Found {len(entries)} copies:")
    
    for entry in entries:
        status = "KEEP" if entry['rank'] == 1 else "DELETE"
        
        # Handle NULL updated_at (consider them safe to delete)
        if entry['updated_at'] is None:
            buffer_note = " [NO TIMESTAMP]"
        else:
            in_buffer = entry['updated_at'] > streaming_buffer_cutoff
            buffer_note = " [STREAMING BUFFER]" if in_buffer else ""
        
        print(f"      [{status}] ID {entry['id']}: {entry['filled_columns']} columns filled, updated {entry['updated_at']}{buffer_note}")

print(f"\n{'='*80}")
print("SUMMARY")
print('='*80)
print(f"Total duplicate entries: {len(results)}")
print(f"URLs affected: {len(duplicates_by_url)}")
print(f"Rows to keep: {len(ids_to_keep)}")
print(f"Rows to delete: {len(ids_to_delete)}")
print(f"Rows in streaming buffer (can't delete yet): {len(recent_inserts)}")

if recent_inserts:
    print(f"\n⚠️  WARNING: {len(recent_inserts)} rows are in the streaming buffer")
    print(f"   These were inserted within the last 90 minutes and cannot be deleted yet.")
    print(f"   Wait until they're out of the buffer, then run this script again.")

# Delete the duplicates (excluding those in streaming buffer)
ids_to_delete_now = [id for id in ids_to_delete if id not in recent_inserts]

if ids_to_delete_now:
    print(f"\n{'='*80}")
    print(f"DELETING {len(ids_to_delete_now)} DUPLICATE ROWS")
    print('='*80)
    
    # Delete in batches
    batch_size = 100
    total_deleted = 0
    
    for i in range(0, len(ids_to_delete_now), batch_size):
        batch = ids_to_delete_now[i:i+batch_size]
        batch_ids = ','.join(map(str, batch))
        
        delete_query = f"""
        DELETE FROM `{client.project_id}.{client.dataset_id}.{client.table_id}`
        WHERE id IN ({batch_ids})
        """
        
        try:
            result = client.client.query(delete_query).result()
            total_deleted += len(batch)
            print(f"   ✅ Deleted batch {i//batch_size + 1} ({len(batch)} rows)")
        except Exception as e:
            if "streaming buffer" in str(e).lower():
                print(f"   ❌ Batch {i//batch_size + 1} failed: Still in streaming buffer")
            else:
                print(f"   ❌ Batch {i//batch_size + 1} failed: {e}")
    
    print(f"\n{'='*80}")
    print("COMPLETE!")
    print('='*80)
    print(f"✅ Successfully deleted {total_deleted} duplicate rows")
    
    if recent_inserts:
        print(f"\n⚠️  {len(recent_inserts)} rows still need to be deleted after streaming buffer expires")
else:
    print(f"\n⚠️  No rows can be deleted right now (all are in streaming buffer)")
    print(f"   Wait 90 minutes after insertion, then run this script again.")
