#!/usr/bin/env python3
"""Delete specific URLs from the Excel file"""

import pandas as pd
from bigquery_client import BigQueryClient

# Read the Excel file
df = pd.read_excel('attached_assets/delete_1761291511195.xlsx')
urls_to_delete = df['url'].tolist()

print(f"📄 Found {len(urls_to_delete)} URLs to delete")

bq_client = BigQueryClient()
table = f"{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}"

# Try to delete by URL (WHERE url IN ...)
# Create batches to avoid query size limits
batch_size = 100
total_deleted = 0
errors = []

for i in range(0, len(urls_to_delete), batch_size):
    batch = urls_to_delete[i:i+batch_size]
    
    # Create parameterized query
    url_list = "', '".join(batch)
    delete_query = f"""
    DELETE FROM `{table}`
    WHERE url IN ('{url_list}')
    """
    
    print(f"\n🗑️  Deleting batch {i//batch_size + 1} ({len(batch)} URLs)...")
    
    try:
        result = bq_client.client.query(delete_query).result()
        print(f"✅ Batch {i//batch_size + 1} deleted successfully")
        total_deleted += len(batch)
    except Exception as e:
        error_msg = str(e)
        if "streaming buffer" in error_msg.lower():
            print(f"❌ Cannot delete - rows are in streaming buffer (must wait 90 minutes)")
            print(f"   These rows were inserted recently and BigQuery prevents immediate deletion")
            errors.append(f"Streaming buffer error for batch {i//batch_size + 1}")
            break
        else:
            print(f"❌ Error: {error_msg}")
            errors.append(error_msg)

print(f"\n{'='*70}")
if errors:
    print(f"⚠️  Deletion failed: {errors[0]}")
    print(f"\n💡 Solution: Wait 90 minutes after insertion, then run this script again")
else:
    print(f"✅ Successfully deleted {total_deleted} URLs!")
