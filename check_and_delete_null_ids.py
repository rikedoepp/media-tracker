#!/usr/bin/env python3
"""Check and delete rows from mediatracker that have NULL ID"""

from bigquery_client import BigQueryClient

def main():
    bq_client = BigQueryClient()
    table = f"{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}"
    
    # First, check how many rows have NULL ID
    count_query = f"""
    SELECT COUNT(*) as null_id_count
    FROM `{table}`
    WHERE id IS NULL
    """
    
    print("🔍 Checking for rows with NULL ID...")
    result = list(bq_client.client.query(count_query).result())
    null_count = result[0].null_id_count
    
    print(f"Found {null_count} rows with NULL ID")
    
    if null_count == 0:
        print("✅ No rows to delete!")
        return
    
    # Show sample of rows to be deleted
    sample_query = f"""
    SELECT url, title, domain
    FROM `{table}`
    WHERE id IS NULL
    LIMIT 5
    """
    
    print("\nSample rows to be deleted:")
    samples = list(bq_client.client.query(sample_query).result())
    for i, row in enumerate(samples, 1):
        print(f"  {i}. {row.url[:60]}... | {row.title[:40] if row.title else 'No title'}...")
    
    # Delete rows with NULL ID
    delete_query = f"""
    DELETE FROM `{table}`
    WHERE id IS NULL
    """
    
    print(f"\n🗑️  Deleting {null_count} rows with NULL ID...")
    bq_client.client.query(delete_query).result()
    
    print(f"✅ Successfully deleted {null_count} rows with NULL ID!")
    
    # Verify
    verify_result = list(bq_client.client.query(count_query).result())
    remaining = verify_result[0].null_id_count
    print(f"✅ Verified: {remaining} rows with NULL ID remaining")

if __name__ == "__main__":
    main()
