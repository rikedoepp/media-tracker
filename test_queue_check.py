#!/usr/bin/env python3
"""Check what's in the BigQuery processing queue"""

from bigquery_client import BigQueryClient

def main():
    # Initialize BigQuery client
    bq_client = BigQueryClient()
    
    print("✅ BigQuery client initialized")
    
    # Query the processing queue
    queue_table = f"{bq_client.project_id}.{bq_client.dataset_id}.processing_queue"
    
    query = f"""
    SELECT 
        id,
        url,
        status,
        batch_name,
        created_at,
        updated_at,
        error_message
    FROM `{queue_table}`
    ORDER BY created_at DESC
    LIMIT 20
    """
    
    try:
        print(f"\n📊 Querying {queue_table}...")
        results = list(bq_client.client.query(query).result())
        
        if not results:
            print("\n⚠️ Processing queue is EMPTY - no URLs found!")
        else:
            print(f"\n✅ Found {len(results)} URLs in queue:\n")
            for row in results:
                print(f"  • [{row.status:10}] {row.url[:60]}")
                print(f"    Batch: {row.batch_name}, Created: {row.created_at}")
                if row.error_message:
                    print(f"    Error: {row.error_message}")
                print()
        
        # Get status counts
        count_query = f"""
        SELECT status, COUNT(*) as count
        FROM `{queue_table}`
        GROUP BY status
        ORDER BY status
        """
        
        counts = list(bq_client.client.query(count_query).result())
        if counts:
            print("\n📈 Queue Status Summary:")
            for row in counts:
                print(f"  {row.status}: {row.count}")
        
    except Exception as e:
        print(f"❌ Error querying queue: {str(e)}")

if __name__ == "__main__":
    main()
