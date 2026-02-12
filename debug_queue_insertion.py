#!/usr/bin/env python3
"""Debug why URLs aren't being added to processing queue"""

from bigquery_client import BigQueryClient

def main():
    bq_client = BigQueryClient()
    
    print("✅ BigQuery client initialized\n")
    
    # Test URLs
    test_urls = [
        "https://www.example.com/test-1",
        "https://www.example.com/test-2",  
        "https://www.example.com/test-3"
    ]
    
    print(f"🧪 Testing with {len(test_urls)} sample URLs\n")
    
    # Try to add them to queue
    print("➡️ Attempting to add URLs to processing queue...")
    success, batch_name = bq_client.add_urls_to_processing_queue(test_urls, "test_batch")
    
    print(f"\nResult: {'✅ Success' if success else '❌ Failed'}")
    if batch_name:
        print(f"Batch name: {batch_name}")
    
    # Check queue after
    queue_table = f"{bq_client.project_id}.{bq_client.dataset_id}.processing_queue"
    count_query = f"SELECT COUNT(*) as count FROM `{queue_table}`"
    
    try:
        result = list(bq_client.client.query(count_query).result())[0]
        print(f"\n📊 Processing queue now has: {result.count} URLs")
        
        if result.count > 0:
            # Show what's in there
            select_query = f"""
            SELECT url, status, batch_name 
            FROM `{queue_table}` 
            ORDER BY created_at DESC 
            LIMIT 5
            """
            rows = list(bq_client.client.query(select_query).result())
            print("\n🔗 URLs in queue:")
            for row in rows:
                print(f"  [{row.status}] {row.url} (batch: {row.batch_name})")
    except Exception as e:
        print(f"❌ Error checking queue: {str(e)}")

if __name__ == "__main__":
    main()
