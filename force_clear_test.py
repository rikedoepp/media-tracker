#!/usr/bin/env python3
"""Force process the test URLs to clear them out"""

from bigquery_client import BigQueryClient

def main():
    bq_client = BigQueryClient()
    
    print("🧹 Clearing test batch...")
    
    # Process each test URL
    for i in range(3):
        result = bq_client.process_next_url_from_queue()
        if result:
            print(f"  ✓ Processed URL {i+1}/3")
    
    # Check queue
    queue_table = f"{bq_client.project_id}.{bq_client.dataset_id}.processing_queue"
    count_query = f"SELECT COUNT(*) as count FROM `{queue_table}` WHERE status = 'pending'"
    
    result = list(bq_client.client.query(count_query).result())[0]
    
    if result.count == 0:
        print("\n✅ Queue is clear! Ready for your real URLs.")
    else:
        print(f"\n⏳ {result.count} URLs still pending")

if __name__ == "__main__":
    main()
