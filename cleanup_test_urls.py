#!/usr/bin/env python3
"""Clean up test URLs from processing queue"""

from bigquery_client import BigQueryClient

def main():
    bq_client = BigQueryClient()
    
    queue_table = f"{bq_client.project_id}.{bq_client.dataset_id}.processing_queue"
    
    # Delete test URLs
    delete_query = f"""
    DELETE FROM `{queue_table}`
    WHERE batch_name = 'test_batch'
    """
    
    try:
        bq_client.client.query(delete_query).result()
        print("✅ Cleaned up test URLs")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
