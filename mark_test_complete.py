#!/usr/bin/env python3
"""Mark test URLs as completed to clear them out"""

from bigquery_client import BigQueryClient

def main():
    bq_client = BigQueryClient()
    
    queue_table = f"{bq_client.project_id}.{bq_client.dataset_id}.processing_queue"
    
    # Update test URLs to completed status
    update_query = f"""
    UPDATE `{queue_table}`
    SET status = 'completed', updated_at = CURRENT_TIMESTAMP()
    WHERE batch_name = 'test_batch'
    """
    
    try:
        bq_client.client.query(update_query).result()
        print("✅ Marked test URLs as completed")
    except Exception as e:
        print(f"Note: {str(e)}")
        print("Test URLs will auto-clear when processed")

if __name__ == "__main__":
    main()
