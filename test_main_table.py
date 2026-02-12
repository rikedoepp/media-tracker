#!/usr/bin/env python3
"""Check what's in the main mediatracker table"""

from bigquery_client import BigQueryClient

def main():
    bq_client = BigQueryClient()
    
    print("✅ BigQuery client initialized\n")
    
    # Query the main table
    main_table = f"{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}"
    
    # Get total count
    count_query = f"""
    SELECT COUNT(*) as total
    FROM `{main_table}`
    """
    
    try:
        result = list(bq_client.client.query(count_query).result())[0]
        print(f"📊 Total URLs in main table: {result.total}\n")
        
        # Get recent URLs
        recent_query = f"""
        SELECT url, title, domain, publish_date, added_date
        FROM `{main_table}`
        ORDER BY added_date DESC
        LIMIT 10
        """
        
        results = list(bq_client.client.query(recent_query).result())
        
        if results:
            print("🔗 Most recent 10 URLs:")
            for row in results:
                print(f"  • {row.url[:70]}")
                print(f"    Added: {row.added_date}")
            print()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
