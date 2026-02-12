#!/usr/bin/env python3

from bigquery_client import BigQueryClient

def check_unbranded_column():
    """Check if unbranded_win column exists in the mediatracker table"""
    
    try:
        print("Connecting to BigQuery...")
        bq_client = BigQueryClient()
        print("Connected successfully")
        
        # Query to check table schema
        query = """
        SELECT column_name, data_type, is_nullable 
        FROM `media-455519.mediatracker.INFORMATION_SCHEMA.COLUMNS` 
        WHERE table_name = 'mediatracker' 
        ORDER BY ordinal_position
        """
        
        print("Checking table schema...")
        job = bq_client.client.query(query)
        results = job.result()
        
        print("\nTable columns:")
        unbranded_found = False
        for row in results:
            print(f"- {row.column_name} ({row.data_type})")
            if 'unbranded' in row.column_name.lower():
                unbranded_found = True
                print(f"  *** FOUND: {row.column_name} ***")
        
        if not unbranded_found:
            print("\nNo 'unbranded_win' column found in the table.")
        
    except Exception as e:
        print(f"Error checking table schema: {str(e)}")

if __name__ == "__main__":
    check_unbranded_column()