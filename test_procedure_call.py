#!/usr/bin/env python3

from bigquery_client import BigQueryClient

def test_procedure_call():
    """Test calling the process_new_url procedure"""
    try:
        bq_client = BigQueryClient()
        print("✅ BigQuery client initialized")
        
        # Test URL
        test_url = "https://techcrunch.com/2025/07/28/techcrunch-mobility-tesla-vs-gm-a-tale-of-two-earnings/"
        
        print(f"🔄 Testing procedure call for: {test_url}")
        print("Calling: CALL `media-455519.mediatracker.process_new_url`")
        
        # Call the procedure directly
        query = f"CALL `media-455519.mediatracker.process_new_url`('{test_url}')"
        
        job = bq_client.client.query(query)
        result = job.result()
        
        print("✅ Procedure call completed successfully!")
        print("Result:", result)
        
        # Check if procedure exists by trying to get its info
        print("\n🔍 Checking if procedure exists...")
        check_query = """
        SELECT routine_name, routine_type 
        FROM `media-455519.mediatracker.INFORMATION_SCHEMA.ROUTINES` 
        WHERE routine_name = 'process_new_url'
        """
        
        job = bq_client.client.query(check_query)
        results = job.result()
        
        found_procedure = False
        for row in results:
            found_procedure = True
            print(f"✅ Found procedure: {row.routine_name} ({row.routine_type})")
        
        if not found_procedure:
            print("❌ Procedure 'process_new_url' not found in dataset")
        
    except Exception as e:
        print(f"❌ Error calling procedure: {str(e)}")
        print("This might indicate the procedure doesn't exist or there's a permissions issue")

if __name__ == "__main__":
    test_procedure_call()