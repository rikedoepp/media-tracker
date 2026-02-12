#!/usr/bin/env python3

from bigquery_client import BigQueryClient

def test_duplicate_check():
    """Test the duplicate URL checking functionality"""
    
    try:
        print("🔍 Testing duplicate URL checking...")
        
        # Initialize BigQuery client
        bq_client = BigQueryClient()
        print("✅ BigQuery client initialized")
        
        # Test with a URL that likely exists (from our previous test)
        test_url = "https://test-title-insertion.example.com"
        
        print(f"🔎 Checking if URL exists: {test_url}")
        url_exists = bq_client.check_url_exists(test_url)
        
        if url_exists:
            print("✅ SUCCESS: Duplicate URL detection is working!")
            print(f"   Found existing URL: {test_url}")
        else:
            print("📝 No duplicate found for test URL")
            
        # Test with a URL that definitely doesn't exist
        unique_url = f"https://unique-test-{hash('test123')}.example.com"
        print(f"🔎 Checking unique URL: {unique_url}")
        unique_exists = bq_client.check_url_exists(unique_url)
        
        if not unique_exists:
            print("✅ SUCCESS: Unique URL correctly identified as new")
        else:
            print("⚠️ Unexpected: Unique URL found in database")
            
    except Exception as e:
        print(f"❌ ERROR during duplicate check test: {str(e)}")

if __name__ == "__main__":
    test_duplicate_check()