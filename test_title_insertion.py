#!/usr/bin/env python3

from bigquery_client import BigQueryClient
from datetime import datetime

def test_title_insertion():
    """Test that title field is properly inserted into BigQuery"""
    
    try:
        print("🔍 Testing BigQuery title insertion...")
        
        # Initialize BigQuery client
        bq_client = BigQueryClient()
        print("✅ BigQuery client initialized")
        
        # Create test record with title
        test_record = {
            'url': 'https://test-title-insertion.example.com',
            'content': 'This is test content to verify title insertion works correctly.',
            'domain': 'test-title-insertion.example.com',
            'title': 'TEST HEADLINE: Title Insertion Verification',
            'publish_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'matched_spokespeople': 'Test Spokesperson',
            'matched_portcos': 'Test Company',
            'tagged_antler': True,
            'managed_by_fund': 'Antler'
        }
        
        print("📝 Test record created with title:", test_record['title'])
        
        # Insert the record
        print("💾 Inserting test record...")
        success = bq_client.insert_media_record(test_record)
        
        if success:
            print("✅ SUCCESS: Title insertion test completed!")
            print(f"   Title saved: '{test_record['title']}'")
            print(f"   Managed by fund: '{test_record['managed_by_fund']}'")
        else:
            print("❌ FAILED: Title insertion test failed")
            
    except Exception as e:
        print(f"❌ ERROR during title insertion test: {str(e)}")

if __name__ == "__main__":
    test_title_insertion()