#!/usr/bin/env python3
"""Delete test_batch URLs from queue"""

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os

# Load credentials from file
credentials = service_account.Credentials.from_service_account_file(
    'attached_assets/media-455519-e05e80608e53.json'
)

client = bigquery.Client(credentials=credentials, project='media-455519')

# Mark test_batch as completed to hide from view
update_query = """
UPDATE `media-455519.mediatracker.processing_queue`
SET status = 'completed', updated_at = CURRENT_TIMESTAMP()
WHERE batch_name = 'test_batch' AND status = 'pending'
"""

try:
    client.query(update_query).result()
    print("✅ Test batch URLs marked as completed")
except Exception as e:
    if "streaming buffer" in str(e):
        # Can't update in streaming buffer, wait a bit
        print("⏳ URLs too recent to update, waiting...")
        import time
        time.sleep(5)
        try:
            client.query(update_query).result()
            print("✅ Test batch URLs marked as completed")
        except Exception as e2:
            print(f"Trying alternative: {str(e2)}")
            # Insert duplicate completed records to override
            insert_query = """
            INSERT INTO `media-455519.mediatracker.processing_queue` (id, url, status, batch_name, created_at, updated_at, error_message, retry_count)
            SELECT 
                CONCAT(id, '_completed') as id,
                url,
                'completed' as status,
                batch_name,
                created_at,
                CURRENT_TIMESTAMP() as updated_at,
                'Manually marked complete' as error_message,
                retry_count
            FROM `media-455519.mediatracker.processing_queue`
            WHERE batch_name = 'test_batch' AND status = 'pending'
            """
            client.query(insert_query).result()
            print("✅ Created completed records to override test batch")
    else:
        print(f"Error: {str(e)}")
