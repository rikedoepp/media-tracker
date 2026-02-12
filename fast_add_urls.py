#!/usr/bin/env python3
"""
Fast batch URL adder - optimized for large batches
Checks all URLs against database in one query instead of one-by-one
"""

import sys
from bigquery_client import BigQueryClient
from datetime import datetime
import uuid

def main():
    if len(sys.argv) < 2:
        print("Usage: python fast_add_urls.py <file_with_urls.txt>")
        return
    
    filename = sys.argv[1]
    
    try:
        with open(filename, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        print(f"📂 Found {len(urls)} URLs in {filename}\n")
        
        if not urls:
            print("❌ No URLs found")
            return
        
        bq_client = BigQueryClient()
        
        # Normalize all URLs first
        print("🔧 Normalizing URLs...")
        normalized_urls = [bq_client.normalize_url(url) for url in urls]
        
        # Remove duplicates within the input
        unique_urls = list(dict.fromkeys(normalized_urls))
        if len(unique_urls) < len(normalized_urls):
            print(f"   Removed {len(normalized_urls) - len(unique_urls)} duplicate URLs from input")
        
        # Filter out antler.co URLs
        filtered_urls = [url for url in unique_urls if 'antler.co' not in url.lower()]
        skipped_antler = len(unique_urls) - len(filtered_urls)
        if skipped_antler > 0:
            print(f"   Filtered out {skipped_antler} antler.co URLs")
        
        print(f"✅ {len(filtered_urls)} URLs ready to check\n")
        
        # Batch check against database - FAST!
        print("🔍 Checking against database (this is fast now)...")
        
        main_table = f"{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}"
        queue_table = f"{bq_client.project_id}.{bq_client.dataset_id}.processing_queue"
        
        # Create temp table with URLs to check
        temp_table_name = f"temp_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        temp_table = f"{bq_client.project_id}.{bq_client.dataset_id}.{temp_table_name}"
        
        # Insert URLs into temp table
        from google.cloud import bigquery
        
        schema = [bigquery.SchemaField("url", "STRING")]
        temp_table_obj = bigquery.Table(temp_table, schema=schema)
        bq_client.client.create_table(temp_table_obj, exists_ok=True)
        
        rows = [{"url": url} for url in filtered_urls]
        bq_client.client.insert_rows_json(temp_table, rows)
        
        # Check against main table and queue in ONE query
        check_query = f"""
        SELECT t.url,
               CASE 
                 WHEN m.url IS NOT NULL THEN 'in_main_table'
                 WHEN q.url IS NOT NULL THEN 'in_queue'
                 ELSE 'new'
               END as status
        FROM `{temp_table}` t
        LEFT JOIN `{main_table}` m ON t.url = m.url
        LEFT JOIN `{queue_table}` q ON t.url = q.url
        """
        
        results = list(bq_client.client.query(check_query).result())
        
        # Categorize results
        new_urls = [row.url for row in results if row.status == 'new']
        existing_urls = [row.url for row in results if row.status != 'new']
        
        print(f"✅ Check complete!")
        print(f"   🆕 New URLs: {len(new_urls)}")
        print(f"   ⏭️  Already exist: {len(existing_urls)}\n")
        
        # Clean up temp table
        bq_client.client.delete_table(temp_table)
        
        if not new_urls:
            print("⚠️  All URLs already exist in database or queue!")
            return
        
        # Add new URLs to queue
        print(f"➕ Adding {len(new_urls)} new URLs to queue...")
        
        batch_name = f"fast_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        queue_rows = [{
            'id': str(uuid.uuid4()),
            'url': url,
            'status': 'pending',
            'batch_name': batch_name,
            'created_at': current_time,
            'updated_at': current_time,
            'error_message': None,
            'retry_count': 0
        } for url in new_urls]
        
        # Create queue table if needed
        create_queue = f"""
        CREATE TABLE IF NOT EXISTS `{queue_table}` (
            id STRING,
            url STRING,
            status STRING,
            batch_name STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            error_message STRING,
            retry_count INT64
        )
        """
        bq_client.client.query(create_queue).result()
        
        # Insert to queue
        table = bq_client.client.get_table(queue_table)
        errors = bq_client.client.insert_rows_json(table, queue_rows)
        
        if not errors:
            print(f"✅ SUCCESS!")
            print(f"   📋 Batch: {batch_name}")
            print(f"   🎯 {len(new_urls)} URLs added to queue")
            print(f"   ⏭️  {len(existing_urls)} duplicates skipped")
            if skipped_antler > 0:
                print(f"   🚫 {skipped_antler} antler.co URLs filtered")
            print(f"\n🚀 Run 'python process_queue_background.py' to start processing!")
        else:
            print(f"❌ Error: {errors}")
        
    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
