#!/usr/bin/env python3
"""
Add URLs directly to processing queue from a text file
Usage: python add_urls_directly.py urls.txt
"""

import sys
from bigquery_client import BigQueryClient
from datetime import datetime

def main():
    if len(sys.argv) < 2:
        print("Usage: python add_urls_directly.py <file_with_urls.txt>")
        print("\nExample:")
        print("  1. Create a file called 'my_urls.txt' with one URL per line")
        print("  2. Run: python add_urls_directly.py my_urls.txt")
        return
    
    filename = sys.argv[1]
    
    try:
        with open(filename, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        print(f"📂 Found {len(urls)} URLs in {filename}")
        
        if not urls:
            print("❌ No URLs found in file")
            return
        
        # Initialize BigQuery
        bq_client = BigQueryClient()
        
        # Add to queue with force option (skip duplicate checking)
        batch_name = f"direct_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print(f"\n🚀 Adding URLs to processing queue...")
        print(f"📋 Batch name: {batch_name}")
        
        success, batch_id, details = bq_client.add_urls_to_processing_queue(urls, batch_name)
        
        if success:
            added = details.get('added', 0)
            skipped_dupes = details.get('skipped_duplicates', 0)
            skipped_antler = details.get('skipped_antler', 0)
            
            print(f"\n✅ Results:")
            print(f"   Added to queue: {added}")
            if skipped_dupes > 0:
                print(f"   Skipped (duplicates): {skipped_dupes}")
            if skipped_antler > 0:
                print(f"   Skipped (antler.co): {skipped_antler}")
            
            print(f"\n🤖 URLs are now being processed automatically!")
            print(f"   Check the app to see progress")
        else:
            error = details.get('error', 'Unknown error')
            print(f"❌ Failed: {error}")
    
    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
        print("\nCreate a text file with your URLs (one per line), then run again.")
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
