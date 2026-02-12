#!/usr/bin/env python3
"""
Background URL processor - processes URLs from the queue without Streamlit
Run this in a separate terminal to process all pending URLs
"""

from bigquery_client import BigQueryClient
import time

def main():
    bq_client = BigQueryClient()
    
    print("🤖 Background URL Processor Started")
    print("=" * 60)
    
    # Get initial queue status
    queue_table = f"{bq_client.project_id}.{bq_client.dataset_id}.processing_queue"
    count_query = f"""
    SELECT COUNT(*) as count 
    FROM `{queue_table}` 
    WHERE status = 'pending' AND batch_name != 'test_batch'
    """
    
    result = list(bq_client.client.query(count_query).result())[0]
    total_pending = result.count
    
    print(f"📊 Found {total_pending} URLs to process")
    
    if total_pending == 0:
        print("✅ No URLs to process!")
        return
    
    print(f"\n🚀 Starting processing...")
    print(f"   Processing in batches of 10")
    print(f"   You can stop anytime with Ctrl+C\n")
    
    processed = 0
    successful = 0
    failed = 0
    
    try:
        while True:
            # Process batch of 10
            batch_results = []
            for _ in range(10):
                result = bq_client.process_next_url_from_queue()
                if result:
                    batch_results.append(result)
                else:
                    break
            
            if not batch_results:
                print("\n✅ All URLs processed!")
                break
            
            # Update counters
            for result in batch_results:
                processed += 1
                if result.get('status') == 'completed':
                    successful += 1
                    url_short = result.get('url', 'URL')[:50]
                    title_short = result.get('title', 'N/A')[:40]
                    print(f"✅ [{processed}/{total_pending}] {url_short}... - {title_short}...")
                else:
                    failed += 1
                    url_short = result.get('url', 'URL')[:50]
                    error = result.get('error', 'Unknown')[:50]
                    print(f"❌ [{processed}/{total_pending}] {url_short}... - {error}")
            
            # Show progress
            progress_pct = (processed / total_pending) * 100
            print(f"\n📊 Progress: {processed}/{total_pending} ({progress_pct:.1f}%) | ✅ {successful} | ❌ {failed}\n")
            
            # Small delay to avoid rate limits
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        print("\n\n⏸️ Processing stopped by user")
        print(f"📊 Final stats: {processed} processed | ✅ {successful} successful | ❌ {failed} failed")
    
    print("\n" + "=" * 60)
    print("🏁 Background processor finished")

if __name__ == "__main__":
    main()
