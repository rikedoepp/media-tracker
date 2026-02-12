#!/usr/bin/env python3
"""Run light scraping in continuous loop until done."""

from bigquery_client import BigQueryClient
from web_scraper import scrape_light
import time
import sys

def process_batch(bq, batch_size=50):
    """Process one batch of entries."""
    q = f'''
    SELECT id, url
    FROM `media-455519.mediatracker.mediatracker`
    WHERE TRIM(content) = 'Antler'
    ORDER BY id
    LIMIT {batch_size}
    '''
    
    results = list(bq.client.query(q).result())
    
    if not results:
        return 0, 0, True  # done
    
    success = 0
    failed = 0
    
    for row in results:
        try:
            data = scrape_light(row.url)
            if data and (data.get('title') or data.get('content')):
                title = (data.get('title') or '').replace('"', "'").replace('\n', ' ')[:500]
                content = (data.get('content') or '').replace('"', "'").replace('\n', ' ')[:5000]
                domain = data.get('domain', '')
                update_q = f'''UPDATE `media-455519.mediatracker.mediatracker`
                    SET title = "{title}", content = "{content}", domain = "{domain}", updated_at = CURRENT_TIMESTAMP()
                    WHERE id = {row.id}'''
                bq.client.query(update_q).result()
                success += 1
                print(f"✓ {row.id}", flush=True)
            else:
                bq.client.query(f'''UPDATE `media-455519.mediatracker.mediatracker` SET content = "", updated_at = CURRENT_TIMESTAMP() WHERE id = {row.id}''').result()
                failed += 1
                print(f"✗ {row.id} - no content", flush=True)
        except Exception as e:
            try:
                bq.client.query(f'''UPDATE `media-455519.mediatracker.mediatracker` SET content = "", updated_at = CURRENT_TIMESTAMP() WHERE id = {row.id}''').result()
            except:
                pass
            failed += 1
            print(f"✗ {row.id} - error", flush=True)
    
    return success, failed, False

def main():
    bq = BigQueryClient()
    
    total_success = 0
    total_failed = 0
    batch_num = 0
    
    # Check initial count
    remaining = list(bq.client.query('''SELECT COUNT(*) as cnt FROM `media-455519.mediatracker.mediatracker` WHERE TRIM(content) = 'Antler' ''').result())[0].cnt
    print(f"Starting with {remaining} entries to process", flush=True)
    
    while True:
        batch_num += 1
        print(f"\n--- Batch {batch_num} ---", flush=True)
        
        success, failed, done = process_batch(bq, batch_size=50)
        
        if done:
            print(f"\n=== COMPLETE ===", flush=True)
            print(f"Total: {total_success} success, {total_failed} failed", flush=True)
            break
        
        total_success += success
        total_failed += failed
        
        # Check remaining every 5 batches
        if batch_num % 5 == 0:
            remaining = list(bq.client.query('''SELECT COUNT(*) as cnt FROM `media-455519.mediatracker.mediatracker` WHERE TRIM(content) = 'Antler' ''').result())[0].cnt
            print(f"\n>>> Remaining: {remaining} | Processed: {total_success} success, {total_failed} failed <<<", flush=True)
        
        time.sleep(0.1)

if __name__ == "__main__":
    main()
