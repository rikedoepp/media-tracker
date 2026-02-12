#!/usr/bin/env python3
"""Batch light scrape for entries with just 'Antler' as content."""

from bigquery_client import BigQueryClient
from web_scraper import scrape_light
import time
import sys

def main():
    bq = BigQueryClient()
    
    batch_size = 100
    total_success = 0
    total_failed = 0
    batch_num = 0
    
    while True:
        batch_num += 1
        
        q = f'''
        SELECT id, url
        FROM `media-455519.mediatracker.mediatracker`
        WHERE TRIM(content) = 'Antler'
        ORDER BY id
        LIMIT {batch_size}
        '''
        
        results = list(bq.client.query(q).result())
        
        if not results:
            print(f"\nAll done! Total: {total_success} success, {total_failed} failed")
            break
        
        print(f"\n--- Batch {batch_num} ({len(results)} entries) ---", flush=True)
        
        for row in results:
            try:
                data = scrape_light(row.url)
                
                if data and (data.get('title') or data.get('content')):
                    title = (data.get('title') or '').replace('"', "'").replace('\n', ' ')[:500]
                    content = (data.get('content') or '').replace('"', "'").replace('\n', ' ')[:5000]
                    domain = data.get('domain', '')
                    
                    update_q = f'''
                    UPDATE `media-455519.mediatracker.mediatracker`
                    SET 
                        title = "{title}",
                        content = "{content}",
                        domain = "{domain}",
                        updated_at = CURRENT_TIMESTAMP()
                    WHERE id = {row.id}
                    '''
                    bq.client.query(update_q).result()
                    total_success += 1
                else:
                    update_q = f'''
                    UPDATE `media-455519.mediatracker.mediatracker`
                    SET content = "", updated_at = CURRENT_TIMESTAMP()
                    WHERE id = {row.id}
                    '''
                    bq.client.query(update_q).result()
                    total_failed += 1
            except Exception as e:
                try:
                    update_q = f'''
                    UPDATE `media-455519.mediatracker.mediatracker`
                    SET content = "", updated_at = CURRENT_TIMESTAMP()
                    WHERE id = {row.id}
                    '''
                    bq.client.query(update_q).result()
                except:
                    pass
                total_failed += 1
        
        print(f"Progress: {total_success} success, {total_failed} failed", flush=True)
        time.sleep(0.2)

if __name__ == "__main__":
    main()
