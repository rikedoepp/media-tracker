#!/usr/bin/env python3
"""
FILL ALL CELLS - Main data completeness script

Goal: Ensure ALL 21 required fields are filled for every article.

Required fields for completeness:
1. id - auto-assigned on insert
2. publish_date - from scraping
3. url - user provided
4. domain - from URL parsing
5. country - from domain lookup (media_data table)
6. content - from scraping
7. matched_spokespeople - from content matching
8. matched_vc_investors - from content matching
9. matched_portcos - from content matching
10. tagged_antler - from content matching
11. language - from content analysis (enrichment)
12. managed_by_fund - from domain lookup (media_data table)
13. kill_pill - from content matching (enrichment)
14. kill_pill_context - from content matching (enrichment)
15. kill_pill_count - from content matching (enrichment)
16. unwanted - from content matching (enrichment)
17. unwanted_context - from content matching (enrichment)
18. unwanted_count - from content matching (enrichment)
19. month - derived from publish_date
20. tagged_portco - from portfolio matching
21. matched_vehicle - from content matching

Process:
1. Scrape content for articles missing it
2. Run enrichment procedure to fill country, language, kill_pill, unwanted, etc.
3. Mark articles as complete
"""

from google.cloud import bigquery
import os
import sys
import time
import warnings
warnings.filterwarnings('ignore')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'attached_assets/media-455519-e05e80608e53.json'
sys.path.insert(0, '/home/runner/workspace')

from web_scraper import scrape_article_data_fast

def get_client():
    return bigquery.Client(project='media-455519')

def get_completeness_report(client):
    """Get current completeness stats for all 21 required fields"""
    result = client.query('''
        SELECT 
            COUNT(*) as total,
            COUNTIF(id IS NOT NULL) as has_id,
            COUNTIF(publish_date IS NOT NULL) as has_publish_date,
            COUNTIF(url IS NOT NULL AND url != '') as has_url,
            COUNTIF(domain IS NOT NULL AND domain != '') as has_domain,
            COUNTIF(country IS NOT NULL) as has_country,
            COUNTIF(content IS NOT NULL AND content != '') as has_content,
            COUNTIF(matched_spokespeople IS NOT NULL) as has_matched_spokespeople,
            COUNTIF(matched_vc_investors IS NOT NULL) as has_matched_vc_investors,
            COUNTIF(matched_portcos IS NOT NULL) as has_matched_portcos,
            COUNTIF(tagged_antler IS NOT NULL) as has_tagged_antler,
            COUNTIF(language IS NOT NULL) as has_language,
            COUNTIF(managed_by_fund IS NOT NULL) as has_managed_by_fund,
            COUNTIF(kill_pill IS NOT NULL) as has_kill_pill,
            COUNTIF(kill_pill_context IS NOT NULL) as has_kill_pill_context,
            COUNTIF(kill_pill_count IS NOT NULL) as has_kill_pill_count,
            COUNTIF(unwanted IS NOT NULL) as has_unwanted,
            COUNTIF(unwanted_context IS NOT NULL) as has_unwanted_context,
            COUNTIF(unwanted_count IS NOT NULL) as has_unwanted_count,
            COUNTIF(month IS NOT NULL) as has_month,
            COUNTIF(tagged_portco IS NOT NULL) as has_tagged_portco,
            COUNTIF(matched_vehicle IS NOT NULL) as has_matched_vehicle
        FROM `media-455519.mediatracker.mediatracker`
    ''').result()
    
    for row in result:
        return {
            'total': row.total,
            'id': row.has_id,
            'publish_date': row.has_publish_date,
            'url': row.has_url,
            'domain': row.has_domain,
            'country': row.has_country,
            'content': row.has_content,
            'matched_spokespeople': row.has_matched_spokespeople,
            'matched_vc_investors': row.has_matched_vc_investors,
            'matched_portcos': row.has_matched_portcos,
            'tagged_antler': row.has_tagged_antler,
            'language': row.has_language,
            'managed_by_fund': row.has_managed_by_fund,
            'kill_pill': row.has_kill_pill,
            'kill_pill_context': row.has_kill_pill_context,
            'kill_pill_count': row.has_kill_pill_count,
            'unwanted': row.has_unwanted,
            'unwanted_context': row.has_unwanted_context,
            'unwanted_count': row.has_unwanted_count,
            'month': row.has_month,
            'tagged_portco': row.has_tagged_portco,
            'matched_vehicle': row.has_matched_vehicle
        }

def print_report(stats):
    """Print completeness report"""
    total = stats['total']
    print(f"\n{'='*50}")
    print(f"COMPLETENESS REPORT - {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    print(f"Total articles: {total:,}")
    print()
    print("FIELD              FILLED      MISSING     %")
    print("-" * 50)
    
    fields = [
        ('Title', 'title'),
        ('Content', 'content'),
        ('Domain', 'domain'),
        ('Publish Date', 'publish_date'),
        ('Country', 'country'),
        ('Page Rank', 'page_rank'),
        ('Tier', 'tier'),
        ('Language', 'language'),
        ('Tagged Antler', 'tagged_antler'),
        ('Tagged Portco', 'tagged_portco'),
    ]
    
    for label, key in fields:
        filled = stats[key]
        missing = total - filled
        pct = round(filled / total * 100) if total > 0 else 0
        status = "✓" if pct == 100 else "○"
        print(f"{status} {label:<16} {filled:>8,}   {missing:>8,}   {pct:>3}%")
    
    print("-" * 50)
    complete = stats.get('complete', 0) or 0
    print(f"FULLY COMPLETE:    {complete:,} / {total:,} ({round(complete/total*100) if total > 0 else 0}%)")
    print()

def scrape_missing_content(client, limit=50):
    """Scrape content for articles missing it"""
    result = client.query(f'''
        SELECT id, url
        FROM `media-455519.mediatracker.mediatracker`
        WHERE (content IS NULL OR content = '')
          AND url IS NOT NULL
        ORDER BY id
        LIMIT {limit}
    ''').result()
    
    articles = list(result)
    if not articles:
        print("No articles missing content.")
        return 0, 0
    
    print(f"Scraping {len(articles)} articles missing content...")
    success = 0
    failed = 0
    
    for article in articles:
        try:
            scraped = scrape_article_data_fast(article.url)
            
            if scraped and scraped.get('content') and len(scraped.get('content', '')) > 50:
                client.query(f'''
                    UPDATE `media-455519.mediatracker.mediatracker`
                    SET content = @content,
                        title = COALESCE(NULLIF(title, ''), @title),
                        publish_date = COALESCE(publish_date, SAFE.PARSE_TIMESTAMP("%Y-%m-%d", @pub_date)),
                        scrape_date = CURRENT_TIMESTAMP(),
                        data_ingestion = FALSE
                    WHERE id = {article.id}
                ''', job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter('content', 'STRING', scraped['content']),
                        bigquery.ScalarQueryParameter('title', 'STRING', scraped.get('title', '')),
                        bigquery.ScalarQueryParameter('pub_date', 'STRING', scraped.get('publish_date', '')),
                    ]
                )).result()
                success += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  Error scraping {article.id}: {str(e)[:60]}")
            failed += 1
    
    print(f"  Scraped: {success} success, {failed} failed")
    return success, failed

def run_enrichment(client):
    """Run the enrichment procedure to fill country, page_rank, tier, language"""
    try:
        print("Running enrichment procedure...")
        client.query('CALL `media-455519.mediatracker.process_backlog_bulk`()').result()
        print("  Enrichment complete!")
        return True
    except Exception as e:
        if 'streaming buffer' in str(e).lower():
            print("  Cannot run enrichment - data in streaming buffer. Wait ~90 minutes after last insert.")
        else:
            print(f"  Enrichment error: {str(e)[:100]}")
        return False

def mark_complete(client):
    """Mark articles as complete/incomplete based on field presence"""
    try:
        client.query('CALL `media-455519.mediatracker.mark_complete`()').result()
        print("  Marked complete/incomplete status")
        return True
    except Exception as e:
        if 'streaming buffer' in str(e).lower():
            print("  Cannot update complete status - data in streaming buffer")
        else:
            print(f"  Error: {str(e)[:100]}")
        return False

def main():
    """Main fill-all-cells process"""
    client = get_client()
    
    print("\n" + "="*50)
    print("FILL ALL CELLS PROCESS")
    print("="*50)
    
    # Step 1: Show current status
    stats = get_completeness_report(client)
    print_report(stats)
    
    # Step 2: Scrape missing content
    if stats['content'] < stats['total']:
        missing = stats['total'] - stats['content']
        print(f"\nStep 1: Scraping content ({missing:,} articles missing)")
        success, failed = scrape_missing_content(client, limit=50)
    else:
        print("\nStep 1: All articles have content ✓")
    
    # Step 3: Run enrichment
    print("\nStep 2: Running enrichment (country, page_rank, tier, language)")
    run_enrichment(client)
    
    # Step 4: Mark complete
    print("\nStep 3: Updating completeness status")
    mark_complete(client)
    
    # Step 5: Final report
    stats = get_completeness_report(client)
    print_report(stats)

if __name__ == "__main__":
    main()
