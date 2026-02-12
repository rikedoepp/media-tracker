"""
Syften Twitter Mentions Sync

Pulls Twitter mentions from Syften feed, extracts article URLs,
and matches them to existing articles in BigQuery to update share counts.
"""

import os
import re
import json
import requests
from datetime import datetime
from bigquery_client import BigQueryClient

SYNC_LOG_FILE = 'twitter_sync_log.json'

def load_sync_log():
    """Load existing sync log."""
    if os.path.exists(SYNC_LOG_FILE):
        try:
            with open(SYNC_LOG_FILE, 'r') as f:
                return json.load(f)
        except:
            return {'syncs': [], 'matches': []}
    return {'syncs': [], 'matches': []}

def save_sync_log(log):
    """Save sync log."""
    with open(SYNC_LOG_FILE, 'w') as f:
        json.dump(log, f, indent=2, default=str)

def get_syften_mentions():
    """Fetch mentions from Syften JSON feed."""
    feed_url = os.environ.get('SYFTEN_FEED_URL')
    if not feed_url:
        print("Error: SYFTEN_FEED_URL not set")
        return []
    
    try:
        resp = requests.get(feed_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get('items', [])
    except Exception as e:
        print(f"Error fetching Syften feed: {e}")
        return []

def extract_urls_from_text(text):
    """Extract URLs from tweet text."""
    url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
    return re.findall(url_pattern, text or '')

def is_article_url(url):
    """Check if URL is likely a news article (not Twitter/social media)."""
    social_domains = ['twitter.com', 'x.com', 't.co', 'facebook.com', 'instagram.com', 
                      'linkedin.com', 'youtube.com', 'tiktok.com', 'reddit.com']
    url_lower = url.lower()
    return not any(domain in url_lower for domain in social_domains)

def normalize_url(url):
    """Normalize URL for matching."""
    if not url:
        return ''
    url = url.split('?')[0].split('#')[0].rstrip('/')
    url = url.replace('http://', 'https://').replace('www.', '')
    return url.lower()

def sync_syften_mentions():
    """Main sync function - pull mentions, extract URLs, match to existing articles."""
    print(f"[{datetime.now()}] Starting Syften sync...")
    
    mentions = get_syften_mentions()
    print(f"Fetched {len(mentions)} mentions from Syften")
    
    if not mentions:
        return {'total': 0, 'matched': 0, 'urls_found': 0, 'twitter_only': 0}
    
    # Filter to Twitter/X only
    twitter_mentions = [m for m in mentions if 'x.com' in m.get('url', '') or 'twitter.com' in m.get('url', '')]
    print(f"Twitter mentions: {len(twitter_mentions)}")
    
    # Filter out tweets from accounts containing "antler" in username
    filtered_mentions = []
    for m in twitter_mentions:
        author = m.get('author', {})
        author_name = (author.get('name', '') or '').lower()
        author_url = (author.get('url', '') or '').lower()
        if 'antler' not in author_name and 'antler' not in author_url:
            filtered_mentions.append(m)
    
    print(f"After filtering out 'antler' accounts: {len(filtered_mentions)}")
    twitter_mentions = filtered_mentions
    
    # Filter for VC/startup context (reduce noise from deer, games, etc.)
    vc_context_keywords = [
        'startup', 'startups', 'founder', 'founders', 'vc', 'venture', 
        'investment', 'investor', 'accelerator', 'incubator', 'portfolio',
        'funding', 'seed', 'pre-seed', 'series a', 'raise', 'raising',
        'pitch', 'residency', 'cohort', 'batch', 'demo day',
        'magnus grimeland', 'jussi salovaara', 'nitin sharma', 'tobias bengtsdahl',
        'antlerglobal', '@antler'
    ]
    
    # Publication names/domains - also valid context
    publications = [
        'techcrunch', 'wsj', 'forbes', 'bloomberg', 'reuters', 'ft.com',
        'cnbc', 'businessinsider', 'fortune', 'wired', 'theverge', 'venturebeat',
        'axios', 'sifted', 'eu-startups', 'techinasia', 'dealstreetasia', 'e27',
        'yourstory', 'inc42', 'technode', 'straitstimes', 'scmp', 'nikkei',
        'economist', 'guardian', 'bbc', 'nytimes', 'washingtonpost', 'fastcompany',
        'inc.com', 'entrepreneur', 'zdnet', 'cnet', 'mashable', 'arstechnica',
        'afr', 'theaustralian', 'channelnewsasia', 'economictimes', 'livemint',
        'bangkokpost', 'koreaherald', 'japantimes', 'handelsblatt'
    ]
    vc_context_keywords.extend(publications)
    
    # Noise keywords to exclude
    noise_keywords = [
        'deer', 'elk', 'moose', 'caribou', 'reindeer', 'hunting', 'hunter',
        'luggage', 'suitcase', 'bag', 'travel', 'taxidermy', 'horn', 'bone',
        'decor', 'yellowjackets', 'red antler', 'chew', 'dog', 'pet',
        'game', 'gaming', 'video game', 'playing cards', 'deck', 'cards',
        'restaurant', 'kitchen', 'bar', 'food', 'menu',
        'costume', 'headband', 'hat', 'cosplay', 'furry',
        'tornado', 'weather', 'storm'
    ]
    
    context_filtered = []
    for m in filtered_mentions:
        text = ((m.get('title', '') or '') + ' ' + (m.get('summary', '') or '')).lower()
        
        # Check for noise - skip if contains noise keywords
        has_noise = any(noise in text for noise in noise_keywords)
        if has_noise:
            continue
            
        # Check for VC context - keep if contains VC keywords
        has_context = any(kw in text for kw in vc_context_keywords)
        if has_context:
            context_filtered.append(m)
    
    print(f"After VC context filtering: {len(context_filtered)}")
    twitter_mentions = context_filtered
    
    # Extract article URLs from tweets
    article_urls = {}
    for mention in twitter_mentions:
        summary = mention.get('summary', '') or ''
        title = mention.get('title', '') or ''
        full_text = f"{title} {summary}"
        
        urls = extract_urls_from_text(full_text)
        for url in urls:
            if is_article_url(url):
                norm_url = normalize_url(url)
                if norm_url:
                    if norm_url not in article_urls:
                        article_urls[norm_url] = 0
                    article_urls[norm_url] += 1
    
    print(f"Found {len(article_urls)} unique article URLs in tweets")
    
    if not article_urls:
        return {'total': len(mentions), 'matched': 0, 'urls_found': 0, 'twitter_only': len(twitter_mentions)}
    
    # Get all articles from database
    client = BigQueryClient()
    query = f"""
    SELECT id, url, social_shares_count
    FROM `{client.project_id}.{client.dataset_id}.{client.table_id}`
    WHERE url IS NOT NULL
    """
    
    print("Fetching articles from database...")
    articles = list(client.client.query(query).result())
    print(f"Got {len(articles)} articles to match against")
    
    # Build lookup map
    db_articles = {}
    for article in articles:
        if article.url:
            norm = normalize_url(article.url)
            db_articles[norm] = {
                'id': article.id,
                'original_url': article.url,
                'current_count': article.social_shares_count or 0
            }
    
    # Load sync log for tracking
    sync_log = load_sync_log()
    sync_timestamp = datetime.now().isoformat()
    
    # Match URLs
    matched_count = 0
    matches_this_sync = []
    
    for tweet_url, tweet_count in article_urls.items():
        # Try exact match first
        if tweet_url in db_articles:
            article = db_articles[tweet_url]
            new_count = article['current_count'] + tweet_count
            try:
                update_query = f"""
                UPDATE `{client.project_id}.{client.dataset_id}.{client.table_id}`
                SET social_shares_count = {new_count}
                WHERE id = {article['id']}
                """
                client.client.query(update_query).result()
                matched_count += 1
                print(f"  Matched: {tweet_url[:60]}... (+{tweet_count} shares)")
                
                matches_this_sync.append({
                    'timestamp': sync_timestamp,
                    'article_id': article['id'],
                    'article_url': article['original_url'],
                    'tweet_url': tweet_url,
                    'shares_added': tweet_count,
                    'new_total': new_count
                })
            except Exception as e:
                print(f"  Error updating: {e}")
            continue
        
        # Try partial match
        for db_url, article in db_articles.items():
            if tweet_url in db_url or db_url in tweet_url:
                new_count = article['current_count'] + tweet_count
                try:
                    update_query = f"""
                    UPDATE `{client.project_id}.{client.dataset_id}.{client.table_id}`
                    SET social_shares_count = {new_count}
                    WHERE id = {article['id']}
                    """
                    client.client.query(update_query).result()
                    matched_count += 1
                    print(f"  Matched: {tweet_url[:60]}... (+{tweet_count} shares)")
                    
                    matches_this_sync.append({
                        'timestamp': sync_timestamp,
                        'article_id': article['id'],
                        'article_url': article['original_url'],
                        'tweet_url': tweet_url,
                        'shares_added': tweet_count,
                        'new_total': new_count
                    })
                except Exception as e:
                    print(f"  Error updating: {e}")
                break
    
    # Save to log
    sync_log['syncs'].append({
        'timestamp': sync_timestamp,
        'total_mentions': len(mentions),
        'twitter_mentions': len(twitter_mentions),
        'urls_found': len(article_urls),
        'matched': matched_count
    })
    sync_log['matches'].extend(matches_this_sync)
    save_sync_log(sync_log)
    
    print(f"\nSync complete: {matched_count} articles matched with Twitter shares")
    print(f"Log saved to {SYNC_LOG_FILE}")
    
    return {
        'total': len(mentions),
        'twitter_only': len(twitter_mentions),
        'urls_found': len(article_urls),
        'matched': matched_count
    }

if __name__ == '__main__':
    result = sync_syften_mentions()
    print(f"\nResults: {result}")
