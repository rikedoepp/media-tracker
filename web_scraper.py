import trafilatura
from urllib.parse import urlparse
import requests
from datetime import datetime
import dateutil.parser
import os
import re

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False
    st = None

try:
    from firecrawl import FirecrawlApp
    HAS_FIRECRAWL = True
except ImportError:
    HAS_FIRECRAWL = False
    FirecrawlApp = None

# Firecrawl API for paywalled content
FIRECRAWL_API_KEY = os.environ.get('FIRECRAWL_API_KEY', '')

# Maximum content length to avoid BigQuery/Streamlit display issues
MAX_CONTENT_LENGTH = 50000

def truncate_content(content: str) -> str:
    """Truncate content to MAX_CONTENT_LENGTH characters"""
    if content and len(content) > MAX_CONTENT_LENGTH:
        return content[:MAX_CONTENT_LENGTH]
    return content

# Domains known to have paywalls - use Firecrawl for these
PAYWALL_DOMAINS = [
    'wsj.com',
    'ft.com',
    'nytimes.com',
    'economist.com',
    'bloomberg.com',
    'barrons.com',
    'telegraph.co.uk',
    'thetimes.co.uk',
    'washingtonpost.com',
    'hbr.org',
    'fortune.com',
    'techinasia.com',
    'sifted.eu',
]

def _warn(msg):
    if HAS_STREAMLIT and st:
        try:
            st.warning(msg)
        except:
            print(f"Warning: {msg}")
    else:
        print(f"Warning: {msg}")

def is_paywall_domain(url: str) -> bool:
    """Check if the URL is from a known paywalled domain"""
    domain = extract_domain_from_url(url)
    return any(paywall in domain for paywall in PAYWALL_DOMAINS)

def clean_markdown_content(content: str) -> str:
    """
    Clean up scraped markdown content by removing navigation, ads, 
    subscription prompts, job listings, and other website chrome.
    """
    if not content:
        return ""
    
    lines = content.split('\n')
    cleaned_lines = []
    skip_section = False
    article_started = False
    
    # Patterns to skip (navigation, ads, etc.)
    skip_patterns = [
        '- [Premium]',
        '- [Visuals]',
        '- [News]',
        '- [Paid Partnership]',
        '- [Press Releases]',
        '- More',
        '[Free newsletter]',
        '[Subscribe]',
        'Tired of ads?',
        'signing up',
        'Premium Content',
        'It takes our newsroom',
        "You can't find them",
        'anywhere else',
        'This is premium content',
        'Subscribe to read',
        'We know this is not ideal',
        'Sign up in 20 seconds',
        'Cancel anytime',
        'For learners',
        'For professionals',
        'Best value',
        'Billed annually',
        'Get instant access',
        'premium content',
        'Unlimited news content',
        'Unlimited company database',
        'Ad-free reading',
        'Just US$',
        '[Compare]',
        '[Subscribe now',
        'Already a subscriber',
        '[Log in Here]',
        'Our subscriber community',
        '### [🏆 Premium',
        '### [💼 Latest Jobs',
        '📅 Upcoming Events',
        'More articles ↓',
        'NextPrev',
        'Featured',
        'TIA Writer',
        '· 2d ago ·',
        '· 1d ago ·',
        '· 3d ago ·',
        'min read',
    ]
    
    # Patterns that indicate end of article content
    end_patterns = [
        '## Stay ahead in Asia',
        'This is premium content. Subscribe',
        '### [🏆 Premium Content]',
        '### [💼 Latest Jobs]',
    ]
    
    for line in lines:
        stripped = line.strip()
        
        # Check if we've hit end of article
        if any(pattern in line for pattern in end_patterns):
            break
        
        # Skip empty lines at the start
        if not article_started and not stripped:
            continue
        
        # Skip navigation and chrome
        if any(pattern in line for pattern in skip_patterns):
            continue
        
        # Skip image-only lines (markdown images without text)
        if stripped.startswith('![') and stripped.endswith(')') and len(stripped) < 200:
            # Allow images with captions (longer lines)
            if '/' not in stripped[3:50]:  # Skip if it looks like a nav image
                continue
        
        # Skip lines that are just links
        if stripped.startswith('[') and stripped.endswith(')') and '](' in stripped:
            link_text = stripped.split('](')[0][1:]
            if len(link_text) < 50:  # Short link text = likely navigation
                continue
        
        # Skip lines with multiple navigation-style links
        if stripped.count('](http') > 2:
            continue
        
        # Skip job listings
        if '**Mandarin Teacher**' in line or '**Online Sales' in line or 'IDR ' in line:
            continue
        
        # Detect article start (headline)
        if stripped.startswith('# ') and not article_started:
            article_started = True
        
        # Add valid content
        if stripped or article_started:
            cleaned_lines.append(line)
            if stripped:
                article_started = True
    
    # Join and clean up extra whitespace
    result = '\n'.join(cleaned_lines)
    
    # Remove multiple consecutive blank lines
    while '\n\n\n' in result:
        result = result.replace('\n\n\n', '\n\n')
    
    # Remove markdown link syntax, keep just the text
    # [text](url) -> text
    result = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', result)
    
    # Remove image references
    result = re.sub(r'!\[[^\]]*\]\([^)]+\)', '', result)
    
    # Clean up any remaining Base64 image references
    result = re.sub(r'<Base64-Image-Removed>', '', result)
    
    # Remove duplicate title (non-heading version before the # heading)
    lines = result.split('\n')
    if len(lines) > 2:
        # Find the main headline
        for i, line in enumerate(lines):
            if line.strip().startswith('# '):
                headline = line.strip()[2:].strip()
                # Check if previous non-empty lines match the headline
                new_lines = []
                for j, l in enumerate(lines):
                    if j < i and l.strip() == headline:
                        continue  # Skip duplicate title
                    new_lines.append(l)
                result = '\n'.join(new_lines)
                break
    
    # Remove multiple consecutive blank lines again
    while '\n\n\n' in result:
        result = result.replace('\n\n\n', '\n\n')
    
    return result.strip()

def scrape_with_firecrawl(url: str) -> dict:
    """
    Use Firecrawl API to scrape paywalled content.
    Returns dict with 'content', 'title', 'publish_date' or None on failure.
    """
    if not FIRECRAWL_API_KEY:
        _warn("Firecrawl API key not configured")
        return None
    
    if not HAS_FIRECRAWL:
        _warn("Firecrawl SDK not installed")
        return None
    
    try:
        app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
        
        result = app.scrape(url, formats=['markdown'])
        
        if result:
            raw_content = result.markdown if hasattr(result, 'markdown') else ''
            metadata = result.metadata if hasattr(result, 'metadata') else None
            
            # Clean up the content
            content = clean_markdown_content(raw_content)
            
            title = ''
            publish_date = datetime.now().strftime('%Y-%m-%d')
            
            if metadata:
                title = metadata.title or getattr(metadata, 'og_title', '') or ''
                pub_time = getattr(metadata, 'published_time', None) or getattr(metadata, 'publishedTime', None) or getattr(metadata, 'og_published_time', None) or getattr(metadata, 'article:published_time', None)
                if pub_time:
                    try:
                        parsed_date = dateutil.parser.parse(pub_time)
                        publish_date = parsed_date.strftime('%Y-%m-%d')
                    except:
                        pass
                
                if publish_date == datetime.now().strftime('%Y-%m-%d'):
                    mod_time = getattr(metadata, 'modified_time', None) or getattr(metadata, 'modifiedTime', None)
                    if mod_time:
                        try:
                            parsed_date = dateutil.parser.parse(mod_time)
                            publish_date = parsed_date.strftime('%Y-%m-%d')
                        except:
                            pass
            
            if content:
                return {
                    'content': truncate_content(content),
                    'title': title,
                    'publish_date': publish_date
                }
        
        return None
        
    except Exception as e:
        _warn(f"Firecrawl error: {str(e)}")
        return None

def get_website_text_content(url: str) -> str:
    """
    This function takes a url and returns the main text content of the website.
    Uses Firecrawl for paywalled sites, trafilatura for others.
    """
    try:
        # Check if this is a paywalled domain - use Firecrawl
        if is_paywall_domain(url) and FIRECRAWL_API_KEY:
            result = scrape_with_firecrawl(url)
            if result and result.get('content'):
                return result['content']
        
        # Standard scraping with trafilatura
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        response = session.get(url, timeout=8)
        response.raise_for_status()
        
        text = trafilatura.extract(response.text)
        
        # If content is too short and we have Firecrawl, try it as fallback
        if (not text or len(text) < 100) and FIRECRAWL_API_KEY:
            result = scrape_with_firecrawl(url)
            if result and result.get('content') and len(result['content']) > len(text or ''):
                return result['content']
        
        return text if text else ""
    except Exception as e:
        # If standard scraping fails and we have Firecrawl, try it
        if FIRECRAWL_API_KEY:
            result = scrape_with_firecrawl(url)
            if result and result.get('content'):
                return result['content']
        _warn(f"Could not scrape content from URL: {str(e)}")
        return ""

def get_article_title(url: str) -> str:
    """
    Extract the title/headline from the article URL - optimized single request
    """
    try:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        response = session.get(url, timeout=8)
        response.raise_for_status()
        
        # Extract metadata first (faster)
        metadata = trafilatura.extract_metadata(response.text)
        if metadata and metadata.title:
            return metadata.title
        
        # Fallback: extract content and use first line
        text = trafilatura.extract(response.text)
        if text:
            lines = text.split('\n')
            return lines[0] if lines else ""
        return ""
    except Exception as e:
        return ""

def extract_domain_from_url(url: str) -> str:
    """
    Extract domain from URL and remove www. prefix
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove www. prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return ""

def extract_publish_date(html_content: str, url: str) -> str:
    """
    Extract publish date from HTML content using multiple methods
    """
    try:
        # Method 1: Try trafilatura's date extraction
        date = trafilatura.extract_metadata(html_content)
        if date and hasattr(date, 'date') and date.date:
            try:
                # Parse and format the date
                parsed_date = dateutil.parser.parse(date.date)
                return parsed_date.strftime('%Y-%m-%d')
            except:
                pass
        
        # Method 2: Extract with full metadata context
        extracted_data = trafilatura.extract(html_content, include_comments=False, 
                                           include_tables=False, include_formatting=False,
                                           include_links=False, favor_precision=True,
                                           with_metadata=True, output_format='python')
        
        if extracted_data and isinstance(extracted_data, dict):
            # Check for date in metadata
            date_value = extracted_data.get('date') if hasattr(extracted_data, 'get') else None
            if date_value and isinstance(date_value, str):
                try:
                    parsed_date = dateutil.parser.parse(date_value)
                    return parsed_date.strftime('%Y-%m-%d')
                except:
                    pass
        
        # Method 3: Search for common date patterns in HTML
        import re
        # Look for JSON-LD structured data
        json_ld_pattern = r'"datePublished"\s*:\s*"([^"]+)"'
        match = re.search(json_ld_pattern, html_content)
        if match:
            try:
                parsed_date = dateutil.parser.parse(match.group(1))
                return parsed_date.strftime('%Y-%m-%d')
            except:
                pass
        
        # Look for meta tags
        meta_patterns = [
            r'<meta[^>]*property=["\']article:published_time["\'][^>]*content=["\']([^"\'>]+)["\']',
            r'<meta[^>]*name=["\']pubdate["\'][^>]*content=["\']([^"\'>]+)["\']',
            r'<meta[^>]*name=["\']date["\'][^>]*content=["\']([^"\'>]+)["\']',
            r'<time[^>]*datetime=["\']([^"\'>]+)["\']'
        ]
        
        for pattern in meta_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                try:
                    parsed_date = dateutil.parser.parse(match.group(1))
                    return parsed_date.strftime('%Y-%m-%d')
                except:
                    continue
        
        # Return today's date as fallback
        return datetime.now().strftime('%Y-%m-%d')
    
    except Exception as e:
        return datetime.now().strftime('%Y-%m-%d')

def scrape_metadata_only(url: str):
    """
    FAST metadata-only scraping - extracts domain, title, publish_date WITHOUT full text content
    Use this for quick URL processing, then scrape full content later
    """
    try:
        if not url:
            return None
        
        # Single optimized request with short timeout (3 seconds for speed)
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        response = session.get(url, timeout=3)
        response.raise_for_status()
        
        # Extract metadata for title (fast - no full content extraction)
        metadata = trafilatura.extract_metadata(response.text)
        title = metadata.title if metadata and metadata.title else "No title"
        
        # Extract publish date (fast)
        publish_date = extract_publish_date(response.text, url)
        
        # Extract domain (instant)
        domain = extract_domain_from_url(url)
        
        return {
            'url': url,
            'domain': domain,
            'content': None,  # No content - will scrape later
            'title': title,
            'publish_date': publish_date
        }
    
    except Exception as e:
        # Return minimal data even on error
        return {
            'url': url,
            'domain': extract_domain_from_url(url),
            'content': None,
            'title': "Error: " + str(e)[:50],
            'publish_date': datetime.now().strftime('%Y-%m-%d')
        }

def scrape_light(url: str, brand: str = ""):
    """
    LIGHT scraping - extracts domain, title, publish_date + sentences mentioning Antler/brand
    Used for data ingestion - procedure fills in the rest
    """
    try:
        if not url:
            return None
        
        # Single optimized request with short timeout (5 seconds)
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        response = session.get(url, timeout=5)
        response.raise_for_status()
        
        # Extract metadata for title
        metadata = trafilatura.extract_metadata(response.text)
        title = metadata.title if metadata and metadata.title else ""
        
        # Extract publish date
        publish_date = extract_publish_date(response.text, url)
        
        # Extract domain
        domain = extract_domain_from_url(url)
        
        # Extract full content for searching
        full_content = trafilatura.extract(response.text)
        snippet = ""
        
        if full_content:
            # Split into sentences
            import re
            sentences = re.split(r'(?<=[.!?])\s+', full_content)
            
            # Build list of keywords to search for
            keywords = ['antler']
            if brand and len(brand) > 0 and brand.lower() != 'antler':
                keywords.append(brand.lower())
            
            # Find sentences containing keywords
            relevant_sentences = []
            for i, sentence in enumerate(sentences):
                sentence_lower = sentence.lower()
                if any(keyword in sentence_lower for keyword in keywords):
                    # Include the sentence before (context) if available
                    if i > 0 and sentences[i-1] not in relevant_sentences:
                        relevant_sentences.append(sentences[i-1])
                    relevant_sentences.append(sentence)
                    # Include sentence after (context) if available
                    if i + 1 < len(sentences) and len(relevant_sentences) < 5:
                        relevant_sentences.append(sentences[i+1])
                    
                    # Limit to ~5 sentences for context
                    if len(relevant_sentences) >= 5:
                        break
            
            if relevant_sentences:
                # Join relevant sentences
                snippet = ' '.join(relevant_sentences)
                # Limit to ~500 chars max, end at sentence boundary
                if len(snippet) > 500:
                    # Find last sentence end within 500 chars
                    cut_point = 500
                    for punct in ['. ', '! ', '? ']:
                        last_punct = snippet[:500].rfind(punct)
                        if last_punct > 200:
                            cut_point = last_punct + 1
                            break
                    snippet = snippet[:cut_point]
            else:
                # Fallback: no keyword found, use first 2-3 sentences
                snippet = ' '.join(sentences[:3])
                # End at sentence boundary within 300 chars
                if len(snippet) > 300:
                    cut_point = 300
                    for punct in ['. ', '! ', '? ']:
                        last_punct = snippet[:300].rfind(punct)
                        if last_punct > 100:
                            cut_point = last_punct + 1
                            break
                    snippet = snippet[:cut_point]
        
        # If no title from metadata, use first part of content
        if not title and snippet:
            title = snippet[:100] + "..." if len(snippet) > 100 else snippet
        
        return {
            'url': url,
            'domain': domain,
            'content': snippet,  # Sentences mentioning Antler/brand
            'title': title,
            'publish_date': publish_date
        }
    
    except Exception as e:
        # Return minimal data even on error
        return {
            'url': url,
            'domain': extract_domain_from_url(url),
            'content': None,
            'title': "",
            'publish_date': datetime.now().strftime('%Y-%m-%d'),
            'error': str(e)
        }

def scrape_article_data_fast(url: str):
    """
    Fast scraping - downloads page once and extracts content, title, and publish date.
    Uses Firecrawl for paywalled sites.
    """
    try:
        if not url:
            return None
        
        domain = extract_domain_from_url(url)
        
        # For paywalled sites, use Firecrawl directly
        if is_paywall_domain(url) and FIRECRAWL_API_KEY:
            result = scrape_with_firecrawl(url)
            if result and result.get('content'):
                publish_date = result.get('publish_date', datetime.now().strftime('%Y-%m-%d'))
                if publish_date == datetime.now().strftime('%Y-%m-%d'):
                    try:
                        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                        publish_date = extract_publish_date(response.text, url)
                    except:
                        pass
                return {
                    'url': url,
                    'domain': domain,
                    'content': result['content'],
                    'title': result.get('title', ''),
                    'publish_date': publish_date
                }
        
        # Standard scraping with trafilatura
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        response = session.get(url, timeout=8)
        response.raise_for_status()
        
        # Extract metadata for title (fast)
        metadata = trafilatura.extract_metadata(response.text)
        title = metadata.title if metadata and metadata.title else ""
        
        # Extract content
        content = trafilatura.extract(response.text)
        
        # If content is too short and we have Firecrawl, try it as fallback
        if (not content or len(content) < 100) and FIRECRAWL_API_KEY:
            result = scrape_with_firecrawl(url)
            if result and result.get('content') and len(result['content']) > len(content or ''):
                fc_date = result.get('publish_date', datetime.now().strftime('%Y-%m-%d'))
                if fc_date == datetime.now().strftime('%Y-%m-%d'):
                    fc_date = extract_publish_date(response.text, url)
                return {
                    'url': url,
                    'domain': domain,
                    'content': truncate_content(result['content']),
                    'title': result.get('title', title),
                    'publish_date': fc_date
                }
        
        if not content:
            return None
        
        # If no title from metadata, use first line of content
        if not title:
            lines = content.split('\n')
            title = lines[0] if lines else ""
        
        # Extract publish date
        publish_date = extract_publish_date(response.text, url)
        
        return {
            'url': url,
            'domain': domain,
            'content': truncate_content(content),
            'title': title,
            'publish_date': publish_date
        }
    
    except Exception as e:
        # If standard scraping fails and we have Firecrawl, try it
        if FIRECRAWL_API_KEY:
            result = scrape_with_firecrawl(url)
            if result and result.get('content'):
                fc_date = result.get('publish_date', datetime.now().strftime('%Y-%m-%d'))
                if fc_date == datetime.now().strftime('%Y-%m-%d'):
                    try:
                        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                        fc_date = extract_publish_date(resp.text, url)
                    except:
                        pass
                return {
                    'url': url,
                    'domain': extract_domain_from_url(url),
                    'content': truncate_content(result['content']),
                    'title': result.get('title', ''),
                    'publish_date': fc_date
                }
        _warn(f"Could not scrape content from URL: {str(e)}")
        return None

def scrape_article_data(url: str):
    """
    Scrape article data from URL and return structured data
    """
    if not url:
        return None
    
    domain = extract_domain_from_url(url)
    content = get_website_text_content(url)
    
    if not content:
        return None
    
    return {
        'url': url,
        'domain': domain,
        'content': truncate_content(content),
        'title': content[:100] + "..." if len(content) > 100 else content  # Extract title from content
    }