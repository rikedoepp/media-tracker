import trafilatura
from urllib.parse import urlparse
import streamlit as st
import requests
from datetime import datetime
import dateutil.parser

def get_website_text_content(url: str) -> str:
    """
    This function takes a url and returns the main text content of the website.
    The text content is extracted using trafilatura and easier to understand.
    """
    try:
        # Send a request with custom timeout using requests directly
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        response = session.get(url, timeout=8)
        response.raise_for_status()
        
        text = trafilatura.extract(response.text)
        return text if text else ""
    except Exception as e:
        st.warning(f"Could not scrape content from URL: {str(e)}")
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
    Fast scraping - downloads page once and extracts content, title, and publish date
    """
    try:
        if not url:
            return None
        
        # Single optimized request with timeout
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
        if not content:
            return None
        
        # If no title from metadata, use first line of content
        if not title:
            lines = content.split('\n')
            title = lines[0] if lines else ""
        
        # Extract publish date
        publish_date = extract_publish_date(response.text, url)
        
        domain = extract_domain_from_url(url)
        
        return {
            'url': url,
            'domain': domain,
            'content': content,
            'title': title,
            'publish_date': publish_date
        }
    
    except Exception as e:
        st.warning(f"Could not scrape content from URL: {str(e)}")
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
        'content': content,
        'title': content[:100] + "..." if len(content) > 100 else content  # Extract title from content
    }