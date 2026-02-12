import os
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

WSJ_EMAIL = os.environ.get('WSJ_EMAIL')
WSJ_PASSWORD = os.environ.get('WSJ_PASSWORD')

def scrape_wsj_article(url: str, max_retries: int = 2) -> dict:
    """
    Scrape a WSJ article using Playwright with login.
    Returns dict with 'title', 'content', 'success', 'error'
    """
    if not WSJ_EMAIL or not WSJ_PASSWORD:
        return {
            'success': False,
            'error': 'WSJ credentials not configured',
            'title': None,
            'content': None
        }
    
    for attempt in range(max_retries):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1920, 'height': 1080}
                )
                page = context.new_page()
                
                # Go to article first
                page.goto(url, wait_until='domcontentloaded', timeout=30000)
                time.sleep(2)
                
                # Check if we need to log in (look for paywall)
                needs_login = False
                try:
                    paywall = page.query_selector('[class*="paywall"], [class*="snippet"], .wsj-snippet-login')
                    if paywall:
                        needs_login = True
                except:
                    pass
                
                # Also check if content is truncated
                content_check = page.query_selector('article, [class*="article-content"]')
                if not content_check:
                    needs_login = True
                
                if needs_login:
                    # Navigate to login
                    page.goto('https://sso.accounts.dowjones.com/login', wait_until='domcontentloaded', timeout=30000)
                    time.sleep(2)
                    
                    # Enter email
                    email_input = page.wait_for_selector('input[name="username"], input[type="email"], #username', timeout=10000)
                    email_input.fill(WSJ_EMAIL)
                    
                    # Click continue/next button
                    continue_btn = page.query_selector('button[type="submit"], .sign-in-submit, button:has-text("Continue"), button:has-text("Sign In")')
                    if continue_btn:
                        continue_btn.click()
                        time.sleep(2)
                    
                    # Enter password
                    password_input = page.wait_for_selector('input[name="password"], input[type="password"], #password', timeout=10000)
                    password_input.fill(WSJ_PASSWORD)
                    
                    # Click sign in
                    signin_btn = page.query_selector('button[type="submit"], .sign-in-submit, button:has-text("Sign In")')
                    if signin_btn:
                        signin_btn.click()
                        time.sleep(5)
                    
                    # Navigate back to article
                    page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    time.sleep(3)
                
                # Extract title
                title = None
                title_selectors = ['h1', 'article h1', '.wsj-article-headline', '[class*="headline"]']
                for sel in title_selectors:
                    try:
                        title_el = page.query_selector(sel)
                        if title_el:
                            title = title_el.inner_text().strip()
                            if title:
                                break
                    except:
                        pass
                
                # Extract content
                content = None
                content_selectors = [
                    'article p',
                    '.article-content p',
                    '[class*="article-body"] p',
                    '.wsj-snippet-body p',
                    'main p'
                ]
                
                for sel in content_selectors:
                    try:
                        paragraphs = page.query_selector_all(sel)
                        if paragraphs:
                            texts = []
                            for p in paragraphs:
                                text = p.inner_text().strip()
                                if text and len(text) > 20:
                                    texts.append(text)
                            if texts:
                                content = '\n\n'.join(texts)
                                break
                    except:
                        pass
                
                browser.close()
                
                if content and len(content) >= 100:
                    return {
                        'success': True,
                        'title': title,
                        'content': content,
                        'error': None
                    }
                else:
                    return {
                        'success': False,
                        'title': title,
                        'content': content,
                        'error': f'Content too short or not found ({len(content) if content else 0} chars)'
                    }
                    
        except PlaywrightTimeout as e:
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return {
                'success': False,
                'error': f'Timeout: {str(e)}',
                'title': None,
                'content': None
            }
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return {
                'success': False,
                'error': str(e),
                'title': None,
                'content': None
            }
    
    return {
        'success': False,
        'error': 'Max retries exceeded',
        'title': None,
        'content': None
    }


def test_wsj_login():
    """Test if WSJ login works"""
    if not WSJ_EMAIL or not WSJ_PASSWORD:
        print('WSJ credentials not configured')
        return False
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            # Go to login page
            page.goto('https://sso.accounts.dowjones.com/login', wait_until='domcontentloaded', timeout=30000)
            time.sleep(2)
            
            # Enter email
            email_input = page.wait_for_selector('input[name="username"], input[type="email"], #username', timeout=10000)
            email_input.fill(WSJ_EMAIL)
            print(f'Entered email: {WSJ_EMAIL}')
            
            # Click continue
            continue_btn = page.query_selector('button[type="submit"]')
            if continue_btn:
                continue_btn.click()
                time.sleep(2)
            
            # Enter password
            password_input = page.wait_for_selector('input[name="password"], input[type="password"]', timeout=10000)
            password_input.fill(WSJ_PASSWORD)
            print('Entered password')
            
            # Click sign in
            signin_btn = page.query_selector('button[type="submit"]')
            if signin_btn:
                signin_btn.click()
                time.sleep(5)
            
            # Check if logged in
            current_url = page.url
            print(f'Current URL after login: {current_url}')
            
            browser.close()
            
            if 'login' not in current_url.lower():
                print('Login appears successful!')
                return True
            else:
                print('Login may have failed - still on login page')
                return False
                
    except Exception as e:
        print(f'Login test failed: {e}')
        return False


if __name__ == '__main__':
    print('Testing WSJ login...')
    test_wsj_login()
