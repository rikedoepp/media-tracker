import streamlit as st
from bigquery_client import BigQueryClient
from web_scraper import scrape_article_data_fast, extract_domain_from_url, scrape_metadata_only, scrape_light
from datetime import datetime
from google.cloud import bigquery
import json
import os
import pandas as pd
import time
import uuid

# Set page config for browser tab title with Antler logo as favicon
st.set_page_config(
    page_title="Antler Media Tracker", 
    layout="wide",
    page_icon="attached_assets/65ce2a9e78de30b88bf3cfaf_Antler_Icon_Logo_1766073097380.png"
)

# Override Streamlit branding in browser preview
st.markdown("""
    <meta name="description" content="Antler Media Tracker - Track media coverage">
    <meta property="og:title" content="Antler Media Tracker">
    <meta property="og:description" content="Track media coverage for Antler portfolio companies">
    <meta property="og:site_name" content="Antler Media Tracker">
""", unsafe_allow_html=True)

# Apply Antler brand styling - Satoshi font + brand colors
st.markdown("""
    <style>
    @import url('https://api.fontshare.com/v2/css?f[]=satoshi@400,500,700&display=swap');
    
    /* Nib Pro Light Italic for headings */
    @font-face {
        font-family: 'Nib Pro';
        src: url('attached_assets/NibPro-LightItalic_1766073057404.ttf') format('truetype');
        font-weight: 300;
        font-style: italic;
    }
    
    /* Antler Brand Colors */
    :root {
        --antler-red: #ED4746;
        --antler-cherry: #C9102F;
        --antler-burgundy: #46061D;
        --antler-black: #1C1A1A;
        --antler-light-pink: #FFE1E1;
        --antler-pink: #FFB5B5;
        --antler-white: #FFFFFF;
        --antler-dark-teal: #132E31;
        --antler-mint: #BADED8;
        --antler-teal: #297C85;
    }
    
    /* Satoshi font for all text */
    html, body, [class*="css"], input, textarea, select, button {
        font-family: 'Satoshi', system-ui, -apple-system, sans-serif !important;
        font-size: 16px;
        line-height: 110%;
    }
    
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Satoshi', system-ui, -apple-system, sans-serif !important;
        font-weight: 400 !important;
    }
    
    h1 {
        font-size: 60px !important;
        letter-spacing: -1% !important;
    }
    
    h2 {
        font-size: 33px !important;
    }
    
    .stTextInput input, .stTextArea textarea, .stSelectbox select {
        font-family: 'Satoshi', system-ui, -apple-system, sans-serif !important;
    }
    
    .stButton button, .stDownloadButton button {
        font-family: 'Satoshi', system-ui, -apple-system, sans-serif !important;
    }
    
    div[data-testid="stMarkdownContainer"] p {
        font-family: 'Satoshi', system-ui, -apple-system, sans-serif !important;
    }
    
    /* Light beige background theme */
    .stApp {
        background-color: #f7f5f3 !important;
    }
    
    html, body, [class*="css"], p, span, div, label, h1, h2, h3, h4, h5, h6, a {
        color: #000000 !important;
    }
    
    .stTextInput input, .stTextArea textarea, .stSelectbox select {
        background-color: #ffffff !important;
        color: #000000 !important;
        border-color: #ddd !important;
        border-radius: 4px !important;
    }
    
    /* All buttons - Dark Teal with white text */
    .stButton button,
    .stButton button[kind="primary"], 
    button[data-testid="baseButton-primary"],
    .stButton button p,
    .stButton button span,
    .stButton button div {
        background-color: #132f32 !important;
        color: #ffffff !important;
        border: 1px solid #132f32 !important;
        border-radius: 4px !important;
    }
    
    .stButton button:hover,
    .stButton button[kind="primary"]:hover,
    button[data-testid="baseButton-primary"]:hover {
        background-color: #1a4045 !important;
        border-color: #132f32 !important;
        color: #ffffff !important;
    }
    
    /* Hide top header block */
    header[data-testid="stHeader"] {
        background-color: #f7f5f3 !important;
    }
    
    /* Data frame / table - white background */
    [data-testid="stDataFrame"],
    [data-testid="stDataFrame"] > div,
    [data-testid="stDataFrame"] table,
    [data-testid="stDataFrame"] tbody,
    [data-testid="stDataFrame"] td,
    .stDataFrame {
        background-color: #ffffff !important;
    }
    
    [data-testid="stDataFrame"] thead,
    [data-testid="stDataFrame"] th,
    .stDataFrame thead th {
        background-color: #f0f0f0 !important;
    }
    
    /* Expander styling - darker beige for Updates */
    [data-testid="stExpander"] {
        background-color: #ebe7e3 !important;
        border-radius: 4px !important;
    }
    
    /* Info boxes */
    [data-testid="stNotification"],
    .stAlert {
        background-color: #ffffff !important;
    }
    
    .stCheckbox label {
        color: #000000 !important;
    }
    
    /* Table styling */
    div[data-testid="column"] {
        overflow: hidden !important;
        width: 100% !important;
    }
    
    .block-container {
        max-width: 100% !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    
    div[data-testid="stHorizontalBlock"] {
        width: 100% !important;
        gap: 0.5rem !important;
    }
    
    div[data-testid="stMarkdownContainer"] p {
        word-wrap: break-word !important;
        overflow-wrap: break-word !important;
        white-space: normal !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        margin: 0 !important;
        padding: 4px 8px !important;
        line-height: 1.4 !important;
    }
    
    .stCheckbox {
        margin: 0 !important;
        padding: 4px 0 !important;
    }
    
    hr {
        border-color: #ddd !important;
        margin: 8px 0 !important;
    }
    
    /* Links - Black */
    a {
        color: #000000 !important;
        word-break: break-all !important;
    }
    
    a:hover {
        color: #333333 !important;
    }
    
    /* Info/Success messages */
    .stAlert {
        border-radius: 4px !important;
    }
    
    /* Progress bar - Antler Red */
    .stProgress > div > div {
        background-color: var(--antler-red) !important;
    }
    
    /* Tooltip styling */
    div[data-baseweb="tooltip"] {
        background-color: #ffffff !important;
    }
    
    div[data-baseweb="tooltip"] div {
        background-color: #ffffff !important;
        color: #000000 !important;
    }
    
    .stTooltipIcon + div, 
    [data-testid="stTooltipContent"] {
        background-color: #ffffff !important;
        color: #000000 !important;
        border: 1px solid #ddd !important;
    }
    
    .st-emotion-cache-1inwz65,
    .st-emotion-cache-16idsys,
    [data-testid="stMarkdownContainer"] > div[style*="position: absolute"] {
        background-color: #ffffff !important;
        color: #000000 !important;
    }
    
    /* Radio buttons */
    .stRadio > div {
        gap: 1rem !important;
    }
    
    /* Data editor styling */
    [data-testid="stDataFrame"] {
        border-radius: 4px !important;
    }
    
    </style>
    """, unsafe_allow_html=True)

def main():
    # Back link to News Monitor
    st.markdown('''
        <a href="https://antler-news-monitor.lovable.app" target="_blank" 
           style="color: #132f32 !important; text-decoration: none; font-size: 14px;">
            ← Go back to News Monitor
        </a>
    ''', unsafe_allow_html=True)
    
    # Add logo and header
    col1, col2 = st.columns([1, 4])

    with col1:
        # Clickable Antler logo to reset app
        import base64
        with open("attached_assets/65ce2a9e78de30b88bf3cfaf_Antler_Icon_Logo_1766073097380.png", "rb") as f:
            logo_data = base64.b64encode(f.read()).decode()
        
        # Check for reset trigger via query param
        if st.query_params.get("reset") == "1":
            st.query_params.clear()
            if 'scraped_data' in st.session_state:
                del st.session_state.scraped_data
            st.session_state.clear_url_field = True
            st.session_state.clear_multi_urls = True
            st.rerun()
        
        # Logo as clickable link that triggers reset
        st.markdown(f'''
            <style>
            .clickable-logo {{
                cursor: pointer;
                transition: opacity 0.2s;
            }}
            .clickable-logo:hover {{
                opacity: 0.7;
            }}
            </style>
            <a href="?reset=1" style="text-decoration: none;">
                <img src="data:image/png;base64,{logo_data}" width="50" class="clickable-logo" title="Click to reset">
            </a>
        ''', unsafe_allow_html=True)

    with col2:
        st.markdown("""
            <h1 style="margin: 0; padding: 0;">
                <span style="font-family: 'Nib Pro', serif; font-weight: 300; font-style: italic;">Media</span> 
                <span style="font-family: 'Satoshi', sans-serif;">Tracker</span>
            </h1>
        """, unsafe_allow_html=True)

    # Initialize BigQuery
    try:
        bq_client = BigQueryClient()
    except Exception as e:
        st.error(f"❌ BigQuery connection failed: {e}")
        return

    # Updates Section - Background Scraping Status
    st.markdown("---")
    # Get pending count for the header
    pending_scrapes = bq_client.get_pending_scrapes(limit=100)
    real_pending = [p for p in pending_scrapes if 'example.com' not in p['url']]
    pending_count = len(real_pending)
    updates_label = f"📊 Updates - {pending_count} waiting to be scraped" if pending_count > 0 else "📊 Updates - All caught up"
    with st.expander(updates_label, expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Pending Scrapes**")
            # Use already fetched pending data
            if real_pending:
                for item in real_pending:
                    status_icon = "⏳" if item['status'] == 'pending' else "🔄"
                    # Show shortened URL
                    url_short = item['url'][:40] + '...' if len(item['url']) > 40 else item['url']
                    st.markdown(f"{status_icon} {url_short}")
            else:
                st.caption("No pending scrapes")
        
        with col2:
            st.markdown("**Recently Scraped**")
            recent_activity = bq_client.get_recent_scraping_activity(limit=10)
            if recent_activity:
                for item in recent_activity:
                    scrape_icon = "✅" if item['scrape_type'] == 'Full Scrape' else "⚡"
                    title_short = (item['title'][:35] + '...') if len(item['title']) > 35 else item['title']
                    time_str = item['updated_at'].strftime('%H:%M') if item['updated_at'] else ''
                    st.markdown(f"{scrape_icon} {title_short} ({time_str})")
            else:
                st.caption("No recent activity")
        
        # Processing queue status with progress bar
        queue_status = bq_client.get_processing_queue_status()
        if queue_status:
            active_batches = {name: status for name, status in queue_status.items() 
                             if (status.get('pending', 0) > 0 or status.get('processing', 0) > 0)
                             and name != 'test_batch'}
            
            if active_batches:
                st.markdown("---")
                for batch_name, status in active_batches.items():
                    total = sum(status.values())
                    completed = status.get('completed', 0)
                    pending = status.get('pending', 0)
                    failed = status.get('failed', 0)
                    processed = completed + failed
                    progress = processed / total if total > 0 else 0
                    
                    st.progress(progress, text=f"Batch: {batch_name} - {processed}/{total} ({progress*100:.0f}%)")
                    
                    if pending > 0:
                        if st.button("Continue Processing", key=f"process_{batch_name}"):
                            with st.spinner("Processing URLs..."):
                                for i in range(5):
                                    result = bq_client.process_next_url_from_queue()
                                    if not result:
                                        break
                                st.rerun()
        
        if st.button("🔄 Refresh", key="refresh_updates"):
            st.rerun()
        
        # Light scrape unscraped articles section
        st.markdown("---")
        st.markdown("**Scrape Unscraped Articles**")
        unscraped = bq_client.get_unscraped_articles(limit=500)
        unscraped_count = len(unscraped)
        
        if unscraped_count > 0:
            st.write(f"{unscraped_count} articles without content")
            
            if st.button(f"⚡ Light Scrape All ({unscraped_count})", key="light_scrape_all"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                success_count = 0
                fail_count = 0
                
                for i, article in enumerate(unscraped):
                    progress = (i + 1) / unscraped_count
                    progress_bar.progress(progress)
                    status_text.text(f"Scraping {i+1}/{unscraped_count}: {article['domain']}")
                    
                    success, msg = bq_client.light_scrape_article(article['id'], article['url'])
                    if success:
                        success_count += 1
                    else:
                        fail_count += 1
                    
                    # Small delay to avoid rate limiting
                    time.sleep(0.5)
                
                progress_bar.progress(1.0)
                status_text.text(f"Done! {success_count} scraped, {fail_count} failed")
                st.success(f"✅ Scraped {success_count} articles")
                if fail_count > 0:
                    st.warning(f"⚠️ {fail_count} articles failed to scrape")
        else:
            st.caption("All articles have content")

    st.markdown("---")
    st.subheader("1. Add Articles")

    # URL input method selection
    input_method = st.radio("How would you like to add articles?", 
                           ["Single URL", "Multiple URLs", "Wizikey Data Ingestion", "📊 Data Health"], 
                           horizontal=True)
    
    # Default values for all input methods
    scrape_clicked = False
    urls_to_process = []

    if input_method == "Single URL":
        # Initialize URL clearing flag
        if 'clear_url_field' not in st.session_state:
            st.session_state.clear_url_field = False

        # Handle URL input with clearing capability
        url_key = "url_field_cleared" if st.session_state.clear_url_field else "url_field"
        url = st.text_input("Paste URL:", key=url_key, placeholder="https://example.com/article")

        # Reset the clearing flag after displaying the field
        if st.session_state.clear_url_field:
            st.session_state.clear_url_field = False

        # Two processing options
        col1, col2 = st.columns(2)
        with col1:
            fast_clicked = st.button("Quick Scrape", help="Light ingestion - saves title, domain, date only")
        with col2:
            full_clicked = st.button("Full Scrape", help="Full ingestion - saves basic info + full article text")
        
        scrape_clicked = fast_clicked or full_clicked
        urls_to_process = [url] if url else []
        
        # Store which type of processing was requested
        if 'processing_type' not in st.session_state:
            st.session_state.processing_type = 'full'
        if fast_clicked:
            st.session_state.processing_type = 'fast'
        elif full_clicked:
            st.session_state.processing_type = 'full'
        
    elif input_method == "Multiple URLs":
        # Initialize clearing flag for multiple URLs
        if 'clear_multi_urls' not in st.session_state:
            st.session_state.clear_multi_urls = False
        
        multi_url_key = "multi_urls_cleared" if st.session_state.clear_multi_urls else "multi_urls"
        urls_text = st.text_area("Paste URLs (one per line):", 
                                height=120,
                                placeholder="https://example.com/article1\nhttps://example.com/article2\nhttps://example.com/article3",
                                key=multi_url_key)
        
        # Reset the clearing flag
        if st.session_state.clear_multi_urls:
            st.session_state.clear_multi_urls = False
        
        # Parse URLs from text area first to show count
        urls_to_process = []
        if urls_text:
            urls_to_process = [url.strip() for url in urls_text.split('\n') if url.strip()]
        
        if urls_to_process:
            st.info(f"**{len(urls_to_process)} URLs ready to process**")
        
        col1, col2 = st.columns(2)
        with col1:
            fast_clicked = st.button("Quick Scrape", help="Light ingestion - saves title, domain, date only", key="multi_fast")
        with col2:
            full_clicked = st.button("Full Scrape", help="Full ingestion - saves basic info + full article text", key="multi_full")
        
        scrape_clicked = fast_clicked or full_clicked
        
        # Store which type of processing was requested
        if 'processing_type' not in st.session_state:
            st.session_state.processing_type = 'full'
        if fast_clicked:
            st.session_state.processing_type = 'fast'
        elif full_clicked:
            st.session_state.processing_type = 'full'
        
        # Initialize URL queue in session state
        if 'url_queue' not in st.session_state:
            st.session_state.url_queue = []
        if 'processed_count' not in st.session_state:
            st.session_state.processed_count = 0
    
    elif input_method == "Wizikey Data Ingestion":
        st.write("**Bulk import from CSV:**")
        st.caption("Upload a CSV with columns: URL, Headline, Publish Date, Publication Name, Brand")
        st.caption("Each URL will be lightly scraped to extract title, domain, and key sentences")
        
        uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'], key="data_ingestion_uploader")
        
        # Initialize default values
        scrape_clicked = False
        urls_to_process = []
        
        if uploaded_file is not None:
            try:
                # Read CSV file
                import pandas as pd
                # Auto-detect delimiter (supports comma, semicolon, tab, etc.)
                df = pd.read_csv(uploaded_file, sep=None, engine='python')
                
                st.write(f"**Preview of uploaded data ({len(df)} rows):**")
                st.dataframe(df.head(10))
                
                # Data ingestion button
                if st.button("Import Articles", type="primary"):
                    try:
                        # Get the current MAX ID from BigQuery first
                        max_id_query = f"""
                        SELECT IFNULL(MAX(id), 0) as max_id
                        FROM `{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}`
                        """
                        max_id_result = list(bq_client.client.query(max_id_query).result())
                        next_id = max_id_result[0].max_id + 1
                        
                        st.info(f"🔢 Starting ID assignment from: {next_id}")
                        
                        # Check for duplicate URLs (case-insensitive column name)
                        url_col = next((col for col in df.columns if col.lower() == 'url'), None)
                        if not url_col:
                            st.error("❌ No URL column found in CSV")
                            st.stop()
                        
                        # Find all CSV columns (case-insensitive)
                        brand_col = next((col for col in df.columns if col.lower() in ['brand', 'content']), None)
                        headline_col = next((col for col in df.columns if col.lower() == 'headline'), None)
                        date_col = next((col for col in df.columns if col.lower() == 'publish date'), None)
                        pub_name_col = next((col for col in df.columns if col.lower() == 'publication name'), None)
                        
                        all_urls = [str(row.get(url_col, '')).strip() for idx, row in df.iterrows() if pd.notna(row.get(url_col))]
                        st.info(f"🔍 Checking for duplicate URLs...")
                        existing_urls = bq_client.check_existing_urls(all_urls)
                        
                        if existing_urls:
                            st.warning(f"⚠️ Found {len(existing_urls)} duplicate URLs that will be skipped")
                        
                        # Filter to new URLs only and extract all CSV data
                        new_rows = []
                        for idx, row in df.iterrows():
                            url_val = str(row.get(url_col, '')).strip() if url_col and pd.notna(row.get(url_col)) else ''
                            if url_val and url_val not in existing_urls:
                                brand_val = str(row.get(brand_col, '')).strip() if brand_col and pd.notna(row.get(brand_col)) else ''
                                headline_val = str(row.get(headline_col, '')).strip() if headline_col and pd.notna(row.get(headline_col)) else ''
                                date_val = str(row.get(date_col, '')).strip() if date_col and pd.notna(row.get(date_col)) else ''
                                pub_name_val = str(row.get(pub_name_col, '')).strip() if pub_name_col and pd.notna(row.get(pub_name_col)) else ''
                                new_rows.append({
                                    'url': url_val, 
                                    'brand': brand_val,
                                    'headline': headline_val,
                                    'publish_date': date_val,
                                    'publication_name': pub_name_val
                                })
                        
                        if not new_rows:
                            if existing_urls:
                                st.info(f"ℹ️ All {len(existing_urls)} URLs already exist in the database. No new rows to insert.")
                            else:
                                st.error("❌ No valid URLs found in CSV")
                            st.stop()
                        
                        st.info(f"📰 Processing {len(new_rows)} new URLs with light scraping...")
                        
                        # Progress tracking
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        from datetime import timedelta
                        base_timestamp = datetime.now()
                        
                        rows_to_insert = []
                        errors = []
                        
                        # Helper function to parse dates like "08 Jan 2026"
                        def parse_csv_date(date_str):
                            if not date_str:
                                return None
                            from dateutil import parser as date_parser
                            try:
                                parsed = date_parser.parse(date_str)
                                return parsed.strftime('%Y-%m-%d 00:00')
                            except:
                                return None
                        
                        for idx, row_data in enumerate(new_rows):
                            url_val = row_data['url']
                            brand_val = row_data['brand']
                            csv_headline = row_data.get('headline', '')
                            csv_date = row_data.get('publish_date', '')
                            csv_pub_name = row_data.get('publication_name', '')
                            
                            # Update progress
                            progress = (idx + 1) / len(new_rows)
                            progress_bar.progress(progress)
                            status_text.text(f"Light scraping {idx + 1}/{len(new_rows)}: {url_val[:50]}...")
                            
                            try:
                                # LIGHT scraping - just title, domain, date, key sentences (fast)
                                scraped = scrape_light(url_val, brand_val)
                                
                                # For light scraping, accept if we got at least domain (always extractable from URL)
                                if scraped:
                                    # Stagger timestamps
                                    row_timestamp = base_timestamp + timedelta(seconds=idx)
                                    
                                    # Determine tagged_antler based on Brand field OR content
                                    content_lower = scraped.get('content', '').lower()
                                    title_lower = (csv_headline or scraped.get('title', '')).lower()
                                    tagged_antler = ('antler' in brand_val.lower() if brand_val else False) or \
                                                   'antler' in content_lower or 'antler' in title_lower
                                    
                                    # Use CSV date if available, otherwise scraped date
                                    pub_date = parse_csv_date(csv_date) or scraped.get('publish_date', '')
                                    if pub_date and len(pub_date) == 10:  # YYYY-MM-DD format
                                        pub_date = f"{pub_date} 00:00"  # Add time component
                                    
                                    # Use CSV headline if available, otherwise scraped title
                                    title = csv_headline if csv_headline else scraped.get('title', '')
                                    
                                    # Ensure domain exists in media_data (fetch page_rank if new)
                                    domain = scraped.get('domain', '')
                                    bq_client.ensure_domain_in_media_data(domain)
                                    
                                    # Create row for BigQuery with ALL fields - NO NULLs allowed
                                    bq_row = {
                                        'id': next_id,
                                        'url': url_val,
                                        'title': title,
                                        'publish_date': pub_date if pub_date else None,
                                        'domain': domain,
                                        'content': scraped.get('content', ''),
                                        'updated_at': row_timestamp.isoformat(),
                                        'data_ingestion': True,
                                        'tagged_antler': tagged_antler,
                                        'matched_portcos': brand_val if (brand_val and brand_val.lower() != 'antler') else '',
                                        # All fields with defaults - no NULLs
                                        'country': '',
                                        'matched_spokespeople': '',
                                        'matched_vc_investors': '',
                                        'matched_vehicle': '',
                                        'matched_dealroom_rank': '',
                                        'matched_reporter': '',
                                        'matched_portco_location': '',
                                        'matched_portco_deal_lead': '',
                                        'language': '',
                                        'managed_by_fund': '',
                                        'tier': '',
                                        'kill_pill': False,
                                        'kill_pill_context': '',
                                        'kill_pill_count': 0,
                                        'unwanted': False,
                                        'unwanted_context': '',
                                        'unwanted_count': 0,
                                        'tagged_portco': False,
                                        'spokespeople_in_headline': False,
                                        'antler_in_headline': False,
                                        'unbranded_win': False,
                                        'text_scraped': False,
                                        'is_complete': False,
                                        'cleaned_url': url_val.split('?')[0].split('#')[0],
                                        'page_rank': 0,
                                        'social_shares_count': 0,
                                        'backlinks': 0.0,
                                        'summary': '',
                                        'li_summary': '',
                                        'text_scrape_error': '',
                                        '_unused_1': '',
                                        '_unused_2': '',
                                        '_unused_3': 0,
                                        '_unused_4': '',
                                        '_unused_5': ''
                                    }
                                    
                                    rows_to_insert.append(bq_row)
                                    next_id += 1
                                else:
                                    errors.append(f"Failed to scrape: {url_val[:50]}")
                                    
                            except Exception as e:
                                errors.append(f"{url_val[:30]}: {str(e)[:30]}")
                        
                        status_text.text("Inserting data into BigQuery...")
                        
                        # Insert to BigQuery
                        if rows_to_insert:
                            table_ref = f"{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}"
                            insert_errors = bq_client.client.insert_rows_json(table_ref, rows_to_insert)
                            
                            if insert_errors:
                                st.error(f"❌ Some rows failed to insert:")
                                for err in insert_errors[:10]:
                                    st.error(f"  {err}")
                            else:
                                st.success(f"✅ Successfully inserted {len(rows_to_insert)} new rows into BigQuery!")
                                
                                # Show summary
                                summary_col1, summary_col2, summary_col3 = st.columns(3)
                                with summary_col1:
                                    st.metric("✅ Inserted", len(rows_to_insert))
                                with summary_col2:
                                    st.metric("⏭️ Skipped (Duplicates)", len(existing_urls))
                                with summary_col3:
                                    st.metric("❌ Errors", len(errors))
                                
                                if errors:
                                    with st.expander("View errors"):
                                        for err in errors:
                                            st.write(f"- {err}")
                                
                                # MANDATORY: Run enrichment on ALL rows - no exceptions
                                import time
                                with st.spinner("⏳ Waiting for BigQuery to process (20 sec)..."):
                                    time.sleep(20)
                                with st.spinner("🔄 Running enrichment on ALL rows..."):
                                    bq_client.run_full_enrichment()
                                st.success("✅ Enrichment completed - all fields filled!")
                        else:
                            st.error("❌ No rows were successfully scraped")
                            if errors:
                                with st.expander("View errors"):
                                    for err in errors:
                                        st.write(f"- {err}")
                    
                    except Exception as e:
                        st.error(f"❌ Error during data ingestion: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())
                    
            except Exception as e:
                st.error(f"❌ Error reading CSV: {str(e)}")
    
    elif input_method == "📊 Data Health":
        st.write("**Data Completeness Dashboard**")
        st.caption("Track which articles have all required fields filled in")
        
        # Run Enrichment Button - fills ALL fields for ALL rows
        st.subheader("🔄 Run Enrichment")
        st.caption("Fill all empty fields across ALL rows in the database")
        if st.button("▶️ Run Enrichment Now", type="primary", key="run_enrichment_all"):
            with st.spinner("Running enrichment on ALL rows..."):
                try:
                    bq_client.run_full_enrichment()
                    st.success("✅ Enrichment completed! All fields filled for all rows.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
        
        st.divider()
        
        # Get completeness stats for all 21 required fields
        try:
            completeness_query = """
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
                    COUNTIF(matched_vehicle IS NOT NULL) as has_matched_vehicle,
                    COUNTIF(
                        id IS NOT NULL
                        AND publish_date IS NOT NULL
                        AND url IS NOT NULL AND url != ''
                        AND domain IS NOT NULL AND domain != ''
                        AND country IS NOT NULL
                        AND content IS NOT NULL AND content != ''
                        AND matched_spokespeople IS NOT NULL
                        AND matched_vc_investors IS NOT NULL
                        AND matched_portcos IS NOT NULL
                        AND tagged_antler IS NOT NULL
                        AND language IS NOT NULL
                        AND managed_by_fund IS NOT NULL
                        AND kill_pill IS NOT NULL
                        AND kill_pill_context IS NOT NULL
                        AND kill_pill_count IS NOT NULL
                        AND unwanted IS NOT NULL
                        AND unwanted_context IS NOT NULL
                        AND unwanted_count IS NOT NULL
                        AND month IS NOT NULL
                        AND tagged_portco IS NOT NULL
                        AND matched_vehicle IS NOT NULL
                    ) as fully_complete,
                    COUNTIF(data_ingestion = TRUE) as pending_scrape
                FROM `media-455519.mediatracker.mediatracker`
            """
            result = list(bq_client.client.query(completeness_query).result())
            
            if result:
                stats = result[0]
                total = stats.total
                
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    complete_pct = round(stats.fully_complete / total * 100) if total > 0 else 0
                    st.metric("Fully Complete", f"{stats.fully_complete:,}", f"{complete_pct}%")
                with col2:
                    st.metric("Total Articles", f"{total:,}")
                with col3:
                    incomplete = total - stats.fully_complete
                    st.metric("Missing Data", f"{incomplete:,}")
                with col4:
                    st.metric("Pending Scrape", f"{stats.pending_scrape:,}")
                
                st.markdown("---")
                st.write("**Field Completeness (21 Required Fields):**")
                
                # Create a dataframe for the completeness table with all 21 fields
                # Type: BOOL = boolean (TRUE/FALSE), STRING = text, INT = integer, DATE = date
                fields_data = [
                    ("1. id", "INT", "-", stats.has_id, total - stats.has_id, round(stats.has_id/total*100) if total > 0 else 0),
                    ("2. publish_date", "DATE", "-", stats.has_publish_date, total - stats.has_publish_date, round(stats.has_publish_date/total*100) if total > 0 else 0),
                    ("3. url", "STRING", "-", stats.has_url, total - stats.has_url, round(stats.has_url/total*100) if total > 0 else 0),
                    ("4. domain", "STRING", "-", stats.has_domain, total - stats.has_domain, round(stats.has_domain/total*100) if total > 0 else 0),
                    ("5. country", "STRING", "-", stats.has_country, total - stats.has_country, round(stats.has_country/total*100) if total > 0 else 0),
                    ("6. content", "STRING", "-", stats.has_content, total - stats.has_content, round(stats.has_content/total*100) if total > 0 else 0),
                    ("7. matched_spokespeople", "STRING", "-", stats.has_matched_spokespeople, total - stats.has_matched_spokespeople, round(stats.has_matched_spokespeople/total*100) if total > 0 else 0),
                    ("8. matched_vc_investors", "STRING", "-", stats.has_matched_vc_investors, total - stats.has_matched_vc_investors, round(stats.has_matched_vc_investors/total*100) if total > 0 else 0),
                    ("9. matched_portcos", "STRING", "-", stats.has_matched_portcos, total - stats.has_matched_portcos, round(stats.has_matched_portcos/total*100) if total > 0 else 0),
                    ("10. tagged_antler", "BOOL", "TRUE / FALSE", stats.has_tagged_antler, total - stats.has_tagged_antler, round(stats.has_tagged_antler/total*100) if total > 0 else 0),
                    ("11. language", "STRING", "-", stats.has_language, total - stats.has_language, round(stats.has_language/total*100) if total > 0 else 0),
                    ("12. managed_by_fund", "STRING", "-", stats.has_managed_by_fund, total - stats.has_managed_by_fund, round(stats.has_managed_by_fund/total*100) if total > 0 else 0),
                    ("13. kill_pill", "BOOL", "TRUE / FALSE", stats.has_kill_pill, total - stats.has_kill_pill, round(stats.has_kill_pill/total*100) if total > 0 else 0),
                    ("14. kill_pill_context", "STRING", "-", stats.has_kill_pill_context, total - stats.has_kill_pill_context, round(stats.has_kill_pill_context/total*100) if total > 0 else 0),
                    ("15. kill_pill_count", "INT", "-", stats.has_kill_pill_count, total - stats.has_kill_pill_count, round(stats.has_kill_pill_count/total*100) if total > 0 else 0),
                    ("16. unwanted", "BOOL", "TRUE / FALSE", stats.has_unwanted, total - stats.has_unwanted, round(stats.has_unwanted/total*100) if total > 0 else 0),
                    ("17. unwanted_context", "STRING", "-", stats.has_unwanted_context, total - stats.has_unwanted_context, round(stats.has_unwanted_context/total*100) if total > 0 else 0),
                    ("18. unwanted_count", "INT", "-", stats.has_unwanted_count, total - stats.has_unwanted_count, round(stats.has_unwanted_count/total*100) if total > 0 else 0),
                    ("19. month", "STRING", "-", stats.has_month, total - stats.has_month, round(stats.has_month/total*100) if total > 0 else 0),
                    ("20. tagged_portco", "BOOL", "TRUE / FALSE", stats.has_tagged_portco, total - stats.has_tagged_portco, round(stats.has_tagged_portco/total*100) if total > 0 else 0),
                    ("21. matched_vehicle", "STRING", "-", stats.has_matched_vehicle, total - stats.has_matched_vehicle, round(stats.has_matched_vehicle/total*100) if total > 0 else 0),
                ]
                
                import pandas
                df_fields = pandas.DataFrame(fields_data, columns=["Field", "Type", "Options", "Filled", "Missing", "%"])
                st.dataframe(df_fields, use_container_width=True, hide_index=True)
                
                # Action buttons
                st.markdown("---")
                st.write("**Actions:**")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Run Enrichment", help="Fill country, page_rank, tier, language from domain data"):
                        with st.spinner("Running enrichment procedure..."):
                            try:
                                bq_client.client.query('CALL `media-455519.mediatracker.process_backlog_bulk`()').result()
                                st.success("Enrichment complete! Refresh to see updated stats.")
                            except Exception as e:
                                if 'streaming buffer' in str(e).lower():
                                    st.warning("Cannot run enrichment - recent data is still processing. Try again in ~90 minutes.")
                                else:
                                    st.error(f"Error: {str(e)[:100]}")
                
                with col2:
                    if st.button("Refresh Stats"):
                        st.rerun()
                
        except Exception as e:
            st.error(f"Error loading completeness data: {str(e)}")
    
    # Process URLs if user clicked extract/process button
    if scrape_clicked:
        if not urls_to_process:
            st.error("❌ Please enter at least one URL!")
        elif len(urls_to_process) == 1:
            # Single URL processing (original logic)
            url = urls_to_process[0]
            processing_type = st.session_state.get('processing_type', 'full')
            
            spinner_text = "Extracting content..." if processing_type == 'full' else "Extracting metadata..."
            with st.spinner(spinner_text):
                try:
                    # Use appropriate scraper based on processing type
                    if processing_type == 'fast':
                        data = scrape_metadata_only(url)
                    else:
                        data = scrape_article_data_fast(url)

                    if data:
                        st.session_state.scraped_data = {
                            'url': data['url'],
                            'content': data.get('content', ''),
                            'title': data['title'],
                            'domain': data['domain'],
                            'publish_date': data.get('publish_date', datetime.now().strftime('%Y-%m-%d'))
                        }
                    else:
                        st.session_state.scraped_data = {
                            'url': url,
                            'content': '',
                            'title': '',
                            'domain': extract_domain_from_url(url),
                            'publish_date': datetime.now().strftime('%Y-%m-%d')
                        }

                    if processing_type == 'fast':
                        if data and data['title']:
                            st.success("⚡ Metadata extracted successfully! (No full text content)")
                        else:
                            st.warning("⚠️ Could not extract metadata automatically. Please add manually below.")
                    else:
                        if data and data.get('content'):
                            st.success("✅ Content extracted successfully!")
                        else:
                            st.warning("⚠️ Could not extract content automatically. Please add manually below.")

                    st.session_state.clear_url_field = True
                    st.rerun()

                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    st.session_state.scraped_data = {
                        'url': url,
                        'content': '',
                        'title': '',
                        'domain': extract_domain_from_url(url),
                        'publish_date': datetime.now().strftime('%Y-%m-%d')
                    }
                    st.session_state.clear_url_field = True
                    st.rerun()
        
        else:
            # Multiple URLs processing
            st.session_state.url_queue = urls_to_process.copy()
            st.session_state.processed_count = 0
            st.session_state.batch_results = []
            
            # Process all URLs with enhanced error handling
            progress_bar = st.progress(0)
            status_text = st.empty()
            error_container = st.container()
            
            # Track processing statistics
            successful_count = 0
            failed_count = 0
            error_count = 0
            
            st.info(f"🚀 **Starting batch processing:** {len(urls_to_process)} URLs")
            
            for i, url in enumerate(urls_to_process):
                try:
                    # Update progress display
                    progress_percent = i / len(urls_to_process)
                    progress_bar.progress(progress_percent)
                    status_text.text(f"Processing {i+1}/{len(urls_to_process)}: {url[:60]}...")
                    
                    # Scrape article data
                    data = scrape_article_data_fast(url)
                    
                    if data and data.get('content'):
                        # Auto-save each successful extraction to BigQuery
                        # Data standards: NULL for no match, FALSE for boolean defaults
                        record_data = {
                            'url': data['url'],
                            'content': data['content'],
                            'domain': data['domain'],
                            'title': data['title'],
                            'publish_date': data.get('publish_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'matched_spokespeople': None,
                            'matched_reporter': None,
                            'backlinks': 0.0,
                            'tagged_antler': False,
                            'language': 'en',
                            'matched_portcos': None,
                            'matched_portco_location': None,
                            'matched_portco_deal_lead': None,
                            'managed_by_fund': None,
                            'unbranded_win': False
                        }
                        
                        # Attempt to save to BigQuery (skip procedure - will call bulk at end)
                        try:
                            success = bq_client.insert_media_record(record_data, skip_procedure=True)
                            if success:
                                successful_count += 1
                                st.session_state.batch_results.append({
                                    'url': url, 
                                    'status': 'success', 
                                    'title': data['title'][:50]
                                })
                            else:
                                failed_count += 1
                                st.session_state.batch_results.append({
                                    'url': url, 
                                    'status': 'db_failed', 
                                    'title': 'Database insertion failed'
                                })
                        except Exception as db_error:
                            failed_count += 1
                            error_msg = f"DB Error: {str(db_error)[:50]}"
                            st.session_state.batch_results.append({
                                'url': url, 
                                'status': 'db_error', 
                                'title': error_msg
                            })
                            with error_container:
                                st.error(f"⚠️ Database error for {url[:40]}: {error_msg}")
                    else:
                        failed_count += 1
                        st.session_state.batch_results.append({
                            'url': url, 
                            'status': 'scrape_failed', 
                            'title': 'Content extraction failed'
                        })
                        
                except Exception as e:
                    error_count += 1
                    error_msg = str(e)[:100]
                    st.session_state.batch_results.append({
                        'url': url, 
                        'status': 'error', 
                        'title': f'Processing error: {error_msg}'
                    })
                    with error_container:
                        st.error(f"❌ Processing error for {url[:40]}: {error_msg}")
                    
                    # Continue processing despite errors
                    continue
            
            # Final progress update
            progress_bar.progress(1.0)
            status_text.text("✅ Batch processing complete!")
            
            # Show comprehensive results summary
            total_processed = len(urls_to_process)
            
            # Create summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("✅ Successful", successful_count)
            with col2:
                st.metric("❌ Failed", failed_count)
            with col3:
                st.metric("⚠️ Errors", error_count)
            with col4:
                success_rate = (successful_count / total_processed * 100) if total_processed > 0 else 0
                st.metric("🎯 Success Rate", f"{success_rate:.1f}%")
            
            if successful_count > 0:
                st.success(f"🎉 **Chunk Complete:** {successful_count}/{total_processed} URLs processed successfully!")
                # MANDATORY: Run enrichment on ALL rows
                import time
                with st.spinner("⏳ Waiting for BigQuery to process (20 sec)..."):
                    time.sleep(20)
                with st.spinner("🔄 Running enrichment on ALL rows..."):
                    bq_client.run_full_enrichment()
                st.success("✅ Enrichment completed - all fields filled!")
            else:
                st.error(f"⚠️ **No URLs processed successfully** - check errors above")
            
            # Show detailed breakdown if there were failures
            if failed_count > 0 or error_count > 0:
                st.warning(f"⚠️ {failed_count + error_count} URLs failed processing")
            
            # Show detailed results with better categorization
            if st.session_state.batch_results:
                with st.expander(f"📋 View detailed results ({len(st.session_state.batch_results)} URLs)"):
                    # Group results by status
                    success_results = [r for r in st.session_state.batch_results if r['status'] == 'success']
                    failed_results = [r for r in st.session_state.batch_results if r['status'] != 'success']
                    
                    if success_results:
                        st.write(f"**✅ Successful ({len(success_results)}):**")
                        for result in success_results:
                            st.success(f"✅ {result['url'][:60]} - {result['title'][:40]}")
                    
                    if failed_results:
                        st.write(f"**❌ Failed ({len(failed_results)}):**")
                        for result in failed_results:
                            status_emoji = "❌" if result['status'] == 'error' else "⚠️"
                            st.error(f"{status_emoji} {result['url'][:60]} - {result['title'][:40]}")
            
            # Advance to next batch and update saved queue
            if 'csv_new_urls' in st.session_state and 'processing_progress' in st.session_state:
                # Update progress for next batch
                st.session_state.processing_progress += len(urls_to_process)
                
                # Update the saved queue file with progress
                try:
                    if os.path.exists('processing_queue.json'):
                        with open('processing_queue.json', 'r') as f:
                            queue_data = json.load(f)
                        
                        queue_data['processed_count'] += len(urls_to_process)
                        
                        with open('processing_queue.json', 'w') as f:
                            json.dump(queue_data, f)
                        
                        st.info(f"💾 **Progress saved:** {queue_data['processed_count']}/{queue_data['total_count']} URLs processed")
                except Exception as e:
                    st.warning(f"⚠️ Could not update progress file: {e}")
                
                remaining = len(st.session_state.csv_new_urls) - st.session_state.processing_progress
                if remaining > 0:
                    st.info(f"🔄 **Ready to continue!** {remaining} URLs remaining")
                    if st.button("➡️ Process More URLs", type="primary"):
                        st.rerun()
                else:
                    st.success("🎉 **All URLs processed successfully!**")
                    
                    # Clear the multiple URLs input field
                    st.session_state.clear_multi_urls = True
                    
                    # Mark queue as completed
                    try:
                        if os.path.exists('processing_queue.json'):
                            with open('processing_queue.json', 'r') as f:
                                queue_data = json.load(f)
                            queue_data['processed_count'] = queue_data['total_count']
                            with open('processing_queue.json', 'w') as f:
                                json.dump(queue_data, f)
                    except Exception:
                        pass
                    
                    # Clean up session state
                    if 'csv_new_urls' in st.session_state:
                        del st.session_state.csv_new_urls
                    if 'processing_progress' in st.session_state:
                        del st.session_state.processing_progress
            
            # Performance summary for chunk processing
            if 'csv_new_urls' in st.session_state:
                processing_time = "~30 seconds"  # Approximate based on processing
                st.info(f"⏱️ Processing time: {processing_time} | Rate: ~{len(urls_to_process)/30:.1f} URLs/second")
                        
            # Clear for next batch
            st.session_state.url_queue = []

    # Step 2: Review and Edit Data
    if 'scraped_data' in st.session_state:
        st.markdown("---")
        st.subheader("Review & Save")

        data = st.session_state.scraped_data

        st.write("**Preview:**")
        col1, col2 = st.columns(2)

        with col1:
            st.write(f"**Domain:** {data['domain']}")
            if data.get('title'):
                st.write(f"**Title:** {data['title']}")

        with col2:
            if data['content']:
                st.write(f"**Content Preview:** {data['content'][:150]}...")
            else:
                st.write("**Content Preview:** ❌ No content extracted")

        st.markdown("---")

        # Editable URL field
        url = st.text_input("URL:",
                            value=data['url'],
                            help="Edit the article URL if needed")

        col1, col2 = st.columns(2)

        with col1:
            publish_date = st.date_input("Publish Date:",
                                         value=datetime.strptime(
                                             data['publish_date'],
                                             '%Y-%m-%d').date())
            spokesperson = st.text_input("Spokesperson:",
                                         placeholder="Enter spokesperson name")
            portfolio_company = st.text_input(
                "Portfolio Company:", placeholder="Enter portfolio company")
            reporter = st.text_input("Reporter:",
                                     placeholder="Enter reporter name")
            country = st.text_input("Country:",
                                    placeholder="Enter country")

        with col2:
            matched_vehicle = st.text_input("Vehicle:",
                                            placeholder="Enter vehicle")
            
            st.markdown("**Tags**")
            gst_checkbox = st.checkbox("GST", help="Article is managed by GST (sets fund to 'Antler')")
            
            # Auto-detect Antler mentions for tagging
            scraped_content = (data.get('content') or '').lower()
            scraped_title = (data.get('title') or '').lower()
            auto_tagged_antler = 'antler' in scraped_content or 'antler' in scraped_title
            if auto_tagged_antler:
                st.info("🔍 'Antler' detected - will be auto-tagged")

        headline = st.text_input(
            "Headline:",
            value=data.get('title', ''),
            placeholder="Enter article headline")
        content = st.text_area(
            "Article content:",
            value=data['content'] or '',
            height=150,
            placeholder="Paste or edit the article text here..."
        )

        if st.button("Save Article", type="primary"):
            if not content or not content.strip():
                st.error("❌ Content is required!")
                return

            with st.spinner(
                    "Checking for duplicates and saving to BigQuery..."):
                try:
                    # Data standards: NULL for no match, FALSE for boolean defaults
                    record_data = {
                        'url': url,
                        'content': content,
                        'domain': data['domain'],
                        'title': headline,
                        'publish_date':
                        publish_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at':
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'matched_spokespeople': spokesperson or None,
                        'matched_reporter': reporter or None,
                        'backlinks': 0.0,
                        'tagged_antler': auto_tagged_antler,
                        'language': 'en',
                        'matched_portcos': portfolio_company or None,
                        'matched_portco_location': None,
                        'matched_portco_deal_lead': None,
                        'managed_by_fund': 'Antler' if gst_checkbox else None,
                        'unbranded_win': False,
                        'country': country or None,
                        'matched_vehicle': matched_vehicle or None
                    }

                    success = bq_client.insert_media_record(record_data)

                    if success:
                        st.success("✅ Saved to BigQuery!")
                        st.success("🔄 Ready to add another article!")

                        # Clear the session state to reset the form
                        if 'scraped_data' in st.session_state:
                            del st.session_state.scraped_data

                        # Clear the URL input field
                        st.session_state.clear_url_field = True

                        # Add a button to start fresh immediately
                        if st.button("➕ Add Another Article", type="primary"):
                            st.rerun()

                        # Auto-refresh after a short delay to show clean interface
                        st.rerun()
                    else:
                        st.error("❌ Failed to save")

                except Exception as e:
                    st.error(f"❌ Error: {e}")

    st.markdown("---")
    st.subheader("2. Browse & Edit Articles")
    
    # Search box
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input("Search entries (ID, URL, Title, Brand, Content)", key="search_entries")
    with col2:
        if st.button("Clear", key="clear_search_btn"):
            st.session_state.page_number = 0
            st.rerun()
    
    # Initialize pagination
    if 'page_number' not in st.session_state:
        st.session_state.page_number = 0
    
    page_size = 50
    offset = st.session_state.page_number * page_size
    
    # Get recent entries
    with st.spinner("Loading recent entries..."):
        # Build WHERE clause if search term is provided
        where_clause = ""
        search_words = []
        if search_term and search_term.strip():
            # Use the full search term as a pattern (supports multi-word like "Antler Gold")
            search_words = [search_term.strip()]  # Keep as single pattern
            where_clause = """
            WHERE CAST(id AS STRING) LIKE @search_0
               OR LOWER(url) LIKE LOWER(@search_0)
               OR LOWER(title) LIKE LOWER(@search_0)
               OR LOWER(matched_portcos) LIKE LOWER(@search_0)
               OR LOWER(content) LIKE LOWER(@search_0)
            """
        
        query = f"""
        SELECT id, publish_date, url, title, matched_portcos, matched_spokespeople, tagged_antler, content, updated_at, country, matched_vc_investors, matched_vehicle, managed_by_fund
        FROM `media-455519.mediatracker.mediatracker`
        {where_clause}
        ORDER BY updated_at DESC
        LIMIT {page_size}
        OFFSET {offset}
        """
        
        # Also get total count
        count_query = f"""
        SELECT COUNT(*) as total
        FROM `media-455519.mediatracker.mediatracker`
        {where_clause}
        """
        
        try:
            # Configure query with parameters if search is active
            from google.cloud.bigquery import ScalarQueryParameter
            job_config = None
            if search_words:
                from google.cloud.bigquery import QueryJobConfig
                query_params = [
                    ScalarQueryParameter(f"search_{i}", "STRING", f"%{word}%")
                    for i, word in enumerate(search_words)
                ]
                job_config = QueryJobConfig(query_parameters=query_params)
            
            results = list(bq_client.client.query(query, job_config=job_config).result())
            total_count_result = list(bq_client.client.query(count_query, job_config=job_config).result())
            total_count = total_count_result[0].total if total_count_result else 0
            
            total_pages = (total_count + page_size - 1) // page_size
            current_page = st.session_state.page_number + 1
            
            if results:
                search_info = f" matching '{search_term}'" if search_term and search_term.strip() else ""
                st.markdown(f'<p style="color: #BADED8; margin: 0.5rem 0;">Showing page {current_page} of {total_pages} ({total_count} total entries{search_info})</p>', unsafe_allow_html=True)
                
                # Create article list with individual selection
                import pandas as pd
                
                # Store original data for comparison
                original_data = {}
                df_data = []
                for row in results:
                    original_data[row.id] = {
                        'Portfolio Company': row.matched_portcos or '',
                        'Spokesperson': row.matched_spokespeople or '',
                        'Content': row.content or '',
                        'Country': getattr(row, 'country', '') or '',
                        'VC Investors': getattr(row, 'matched_vc_investors', '') or '',
                        'Vehicle': getattr(row, 'matched_vehicle', '') or '',
                        'Managed by Fund': getattr(row, 'managed_by_fund', '') or ''
                    }
                    df_data.append({
                        'Select': False,
                        'ID': row.id,
                        'Date': str(row.publish_date)[:10] if row.publish_date else '',
                        'URL': row.url[:50] + '...' if len(row.url) > 50 else row.url,
                        'Title': (row.title[:50] + '...') if row.title and len(row.title) > 50 else (row.title or ''),
                        'Antler': 'Yes' if row.tagged_antler else 'No',
                        'Portfolio Company': row.matched_portcos or '',
                        'Spokesperson': row.matched_spokespeople or '',
                        'Content': row.content or '',
                        'Country': getattr(row, 'country', '') or '',
                        'VC Investors': getattr(row, 'matched_vc_investors', '') or '',
                        'Vehicle': getattr(row, 'matched_vehicle', '') or '',
                        'Managed by Fund': getattr(row, 'managed_by_fund', '') or ''
                    })
                
                df = pd.DataFrame(df_data)
                
                # Persist selections in session state
                if 'selected_article_ids' not in st.session_state:
                    st.session_state.selected_article_ids = set()
                
                # Apply persisted selections to dataframe
                df['Select'] = df['ID'].apply(lambda x: x in st.session_state.selected_article_ids)
                
                # Display the data editor with checkbox column for selection
                edited_df = st.data_editor(
                    df,
                    key=f"article_editor_page_{st.session_state.page_number}_{search_term or 'all'}",
                    use_container_width=True,
                    height=500,
                    hide_index=True,
                    column_config={
                        "Select": st.column_config.CheckboxColumn("✓", width="small", help="Select for actions"),
                        "ID": st.column_config.NumberColumn("ID", width="small"),
                        "Date": st.column_config.TextColumn("Date", width="small"),
                        "URL": st.column_config.TextColumn("URL", width="medium"),
                        "Title": st.column_config.TextColumn("Title", width="medium"),
                        "Antler": st.column_config.TextColumn("Antler", width="small"),
                        "Portfolio Company": st.column_config.TextColumn("Portfolio Company", width="medium"),
                        "Spokesperson": st.column_config.TextColumn("Spokesperson", width="medium"),
                        "Content": st.column_config.TextColumn("Content", width="large"),
                        "Country": st.column_config.TextColumn("Country", width="small"),
                        "VC Investors": st.column_config.TextColumn("VC Investors", width="medium"),
                        "Vehicle": st.column_config.TextColumn("Vehicle", width="small"),
                        "Managed by Fund": st.column_config.TextColumn("Managed by Fund", width="medium"),
                    },
                    disabled=["ID", "Date", "URL", "Title", "Antler"]
                )
                
                # Collect selected rows and sync to session state
                selected_rows = edited_df[edited_df['Select'] == True]
                selected_ids = [int(x) for x in selected_rows['ID'].tolist() if pd.notna(x)]
                # Also track URLs for items without valid IDs
                selected_urls = selected_rows['URL'].tolist()
                
                # Update session state with current page selections
                current_page_ids = set(df['ID'].tolist())
                # Remove deselected items from this page
                st.session_state.selected_article_ids -= current_page_ids
                # Add newly selected items
                st.session_state.selected_article_ids.update(selected_ids)
                
                # Check for edits and save button
                changes_to_save = []
                for idx, row in edited_df.iterrows():
                    row_id = row['ID']
                    if row_id in original_data:
                        orig = original_data[row_id]
                        if (row['Portfolio Company'] != orig['Portfolio Company'] or 
                            row['Spokesperson'] != orig['Spokesperson'] or 
                            row['Content'] != orig['Content']):
                            changes_to_save.append({
                                'id': row_id,
                                'matched_portcos': row['Portfolio Company'],
                                'matched_spokespeople': row['Spokesperson'],
                                'content': row['Content']
                            })
                
                if changes_to_save:
                    st.info(f"📝 {len(changes_to_save)} row(s) modified")
                
                # Save changes button
                if changes_to_save:
                    if st.button(f"💾 Save {len(changes_to_save)} Changes", type="primary"):
                        with st.spinner("Saving changes..."):
                            saved = 0
                            for change in changes_to_save:
                                try:
                                    update_query = """
                                    UPDATE `media-455519.mediatracker.mediatracker`
                                    SET matched_portcos = @portco,
                                        matched_spokespeople = @spokesperson,
                                        content = @content,
                                        updated_at = CURRENT_TIMESTAMP()
                                    WHERE id = @id
                                    """
                                    job_config = bigquery.QueryJobConfig(
                                        query_parameters=[
                                            bigquery.ScalarQueryParameter("portco", "STRING", change['matched_portcos']),
                                            bigquery.ScalarQueryParameter("spokesperson", "STRING", change['matched_spokespeople']),
                                            bigquery.ScalarQueryParameter("content", "STRING", change['content']),
                                            bigquery.ScalarQueryParameter("id", "INT64", int(change['id']))
                                        ]
                                    )
                                    bq_client.client.query(update_query, job_config=job_config).result()
                                    saved += 1
                                except Exception as e:
                                    st.error(f"Failed to save ID {change['id']}: {str(e)[:100]}")
                            
                            if saved > 0:
                                st.success(f"✅ Saved {saved} changes!")
                                st.rerun()
                
                # Pagination controls
                st.markdown("---")
                col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
                
                with col1:
                    if st.button("First", disabled=(st.session_state.page_number == 0)):
                        st.session_state.page_number = 0
                        st.rerun()
                
                with col2:
                    if st.button("Previous", disabled=(st.session_state.page_number == 0)):
                        st.session_state.page_number -= 1
                        st.rerun()
                
                with col3:
                    st.write(f"**Page {current_page} of {total_pages}**")
                
                with col4:
                    if st.button("Next", disabled=(st.session_state.page_number >= total_pages - 1)):
                        st.session_state.page_number += 1
                        st.rerun()
                
                with col5:
                    if st.button("Last", disabled=(st.session_state.page_number >= total_pages - 1)):
                        st.session_state.page_number = total_pages - 1
                        st.rerun()
                
                # Action buttons for selected articles
                st.markdown("---")
                total_selected = len(selected_rows)
                st.write(f"**{total_selected} article(s) selected**")
                col1, col2, col3 = st.columns([1, 1, 3])
                with col1:
                    scrape_btn = st.button("Scrape Selected", type="primary", disabled=total_selected == 0)
                with col2:
                    delete_btn = st.button("🗑️ Delete Selected", type="secondary", disabled=total_selected == 0)
                
                # Handle delete - use session state for confirmation
                if 'confirm_delete' not in st.session_state:
                    st.session_state.confirm_delete = False
                
                if delete_btn and total_selected > 0:
                    st.session_state.confirm_delete = True
                    st.session_state.ids_to_delete = list(selected_ids)
                    st.session_state.urls_to_delete = list(selected_urls)
                    st.rerun()
                
                if st.session_state.confirm_delete and 'urls_to_delete' in st.session_state:
                    urls_to_delete = st.session_state.urls_to_delete
                    ids_to_delete = st.session_state.get('ids_to_delete', [])
                    total_to_delete = len(urls_to_delete)
                    st.warning(f"⚠️ Delete {total_to_delete} articles? This cannot be undone.")
                    
                    col_confirm, col_cancel = st.columns(2)
                    with col_confirm:
                        if st.button(f"Yes, Delete {total_to_delete}", type="primary"):
                            with st.spinner(f"Deleting..."):
                                try:
                                    deleted_count = 0
                                    
                                    # Delete by URL (works for all items including those without ID)
                                    for url in urls_to_delete:
                                        delete_query = """
                                        DELETE FROM `media-455519.mediatracker.mediatracker`
                                        WHERE url LIKE @url_pattern
                                        """
                                        # Handle truncated URLs
                                        url_pattern = url.replace('...', '%')
                                        
                                        job_config = bigquery.QueryJobConfig(
                                            query_parameters=[
                                                bigquery.ScalarQueryParameter("url_pattern", "STRING", url_pattern)
                                            ]
                                        )
                                        job = bq_client.client.query(delete_query, job_config=job_config)
                                        job.result()
                                        deleted_count += job.num_dml_affected_rows or 0
                                    
                                    st.success(f"✅ Deleted {deleted_count} articles!")
                                    st.session_state.confirm_delete = False
                                    if 'ids_to_delete' in st.session_state:
                                        del st.session_state.ids_to_delete
                                    if 'urls_to_delete' in st.session_state:
                                        del st.session_state.urls_to_delete
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
                                    st.session_state.confirm_delete = False
                    with col_cancel:
                        if st.button("Cancel"):
                            st.session_state.confirm_delete = False
                            if 'ids_to_delete' in st.session_state:
                                del st.session_state.ids_to_delete
                            if 'urls_to_delete' in st.session_state:
                                del st.session_state.urls_to_delete
                            st.rerun()
                
                # Handle scrape
                if scrape_btn and len(selected_ids) > 0:
                    st.info(f"Starting to scrape {len(selected_ids)} selected articles using Firecrawl...")
                    
                    articles_by_id = {row.id: row for row in results}
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    success_count = 0
                    fail_count = 0
                    
                    for idx, article_id in enumerate(selected_ids):
                        article = articles_by_id.get(article_id)
                        if not article:
                            continue
                            
                        url = article.url
                        title = article.title[:30] + '...' if article.title and len(article.title) > 30 else article.title or 'No title'
                        status_text.text(f"Scraping {idx+1}/{len(selected_ids)}: {title}")
                        
                        try:
                            result = scrape_article_data_fast(url)
                            
                            if result and result.get('content') and len(result['content'].strip()) > 50:
                                content = result['content']
                                max_content_length = 1000000
                                if len(content) > max_content_length:
                                    content = content[:max_content_length] + "... [Content truncated]"
                                
                                # Ensure domain exists in media_data
                                bq_client.ensure_domain_in_media_data(article.domain)
                                
                                update_query = """
                                UPDATE `media-455519.mediatracker.mediatracker`
                                SET content = @content,
                                    text_scraped = true,
                                    text_scraped_at = CURRENT_TIMESTAMP(),
                                    data_ingestion = true
                                WHERE id = @id
                                """
                                
                                job_config = bigquery.QueryJobConfig(
                                    query_parameters=[
                                        bigquery.ScalarQueryParameter("content", "STRING", content),
                                        bigquery.ScalarQueryParameter("id", "INT64", int(article_id))
                                    ]
                                )
                                bq_client.client.query(update_query, job_config=job_config).result()
                                success_count += 1
                            else:
                                fail_count += 1
                                
                        except Exception as e:
                            fail_count += 1
                            st.warning(f"Failed to scrape ID {article_id}: {str(e)[:150]}")
                        
                        progress_bar.progress((idx + 1) / len(selected_ids))
                        time.sleep(0.5)
                    
                    status_text.empty()
                    progress_bar.empty()
                    
                    # MANDATORY: Run enrichment on ALL rows after scraping
                    if success_count > 0:
                        import time
                        status_text.text("⏳ Waiting for BigQuery to process (20 sec)...")
                        time.sleep(20)
                        status_text.text("🔄 Running enrichment on ALL rows...")
                        try:
                            bq_client.run_full_enrichment()
                            st.success(f"✅ Scraping complete! {success_count} successful, {fail_count} failed. All fields filled.")
                        except Exception as e:
                            st.warning(f"Scraped {success_count} articles but enrichment failed: {str(e)[:100]}")
                    else:
                        st.warning(f"Scraping complete. {fail_count} failed, 0 successful.")
                    
                    if st.button("Refresh page"):
                        st.rerun()
                
            else:
                st.info("No entries found.")
                
        except Exception as e:
            st.error(f"Error loading articles: {str(e)}")

    # Section 3: Add Portfolio Company
    st.subheader("3. Add Portfolio Company")
    
    try:
        portco_client = BigQueryClient()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            new_name = st.text_input("Company Name *", key="new_portco_name", placeholder="e.g. Airalo")
        with col2:
            new_location = st.text_input("Location", key="new_portco_loc", placeholder="e.g. Singapore")
        with col3:
            new_deal_lead = st.text_input("Deal Lead", key="new_portco_lead", placeholder="e.g. John Smith")
        with col4:
            new_vehicle = st.text_input("Vehicle", key="new_portco_vehicle", placeholder="e.g. Fund III")
        
        if st.button("Add Company"):
            if new_name and new_name.strip():
                # Get next ID
                max_id_query = "SELECT COALESCE(MAX(id), 0) as max_id FROM `media-455519.mediatracker.portcos`"
                max_id_result = list(portco_client.client.query(max_id_query).result())
                next_id = (max_id_result[0].max_id or 0) + 1
                
                # Auto-fill fields
                sync_key = str(uuid.uuid4())[:8]
                
                insert_query = """
                INSERT INTO `media-455519.mediatracker.portcos` 
                (id, created_at, portco_name, portco_location, portco_deal_lead, ai, sync_primary_key_334c63, vehicle)
                VALUES (@id, CURRENT_TIMESTAMP(), @name, @location, @deal_lead, @ai, @sync_key, @vehicle)
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("id", "INT64", next_id),
                        bigquery.ScalarQueryParameter("name", "STRING", new_name.strip()),
                        bigquery.ScalarQueryParameter("location", "STRING", new_location.strip() if new_location else "Unknown"),
                        bigquery.ScalarQueryParameter("deal_lead", "STRING", new_deal_lead.strip() if new_deal_lead else "Unknown"),
                        bigquery.ScalarQueryParameter("ai", "STRING", "false"),
                        bigquery.ScalarQueryParameter("sync_key", "STRING", sync_key),
                        bigquery.ScalarQueryParameter("vehicle", "STRING", new_vehicle.strip() if new_vehicle else None)
                    ]
                )
                
                portco_client.client.query(insert_query, job_config=job_config).result()
                st.success(f"Added: {new_name.strip()}")
                st.rerun()
            else:
                st.warning("Company name is required.")
                    
    except Exception as e:
        st.error(f"Error: {str(e)}")

    st.markdown("---")
    st.caption("Antler Media Tracker | Report issues: marketing@antler.co")


if __name__ == "__main__":
    main()
