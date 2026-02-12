import streamlit as st
import pandas as pd
from datetime import datetime
from bigquery_client import BigQueryClient
from web_scraper import scrape_article_data, extract_domain_from_url
from validation import validate_url

def init_bigquery_client():
    """Initialize BigQuery client"""
    try:
        client = BigQueryClient()
        return client
    except Exception as e:
        st.error(f"Failed to initialize BigQuery client: {str(e)}")
        return None

def main():
    st.set_page_config(
        page_title="Smart Media Tracker",
        page_icon="📰",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    st.title("📰 Smart Media Tracker")
    st.markdown("Track media articles with automatic content extraction and BigQuery storage")
    
    # Initialize BigQuery client
    bq_client = init_bigquery_client()
    
    if bq_client is not None:
        st.success("✅ Connected to BigQuery")
    else:
        st.error("❌ BigQuery connection failed")
        st.stop()
    
    # URL Input Section
    st.subheader("🔗 Article URL")
    url_input = st.text_input(
        "Enter article URL:",
        placeholder="https://example.com/article",
        help="Paste the URL of the article you want to track"
    )
    
    if url_input and validate_url(url_input):
        if st.button("🔍 Extract Article Data"):
            with st.spinner("Extracting article content..."):
                article_data = scrape_article_data(url_input)
                
                if article_data and article_data.get('content'):
                    st.session_state.article_data = article_data
                    st.success("✅ Article content extracted successfully!")
                else:
                    st.error("❌ Could not extract article content. Please check the URL.")
    
    # Display extracted data and save option
    if hasattr(st.session_state, 'article_data'):
        article_data = st.session_state.article_data
        
        st.subheader("📄 Extracted Article Data")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("**URL:**", article_data.get('url', 'N/A'))
            st.write("**Domain:**", article_data.get('domain', 'N/A'))
        with col2:
            st.write("**Title:**", article_data.get('title', 'N/A')[:100] + "..." if len(str(article_data.get('title', ''))) > 100 else article_data.get('title', 'N/A'))
        
        st.write("**Content Preview:**")
        content = article_data.get('content', '')
        st.text_area("", value=content[:500] + "..." if len(content) > 500 else content, height=150, disabled=True)
        
        # Save to BigQuery
        if st.button("💾 Save to BigQuery", type="primary"):
            with st.spinner("Saving to BigQuery..."):
                try:
                    # Prepare record data
                    record_data = {
                        'url': article_data['url'],
                        'content': article_data['content'],
                        'domain': article_data['domain'],
                        'title': article_data.get('title', ''),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'publish_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    # Insert record
                    success = bq_client.insert_media_record(record_data)
                    
                    if success:
                        st.success("🎉 Article saved to BigQuery!")
                        
                        # Trigger stored procedure
                        bq_client.trigger_url_processing(article_data['url'])
                        
                        # Clear session state
                        if 'article_data' in st.session_state:
                            del st.session_state.article_data
                        st.rerun()
                    else:
                        st.error("❌ Failed to save to BigQuery")
                        
                except Exception as e:
                    st.error(f"❌ Error saving to BigQuery: {str(e)}")
    
    # Recent records section
    st.markdown("---")
    st.subheader("📊 Recent Records")
    
    if st.button("🔄 Refresh Records"):
        try:
            recent_records = bq_client.get_recent_records(limit=10)
            if recent_records:
                df = pd.DataFrame(recent_records)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No records found")
        except Exception as e:
            st.error(f"Error loading records: {str(e)}")

if __name__ == "__main__":
    main()