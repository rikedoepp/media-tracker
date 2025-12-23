import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import streamlit as st

class BigQueryClient:
    def __init__(self):
        self.project_id = "media-455519"
        self.dataset_id = "mediatracker"
        self.table_id = "mediatracker"
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.client = self._get_client()

    def _get_client(self):
        # Try file-based credentials first (most reliable)
        try:
            with open('attached_assets/media-455519-e05e80608e53.json', 'r') as f:
                credentials_info = json.load(f)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            client = bigquery.Client(credentials=credentials, project=self.project_id)
            client.query("SELECT 1").result()
            return client
        except Exception as file_error:
            # Fallback to environment variable
            try:
                credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
                if credentials_json:
                    credentials_info = json.loads(credentials_json)
                    credentials = service_account.Credentials.from_service_account_info(credentials_info)
                    client = bigquery.Client(credentials=credentials, project=self.project_id)
                    client.query("SELECT 1").result()
                    return client
            except Exception:
                pass
            
            # Last resort: try credentials path
            try:
                credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
                if credentials_path:
                    credentials = service_account.Credentials.from_service_account_file(credentials_path)
                    client = bigquery.Client(credentials=credentials, project=self.project_id)
                    client.query("SELECT 1").result()
                    return client
            except Exception:
                pass
            
            raise Exception(f"BigQuery authentication failed. File error: {str(file_error)}")

    def check_url_exists(self, url):
        """Check if URL already exists in the database"""
        try:
            query = f"""
            SELECT COUNT(*) as count 
            FROM `{self.full_table_id}` 
            WHERE url = @url
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("url", "STRING", url),
                ]
            )
            
            job = self.client.query(query, job_config=job_config)
            result = job.result()
            
            for row in result:
                return row.count > 0
                
            return False
        except Exception as e:
            st.error(f"Error checking for duplicate URL: {str(e)}")
            return False

    def insert_media_record(self, record_data):
        try:
            # First normalize and check if URL already exists
            url = record_data.get('url', '')
            normalized_url = self.normalize_url(url)
            
            if self.check_url_exists(normalized_url):
                st.warning(f"⚠️ URL already exists in database: {normalized_url}")
                st.info("Skipping insertion to prevent duplicate records.")
                return False
            
            # Update record_data with normalized URL
            record_data['url'] = normalized_url
            st.success("✅ No duplicate found - proceeding with insertion")
            
            prepared_data = self._prepare_record(record_data)

            query = f"""
            INSERT INTO `{self.full_table_id}`
            (url, content, domain, title, publish_date, updated_at, matched_spokespeople, matched_reporter, matched_portcos, tagged_antler, managed_by_fund, unbranded_win)
            VALUES (
                @url, @content, @domain, @title, @publish_date, @updated_at, @matched_spokespeople, @matched_reporter, @matched_portcos, @tagged_antler, @managed_by_fund, @unbranded_win
            )
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("url", "STRING", prepared_data["url"]),
                    bigquery.ScalarQueryParameter("content", "STRING", prepared_data["content"]),
                    bigquery.ScalarQueryParameter("domain", "STRING", prepared_data["domain"]),
                    bigquery.ScalarQueryParameter("title", "STRING", prepared_data["title"]),
                    bigquery.ScalarQueryParameter("publish_date", "TIMESTAMP", prepared_data["publish_date"]),
                    bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", prepared_data["updated_at"]),
                    bigquery.ScalarQueryParameter("matched_spokespeople", "STRING", prepared_data["matched_spokespeople"]),
                    bigquery.ScalarQueryParameter("matched_reporter", "STRING", prepared_data["matched_reporter"]),
                    bigquery.ScalarQueryParameter("matched_portcos", "STRING", prepared_data["matched_portcos"]),
                    bigquery.ScalarQueryParameter("tagged_antler", "BOOL", prepared_data["tagged_antler"]),
                    bigquery.ScalarQueryParameter("managed_by_fund", "STRING", prepared_data["managed_by_fund"]),
                    bigquery.ScalarQueryParameter("unbranded_win", "BOOL", prepared_data["unbranded_win"]),
                ]
            )

            job = self.client.query(query, job_config=job_config)
            job.result()

            st.success("✅ Record successfully inserted into BigQuery!")

            # Pass the full record data to the procedure
            self.trigger_url_processing(record_data)

            return True

        except Exception as e:
            st.error(f"Error inserting record: {str(e)}")
            return False

    def trigger_url_processing(self, record_data):
        import time
        try:
            st.info("🔄 Processing new URL...")
            
            url = record_data.get('url', '')
            domain = record_data.get('domain', '')
            
            # Wait for streaming buffer to settle (back to original approach)
            st.info("⏳ Waiting 15 seconds for BigQuery streaming buffer...")
            time.sleep(15)
            
            # Call procedure without parameters (original way)
            query = "CALL `media-455519.mediatracker.process_new_url`()"
            
            st.info("⏳ Executing procedure call...")
            job = self.client.query(query)
            job.result()

            st.success(f"✅ URL processing procedure completed successfully!")
            st.success(f"🎯 Processed URL from domain: {domain}")
            st.balloons()  # Celebration - procedure worked!
            return True
        except Exception as e:
            st.error(f"🚨 PROCEDURE CALL FAILED!")
            st.error(f"Error details: {str(e)}")
            st.warning(f"⚠️ Record was saved but URL processing failed")
            st.error("Procedure call attempted: `process_new_url()` (no parameters)")
            return False

    def call_process_backlog_bulk(self):
        """Call the process_backlog_bulk procedure after data ingestion"""
        try:
            st.info("📝 Data successfully saved to BigQuery!")
            st.warning("⏳ **Note:** BigQuery streaming buffer needs time to settle (up to a few minutes).")
            st.info("💡 The `process_backlog_bulk` procedure should be run later to fill in missing columns.")
            st.info("You can run it manually in BigQuery or wait and use the 'Selective Content Scraping' feature.")
            return True
        except Exception as e:
            st.error(f"⚠️ Error: {str(e)}")
            return False

    def _prepare_record(self, record_data):
        publish_date_str = str(record_data['publish_date']).strip()
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return {
            'url': record_data.get('url', ''),
            'content': record_data.get('content', ''),
            'domain': record_data.get('domain', ''),
            'title': record_data.get('title', ''),
            'publish_date': publish_date_str,
            'updated_at': current_timestamp,
            'matched_spokespeople': record_data.get('matched_spokespeople', ''),
            'matched_reporter': record_data.get('matched_reporter', ''),
            'matched_portcos': record_data.get('matched_portcos', ''),
            'tagged_antler': record_data.get('tagged_antler', False),
            'managed_by_fund': record_data.get('managed_by_fund', ''),
            'unbranded_win': record_data.get('unbranded_win', False)
        }

    def get_recent_records(self, limit=5):
        try:
            query = f"""
            SELECT 
                id,
                updated_at,
                publish_date,
                url,
                domain,
                title,
                matched_spokespeople,
                matched_reporter,
                tagged_antler,
                language
            FROM `{self.full_table_id}`
            ORDER BY updated_at DESC
            LIMIT {limit}
            """

            query_job = self.client.query(query)
            results = query_job.result()

            records = []
            for row in results:
                records.append({
                    'ID': row.id,
                    'Updated': str(row.updated_at),
                    'Publish Date': str(row.publish_date),
                    'URL': row.url,
                    'Domain': row.domain,
                    'Title': (row.title[:50] + "..." if row.title and len(row.title) > 50 else row.title) if row.title else "No title",
                    'Spokespeople': row.matched_spokespeople,
                    'Reporter': row.matched_reporter,
                    'Tagged Antler': "Yes" if row.tagged_antler else "No",
                    'Language': row.language
                })

            return records
        except Exception as e:
            st.warning(f"Could not fetch recent records: {str(e)}")
            return []

    def check_recent_antler_tagging(self, limit=10):
        """Check recent entries to see Antler tagging status"""
        try:
            query = f"""
            SELECT 
                id,
                url,
                title,
                tagged_antler,
                matched_spokespeople,
                matched_portcos,
                updated_at
            FROM `{self.full_table_id}`
            ORDER BY updated_at DESC
            LIMIT {limit}
            """

            query_job = self.client.query(query)
            results = query_job.result()

            records = []
            for row in results:
                records.append({
                    'URL': row.url[:60] + "..." if row.url and len(row.url) > 60 else row.url,
                    'ID': row.id,
                    'Title': row.title[:40] + "..." if row.title and len(row.title) > 40 else row.title,
                    'Tagged Antler': "✅ TRUE" if row.tagged_antler else "❌ FALSE",
                    'Matched Spokespeople': row.matched_spokespeople[:30] + "..." if row.matched_spokespeople and len(row.matched_spokespeople) > 30 else row.matched_spokespeople or "None",
                    'Matched Portcos': row.matched_portcos[:30] + "..." if row.matched_portcos and len(row.matched_portcos) > 30 else row.matched_portcos or "None"
                })

            return records
        except Exception as e:
            st.error(f"Error checking recent Antler tagging: {str(e)}")
            return []

    def check_processing_logs(self, limit=20):
        """Check recent processing logs to debug procedure execution"""
        try:
            query = f"""
            SELECT 
                run_id,
                url,
                step,
                severity,
                message,
                context,
                ts
            FROM `media-455519.mediatracker.processing_logs`
            ORDER BY ts DESC
            LIMIT {limit}
            """

            query_job = self.client.query(query)
            results = query_job.result()

            records = []
            for row in results:
                records.append({
                    'Run ID': str(row.run_id)[:8] + "..." if row.run_id else "N/A",
                    'URL': row.url[:50] + "..." if row.url and len(row.url) > 50 else row.url or "N/A",
                    'Step': row.step,
                    'Severity': row.severity,
                    'Message': row.message,
                    'Context': str(row.context)[:100] + "..." if row.context and len(str(row.context)) > 100 else str(row.context) or "",
                    'Timestamp': str(row.ts)
                })

            return records
        except Exception as e:
            st.error(f"Error checking processing logs: {str(e)}")
            return []

    def normalize_url(self, url):
        """Normalize URL for consistent comparison"""
        if not url:
            return url
            
        # Convert to string and strip whitespace
        url = str(url).strip()
        
        # Remove leading slash if present
        if url.startswith('/') and len(url) > 1:
            url = url[1:]
        
        # Add https:// if missing protocol
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        # Remove trailing slash for consistency
        if url.endswith('/') and url.count('/') > 2:  # Don't remove from just "https://"
            url = url.rstrip('/')
        
        # Remove www. prefix to treat www and non-www as same URL
        if '://www.' in url:
            url = url.replace('://www.', '://')
            
        return url

    def check_existing_urls(self, urls_list):
        """Check which URLs already exist in the database with smart normalization"""
        try:
            if not urls_list:
                return []
            
            # Normalize input URLs using Python (more reliable than complex SQL)
            normalized_input_urls = [self.normalize_url(url) for url in urls_list]
            
            # Use faster batch checking approach
            existing_urls = []
            
            # Process URLs in smaller batches to avoid timeouts
            batch_size = 100
            for i in range(0, len(normalized_input_urls), batch_size):
                batch_urls = normalized_input_urls[i:i+batch_size]
                original_batch = urls_list[i:i+batch_size]
                
                # Check this batch against database using parameterized query
                placeholders = ', '.join([f"@url_{idx}" for idx in range(len(batch_urls))])
                query = f"""
                SELECT DISTINCT url
                FROM `{self.full_table_id}`
                WHERE url IN ({placeholders})
                """
                
                # Create query parameters
                query_parameters = [
                    bigquery.ScalarQueryParameter(f"url_{idx}", "STRING", url)
                    for idx, url in enumerate(batch_urls)
                ]
                
                job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
                query_job = self.client.query(query, job_config=job_config)
                results = query_job.result()
                
                # Get existing URLs from this batch
                existing_in_batch = set(row.url for row in results)
                
                # Map back to original URLs
                for j, normalized_url in enumerate(batch_urls):
                    if normalized_url in existing_in_batch:
                        existing_urls.append(original_batch[j])
            
            return existing_urls
            
        except Exception as e:
            st.error(f"Error checking existing URLs: {str(e)}")
            return []

    def delete_urls(self, urls_list):
        """Delete URLs from the database"""
        try:
            if not urls_list:
                return 0, []
            
            # Normalize URLs for consistent matching
            normalized_urls = [self.normalize_url(url) for url in urls_list]
            
            deleted_count = 0
            errors = []
            
            # Process URLs in batches to avoid query limits
            batch_size = 50
            for i in range(0, len(normalized_urls), batch_size):
                batch_urls = normalized_urls[i:i+batch_size]
                
                # Create parameterized delete query
                placeholders = ', '.join([f"@url_{idx}" for idx in range(len(batch_urls))])
                query = f"""
                DELETE FROM `{self.full_table_id}`
                WHERE url IN ({placeholders})
                """
                
                # Create query parameters
                query_parameters = [
                    bigquery.ScalarQueryParameter(f"url_{idx}", "STRING", url)
                    for idx, url in enumerate(batch_urls)
                ]
                
                try:
                    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
                    query_job = self.client.query(query, job_config=job_config)
                    result = query_job.result()
                    
                    # Get number of deleted rows
                    deleted_count += query_job.num_dml_affected_rows or 0
                except Exception as batch_error:
                    errors.append(f"Batch {i//batch_size + 1}: {str(batch_error)}")
            
            return deleted_count, errors
            
        except Exception as e:
            return 0, [str(e)]

    def check_normalize_errors(self, limit=10):
        """Check specifically for NORMALIZE step errors"""
        try:
            query = f"""
            SELECT 
                run_id,
                url,
                step,
                severity,
                message,
                context,
                ts
            FROM `media-455519.mediatracker.processing_logs`
            WHERE step = 'NORMALIZE' AND severity = 'ERROR'
            ORDER BY ts DESC
            LIMIT {limit}
            """

            query_job = self.client.query(query)
            results = query_job.result()

            records = []
            for row in results:
                records.append({
                    'Run ID': str(row.run_id)[:8] + "..." if row.run_id else "N/A",
                    'URL': row.url[:60] + "..." if row.url and len(row.url) > 60 else row.url or "N/A",
                    'Message': row.message,
                    'Context': str(row.context) if row.context else "No context",
                    'Timestamp': str(row.ts)
                })

            return records
        except Exception as e:
            st.error(f"Error checking NORMALIZE errors: {str(e)}")
            return []

    def test_connection(self):
        try:
            table = self.client.get_table(self.full_table_id)
            return True, f"Successfully connected to table: {table.table_id}"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    def get_articles_needing_reprocessing(self, limit=20):
        """Find articles that need reprocessing (missing required fields)"""
        try:
            query = f"""
            SELECT 
                url,
                title,
                domain,
                publish_date,
                updated_at,
                id,
                tagged_antler,
                language,
                tier,
                kill_pill,
                kill_pill_count,
                antler_in_headline,
                unwanted_count,
                cleaned_url,
                unwanted
            FROM `{self.full_table_id}`
            WHERE id IS NULL 
               OR tagged_antler IS NULL 
               OR language IS NULL 
               OR tier IS NULL
               OR kill_pill IS NULL
               OR kill_pill_count IS NULL
               OR antler_in_headline IS NULL
               OR unwanted_count IS NULL
               OR cleaned_url IS NULL
               OR unwanted IS NULL
               OR language = ''
               OR tier = ''
               OR cleaned_url = ''
            ORDER BY updated_at DESC
            LIMIT {limit}
            """

            query_job = self.client.query(query)
            results = query_job.result()

            records = []
            for row in results:
                # Convert ID to string to avoid pandas conversion issues
                id_display = str(row.id) if row.id is not None else "Missing"
                
                records.append({
                    'URL': row.url,
                    'Title': (row.title[:40] + "..." if row.title and len(row.title) > 40 else row.title) if row.title else "No title",
                    'Domain': row.domain,
                    'ID': id_display,
                    'Tagged Antler': "✅" if row.tagged_antler else "❌",
                    'Language': row.language if row.language else "❌",
                    'Tier': row.tier if row.tier else "❌",
                    'Kill Pill': "✅" if row.kill_pill else "❌",
                    'Antler in Headline': "✅" if row.antler_in_headline else "❌",
                    'Cleaned URL': "✅" if row.cleaned_url else "❌"
                })

            return records
        except Exception as e:
            st.error(f"Error finding articles needing reprocessing: {str(e)}")
            return []

    def add_urls_to_processing_queue(self, urls_list, batch_name=None):
        """Add URLs to processing queue for automatic background processing"""
        try:
            from datetime import datetime
            import uuid
            
            # Create processing queue table if it doesn't exist
            queue_table = f"{self.project_id}.{self.dataset_id}.processing_queue"
            
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{queue_table}` (
                id STRING,
                url STRING,
                status STRING,  -- 'pending', 'processing', 'completed', 'failed'
                batch_name STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                error_message STRING,
                retry_count INT64
            )
            """
            
            self.client.query(create_table_query).result()
            
            # Generate batch name if not provided
            if not batch_name:
                batch_name = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Normalize URLs and check for duplicates before adding to queue
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            rows_to_insert = []
            normalized_urls_seen = set()
            skipped_duplicates = 0
            skipped_antler = 0
            
            for url in urls_list:
                normalized_url = self.normalize_url(url)
                
                # Skip antler.co URLs
                if 'antler.co' in normalized_url.lower():
                    skipped_antler += 1
                    continue
                
                # Skip if we've already seen this normalized URL in this batch
                if normalized_url in normalized_urls_seen:
                    skipped_duplicates += 1
                    continue
                    
                # Check if this normalized URL already exists in database or queue
                existing_in_db = self.check_url_exists(normalized_url)
                existing_in_queue_query = f"""
                SELECT COUNT(*) as count 
                FROM `{queue_table}` 
                WHERE url = @url
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("url", "STRING", normalized_url)]
                )
                existing_in_queue = list(self.client.query(existing_in_queue_query, job_config=job_config).result())[0].count > 0
                
                if existing_in_db or existing_in_queue:
                    skipped_duplicates += 1
                    continue
                
                normalized_urls_seen.add(normalized_url)
                rows_to_insert.append({
                    'id': str(uuid.uuid4()),
                    'url': normalized_url,  # Store normalized URL
                    'status': 'pending',
                    'batch_name': batch_name,
                    'created_at': current_time,
                    'updated_at': current_time,
                    'error_message': None,
                    'retry_count': 0
                })
            
            # Insert all rows at once if there are any
            if not rows_to_insert:
                # All URLs were filtered out
                return True, batch_name, {
                    'added': 0,
                    'skipped_duplicates': skipped_duplicates,
                    'skipped_antler': skipped_antler,
                    'total_input': len(urls_list)
                }
            
            table = self.client.get_table(queue_table)
            errors = self.client.insert_rows_json(table, rows_to_insert)
            
            if not errors:
                return True, batch_name, {
                    'added': len(rows_to_insert),
                    'skipped_duplicates': skipped_duplicates,
                    'skipped_antler': skipped_antler,
                    'total_input': len(urls_list)
                }
            else:
                return False, None, {
                    'error': str(errors),
                    'skipped_duplicates': skipped_duplicates,
                    'skipped_antler': skipped_antler,
                    'total_input': len(urls_list)
                }
                
        except Exception as e:
            return False, None, {'error': str(e)}
    
    def get_processing_queue_status(self, batch_name=None):
        """Get status of processing queue"""
        try:
            queue_table = f"{self.project_id}.{self.dataset_id}.processing_queue"
            
            # Check if table exists first
            try:
                self.client.get_table(queue_table)
            except Exception:
                # Table doesn't exist yet - return empty status
                return {}
            
            if batch_name:
                where_clause = f"WHERE batch_name = '{batch_name}'"
            else:
                where_clause = ""
            
            query = f"""
            SELECT 
                status,
                COUNT(*) as count,
                batch_name
            FROM `{queue_table}`
            {where_clause}
            GROUP BY status, batch_name
            ORDER BY batch_name DESC
            """
            
            results = list(self.client.query(query).result())
            
            status_summary = {}
            for row in results:
                batch = row.batch_name
                if batch not in status_summary:
                    status_summary[batch] = {'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0}
                status_summary[batch][row.status] = row.count
                
            return status_summary
            
        except Exception as e:
            # Silently return empty status - table likely doesn't exist yet
            return {}
    
    def get_recent_scraping_activity(self, limit=10):
        """Get recently scraped/updated articles from BigQuery"""
        try:
            query = f"""
            SELECT 
                id,
                url,
                title,
                domain,
                updated_at,
                CASE 
                    WHEN content IS NOT NULL AND LENGTH(content) > 0 THEN 'Full Scrape'
                    ELSE 'Quick Scrape'
                END as scrape_type
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE updated_at IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT {limit}
            """
            
            results = list(self.client.query(query).result())
            
            activity = []
            for row in results:
                activity.append({
                    'id': row.id,
                    'url': row.url,
                    'title': row.title or 'No title',
                    'domain': row.domain,
                    'updated_at': row.updated_at,
                    'scrape_type': row.scrape_type
                })
            
            return activity
            
        except Exception as e:
            return []
    
    def get_pending_scrapes(self, limit=20):
        """Get URLs pending scraping from processing queue"""
        try:
            queue_table = f"{self.project_id}.{self.dataset_id}.processing_queue"
            
            # Check if table exists
            try:
                self.client.get_table(queue_table)
            except Exception:
                return []
            
            query = f"""
            SELECT url, status, created_at, batch_name
            FROM `{queue_table}`
            WHERE status IN ('pending', 'processing')
            ORDER BY created_at DESC
            LIMIT {limit}
            """
            
            results = list(self.client.query(query).result())
            
            pending = []
            for row in results:
                pending.append({
                    'url': row.url,
                    'status': row.status,
                    'created_at': row.created_at,
                    'batch_name': row.batch_name
                })
            
            return pending
            
        except Exception as e:
            return []
    
    def get_unscraped_articles(self, limit=500):
        """Get articles without content (unscraped)"""
        try:
            query = f"""
            SELECT id, url, title, domain
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE content IS NULL OR TRIM(content) = ''
            ORDER BY updated_at DESC
            LIMIT {limit}
            """
            
            results = list(self.client.query(query).result())
            
            articles = []
            for row in results:
                articles.append({
                    'id': row.id,
                    'url': row.url,
                    'title': row.title or 'No title',
                    'domain': row.domain
                })
            
            return articles
            
        except Exception as e:
            return []
    
    def light_scrape_article(self, article_id, url):
        """Light scrape an article and update its content"""
        try:
            from web_scraper import scrape_light
            from datetime import datetime
            
            data = scrape_light(url)
            
            if data and data.get('content'):
                update_query = """
                UPDATE `media-455519.mediatracker.mediatracker`
                SET content = @content,
                    title = COALESCE(NULLIF(@title, ''), title),
                    updated_at = CURRENT_TIMESTAMP()
                WHERE id = @id
                """
                
                from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
                job_config = QueryJobConfig(
                    query_parameters=[
                        ScalarQueryParameter("content", "STRING", data.get('content', '')),
                        ScalarQueryParameter("title", "STRING", data.get('title', '')),
                        ScalarQueryParameter("id", "INT64", int(article_id))
                    ]
                )
                
                self.client.query(update_query, job_config=job_config).result()
                return True, data.get('content', '')[:50]
            else:
                return False, "No content extracted"
                
        except Exception as e:
            return False, str(e)
    
    def process_next_url_from_queue(self):
        """Process one URL from the queue automatically"""
        try:
            queue_table = f"{self.project_id}.{self.dataset_id}.processing_queue"
            
            # Get next pending URL
            query = f"""
            SELECT id, url
            FROM `{queue_table}`
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            """
            
            results = list(self.client.query(query).result())
            
            if not results:
                return None  # No pending URLs
            
            url_record = results[0]
            url_id = url_record.id
            url = url_record.url
            
            # Mark as processing
            update_query = f"""
            UPDATE `{queue_table}`
            SET status = 'processing', updated_at = CURRENT_TIMESTAMP()
            WHERE id = '{url_id}'
            """
            self.client.query(update_query).result()
            
            # Process the URL - FAST MODE: metadata only, no full text
            from web_scraper import scrape_metadata_only
            from datetime import datetime
            
            try:
                data = scrape_metadata_only(url)
                
                if data:
                    # Save to main table with metadata only (no content yet)
                    record_data = {
                        'url': data['url'],
                        'content': '',  # Empty content - will scrape later
                        'domain': data['domain'],
                        'title': data['title'],
                        'publish_date': data.get('publish_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'matched_spokespeople': '',
                        'matched_reporter': '',
                        'backlinks': 0.0,
                        'tagged_antler': False,
                        'language': 'en',
                        'matched_portcos': '',
                        'matched_portco_location': '',
                        'matched_portco_deal_lead': '',
                        'managed_by_fund': '',
                        'unbranded_win': False,
                        'text_scraped': False  # Mark as not scraped yet
                    }
                    
                    success = self.insert_media_record(record_data)
                    
                    if success:
                        # Mark as completed in queue
                        complete_query = f"""
                        UPDATE `{queue_table}`
                        SET status = 'completed', updated_at = CURRENT_TIMESTAMP()
                        WHERE id = '{url_id}'
                        """
                        self.client.query(complete_query).result()
                        return {'status': 'completed', 'url': url, 'title': data.get('title', 'N/A')}
                    else:
                        # Mark as failed - likely duplicate
                        fail_query = f"""
                        UPDATE `{queue_table}`
                        SET status = 'failed', error_message = 'Database insertion failed (likely duplicate)', updated_at = CURRENT_TIMESTAMP()
                        WHERE id = '{url_id}'
                        """
                        self.client.query(fail_query).result()
                        return {'status': 'failed', 'url': url, 'error': 'Database insertion failed'}
                else:
                    # Mark as failed - metadata extraction failed
                    fail_query = f"""
                    UPDATE `{queue_table}`
                    SET status = 'failed', error_message = 'Metadata extraction failed', updated_at = CURRENT_TIMESTAMP()
                    WHERE id = '{url_id}'
                    """
                    self.client.query(fail_query).result()
                    return {'status': 'failed', 'url': url, 'error': 'Metadata extraction failed'}
                    
            except Exception as processing_error:
                # Mark as failed with error
                error_msg = str(processing_error)[:500]
                fail_query = f"""
                UPDATE `{queue_table}`
                SET status = 'failed', error_message = @error_msg, updated_at = CURRENT_TIMESTAMP()
                WHERE id = @url_id
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("error_msg", "STRING", error_msg),
                        bigquery.ScalarQueryParameter("url_id", "STRING", url_id),
                    ]
                )
                
                self.client.query(fail_query, job_config=job_config).result()
                return {'status': 'failed', 'url': url, 'error': error_msg}
                
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def get_urls_needing_text_scraping(self, limit=50):
        """Find entries that need text scraping (text_scraped=FALSE)"""
        try:
            query = f"""
            SELECT url, title, domain, publish_date
            FROM `{self.full_table_id}`
            WHERE text_scraped = FALSE
            ORDER BY updated_at ASC
            LIMIT {limit}
            """
            
            results = list(self.client.query(query).result())
            return [{'url': row.url, 'title': row.title, 'domain': row.domain} for row in results]
            
        except Exception as e:
            return []
    
    def scrape_text_batch(self, batch_size=30):
        """Scrape full text content for URLs that don't have it yet"""
        from web_scraper import scrape_article_data_fast
        from datetime import datetime
        
        try:
            # Get URLs needing text scraping
            urls_to_scrape = self.get_urls_needing_text_scraping(limit=batch_size)
            
            if not urls_to_scrape:
                return {'status': 'no_urls', 'message': 'No URLs need text scraping'}
            
            results = {
                'success': 0,
                'failed': 0,
                'total': len(urls_to_scrape),
                'details': []
            }
            
            for item in urls_to_scrape:
                url = item['url']
                
                try:
                    # Scrape full text content
                    data = scrape_article_data_fast(url)
                    
                    if data and data.get('content'):
                        # Update the record with full text content
                        update_query = f"""
                        UPDATE `{self.full_table_id}`
                        SET content = @content,
                            text_scraped = TRUE,
                            text_scraped_at = CURRENT_TIMESTAMP(),
                            text_scrape_error = NULL,
                            updated_at = CURRENT_TIMESTAMP()
                        WHERE url = @url
                        """
                        
                        job_config = bigquery.QueryJobConfig(
                            query_parameters=[
                                bigquery.ScalarQueryParameter("content", "STRING", data['content']),
                                bigquery.ScalarQueryParameter("url", "STRING", url),
                            ]
                        )
                        
                        self.client.query(update_query, job_config=job_config).result()
                        results['success'] += 1
                        results['details'].append({'url': url, 'status': 'success'})
                    else:
                        # Mark as error
                        error_query = f"""
                        UPDATE `{self.full_table_id}`
                        SET text_scrape_error = 'Content extraction failed',
                            updated_at = CURRENT_TIMESTAMP()
                        WHERE url = @url
                        """
                        
                        job_config = bigquery.QueryJobConfig(
                            query_parameters=[
                                bigquery.ScalarQueryParameter("url", "STRING", url),
                            ]
                        )
                        
                        self.client.query(error_query, job_config=job_config).result()
                        results['failed'] += 1
                        results['details'].append({'url': url, 'status': 'failed', 'error': 'No content extracted'})
                        
                except Exception as e:
                    # Mark as error
                    error_msg = str(e)[:500]
                    error_query = f"""
                    UPDATE `{self.full_table_id}`
                    SET text_scrape_error = @error_msg,
                        updated_at = CURRENT_TIMESTAMP()
                    WHERE url = @url
                    """
                    
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("error_msg", "STRING", error_msg),
                            bigquery.ScalarQueryParameter("url", "STRING", url),
                        ]
                    )
                    
                    self.client.query(error_query, job_config=job_config).result()
                    results['failed'] += 1
                    results['details'].append({'url': url, 'status': 'failed', 'error': error_msg})
            
            return results
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def reprocess_single_article(self, url):
        """Trigger reprocessing for a specific URL"""
        import time
        try:
            st.info(f"🔄 Reprocessing article: {url}")
            
            # Wait for any pending operations
            st.info("⏳ Waiting 5 seconds before reprocessing...")
            time.sleep(5)
            
            # Call procedure to reprocess all incomplete records
            query = "CALL `media-455519.mediatracker.process_new_url`()"
            
            st.info("⏳ Executing reprocessing procedure...")
            job = self.client.query(query)
            job.result()

            st.success(f"✅ Reprocessing completed for: {url}")
            st.balloons()
            return True
        except Exception as e:
            st.error(f"🚨 Reprocessing failed for {url}")
            st.error(f"Error details: {str(e)}")
            return False