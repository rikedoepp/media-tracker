#!/usr/bin/env python3

from bigquery_client import BigQueryClient

def check_recent_records():
    """Check recent records in the BigQuery table"""
    try:
        bq_client = BigQueryClient()
        print("✅ BigQuery client initialized")
        
        # Query recent records
        query = """
        SELECT url, title, publish_date, updated_at 
        FROM `media-455519.mediatracker.mediatracker` 
        ORDER BY updated_at DESC 
        LIMIT 10
        """
        
        job = bq_client.client.query(query)
        results = job.result()
        
        print("\n📋 Recent records in database:")
        print("-" * 80)
        
        count = 0
        for row in results:
            count += 1
            print(f"{count}. URL: {row.url}")
            print(f"   Title: {row.title}")
            print(f"   Updated: {row.updated_at}")
            print()
        
        if count == 0:
            print("No records found in the database")
        
        # Specifically check for the TechCrunch URL
        techcrunch_url = "https://techcrunch.com/2025/07/28/techcrunch-mobility-tesla-vs-gm-a-tale-of-two-earnings/"
        print(f"\n🔍 Checking specifically for TechCrunch URL:")
        print(f"URL: {techcrunch_url}")
        
        specific_query = """
        SELECT url, title, publish_date, updated_at 
        FROM `media-455519.mediatracker.mediatracker` 
        WHERE url = @url
        """
        
        from google.cloud import bigquery
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("url", "STRING", techcrunch_url),
            ]
        )
        
        job = bq_client.client.query(specific_query, job_config=job_config)
        results = job.result()
        
        found = False
        for row in results:
            found = True
            print(f"✅ FOUND: {row.title}")
            print(f"   Updated: {row.updated_at}")
        
        if not found:
            print("❌ TechCrunch URL NOT found in database")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    check_recent_records()