#!/usr/bin/env python3
"""Check if user's URLs are actually duplicates"""

from bigquery_client import BigQueryClient

def main():
    bq_client = BigQueryClient()
    
    # Sample some URLs that user might have
    print("📊 Database stats:")
    
    main_table = f"{bq_client.project_id}.{bq_client.dataset_id}.{bq_client.table_id}"
    
    # Get total count
    count_query = f"SELECT COUNT(*) as total FROM `{main_table}`"
    result = list(bq_client.client.query(count_query).result())[0]
    print(f"  Total URLs in database: {result.total}")
    
    # Get domain breakdown
    domain_query = f"""
    SELECT domain, COUNT(*) as count
    FROM `{main_table}`
    GROUP BY domain
    ORDER BY count DESC
    LIMIT 10
    """
    
    print("\n  Top domains:")
    results = list(bq_client.client.query(domain_query).result())
    for row in results:
        print(f"    {row.domain}: {row.count}")
    
    print("\n💡 If your URLs are from these domains, they may be duplicates.")
    print("   Try checking URLs from a different domain or time period.")

if __name__ == "__main__":
    main()
