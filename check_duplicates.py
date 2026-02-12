from bigquery_client import BigQueryClient
import pandas as pd

# Read the CSV file
csv_file = 'attached_assets/feed-antler-vc-24-10-2025-499263_1761313983769.csv'
print("="*80)
print("CHECKING FOR DUPLICATES")
print("="*80)

df = pd.read_csv(csv_file)
print(f"\n1. Total rows in CSV: {len(df)}")

# Find URL column case-insensitively
url_col = next((col for col in df.columns if col.lower() == 'url'), None)
if not url_col:
    print(f"❌ ERROR: No URL column found in CSV")
    print(f"Available columns: {', '.join(df.columns)}")
    exit(1)

print(f"2. Using column: '{url_col}' as URL column")

# Check for duplicates within the CSV file itself
csv_urls = df[url_col].tolist()
unique_csv_urls = set(csv_urls)
print(f"3. Unique URLs in CSV: {len(unique_csv_urls)}")
print(f"4. Duplicates within CSV: {len(csv_urls) - len(unique_csv_urls)}")

if len(csv_urls) != len(unique_csv_urls):
    # Find which URLs are duplicated
    from collections import Counter
    url_counts = Counter(csv_urls)
    duplicates = {url: count for url, count in url_counts.items() if count > 1}
    
    print(f"\n4. Found {len(duplicates)} URLs that appear multiple times:")
    for url, count in sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"   - {count}x: {url}")

# Now check against the database
print(f"\n{'='*80}")
print("CHECKING AGAINST DATABASE")
print('='*80)

client = BigQueryClient()

# Get all URLs from the database
query = f"""
SELECT url, COUNT(*) as count
FROM `{client.project_id}.{client.dataset_id}.{client.table_id}`
GROUP BY url
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 50
"""

print("\nChecking for duplicate URLs in database...")
results = list(client.client.query(query).result())

if results:
    print(f"\nFound {len(results)} URLs that exist multiple times in database:")
    for i, row in enumerate(results[:20], 1):
        print(f"{i}. {row.count}x: {row.url}")
else:
    print("\n✅ No duplicate URLs found in database")

# Check how many of the CSV URLs already exist in the database
print(f"\n{'='*80}")
print("CHECKING CSV URLS VS DATABASE")
print('='*80)

# Get unique URLs from CSV
unique_csv_urls_list = list(unique_csv_urls)
existing = client.check_existing_urls(unique_csv_urls_list)

print(f"\nUnique URLs in CSV: {len(unique_csv_urls_list)}")
print(f"Already in database: {len(existing)}")
print(f"New URLs: {len(unique_csv_urls_list) - len(existing)}")

if existing:
    print(f"\nFirst 20 URLs that already exist in database:")
    for i, url in enumerate(list(existing)[:20], 1):
        print(f"{i}. {url}")
