from bigquery_client import BigQueryClient
from datetime import datetime, timedelta
import pandas as pd

# Initialize client
client = BigQueryClient()

# Read the CSV file
csv_file = 'attached_assets/feed-antler-vc-24-10-2025-499263_1761313983769.csv'
print("="*80)
print("PROCESSING LARGE CSV FILE")
print("="*80)

df = pd.read_csv(csv_file)
print(f"\n1. Read {len(df)} rows from CSV")

# Find column names case-insensitively
url_col = next((col for col in df.columns if col.lower() == 'url'), None)
title_col = next((col for col in df.columns if col.lower() in ['headline', 'title']), None)
date_col = next((col for col in df.columns if col.lower() in ['publish date', 'date', 'publish_date']), None)
domain_col = next((col for col in df.columns if col.lower() in ['publication name', 'domain', 'publication']), None)
brand_col = next((col for col in df.columns if col.lower() in ['brand', 'content']), None)

if not url_col:
    print("❌ ERROR: No URL column found in CSV")
    print(f"Available columns: {', '.join(df.columns)}")
    exit(1)

print(f"📋 Column mapping:")
print(f"   url: {url_col}")
print(f"   title: {title_col}")
print(f"   date: {date_col}")
print(f"   domain: {domain_col}")
print(f"   brand: {brand_col}")

# Get current MAX ID
max_id_query = f"""
SELECT IFNULL(MAX(id), 0) as max_id
FROM `{client.project_id}.{client.dataset_id}.{client.table_id}`
"""
max_id_result = list(client.client.query(max_id_query).result())
next_id = max_id_result[0].max_id + 1

print(f"\n2. Starting ID assignment from: {next_id}")

# Check for duplicate URLs
all_urls = df[url_col].tolist()
print(f"3. Checking for duplicate URLs...")
existing_urls = client.check_existing_urls(all_urls)

print(f"4. Found {len(existing_urls)} duplicate URLs that will be skipped")

# Prepare rows for insertion
rows_to_insert = []
skipped_duplicates = []
errors = []

base_timestamp = datetime.now()

print(f"\n5. Processing rows...")
for idx, row in df.iterrows():
    try:
        # Extract values from CSV columns (case-insensitive)
        url_val = str(row.get(url_col, '')).strip() if url_col and pd.notna(row.get(url_col)) else ''
        title_val = str(row.get(title_col, '')).strip() if title_col and pd.notna(row.get(title_col)) else ''
        pub_date_val = str(row.get(date_col, '')).strip() if date_col and pd.notna(row.get(date_col)) else None
        domain_val = str(row.get(domain_col, '')).strip() if domain_col and pd.notna(row.get(domain_col)) else ''
        content_val = str(row.get(brand_col, '')).strip() if brand_col and pd.notna(row.get(brand_col)) else ''
        
        if not url_val:
            errors.append(f"Row {idx + 1}: Missing URL")
            continue
        
        # Skip if URL already exists in database
        if url_val in existing_urls:
            skipped_duplicates.append(url_val)
            continue
        
        # Parse publish date
        publish_date_dt = None
        if pub_date_val:
            try:
                from dateutil import parser
                publish_date_dt = parser.parse(pub_date_val)
            except:
                publish_date_dt = None
        
        # Stagger timestamps (1 second apart)
        row_timestamp = base_timestamp + timedelta(seconds=len(rows_to_insert))
        
        # Determine tagged_antler based on Brand field
        tagged_antler = 'antler' in content_val.lower() if content_val else False
        
        # Create row for BigQuery
        bq_row = {
            'id': next_id,
            'url': url_val,
            'title': title_val,
            'publish_date': publish_date_dt.isoformat() if publish_date_dt else None,
            'domain': domain_val,
            'content': None,  # Leave empty for deferred scraping
            'updated_at': row_timestamp.isoformat(),
            'data_ingestion': True,
            'tagged_antler': tagged_antler,
            'matched_portcos': content_val if content_val else None  # Brand goes here, not in content
        }
        
        rows_to_insert.append(bq_row)
        next_id += 1
        
        # Progress indicator every 100 rows
        if len(rows_to_insert) % 100 == 0:
            print(f"   Processed {len(rows_to_insert)} rows so far...")
        
    except Exception as e:
        errors.append(f"Row {idx + 1}: {str(e)}")

print(f"\n6. Summary:")
print(f"   - Ready to insert: {len(rows_to_insert)} rows")
print(f"   - Skipped (duplicates): {len(skipped_duplicates)} rows")
print(f"   - Errors: {len(errors)} rows")

if errors:
    print(f"\n   First 10 errors:")
    for err in errors[:10]:
        print(f"     - {err}")

# Insert to BigQuery in batches (BigQuery has a 10,000 row limit per request)
if rows_to_insert:
    print(f"\n7. Inserting {len(rows_to_insert)} rows to BigQuery...")
    
    batch_size = 5000  # Safe batch size
    total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(rows_to_insert))
        batch = rows_to_insert[start_idx:end_idx]
        
        print(f"   Inserting batch {batch_num + 1}/{total_batches} ({len(batch)} rows)...")
        
        table_ref = f"{client.project_id}.{client.dataset_id}.{client.table_id}"
        insert_errors = client.client.insert_rows_json(table_ref, batch)
        
        if insert_errors:
            print(f"   ❌ Some rows in batch {batch_num + 1} failed:")
            for err in insert_errors[:5]:
                print(f"      {err}")
        else:
            print(f"   ✅ Batch {batch_num + 1} inserted successfully")
    
    print(f"\n{'='*80}")
    print("COMPLETE!")
    print('='*80)
    print(f"\n✅ Successfully inserted {len(rows_to_insert)} new rows")
    print(f"⏭️  Skipped {len(skipped_duplicates)} duplicates")
    print(f"❌ {len(errors)} errors")
else:
    print(f"\n❌ No rows to insert!")
