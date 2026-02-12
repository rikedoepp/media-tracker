#!/usr/bin/env python3
"""Read the Excel file with rows to delete"""

import pandas as pd

# Read the Excel file
df = pd.read_excel('attached_assets/delete_1761291511195.xlsx')

print(f"📄 Found {len(df)} rows to delete")
print(f"\n📋 Columns: {list(df.columns)}")
print(f"\n🔍 First 10 rows:")
print(df.head(10))

# Check if there's a URL column
url_cols = [col for col in df.columns if 'url' in col.lower() or 'URL' in col]
if url_cols:
    print(f"\n🔗 URLs to delete:")
    for i, url in enumerate(df[url_cols[0]].head(20), 1):
        print(f"  {i}. {url}")
