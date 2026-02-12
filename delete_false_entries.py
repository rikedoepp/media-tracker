from bigquery_client import BigQueryClient

client = BigQueryClient()

print("="*80)
print("DELETING FALSE ENTRIES")
print("="*80)

# Step 1: Delete NULL ID rows (82 rows from 7:29 AM)
print("\n1. Deleting NULL ID rows...")
null_delete_query = """
DELETE FROM `media-455519.mediatracker.mediatracker`
WHERE id IS NULL
"""

try:
    result = client.client.query(null_delete_query).result()
    print(f"   ✓ Successfully deleted NULL ID rows")
except Exception as e:
    print(f"   ✗ Error: {e}")

# Step 2: Delete old data ingestion test rows (IDs 63457-63476)
print("\n2. Deleting old test rows (IDs 63457-63476)...")
old_test_delete_query = """
DELETE FROM `media-455519.mediatracker.mediatracker`
WHERE id BETWEEN 63457 AND 63476
"""

try:
    result = client.client.query(old_test_delete_query).result()
    print(f"   ✓ Successfully deleted IDs 63457-63476")
except Exception as e:
    print(f"   ✗ Error: {e}")

# Step 3: Try to delete example.com test rows (may fail if in streaming buffer)
print("\n3. Trying to delete example.com test rows (IDs 63477-63479)...")
example_delete_query = """
DELETE FROM `media-455519.mediatracker.mediatracker`
WHERE domain = 'example.com'
"""

try:
    result = client.client.query(example_delete_query).result()
    print(f"   ✓ Successfully deleted example.com test rows")
except Exception as e:
    if "streaming buffer" in str(e).lower() or "UPDATE or DELETE" in str(e):
        print(f"   ⏳ Still in streaming buffer - need to wait ~60 more minutes")
        print(f"      Run this script again after 10:06 AM to delete these")
    else:
        print(f"   ✗ Error: {e}")

# Verify what's left
print("\n" + "="*80)
print("VERIFICATION")
print("="*80)

verify_query = """
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(id IS NULL) as null_ids,
    COUNTIF(domain = 'example.com') as example_rows
FROM `media-455519.mediatracker.mediatracker`
"""

try:
    results = client.client.query(verify_query).result()
    for row in results:
        print(f"\nTotal rows remaining: {row.total_rows}")
        print(f"NULL ID rows: {row.null_ids}")
        print(f"example.com rows: {row.example_rows}")
        
        if row.null_ids == 0 and row.example_rows == 0:
            print("\n✅ All false entries deleted successfully!")
        elif row.example_rows > 0:
            print(f"\n⏳ {row.example_rows} example.com rows still in streaming buffer")
            print("   Run this script again in ~60 minutes to complete cleanup")
except Exception as e:
    print(f"Error verifying: {e}")
