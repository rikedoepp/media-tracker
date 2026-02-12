from bigquery_client import BigQueryClient

client = BigQueryClient()

# Get the last 18 URLs
query = """
SELECT id, url, title, matched_spokespeople
FROM `media-455519.mediatracker.mediatracker`
ORDER BY id DESC
LIMIT 18
"""

print("="*80)
print("UPDATING LAST 18 URLs WITH SPOKESPERSON")
print("="*80)

results = client.client.query(query).result()

urls_to_update = []
print("\nURLs to update:")
for i, row in enumerate(results, 1):
    print(f"{i}. ID {row.id}: {row.url}")
    print(f"   Current spokesperson: {row.matched_spokespeople}")
    urls_to_update.append(row.id)

# Update the spokesperson
update_query = f"""
UPDATE `media-455519.mediatracker.mediatracker`
SET matched_spokespeople = 'Tobias Bengtsdahl'
WHERE id IN ({','.join(map(str, urls_to_update))})
"""

print(f"\n{'='*80}")
print("Updating spokesperson to 'Tobias Bengtsdahl'...")
print('='*80)

try:
    result = client.client.query(update_query).result()
    print(f"\n✅ Successfully updated {len(urls_to_update)} URLs")
    
    # Verify the update
    verify_query = f"""
    SELECT id, url, matched_spokespeople
    FROM `media-455519.mediatracker.mediatracker`
    WHERE id IN ({','.join(map(str, urls_to_update))})
    ORDER BY id DESC
    """
    
    verify_results = client.client.query(verify_query).result()
    
    print("\nVerification:")
    for i, row in enumerate(verify_results, 1):
        print(f"{i}. ID {row.id}: {row.matched_spokespeople}")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    if "streaming buffer" in str(e).lower():
        print("\nThese URLs are still in the streaming buffer.")
        print("Wait 90 minutes after insertion, then run this script again.")
