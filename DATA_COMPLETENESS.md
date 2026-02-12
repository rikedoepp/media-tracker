# Data Completeness Documentation

## 1. Required Fields (Must Be Filled for Article to be "Complete")

| Field | Current Fill Rate | Source | Filled By |
|-------|------------------|--------|-----------|
| `id` | 100% | Auto-generated | BigQuery on INSERT |
| `url` | 100% | User input | app.py (all input methods) |
| `title` | 100% | Web page | web_scraper.py (scrape functions) |
| `content` | 95% | Web page | web_scraper.py (scrape functions) |
| `domain` | 100% | URL parsing | web_scraper.py (extract_domain_from_url) |
| `publish_date` | 100% | Web page metadata | web_scraper.py (scrape functions) |
| `updated_at` | 100% | Auto-generated | BigQuery on INSERT |
| `country` | **60%** | Domain lookup | process_backlog_bulk procedure (from media_data table) |
| `page_rank` | 85% | Open PageRank API | bigquery_client.py (ensure_domain_in_media_data) |
| `tier` | 85% | Derived from page_rank | process_backlog_bulk procedure |
| `language` | 80% | Domain lookup | process_backlog_bulk procedure (from media_data table) |
| `tagged_antler` | 100% | Content matching | process_backlog_bulk procedure |
| `tagged_portco` | 85% | Content matching | process_backlog_bulk procedure |

**Current Status: 57% of articles have ALL required fields filled (9,603 / 16,770)**

---

## 2. Apps & Procedures That Fill Data

### Streamlit App (app.py)
**Location:** `/home/runner/workspace/app.py`

| Input Method | Fields Filled |
|--------------|---------------|
| Single URL (Quick Scrape) | url, title, domain, publish_date, content (snippets only) |
| Single URL (Full Scrape) | url, title, domain, publish_date, content (full) |
| Multiple URLs | Same as Single URL |
| Wizikey Data Ingestion | url, title, domain, publish_date, content (snippets), tagged_antler, matched_portcos |

### Web Scraper (web_scraper.py)
**Location:** `/home/runner/workspace/web_scraper.py`

| Function | Fields Filled |
|----------|---------------|
| `scrape_article_data_fast()` | title, content, domain, publish_date |
| `scrape_metadata_only()` | title, domain, publish_date |
| `scrape_light()` | title, domain, publish_date, content (key sentences only) |
| `extract_domain_from_url()` | domain |

### BigQuery Client (bigquery_client.py)
**Location:** `/home/runner/workspace/bigquery_client.py`

| Function | Fields Filled |
|----------|---------------|
| `ensure_domain_in_media_data()` | Adds domain to media_data table, fetches page_rank from Open PageRank API |
| `call_process_backlog_bulk()` | Calls the enrichment procedure |

### Stored Procedures (BigQuery)

#### `process_backlog_bulk`
**Purpose:** Main enrichment procedure - fills most derived fields
**Fields Filled:**
- country (from media_data lookup by domain)
- page_rank (from media_data lookup by domain)
- tier (derived from page_rank: 1-3=Tier1, 4-5=Tier2, 6+=Tier3)
- language (from media_data lookup by domain)
- tagged_antler (content search for "antler")
- tagged_portco (content search against portcos table)
- matched_portcos (portfolio company matching)
- matched_spokespeople (from spokespeople table)
- matched_vc_investors (from vc_investor_rankings table)
- matched_reporter (from reporters table)
- matched_portco_location (from portcos table)
- matched_vehicle (from portcos table)
- matched_dealroom_rank (from vc_investor_rankings table)
- antler_in_headline (title search for "antler")
- spokespeople_in_headline (title search against spokespeople table)
- unbranded_win (derived: portco mentioned but not Antler in headline)
- month (derived from publish_date)
- kill_pill / unwanted (content analysis)

#### `process_new_url`
**Purpose:** Enrichment for single URL (called after manual article insert)
**Fields Filled:** Same as process_backlog_bulk but for single article

#### `mark_complete`
**Purpose:** Updates is_complete flag based on field presence
**Fields Filled:** is_complete (TRUE/FALSE)

#### `process_backlog_urls`
**Purpose:** Legacy procedure for batch URL processing

### Fill All Cells Script (fill_all_cells.py)
**Location:** `/home/runner/workspace/fill_all_cells.py`

**Purpose:** Orchestrates the complete data filling process
1. Scrapes missing content
2. Runs enrichment procedure
3. Updates completeness status

### Bulk Scrape Script (run_bulk_scrape.py)
**Location:** `/home/runner/workspace/run_bulk_scrape.py`

**Purpose:** Background scraping for articles with `data_ingestion=TRUE`
**Fields Filled:** title, content, publish_date, scrape_date

---

## 3. Current Completeness Status (All Fields)

| Field | Filled | Missing | % | Type |
|-------|--------|---------|---|------|
| id | 16,770 | 0 | 100% | REQUIRED |
| url | 16,770 | 0 | 100% | REQUIRED |
| title | 16,761 | 9 | 100% | REQUIRED |
| content | 15,959 | 811 | 95% | REQUIRED |
| domain | 16,769 | 1 | 100% | REQUIRED |
| publish_date | 16,770 | 0 | 100% | REQUIRED |
| updated_at | 16,770 | 0 | 100% | REQUIRED |
| **country** | **10,041** | **6,729** | **60%** | REQUIRED |
| page_rank | 14,251 | 2,519 | 85% | REQUIRED |
| tier | 14,251 | 2,519 | 85% | REQUIRED |
| language | 13,469 | 3,301 | 80% | REQUIRED |
| tagged_antler | 16,770 | 0 | 100% | REQUIRED |
| tagged_portco | 14,250 | 2,520 | 85% | REQUIRED |
| matched_portcos | 8,466 | 8,304 | 50% | OPTIONAL |
| matched_spokespeople | 1,258 | 15,512 | 8% | OPTIONAL |
| matched_vc_investors | 2,135 | 14,635 | 13% | OPTIONAL |
| matched_reporter | 486 | 16,284 | 3% | OPTIONAL |
| matched_portco_location | 8,382 | 8,388 | 50% | OPTIONAL |
| matched_portco_deal_lead | 0 | 16,770 | 0% | OPTIONAL |
| matched_vehicle | 8,450 | 8,320 | 50% | OPTIONAL |
| matched_dealroom_rank | 2,084 | 14,686 | 12% | OPTIONAL |
| antler_in_headline | 14,248 | 2,522 | 85% | OPTIONAL |
| spokespeople_in_headline | 14,249 | 2,521 | 85% | OPTIONAL |
| unbranded_win | 14,252 | 2,518 | 85% | OPTIONAL |
| managed_by_fund | 313 | 16,457 | 2% | OPTIONAL |
| month | 14,248 | 2,522 | 85% | OPTIONAL |
| backlinks | 424 | 16,346 | 3% | OPTIONAL |
| social_shares_count | 0 | 16,770 | 0% | OPTIONAL |
| kill_pill | 14,249 | 2,521 | 85% | OPTIONAL |
| unwanted | 14,249 | 2,521 | 85% | OPTIONAL |
| summary | 1,537 | 15,233 | 9% | OPTIONAL |
| li_summary | 530 | 16,240 | 3% | OPTIONAL |

---

## 4. Priority Actions to Improve Completeness

### Highest Impact:
1. **Fix country field (60%)** - 6,729 articles missing
   - Issue: media_data table missing country for some domains
   - Solution: Update media_data table with country data for missing domains

2. **Run enrichment procedure** - Will fill page_rank, tier, language, tagged_portco
   - Blocked by: Streaming buffer (wait 90 min after bulk insert)
   - Command: `CALL media-455519.mediatracker.process_backlog_bulk()`

3. **Complete content scraping** - 811 articles missing content + 2,237 pending
   - Run: `python3 run_bulk_scrape.py`

### Reference Tables Used:
- `media_data` - domain -> country, language, page_rank
- `portcos` - portfolio company names, locations, deal leads, vehicles
- `spokespeople` - spokesperson names
- `reporters` - reporter names  
- `vc_investor_rankings` - VC investor names, dealroom ranks

---

## 5. Tracking Columns (Recently Added)

| Column | Type | Purpose |
|--------|------|---------|
| `ingestion_date` | TIMESTAMP | When article was first imported |
| `scrape_date` | TIMESTAMP | When full content was scraped |
| `is_complete` | BOOLEAN | TRUE when all required fields filled |
| `data_ingestion` | BOOLEAN | TRUE = needs full scraping |

---

*Last updated: January 2026*
