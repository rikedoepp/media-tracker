# Antler Media Tracker

## Overview

A Streamlit web application for tracking media coverage. Users can add article URLs, automatically extract content, and maintain a centralized database of media mentions in BigQuery. Features include bulk import, duplicate detection, selective scraping, and URL management.

## System Architecture

The application follows a simple three-tier architecture:
- **Frontend**: Streamlit web interface for user interaction
- **Processing Layer**: Python modules for web scraping, validation, and data processing
- **Data Storage**: Google BigQuery for persistent data storage

## Key Components

### 1. Frontend (Streamlit Apps)
- **app.py**: Main application interface with comprehensive features
- **app_simple.py**: Simplified version for basic functionality
- **app_fixed.py**: Enhanced version with better validation and error handling

The frontend provides:
- URL input and validation
- Real-time content extraction preview
- Form-based data entry for metadata
- Database connectivity status
- Success/error feedback
- **Selective scraping interface** (NEW): View and manually select articles for content scraping

### 2. Web Scraping Module (web_scraper.py)
- Uses `trafilatura` library for content extraction
- Extracts main text content from web pages
- Retrieves article titles from metadata
- Extracts domain names from URLs
- Handles scraping errors gracefully

### 3. BigQuery Client (bigquery_client.py)
- Manages Google Cloud BigQuery connections
- Handles multiple authentication methods:
  - Service account JSON from environment variables
  - Local service account file
  - Default Google Cloud credentials
- Provides database operations:
  - Record insertion
  - Duplicate URL checking
  - Connection validation

### 4. Validation Module (validation.py)
- URL format validation using urlparse
- Required field validation
- Email format validation
- Date format validation
- Text input sanitization

### 5. Testing Scripts
- **test_title_insertion.py**: Tests title field insertion
- **test_duplicate_check.py**: Tests duplicate URL detection
- **check_unbranded_column.py**: Schema validation utility

## Input Methods

The application supports four different input methods:

### 1. Single URL Input
- Manual entry of individual URLs
- Real-time scraping and preview
- Full content extraction

### 2. Multiple URLs
- Paste multiple URLs at once
- Batch processing
- Full content extraction

### 3. CSV Upload (Fast Processing)
- Upload CSV with URL column
- Quick metadata-only insertion
- Content scraping happens separately

### 4. Data Ingestion (NEW)
- Direct CSV-to-BigQuery insertion with custom column mapping
- **CSV Columns**: URL (or url), Headline, Publish Date, Publication Name, Brand
  - **Note**: Column names are case-insensitive (accepts both "URL" and "url")
- **Auto-populated fields**:
  - `id`: Sequential ID assignment (MAX+1)
  - `updated_at`: Current timestamp (staggered for batch processing)
  - `data_ingestion`: Set to `true` to mark rows from this method
  - `tagged_antler`: Automatically detected if "Antler" appears in Brand field
  - `matched_portcos`: Set to Brand value for portfolio company tracking
- **Workflow**: Save metadata fast, scrape content later
- **No procedure calls**: Enrichment deferred to allow efficient batch processing

## Data Flow

### Standard Flow (Single/Multiple URLs):
1. **User Input**: User enters article URL through Streamlit interface
2. **URL Validation**: System validates URL format and accessibility
3. **Content Extraction**: Web scraper fetches and extracts article content
4. **Data Processing**: System extracts metadata (title, domain, publish date)
5. **User Review**: User reviews extracted data and adds additional metadata
6. **Duplicate Check**: System checks if URL already exists in database
7. **Data Storage**: Record is inserted into BigQuery table
8. **Confirmation**: User receives success/failure feedback

### Data Ingestion Flow:
1. **CSV Upload**: User uploads CSV with required columns
2. **ID Assignment**: System queries MAX(id) and assigns sequential IDs
3. **Metadata Mapping**: CSV columns mapped to database fields
4. **Batch Insert**: All rows inserted with staggered timestamps
5. **Deferred Processing**: Content scraping and enrichment happen later

### Selective Scraping Flow (NEW):
1. **View Unscraped**: System displays all unscraped articles (up to 500) from database
2. **Manual Selection**: User reviews articles and checks boxes for desired items
3. **Batch Controls**: "Select All" and "Clear All" buttons for quick selection
4. **Scrape Selected**: User clicks button to scrape only checked articles
5. **Progress Tracking**: Real-time progress bar and status updates
6. **Results Display**: Success/failure counts shown after completion

## External Dependencies

### Core Libraries
- **streamlit**: Web application framework
- **trafilatura**: Web content extraction
- **google-cloud-bigquery**: Google BigQuery client
- **google-oauth2**: Authentication for Google services
- **pandas**: Data manipulation (in some versions)

### Google Cloud Services
- **BigQuery**: Primary data storage
  - Project ID: `media-455519`
  - Dataset: `mediatracker`
  - Table: `mediatracker`

## Deployment Strategy

The application is designed for deployment on Replit with the following considerations:

### Authentication Setup
- Service account credentials stored in `attached_assets/media-455519-e05e80608e53.json`
- Fallback to environment variable `GOOGLE_APPLICATION_CREDENTIALS_JSON`
- Multiple authentication paths for flexibility

### Configuration
- Hard-coded project configuration for simplicity
- No external configuration files required
- Environment-based credential management

### Dependencies Management
- All Python dependencies should be listed in requirements.txt
- Key dependencies: streamlit, trafilatura, google-cloud-bigquery, google-oauth2

## Changelog

- **December 18, 2025 (Latest)**:
  - Full UI/UX audit: cleaner labels, better space usage
  - Renamed "Data ingestion" to "Bulk Import" (clearer for users)
  - Simplified header - removed extra padding
  - Search field: cleaner placeholder, smaller clear button
  - Review section: simplified to "Review & Save" with shorter labels
  - Tag checkboxes: clearer help text
  - Fixed Portfolio Company field not saving "Antler" (since Antler is your company, not a portfolio company)
  - Multi-word search now works (e.g., "Antler Gold")
  - Delete works for articles without IDs (uses URL matching)
- **December 18, 2025**:
  - Renamed app to "Antler Media Tracker" with clearer section numbering
  - Simplified button labels ("Quick Save" / "Full Save")
  - Added URL deletion feature with confirmation
  - Cleaned up footer and removed unused imports
- **November 6, 2025**:
  - Added "Selective Content Scraping" UI feature allowing manual article selection
  - Users can now view all unscraped articles and choose which ones to scrape
  - Includes "Select All" and "Clear All" bulk selection buttons
  - Real-time selection counter and progress tracking during scraping
  - Fixed SQL injection vulnerability in batch URL checking (parameterized queries)
- **November 5, 2025**: 
  - Standardized URL column handling to be case-insensitive across all CSV processing
  - Code now accepts both "URL" and "url" as column names in uploaded CSVs
  - Deleted 2,021 duplicate URLs (1,849 + 172) from database, keeping rows with most complete data
  - All scripts updated: app.py, process_large_csv.py, check_duplicates.py
- **October 24, 2025**: Added Data ingestion feature with `data_ingestion` column for tracking CSV uploads. System now auto-assigns IDs, detects Antler brand, sets matched_portcos, and defers content scraping for efficient batch processing.
- **June 30, 2025**: Initial setup

## User Preferences

Preferred communication style: Simple, everyday language.

## Notes

- **GitHub Integration**: User declined the Replit GitHub connector. To sync with GitHub in the future, user can either set up the integration or provide a GitHub personal access token as a secret.