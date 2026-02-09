# rs_analytics

**Unified Marketing Analytics Pipeline** — GA4, GSC, Google Ads, Meta Ads → DuckDB → Streamlit Dashboard.

## Documentation

| Document | Purpose |
|----------|---------|
| **README.md** (this file) | Setup, credentials, ETL commands |
| **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** | Code patterns, conventions, extending the project |
| **[data/DATABASE_DESIGN.md](data/DATABASE_DESIGN.md)** | Complete database schema reference |

## Features

- **Multi-Source ETL**: GA4, Google Search Console, Google Ads, Meta Ads, Twitter
- **Local DuckDB Warehouse**: Fast local analytics with 35+ tables
- **Streamlit Dashboard**: Executive summary + per-platform dashboards
- **Production Ready**: Cron scheduling, VPS deployment scripts

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Credentials

See the detailed sections below for each data source:
- [GA4 Credentials](#ga4-google-analytics-4)
- [Search Console Credentials](#google-search-console-seo)
- [Google Ads Credentials](#google-ads-ppc)
- [Meta Ads Credentials](#meta-facebook-ads)

### 3. Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your actual values
# Use absolute paths for all file references
```

### 4. Test Your Connections

```bash
# Test all configured sources at once (recommended)
python scripts/test_connections_unified.py --all

# Or test individual sources
python scripts/test_connections_unified.py --source ga4
python scripts/test_connections_unified.py --source gads
python scripts/test_connections_unified.py --source gsc
python scripts/test_connections_unified.py --source meta
python scripts/test_connections_unified.py --source twitter
```

### 5. Run the ETL Pipelines

```bash
# RECOMMENDED: Use the unified ETL runner
python scripts/run_etl_unified.py --source all --lookback-days 30

# Or run specific sources
python scripts/run_etl_unified.py --source ga4 --lookback-days 30
python scripts/run_etl_unified.py --source gads --lifetime
python scripts/run_etl_unified.py --source gsc --start-date 2024-01-01
```

### 6. Launch the Dashboard

```bash
streamlit run app/main.py --server.port 3000
```

Open **http://localhost:3000** to view your analytics dashboard with 5 tabs:
- 📊 **GA4 Analytics** - Website traffic and user behavior
- 🔍 **Search Console (SEO)** - Organic search performance
- 💰 **Google Ads (PPC)** - Google paid advertising metrics
- 📘 **Meta Ads** - Facebook/Instagram advertising with MBA-level analytics
- ⚙️ **Settings** - Configuration and connection status

---

## Data Sources

### GA4 (Google Analytics 4)

**Authentication:** Service Account  
**Tables Created:** 6  
**Data Coverage:** Traffic, pages, geography, technology, events

#### Setup Steps

1. **Create Service Account** in [Google Cloud Console](https://console.cloud.google.com/)
2. **Download JSON key** and save to `secrets/ga4_service_account.json`
3. **Enable** the [Google Analytics Data API](https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com)
4. **Add service account** to GA4 property (Admin → Property Access Management)
5. **Configure** `.env`:
   ```env
   GA4_PROPERTY_ID=123456789
   GOOGLE_APPLICATION_CREDENTIALS=C:/path/to/secrets/ga4_service_account.json
   ```

#### ETL Commands

```bash
# Standard GA4 ETL (last 30 days)
python scripts/run_etl_unified.py --source ga4 --lookback-days 30

# Comprehensive GA4 ETL (all metrics, lifetime)
python scripts/run_etl_unified.py --source ga4 --comprehensive --lifetime

# Custom date range
python scripts/run_etl_unified.py --source ga4 --start-date 2024-01-01 --end-date 2024-12-31
```

---

### Google Search Console (SEO)

**Authentication:** Service Account  
**Tables Created:** 10  
**Data Coverage:** Queries, pages, countries, devices, daily totals

#### Setup Steps

1. **Create Service Account** (can reuse GA4 service account or create new)
2. **Download JSON key** and save to `secrets/gsc_service_account.json`
3. **Enable** the [Search Console API](https://console.cloud.google.com/apis/library/searchconsole.googleapis.com)
4. **Add service account** to Search Console (Settings → Users and permissions)
5. **Configure** `.env`:
   ```env
   # For domain property (recommended)
   GSC_SITE_URL=sc-domain:yourdomain.com
   
   # OR for URL prefix property
   GSC_SITE_URL=https://www.yourdomain.com/
   
   GOOGLE_SEARCH_CONSOLE_CREDENTIALS=C:/path/to/secrets/gsc_service_account.json
   ```

#### ETL Commands

```bash
# Full lifetime extraction
python scripts/run_etl_gsc.py --lifetime

# Last 90 days
python scripts/run_etl_gsc.py --lookback-days 90

# Custom date range
python scripts/run_etl_gsc.py --start-date 2024-06-01 --end-date 2024-12-31
```

---

### Google Ads (PPC)

**Authentication:** OAuth 2.0 (Desktop App)  
**Tables Created:** 9  
**Data Coverage:** Campaigns, ad groups, keywords, ads, devices, geographic, hourly, conversions

#### Setup Steps

1. **Apply for Google Ads API access** at [Google Ads API Center](https://ads.google.com/home/tools/manager-accounts/)
2. **Create OAuth 2.0 Client** in Google Cloud Console:
   - Go to APIs & Services → Credentials
   - Create OAuth 2.0 Client ID → **Desktop app**
   - Download client ID and client secret
3. **Create `secrets/google_ads.yaml`**:
   ```yaml
   developer_token: YOUR_DEVELOPER_TOKEN
   client_id: YOUR_OAUTH_CLIENT_ID
   client_secret: YOUR_OAUTH_CLIENT_SECRET
   refresh_token: YOUR_REFRESH_TOKEN
   login_customer_id: YOUR_MANAGER_ACCOUNT_ID
   use_proto_plus: True
   ```
4. **Generate Refresh Token**:
   ```bash
   python scripts/generate_gads_refresh_token.py
   ```
5. **Configure** `.env`:
   ```env
   GOOGLE_ADS_YAML_PATH=C:/path/to/secrets/google_ads.yaml
   # Use CLIENT account ID (not manager) for metrics
   GOOGLE_ADS_CUSTOMER_ID=1234567890
   ```

#### Finding Your Customer IDs

```bash
# List all accounts under your manager account
python scripts/list_gads_accounts.py
```

This will show:
- **Manager Account ID** (use for `login_customer_id` in YAML)
- **Client Account IDs** (use for `GOOGLE_ADS_CUSTOMER_ID` in .env)

#### ETL Commands

```bash
# Full lifetime extraction
python scripts/run_etl_gads.py --lifetime

# Last 30 days
python scripts/run_etl_gads.py --lookback-days 30

# Custom date range
python scripts/run_etl_gads.py --start-date 2024-01-01 --end-date 2024-12-31
```

---

### Meta (Facebook) Ads

**Authentication:** Access Token (OAuth 2.0)  
**Tables Created:** 10  
**Data Coverage:** Account metrics, campaigns, ad sets, ads, geographic, device, demographics  
**Data Limitation:** Meta API allows up to 37 months of historical data

#### Setup Steps

1. **Create Meta App** in [Meta for Developers](https://developers.facebook.com/)
2. **Add Marketing API** product to your app
3. **Generate Access Token** with required permissions:
   - `ads_read` - Read ad account data
   - `ads_management` - Manage ads (optional)
   - `business_management` - Access business accounts
4. **Configure** `.env`:
   ```env
   META_ACCESS_TOKEN=your_access_token_here
   META_AD_ACCOUNT_ID=act_XXXXXXXXXX
   # Optional: Additional accounts (comma-separated)
   META_ADDITIONAL_AD_ACCOUNTS=act_YYYYYYYYYY,act_ZZZZZZZZZZ
   ```

#### Finding Your Ad Account IDs

```bash
# Test connection and discover accounts
python scripts/test_meta_connection.py
```

This will show:
- Token validity and permissions
- All accessible ad accounts with IDs
- Account spending history

#### ETL Commands

```bash
# Full extraction (up to 37 months)
python scripts/run_etl_meta.py --lifetime

# Last 30 days
python scripts/run_etl_meta.py --lookback-days 30

# Custom date range
python scripts/run_etl_meta.py --start-date 2024-01-01 --end-date 2024-12-31
```

#### Dashboard Features

The Meta Ads dashboard includes MBA-level marketing analytics:

- **Executive KPIs**: Spend, impressions, reach, clicks, CTR, CPC, CPM
- **Conversion Metrics**: App installs, CPI, purchases, ROAS
- **Period Comparisons**: Automatic delta calculations vs previous period
- **Campaign Analysis**: Performance breakdown with time-series trends
- **Ad Set Analysis**: Targeting group effectiveness
- **Geographic Performance**: Country-level metrics and visualization
- **Device Analysis**: Platform and publisher breakdown
- **Demographics**: Age and gender performance matrix
- **Strategic Insights**: Automated MBA-style recommendations

---

## Project Structure

```
rs_analytics/
├── app/                    # Streamlit dashboard (main.py + components/)
├── etl/                    # Extractors and configs per platform
├── scripts/                # CLI tools (run_etl_*, test_*_connection.py)
├── data/                   # DuckDB warehouse + DATABASE_DESIGN.md
├── docs/                   # ARCHITECTURE.md (code patterns, conventions)
├── secrets/                # Credentials (gitignored)
└── deploy/                 # VPS deployment scripts
```

See **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for detailed structure and code conventions.

---

## Common Errors and Fixes

### GA4 Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "Missing GOOGLE_APPLICATION_CREDENTIALS" | Env var not set | Set absolute path in `.env` |
| "Service account file not found" | Wrong path | Verify file exists, use absolute path |
| "Permission denied (403)" | No GA4 access | Add service account to GA4 property |
| "API has not been used in project" | API not enabled | Enable Analytics Data API in GCP |

### Search Console Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "Configured site not found" | Wrong URL format | Use `sc-domain:` prefix for domain properties |
| "Permission denied" | No access | Add service account in Search Console settings |

### Google Ads Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "unauthorized_client" | Invalid refresh token | Run `generate_gads_refresh_token.py` |
| "Metrics cannot be requested for manager account" | Using MCA ID | Use client account ID in `.env` |
| "DEVELOPER_TOKEN_NOT_APPROVED" | Test token | Apply for basic or standard access |

### Meta Ads Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "Invalid OAuth access token" | Token expired | Generate new token in Meta Business Suite |
| "OAuthException code 190" | Token revoked | Re-authenticate and generate new token |
| "Permissions error" | Missing permissions | Ensure token has `ads_read` permission |
| "Start date beyond 37 months" | Date too old | Meta limits data to 37 months history |
| "Please reduce data amount" | Query too large | Use shorter date range or smaller time increment |

---

## Running in Production

### Option 1: System Cron (Linux/Mac)

```bash
# Edit crontab
crontab -e

# All sources with unified runner
0 6 * * * cd /path/to/rs_analytics && python scripts/run_etl_unified.py --source all >> logs/cron.log 2>&1

# Or individual sources
0 6 * * * cd /path/to/rs_analytics && python scripts/run_etl_unified.py --source ga4 >> logs/cron.log 2>&1
15 6 * * * cd /path/to/rs_analytics && python scripts/run_etl_unified.py --source gsc >> logs/cron.log 2>&1
30 6 * * * cd /path/to/rs_analytics && python scripts/run_etl_unified.py --source gads >> logs/cron.log 2>&1
45 6 * * * cd /path/to/rs_analytics && python scripts/run_etl_unified.py --source meta >> logs/cron.log 2>&1
```

### Option 2: Windows Task Scheduler

Create scheduled tasks for each ETL script with appropriate triggers (daily, specific times).

---

## Security Best Practices

1. **Never commit secrets**: `.env`, `secrets/`, and `*.yaml` are in `.gitignore`
2. **Restrict file permissions**: `chmod 600` on credential files
3. **Use absolute paths**: Prevents working directory issues
4. **Fail fast**: Invalid config stops execution with clear errors
5. **No secrets in logs**: Only paths and status are logged
6. **Separate credentials**: Use different service accounts per service when possible
7. **Rotate tokens**: Meta access tokens should be refreshed regularly

---

## Troubleshooting

If you encounter issues:

1. **Run connection tests** for the problematic service
2. **Check logs** in `logs/` directory
3. **Verify environment** variables in `.env`
4. **Check permissions** for service accounts
5. **Review API quotas** in Google Cloud Console or Meta Business Suite

For Streamlit dashboard issues:
- Configuration validated at startup
- Errors displayed with fix instructions
- Use Settings tab to test connections

---

## License

[Add your license here]
