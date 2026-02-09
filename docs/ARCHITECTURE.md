# RS Analytics - Architecture Reference

> Compact reference for AI assistants and developers. See `README.md` for setup, `data/DATABASE_DESIGN.md` for schema.

## Purpose

Multi-source marketing analytics pipeline: GA4, GSC, Google Ads, Meta Ads, Twitter → DuckDB → Streamlit dashboard.

## Directory Structure

```
rs_analytics/
├── app/                    # Streamlit UI
│   ├── main.py             # Entry point, routing, all dashboard renderers
│   └── components/         # Reusable UI components
│       ├── date_picker.py      # Calendar date range selector
│       ├── executive_dashboard.py  # Cross-platform summary
│       └── ga4_analytics.py    # GA4 BI dashboard (9 components)
├── etl/                    # Data extraction layer
│   ├── base.py             # BaseExtractor (abstract, use for new extractors)
│   ├── utils.py            # Shared: dates, paths, logging
│   ├── config.py           # GA4 config loader
│   ├── {platform}_config.py    # Platform-specific configs
│   └── {platform}_extractor.py # Platform-specific extractors
├── scripts/                # CLI tools
│   ├── run_etl_unified.py  # PRIMARY ETL runner (all sources)
│   ├── run_etl_{platform}.py   # Platform-specific ETL
│   ├── test_connections_unified.py # Test all connections
│   └── utils/              # Shared utilities
│       ├── cli.py          # Arg parsing, logging setup
│       ├── db.py           # DuckDB load/upsert functions
│       └── data_quality.py # Grain validation
├── data/                   # DuckDB warehouse + docs
├── secrets/                # Credentials (gitignored)
├── logs/                   # Runtime logs (gitignored)
└── deploy/                 # VPS deployment scripts
```

## Key Patterns

### ETL Pattern
```
Config → Extractor → DataFrame → DuckDB (upsert)
```
- Configs load from `.env` and validate credentials
- Extractors inherit from `BaseExtractor` (or should)
- Use `scripts/utils/db.py::upsert_to_duckdb()` for loading

### Dashboard Pattern
```
main.py routes → render_{page}() → load data via DuckDB → Plotly charts
```
- All pages in `main.py`, components in `app/components/`
- Date filtering via `date_picker.py::render_date_range_picker()`
- Data loaded with `@st.cache_data` decorator (5-min TTL)

### Database Pattern
```
Bronze (raw tables) → Silver (typed views *_v) → Gold (fact_* views)
```
- Run `python scripts/init_views.py` to create views
- See `data/DATABASE_DESIGN.md` for full schema

## Quick Commands

| Task | Command |
|------|---------|
| Run all ETL | `python scripts/run_etl_unified.py --source all --lookback-days 30` |
| Run GA4 comprehensive | `python scripts/run_etl_unified.py --source ga4 --comprehensive --lifetime` |
| Test connections | `python scripts/test_connections_unified.py --all` |
| Init DB views | `python scripts/init_views.py` |
| Start dashboard | `streamlit run app/main.py` |

## Adding New Features

### New Dashboard Page
1. Add render function in `app/main.py`
2. Add to navigation radio in `main()` function
3. Add routing `elif` block

### New Data Source
1. Create `etl/{source}_config.py` (copy pattern from existing)
2. Create `etl/{source}_extractor.py` inheriting `BaseExtractor`
3. Add to `scripts/run_etl_unified.py` source options
4. Add tables to `data/DATABASE_DESIGN.md`

### New Dashboard Component
1. Create in `app/components/{name}.py`
2. Export in `app/components/__init__.py`
3. Import and call from page renderer

## Code Conventions

- **Type hints** on all function signatures
- **Docstrings** explaining "why" not just "what"
- **Early returns** over nested conditionals
- **Explicit error handling** with user-friendly messages
- **SQL aggregations** in DuckDB, not Python loops
- **Absolute paths** for all file references in `.env`

## File Naming

| Type | Pattern | Example |
|------|---------|---------|
| Config | `{platform}_config.py` | `gads_config.py` |
| Extractor | `{platform}_extractor.py` | `gads_extractor.py` |
| ETL script | `run_etl_{platform}.py` | `run_etl_gads.py` |
| Test script | `test_{platform}_connection.py` | `test_gads_connection.py` |
| DB table | `{platform}_{entity}` | `gads_campaigns` |
| DB view | `{table}_v` | `gads_campaigns_v` |
| Fact view | `fact_{domain}_{grain}` | `fact_paid_daily` |

## Data Flow

```
GA4 API ────┐
GSC API ────┤
GAds API ───┼──→ Extractors ──→ DuckDB ──→ Views ──→ Streamlit
Meta API ───┤       ↓
Twitter ────┘   pandas.DataFrame
```

## Security Rules

- Never commit: `.env`, `secrets/`, `*.yaml`, `warehouse.duckdb`
- Use `GOOGLE_APPLICATION_CREDENTIALS` env var (absolute path)
- OAuth tokens in YAML files under `secrets/`
- No secrets in logs - only paths and status

## Table Counts (approximate)

| Source | Tables | Primary Tables |
|--------|--------|----------------|
| GA4 | 6 | `ga4_traffic_overview`, `ga4_page_performance` |
| GSC | 10 | `gsc_queries`, `gsc_pages` |
| Google Ads | 9 | `gads_campaigns`, `gads_keywords` |
| Meta | 10 | `meta_campaign_insights`, `meta_adset_insights` |
