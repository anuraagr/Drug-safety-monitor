# Drug Safety Signal Monitor

**Microsoft Fabric Real-Time Intelligence — Pharmacovigilance Demo**

A polished, production-grade demonstration of adverse-event signal detection built entirely on Microsoft Fabric Real-Time Intelligence. Ingests real FDA FAERS data across a multi-drug portfolio, detects safety-signal spikes with KQL analytics, surfaces results in a **14-tile Signal Room dashboard**, and escalates critical findings via Activator alerts.

---

## Why This Demo?

| Differentiator | Detail |
|---|---|
| **Regulatory-critical workflow** | Targets *pharmacovigilance operations* — a core regulatory requirement for every pharma & biotech company |
| **Real FDA data** | openFDA FAERS is public, legitimate FDA data (not synthetic). Covers 2004–2026, updated quarterly |
| **Multi-drug portfolio view** | Pre-built profiles for cardiology, oncology, immunology, and mixed portfolios — or define your own |
| **End-to-end Fabric** | Notebook ingestion → spike detection → real-time dashboard → alert escalation — all in one workspace |
| **Live demo mode** | Replay events with color-coded console output for screen-sharing during customer presentations |
| **Composite risk scoring** | `RiskScoreCard()` function produces a weighted 0-100 risk score combining seriousness, mortality, velocity, and reaction breadth |
| **Head-to-head drug comparison** | `DrugComparisonReport()` function for side-by-side safety profile analysis during live Q&A |

---

## Architecture

```
                         ┌──────────────────────────────────────────────┐
                         │             openFDA FAERS API                │
                         │   https://api.fda.gov/drug/event.json       │
                         └───────────────┬──────────────────────────────┘
                                         │
                    ┌────────────────────┬┴─────────────────────┐
                    ▼                    ▼                      ▼
      Fabric Notebook ★        ingest_faers_stream.py    live_demo.py
      (primary method)         (stream → Eventstream)    (replay → console)
          │                          │
          ▼                          ▼
      Kusto SDK direct         Fabric Eventstream
      (built-in auth)          (Custom Endpoint)
          │                          │
          └────────────┬─────────────┘
                       ▼
               ┌──────────────────────────────┐
               │     Eventhouse (KQL DB)      │
               │     Drug-safety-signal-eh    │
               │                              │
               │  Table: AdverseEvents        │
               │  Views: RecentEvents_24h     │
               │         SeriousEvents_30Day  │
               │         ReactionCounts_7Day  │
               │         DrugSummary_30Day    │
               │  Functions:                  │
               │    DetectSafetySignals       │
               │    PatientDemographics       │
               │    DrugComparisonReport      │
               │    ReactionTimeline          │
               │    RiskScoreCard             │
               └──────────┬───────────────────┘
                          │
           ┌──────────────┼──────────────────┐
           ▼              ▼                  ▼
   Real-Time Dashboard   Activator        KQL Queryset
   (14-tile Signal Room) Alerts           (ad-hoc queries)
                         Teams / Email
```

---

## Project Structure

```
Drug-safety-monitor/
├── scripts/
│   ├── fetch_fda_portfolio.py    # Multi-drug openFDA fetcher (batch)
│   ├── live_demo.py              # Real-time replay for presentations
│   ├── load_to_eventhouse.py     # Direct Kusto SDK ingestion
│   ├── ingest_faers_stream.py    # Stream to Fabric Eventstream
│   ├── generate_demo_data.py     # Simple single-drug data generator
│   ├── deploy_and_validate.py    # Schema deployment validator
│   ├── setup_eventhouse.kql      # Full KQL schema (reference)
│   └── setup_steps/              # Individual KQL steps (STEP1–STEP12)
├── dashboards/
│   └── tiles/                    # 14-tile Signal Room dashboard
│       ├── TILE1_command_center.kql     # Stat cards (8 KPIs)
│       ├── TILE2_arrival_rate.kql       # Stacked area (24h)
│       ├── TILE3_signal_alerts.kql      # ⭐ Signal detection table
│       ├── TILE4_portfolio_heatmap.kql  # Drug risk-level table
│       ├── TILE5_top_reactions.kql      # Stacked bar (top 10)
│       ├── TILE6_geo_map.kql            # Country map
│       ├── TILE7_trend_by_drug.kql      # Multi-line chart (30d)
│       ├── TILE8_demographics.kql       # Sex distribution pie
│       ├── TILE9_case_drilldown.kql     # Scrollable case table
│       ├── TILE10_age_distribution.kql  # Age band bar chart
│       ├── TILE11_outcome_funnel.kql    # Severity cascade funnel
│       ├── TILE12_reporter_quality.kql  # Reporter qualification donut
│       ├── TILE13_drug_reaction_crosstab.kql  # Drug×Reaction heatmap
│       └── TILE14_signal_timeline.kql   # Annotated spike timeline
├── alerts/
│   └── activator_rule.json
├── docs/
│   ├── README.md                  # Detailed documentation
│   ├── DEMO_SCRIPT.md            # 10-minute presentation walkthrough
│   ├── DASHBOARD_LAYOUT.md       # 14-tile grid layout + presentation tips
│   └── config.json               # Customer parameterization
├── sample_data/                   # Generated datasets
└── requirements.txt
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| **Microsoft Fabric workspace** | Trial, F2, or higher capacity |
| **Python 3.9+** | Tested on 3.13 / 3.14 |
| **pip packages** | `pip install -r requirements.txt` |
| **openFDA API key** (optional) | Raises rate limit 40 → 240 req/min. Free at [open.fda.gov](https://open.fda.gov/apis/authentication/) |

---

## Quick Start (15 minutes)

### 1. Clone & install

```bash
git clone https://github.com/anuraagr/Drug-safety-monitor.git
cd Drug-safety-monitor
pip install -r requirements.txt
```

### 2. Fetch multi-drug portfolio data from FDA

```bash
# Fetch 2,000 events across 7 drugs with an aspirin spike for signal detection
python scripts/fetch_fda_portfolio.py --count 2000 --profile mixed --spike-drug aspirin

# Or pick a specific therapeutic area
python scripts/fetch_fda_portfolio.py --count 1500 --profile oncology --spike-drug pembrolizumab
```

Output: `sample_data/fda_portfolio_<profile>.ndjson` + summary report.

### 3. Create Fabric Eventhouse

1. Open [app.fabric.microsoft.com](https://app.fabric.microsoft.com)
2. **New → Eventhouse** → name: `Drug-safety-signal-eh`
3. Open the auto-created KQL database → **Query**

### 4. Deploy KQL schema

Run each file in `scripts/setup_steps/` sequentially (STEP1 → STEP12):
- STEP1: Create `AdverseEvents` table
- STEP2: JSON ingestion mapping
- STEP3a/b: Retention (90d) and caching (30d) policies
- STEP4–7: Four materialized views
- STEP8–10: Three stored functions (DetectSafetySignals, PatientDemographics)
- STEP11–12: Verification queries
- STEP13–15: DrugComparisonReport, ReactionTimeline, RiskScoreCard functions

### 5. Load data into Eventhouse

**Option A — Fabric Notebook (recommended, used in this demo):**
1. Upload `notebooks/fabric_data_ingestion.ipynb` to your Fabric workspace
2. Update the **Configuration** cell with your Eventhouse cluster URI and database name
3. Set `DATA_SOURCE = "openfda"` to fetch live FDA data (or `"lakehouse"` / `"synthetic"`)
4. **Run All Cells** — ingests data directly into the Eventhouse with progress tracking
5. The notebook handles authentication automatically via Fabric's built-in credentials

**Option B — Fabric portal upload:**
1. KQL database → **Get data** → **Local file**
2. Select `sample_data/fda_portfolio_mixed.ndjson`
3. Target table: `AdverseEvents`, mapping: `AdverseEvents_JsonMapping`

**Option C — Direct Kusto SDK (local):**
```bash
pip install azure-kusto-data azure-kusto-ingest azure-identity
python scripts/load_to_eventhouse.py \
    --file sample_data/fda_portfolio_mixed.ndjson \
    --cluster-uri "https://your-eventhouse.z0.kusto.fabric.microsoft.com" \
    --auth interactive
```

**Option D — Eventstream (real-time path):**
1. Create Eventstream → add Custom Endpoint source → copy connection string
2. Add Eventhouse destination → select `AdverseEvents` with JSON mapping
3. Update `docs/config.json` → `fabric_eventhub.connection_string`
4. Run: `python scripts/ingest_faers_stream.py --config docs/config.json --max-events 500`

### 6. Build the dashboard

1. **New → Real-Time Dashboard** → name: `Signal Room`
2. Connect data source to your KQL database
3. Add 14 tiles using queries from `dashboards/tiles/` (see `docs/DASHBOARD_LAYOUT.md` for grid placement):

| # | Tile | Visualization | Refresh |
|---|------|--------------|--------|
| 1 | Command Center | Stat cards (8 key metrics) | 30s |
| 2 | Arrival Rate | Stacked area chart | 30s |
| 3 | Signal Alerts ⭐ | Table w/ severity badges (🔴🟠🟡🔵) | 30s |
| 4 | Portfolio Heatmap | Risk-level table w/ data bars | 60s |
| 5 | Top Reactions | Stacked bar chart (top 10) | 60s |
| 6 | Geographic Map | Country map w/ drill-down | 60s |
| 7 | Trend by Drug | Multi-line chart (30d) | 60s |
| 8 | Sex Distribution | Pie chart | 120s |
| 9 | Case Drilldown | Scrollable case table (50 rows) | 30s |
| 10 | Age Distribution | Stacked bar (age bands + seriousness) | 120s |
| 11 | Outcome Funnel | Bar chart (Total→Serious→Hosp→Death) | 60s |
| 12 | Reporter Quality | Donut (Physician/Pharmacist/Consumer) | 120s |
| 13 | Drug×Reaction Cross-tab | Heatmap table (top 8 × top 8) | 60s |
| 14 | Signal Timeline | Annotated line (daily vs 7d avg + spikes) | 60s |

4. Set dashboard auto-refresh to **30 seconds**

### 7. Configure alerts (optional)

1. **New → Activator** in workspace
2. Follow instructions in `alerts/activator_rule.json`
3. Set Teams webhook URL and adjust spike threshold

---

## Drug Portfolio Profiles

| Profile | Drugs | Use Case |
|---------|-------|----------|
| **mixed** | aspirin, metformin, atorvastatin, omeprazole, lisinopril, sertraline, amoxicillin | General demo — high-volume drugs across classes |
| **cardiology** | aspirin, warfarin, atorvastatin, metoprolol, lisinopril | Cardiovascular safety monitoring |
| **oncology** | pembrolizumab, nivolumab, trastuzumab, lenalidomide, ibrutinib | Oncology immuno/targeted therapy |
| **immunology** | adalimumab, infliximab, etanercept, rituximab, tocilizumab | Autoimmune biologics |

### Custom drug list

```bash
python scripts/fetch_fda_portfolio.py --drugs nusinersen ivacaftor --count 500
```

### Spike injection

The `--spike-drug` flag clusters ~25% of that drug's events into a 48-hour window centered 36 hours ago — guaranteed to trigger `DetectSafetySignals()` during the demo.

---

## Script Reference

### `fetch_fda_portfolio.py` — Multi-Drug FDA Fetcher

```bash
python scripts/fetch_fda_portfolio.py \
    --count 2000         \
    --profile mixed      \
    --spike-drug aspirin \
    --api-key YOUR_KEY   \
    --output-dir ./data
```

**Outputs:** `fda_portfolio_<profile>.ndjson` + `_summary.txt`

### `live_demo.py` — Real-Time Demo Replay

```bash
# Color-coded console replay (for screen-sharing)
python scripts/live_demo.py --source sample_data/fda_portfolio_mixed.ndjson --speed 3

# With live publishing to Fabric Eventstream
python scripts/live_demo.py --source data.ndjson --speed 5 --publish --conn-str "Endpoint=sb://..."
```

Console colors: 🔴 Death → 🔴 Hospitalization → 🟡 Life-threatening → 🟢 Routine

### `load_to_eventhouse.py` — Direct Kusto Ingestion

```bash
python scripts/load_to_eventhouse.py \
    --file sample_data/fda_portfolio_mixed.ndjson \
    --cluster-uri "https://your-eventhouse.z0.kusto.fabric.microsoft.com" \
    --auth interactive    # or: cli, default
    --method streaming    # or: queued
```

### `ingest_faers_stream.py` — Eventstream Publisher

```bash
# Dry run (console only)
python scripts/ingest_faers_stream.py --config docs/config.json --dry-run --max-events 10

# Live to Eventstream
python scripts/ingest_faers_stream.py --config docs/config.json --max-events 500
```

---

## Dashboard Tiles

| Tile | Key Insight |
|------|------------|
| **Command Center** | 8 stat cards: total events, serious, deaths, hospitalizations, life-threatening, unique drugs, countries, serious rate % |
| **Arrival Rate** | Stacked area — serious vs. non-serious per hour (24h) |
| **Signal Alerts** ⭐ | 🔴 CRITICAL / 🟠 HIGH / 🟡 ELEVATED / 🔵 MEDIUM — spike ratio, event count, formatted dates |
| **Portfolio Heatmap** | Per-drug risk level with serious rate and death count |
| **Top Reactions** | Top 10 stacked by severity (deaths, life-threatening, hospitalizations, other) |
| **Geographic Map** | Country-level distribution with serious rate and drug count |
| **Trend by Drug** | 30-day multi-line chart — one line per drug |
| **Sex Distribution** | Patient sex distribution (pie chart) |
| **Case Drilldown** | 50 most recent serious cases with outcome icons, drug, reaction, demographics |
| **Age Distribution** | Stacked bars by age band (Pediatric → Elderly) with serious vs non-serious |
| **Outcome Funnel** | Severity cascade: Total → Serious → Hospitalization → Life-Threatening → Death |
| **Reporter Quality** | Physician / Pharmacist / Consumer / Lawyer breakdown — data quality indicator |
| **Drug×Reaction Cross-tab** | Pivot heatmap of top 8 drugs × top 8 reactions — hotspot detection |
| **Signal Timeline** | Daily serious events vs 7-day rolling average with anomaly spike markers |

---

## KQL Stored Functions

| Function | Purpose |
|----------|--------|
| `DetectSafetySignals(2.0, 3)` | Compares 7d vs 30d baselines to flag drug+reaction spikes |
| `PatientDemographics("aspirin")` | Age group × sex breakdown for a specific drug |
| `DrugComparisonReport("aspirin", "pembrolizumab")` | Head-to-head safety profile comparison |
| `ReactionTimeline("aspirin", 30d)` | Daily reaction counts for a drug over configurable window |
| `RiskScoreCard("aspirin")` | Composite 0–100 risk score (seriousness, mortality, velocity, breadth) |

## Signal Detection Logic

`DetectSafetySignals(threshold, min_count)`:

1. Counts serious events per drug+reaction in the **last 7 days**
2. Calculates the **30-day daily average** for the same pairs
3. Computes **expected 7-day count** = daily_avg × 7
4. Flags: `current ≥ threshold × expected` AND `current ≥ min_count`
5. Severity: **CRITICAL** (deaths) → **HIGH** (hospitalizations) → **MEDIUM** (other)

Default: `DetectSafetySignals(2.0, 3)` — 2× above baseline, minimum 3 events.

---

## Configuration

Edit `docs/config.json`:

| Section | Key Fields |
|---------|-----------|
| `drug_filter` | `generic_name`, `therapeutic_area` — single-drug scripts |
| `drug_portfolio` | `profile`, `target_count`, `spike_drug` — multi-drug fetcher |
| `openfda_api` | `api_key`, `date_range_start/end` |
| `fabric_eventhub` | `connection_string` — Eventstream custom endpoint |
| `fabric_eventhouse` | `cluster_uri`, `database_name` — direct Kusto ingestion |
| `signal_detection` | `spike_threshold_multiplier`, `minimum_event_count` |
| `alerting` | `teams_webhook_url`, `alert_cooldown_minutes` |
| `dashboard` | `refresh_interval_seconds`, `trend_days`, `top_n_reactions` |

---

## Troubleshooting

| Issue | Resolution |
|---|---|
| `HTTP 429` from openFDA | Register for free API key, or reduce `--count` |
| No data in Eventhouse | Check Eventstream mapping → `AdverseEvents_JsonMapping` |
| Materialized views unhealthy | `.show materialized-views` — may need recreation |
| Dashboard tiles empty | Ensure time range covers data (`ingest_timestamp > ago(24h)`) |
| No signals detected | Run `DetectSafetySignals(1.0, 1)` to lower thresholds; confirm spike drug has clustered events |
| Auth fails (Kusto SDK) | Try `--auth interactive` for browser login |
| Live demo colors missing | Use Windows Terminal (supports ANSI escape codes) |

---

## References

- [openFDA FAERS API](https://open.fda.gov/apis/drug/event/)
- [Fabric Eventhouse](https://learn.microsoft.com/fabric/real-time-intelligence/eventhouse)
- [Fabric Real-Time Dashboard](https://learn.microsoft.com/fabric/real-time-intelligence/dashboard-real-time-create)
- [Fabric Activator](https://learn.microsoft.com/fabric/real-time-intelligence/activator)
- [KQL Quick Reference](https://learn.microsoft.com/kusto/query/kql-quick-reference)
- [Kusto Python SDK](https://learn.microsoft.com/kusto/api/python/kusto-python-client-library)

---

*openFDA data is public domain (U.S. Government work). Adapt for your organization's drug portfolio and compliance requirements.*
