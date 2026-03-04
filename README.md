# Drug Safety Signal Monitor

**Microsoft Fabric Real-Time Intelligence — Pharmacovigilance Demo**

Demo of adverse-event signal detection built on Microsoft Fabric Real-Time Intelligence. Ingests real FDA FAERS data across a multi-drug portfolio, detects safety-signal spikes with KQL analytics, surfaces results in a **14-tile Signal Room dashboard**, and escalates critical findings via Activator alerts.

---

## Highlights

- **Fabric Notebook ingestion** — The primary method used in this demo; runs directly in Microsoft Fabric with zero local setup and built-in authentication
- **Real FDA data** — openFDA FAERS (2004–2026, updated quarterly)
- **Multi-drug portfolios** — cardiology, oncology, immunology, or custom drug lists
- **End-to-end Fabric** — Notebook ingestion → spike detection → real-time dashboard → alert escalation
- **Composite risk scoring** — Weighted 0–100 score combining seriousness, mortality, velocity, and reaction breadth
- **Live demo mode** — Replay events with color-coded console output for presentations

## Architecture

```
openFDA FAERS API
       │
       ▼
  Fabric Notebook (notebooks/fabric_data_ingestion.ipynb)   ← primary method
       │  fetches from openFDA, lakehouse, or synthetic source
       │  ingests directly into Eventhouse via Kusto SDK
       ▼
  Fabric Eventhouse (KQL DB)
       ├── Materialized views & stored functions
       ├── DetectSafetySignals / RiskScoreCard / DrugComparisonReport
       │
  ┌────┼──────────────┐
  ▼    ▼              ▼
Dashboard  Activator   KQL Queryset
(14 tiles) Alerts
```

Alternative ingestion paths (see [docs/README.md](docs/README.md)):
- `scripts/ingest_faers_stream.py` — stream to Fabric Eventstream
- `scripts/fetch_fda_portfolio.py` + `scripts/load_to_eventhouse.py` — batch fetch & load
- `scripts/live_demo.py` — replay events to console for presentations

## Quick Start

### Recommended: Fabric Notebook (used in this demo)

1. Upload `notebooks/fabric_data_ingestion.ipynb` to your Fabric workspace
2. Update the **Configuration** cell with your Eventhouse cluster URI
3. Choose a data source: `openfda` (live fetch), `lakehouse` (pre-uploaded NDJSON), or `synthetic`
4. **Run All Cells** — data loads automatically with progress tracking

### Alternative: Local scripts

```bash
git clone https://github.com/anuraagr/Drug-safety-monitor.git
cd Drug-safety-monitor
pip install -r requirements.txt

# Fetch multi-drug portfolio data from FDA
python scripts/fetch_fda_portfolio.py --count 2000 --profile mixed --spike-drug aspirin

# Dry-run streaming ingestion
python scripts/ingest_faers_stream.py --config docs/config.json --dry-run --max-events 50
```

See **[docs/README.md](docs/README.md)** for full setup instructions, dashboard configuration, and KQL schema deployment.

## Project Structure

```
├── notebooks/            # Fabric notebook for data ingestion (primary method)
├── scripts/              # Alternative ingestion, deployment, and demo scripts
├── dashboards/tiles/     # 14-tile Signal Room KQL queries
├── alerts/               # Activator rule configuration
├── docs/                 # Full documentation, config
├── cloud_ingestion/      # Azure Function app for cloud deployment
├── sample_data/          # Pre-fetched FDA FAERS demo datasets
└── requirements.txt
```

## Documentation

| Doc | Description |
|-----|-------------|
| [Full README](docs/README.md) | Complete setup guide, script reference, troubleshooting |
| [Dashboard Layout](docs/DASHBOARD_LAYOUT.md) | 14-tile grid layout and presentation tips |

## License

openFDA data is public domain (U.S. Government work). Adapt for your organization's drug portfolio and compliance requirements.
