# Drug Safety Signal Monitor

**Microsoft Fabric Real-Time Intelligence — Pharmacovigilance Demo**

Demo of adverse-event signal detection built on Microsoft Fabric Real-Time Intelligence. Ingests real FDA FAERS data across a multi-drug portfolio, detects safety-signal spikes with KQL analytics, surfaces results in a **14-tile Signal Room dashboard**, and escalates critical findings via Activator alerts.

---

## Highlights

- **Real FDA data** — openFDA FAERS (2004–2026, updated quarterly)
- **Multi-drug portfolios** — cardiology, oncology, immunology, or custom drug lists
- **End-to-end Fabric** — Streaming ingestion → spike detection → real-time dashboard → alert escalation
- **Composite risk scoring** — Weighted 0–100 score combining seriousness, mortality, velocity, and reaction breadth
- **Live demo mode** — Replay events with color-coded console output for presentations

## Architecture

```
openFDA FAERS API
       │
       ├── fetch_fda_portfolio.py  (batch → NDJSON)
       ├── ingest_faers_stream.py  (stream → Eventstream)
       └── live_demo.py            (replay → console)
               │
               ▼
       Fabric Eventhouse (KQL DB)
       ├── Materialized views & stored functions
       ├── DetectSafetySignals / RiskScoreCard / DrugComparisonReport
               │
       ┌───────┼────────────┐
       ▼       ▼            ▼
   Dashboard  Activator   KQL Queryset
   (14 tiles) Alerts
```

## Quick Start

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
├── scripts/              # Ingestion, deployment, and demo scripts
├── dashboards/tiles/     # 14-tile Signal Room KQL queries
├── alerts/               # Activator rule configuration
├── docs/                 # Full documentation, config, demo script
├── notebooks/            # Fabric notebook for data ingestion
├── cloud_ingestion/      # Azure Function app for cloud deployment
├── sample_data/          # Pre-fetched FDA FAERS demo datasets
└── requirements.txt
```

## Documentation

| Doc | Description |
|-----|-------------|
| [Full README](docs/README.md) | Complete setup guide, script reference, troubleshooting |
| [Demo Script](docs/DEMO_SCRIPT.md) | 10-minute presentation walkthrough |
| [Dashboard Layout](docs/DASHBOARD_LAYOUT.md) | 14-tile grid layout and presentation tips |

## License

openFDA data is public domain (U.S. Government work). Adapt for your organization's drug portfolio and compliance requirements.
