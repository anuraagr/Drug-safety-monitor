#!/usr/bin/env python3
"""
Generate a large sample dataset from openFDA FAERS and output as:
  1. NDJSON file — for Fabric "Get data > Local file" upload
  2. KQL .ingest inline commands — for pasting directly in Queryset

Usage:
    python generate_demo_data.py --count 500 --output ../sample_data
"""

import argparse
import datetime
import json
import os
import sys
import uuid
import time
from pathlib import Path

# Add parent path so we can reuse transform_event from the ingestion script
sys.path.insert(0, str(Path(__file__).parent))
from ingest_faers_stream import OpenFDAClient, transform_event

def main():
    parser = argparse.ArgumentParser(description="Generate demo adverse event data")
    parser.add_argument("--count", type=int, default=500, help="Number of events to generate")
    parser.add_argument("--output", type=str, default="../sample_data", help="Output directory")
    parser.add_argument("--drug", type=str, default="aspirin", help="Drug generic name to filter")
    parser.add_argument("--config", type=str, default="../docs/config.json", help="Config file path")
    args = parser.parse_args()

    # Resolve paths relative to script location
    script_dir = Path(__file__).parent
    output_dir = (script_dir / args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    config_path = (script_dir / args.config).resolve()

    # Load config if available, otherwise use defaults
    config = {}
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)

    # Override drug filter if specified via CLI
    if "drug_filter" not in config:
        config["drug_filter"] = {}
    config["drug_filter"]["generic_name"] = args.drug
    if "openfda_api" not in config:
        config["openfda_api"] = {
            "base_url": "https://api.fda.gov/drug/event.json",
            "rate_limit_per_minute": 40,
            "max_records_per_request": 100,
            "date_range_start": "20200101",
            "date_range_end": "20260101",
        }

    # Initialize openFDA client
    client = OpenFDAClient(config)

    print(f"Fetching {args.count} adverse events for '{args.drug}' from openFDA...")
    total = client.fetch_total_count()
    print(f"  Total matching records: {total:,}")

    events = []
    skip = 0
    page_size = 100

    while len(events) < args.count:
        needed = min(page_size, args.count - len(events))
        page = client.fetch_page(skip=skip, limit=needed)
        results = page.get("results", [])
        if not results:
            print(f"  No more results at skip={skip}")
            break

        for raw in results:
            transformed = transform_event(raw, simulate_realtime=True)
            # Spread ingest_timestamps across last 30 days for realistic demo
            days_back = (len(events) % 30)
            hours_back = (len(events) % 24)
            offset = datetime.timedelta(days=days_back, hours=hours_back, minutes=len(events) % 60)
            ts = datetime.datetime.now(datetime.timezone.utc) - offset
            transformed["ingest_timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            events.append(transformed)
            if len(events) >= args.count:
                break

        skip += len(results)
        print(f"  Fetched {len(events)}/{args.count} events...")

    print(f"\nGenerated {len(events)} events total.")

    # --- Write NDJSON file (for Fabric "Get data > Local file") ---
    ndjson_path = output_dir / "adverse_events_demo.json"
    with open(ndjson_path, "w") as f:
        for evt in events:
            f.write(json.dumps(evt) + "\n")
    file_size = os.path.getsize(ndjson_path)
    print(f"\n[1] NDJSON file: {ndjson_path}")
    print(f"    Size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
    print(f"    Records: {len(events)}")

    # --- Write summary stats ---
    serious_count = sum(1 for e in events if e["is_serious"])
    death_count = sum(1 for e in events if e["death"])
    hosp_count = sum(1 for e in events if e["hospitalization"])
    countries = set(e["country"] for e in events if e["country"])
    reactions = set(e["primary_reaction"] for e in events if e["primary_reaction"])

    stats_path = output_dir / "data_summary.txt"
    with open(stats_path, "w") as f:
        f.write(f"Drug Safety Signal Monitor — Demo Data Summary\n")
        f.write(f"{'='*50}\n")
        f.write(f"Generated: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")
        f.write(f"Drug filter: {args.drug}\n")
        f.write(f"Total events: {len(events)}\n")
        f.write(f"Serious events: {serious_count} ({100*serious_count/len(events):.0f}%)\n")
        f.write(f"Deaths: {death_count}\n")
        f.write(f"Hospitalizations: {hosp_count}\n")
        f.write(f"Unique countries: {len(countries)}\n")
        f.write(f"Unique reactions: {len(reactions)}\n")
        f.write(f"Date range of ingest_timestamps: last 30 days\n")
        f.write(f"\nTop 10 reactions:\n")
        reaction_counts = {}
        for e in events:
            r = e.get("primary_reaction", "")
            if r:
                reaction_counts[r] = reaction_counts.get(r, 0) + 1
        for r, c in sorted(reaction_counts.items(), key=lambda x: -x[1])[:10]:
            f.write(f"  {r}: {c}\n")

    print(f"\n[2] Data summary: {stats_path}")

    # --- Print upload instructions ---
    print(f"""
{'='*60}
NEXT STEPS — Load data into Fabric Eventhouse
{'='*60}

OPTION A: Upload via "Get data" wizard (RECOMMENDED)
-----------------------------------------------------
1. Go to your Fabric Eventhouse database (Drug-safety-signal-eh)
2. Click "Get data" button (green button in center, or toolbar)
3. Select "Local file" as the source
4. Browse to: {ndjson_path}
5. Target table: AdverseEvents (select existing)
6. Format: JSON (auto-detected)
7. Mapping: Select "AdverseEvents_JsonMapping"
8. Click "Finish" to import

OPTION B: Create Eventstream for live streaming
------------------------------------------------
1. In Fabric workspace, click "+ New" > "Eventstream"
2. Name: "faers-adverse-events"
3. Add Source > "Custom App" (gives you Event Hub connection string)
4. Add Destination > select your Eventhouse DB + AdverseEvents table
5. Copy the connection string into docs/config.json > fabric_eventhub.connection_string
6. Run: python ingest_faers_stream.py --config ../docs/config.json --max-events 500

After loading data, test with these queries in the Queryset:

  AdverseEvents | count
  AdverseEvents | take 10
  DetectSafetySignals(2.0, 3)
  PatientDemographics("aspirin")
""")


if __name__ == "__main__":
    main()
