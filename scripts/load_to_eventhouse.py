#!/usr/bin/env python3
"""
Drug Safety Signal Monitor — Direct Eventhouse Loader
======================================================
Loads NDJSON data directly into a Fabric Eventhouse KQL database
using the Azure Kusto Python SDK. This bypasses Eventstream entirely
and is the fastest way to populate the demo.

Authentication options:
  1. Interactive browser login (default — best for demos)
  2. Azure CLI (if already logged in via `az login`)
  3. Service principal (for automation)

Prerequisites:
    pip install azure-kusto-data azure-kusto-ingest azure-identity

Usage:
    python load_to_eventhouse.py --file ../sample_data/adverse_events_portfolio.json
    python load_to_eventhouse.py --file ../sample_data/adverse_events_portfolio.json --cluster-uri https://xyz.z1.kusto.fabric.microsoft.com

Author: Pharmacovigilance Demo — Microsoft Fabric RTI
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    os.environ.setdefault("PYTHONUTF8", "1")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

# ─── Conditional imports ──────────────────────────────────────────────────────

KUSTO_OK = False
IDENTITY_OK = False

try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    from azure.kusto.data.exceptions import KustoServiceError
    from azure.kusto.ingest import QueuedIngestClient, IngestionProperties
    from azure.kusto.ingest.ingestion_properties import DataFormat, ReportLevel
    KUSTO_OK = True
except ImportError:
    pass

try:
    from azure.identity import (
        InteractiveBrowserCredential,
        DefaultAzureCredential,
        AzureCliCredential,
        DeviceCodeCredential,
    )
    IDENTITY_OK = True
except ImportError:
    pass


# ─── Helpers ──────────────────────────────────────────────────────────────────

class Console:
    @staticmethod
    def header(text: str):
        print(f"\n{'─'*60}")
        print(f"  {text}")
        print(f"{'─'*60}")

    @staticmethod
    def info(t: str):   print(f"  ℹ  {t}")
    @staticmethod
    def ok(t: str):     print(f"  ✓  {t}")
    @staticmethod
    def warn(t: str):   print(f"  ⚠  {t}")
    @staticmethod
    def err(t: str):    print(f"  ✗  {t}")

    @staticmethod
    def progress(cur, tot, label=""):
        pct = cur / tot * 100 if tot > 0 else 0
        filled = int(30 * cur / tot) if tot > 0 else 0
        bar = "█" * filled + "░" * (30 - filled)
        print(f"\r  [{bar}] {pct:5.1f}%  {cur:,}/{tot:,}  {label}    ", end="", flush=True)


con = Console()


def load_config(config_path: str) -> Dict[str, Any]:
    """Load config.json if it exists."""
    p = Path(config_path)
    if p.exists():
        with open(p) as f:
            raw = json.load(f)
        return {k: v for k, v in raw.items() if not k.startswith("//")}
    return {}


# ─── Kusto Ingestion ─────────────────────────────────────────────────────────

def ingest_via_streaming(
    cluster_uri: str,
    database: str,
    table: str,
    mapping: str,
    events: List[Dict],
    batch_size: int = 500,
    auth_method: str = "device_code",
    clear_first: bool = False,
):
    """
    Ingest events into Eventhouse using Kusto streaming ingestion.
    Falls back to queued ingestion if streaming is not available.
    """
    if not KUSTO_OK:
        con.err("azure-kusto-data / azure-kusto-ingest not installed.")
        con.info("Run: pip install azure-kusto-data azure-kusto-ingest azure-identity")
        sys.exit(1)
    if not IDENTITY_OK:
        con.err("azure-identity not installed.")
        con.info("Run: pip install azure-identity")
        sys.exit(1)

    # Build connection string with authentication
    con.header("Authenticating to Fabric Eventhouse")
    con.info(f"Cluster:  {cluster_uri}")
    con.info(f"Database: {database}")
    con.info(f"Table:    {table}")
    con.info(f"Auth:     {auth_method}")

    if auth_method == "device_code":
        con.info("Device code authentication — follow the prompt below.")
        con.info("A URL and code will appear. Open the URL in any browser and enter the code.")
        credential = DeviceCodeCredential()
        credential.get_token("https://kusto.kusto.windows.net/.default")
        con.ok("Authenticated successfully")
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster_uri, credential)
    elif auth_method == "interactive":
        credential = InteractiveBrowserCredential()
        con.info("Opening browser for authentication...")
        credential.get_token("https://kusto.kusto.windows.net/.default")
        con.ok("Authenticated successfully")
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster_uri, credential)
    elif auth_method == "cli":
        credential = AzureCliCredential()
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster_uri, credential)
    elif auth_method == "default":
        credential = DefaultAzureCredential()
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster_uri, credential)
    else:
        con.err(f"Unknown auth method: {auth_method}")
        sys.exit(1)

    # Query current table state before ingestion
    con.header(f"Ingesting {len(events):,} events into {database}.{table}")

    client = KustoClient(kcsb)
    try:
        r = client.execute(database, f"{table} | count")
        initial_count = r.primary_results[0][0][0]
        con.info(f"Current row count: {initial_count:,}")
    except Exception:
        initial_count = None

    # Optionally clear existing data before loading
    if clear_first and initial_count and initial_count > 0:
        con.info(f"Clearing {initial_count:,} existing rows...")
        try:
            client.execute(database, f".drop extents <| .show table {table} extents")
            con.ok("Existing data cleared")
            time.sleep(3)  # Brief pause for consistency
        except Exception as e:
            con.warn(f"Clear failed: {str(e)[:100]}")
            con.info("You can manually clear with: .drop extents <| .show table AdverseEvents extents")

    total = len(events)
    ingested = 0
    errors = 0

    for batch_start in range(0, total, batch_size):
        batch = events[batch_start:batch_start + batch_size]

        try:
            kql = _build_set_or_append(table, batch)
            client.execute(database, kql)
            ingested += len(batch)
        except Exception as e:
            error_msg = str(e)
            if "Forbidden" in error_msg or "Unauthorized" in error_msg:
                con.err("Authentication failed. Check permissions on the Eventhouse database.")
                con.info("Required role: 'Table Ingestor' or 'Admin' on the database.")
                if auth_method != "device_code":
                    con.info("Tip: Try --auth device_code for more reliable authentication.")
                sys.exit(1)
            # Try one-by-one for more resilient ingestion
            con.warn(f"Batch failed ({error_msg[:80]}), trying individual events...")
            for evt in batch:
                try:
                    kql_single = _build_set_or_append(table, [evt])
                    client.execute(database, kql_single)
                    ingested += 1
                except Exception:
                    errors += 1

        con.progress(ingested, total, f"({errors} errors)" if errors else "")

    print()  # Newline after progress bar

    con.ok(f"Ingested {ingested:,} of {total:,} events")
    if errors:
        con.warn(f"{errors} events failed to ingest")

    # Post-ingestion verification
    con.header("Verification")
    try:
        r = client.execute(database, f"{table} | count")
        final_count = r.primary_results[0][0][0]
        con.ok(f"Final row count: {final_count:,}")
        if initial_count is not None:
            con.info(f"New rows added: {final_count - initial_count:,}")

        r2 = client.execute(database, f"{table} | summarize dcount(event_id)")
        distinct = r2.primary_results[0][0][0]
        con.info(f"Distinct events: {distinct:,}")

        r3 = client.execute(database,
            f"{table} | summarize count() by generic_name | order by count_ desc | take 5")
        con.info("Top drugs:")
        for row in r3.primary_results[0]:
            print(f"      {row['generic_name']:25s} {row['count_']:>5,}")
    except Exception as e:
        con.warn(f"Verification query failed: {str(e)[:100]}")

    client.close()
    return ingested


# Full 30-column schema matching the Eventhouse table exactly
# (includes 2 auto-generated columns: event_id_1:guid, drug_characterization_1:long)
TABLE_COLUMNS = [
    ("event_id", "string"),
    ("safety_report_id", "string"),
    ("receive_date", "datetime"),
    ("ingest_timestamp", "datetime"),
    ("is_serious", "bool"),
    ("hospitalization", "bool"),
    ("death", "bool"),
    ("life_threatening", "bool"),
    ("disability", "bool"),
    ("congenital_anomaly", "bool"),
    ("required_intervention", "bool"),
    ("patient_age", "string"),
    ("patient_age_unit", "string"),
    ("patient_sex", "string"),
    ("patient_weight", "string"),
    ("drug_name", "string"),
    ("generic_name", "string"),
    ("brand_name", "string"),
    ("drug_route", "string"),
    ("drug_indication", "string"),
    ("drug_characterization", "string"),
    ("reaction_terms", "string"),
    ("primary_reaction", "string"),
    ("reaction_count", "int"),
    ("country", "string"),
    ("sender_organization", "string"),
    ("report_type", "string"),
    ("qualification", "string"),
    ("event_id_1", "guid"),               # auto-generated column
    ("drug_characterization_1", "long"),   # auto-generated column
]


def _build_set_or_append(table: str, events: List[Dict]) -> str:
    """Build a .set-or-append KQL command for a batch of events (full 30-col schema)."""
    col_defs = ", ".join(f"{n}:{t}" for n, t in TABLE_COLUMNS)

    rows = []
    for evt in events:
        values = []
        for name, ctype in TABLE_COLUMNS:
            val = evt.get(name, "")
            if ctype == "bool":
                values.append("true" if val else "false")
            elif ctype == "int":
                values.append(str(int(val)) if val else "0")
            elif ctype == "long":
                values.append(str(int(val)) if val else "long(null)")
            elif ctype == "datetime":
                values.append(f"datetime({val})" if val else "datetime(null)")
            elif ctype == "guid":
                values.append(f"guid({val})" if val else "guid(null)")
            else:
                safe_val = str(val).replace("'", "\\'").replace("\n", " ").replace("\r", "")
                values.append(f"'{safe_val}'")
        rows.append(f"    {', '.join(values)}")

    kql = f".set-or-append {table} <|\n"
    kql += f"  datatable({col_defs})\n"
    kql += f"  [\n"
    kql += ",\n".join(rows)
    kql += f"\n  ]"

    return kql


# ─── Queued Ingestion (alternative) ──────────────────────────────────────────

def ingest_via_queued(
    cluster_uri: str,
    database: str,
    table: str,
    mapping: str,
    ndjson_path: str,
    auth_method: str = "interactive",
):
    """
    Ingest an NDJSON file using Kusto queued ingestion.
    This is more reliable for very large datasets.
    """
    if not KUSTO_OK:
        con.err("azure-kusto-ingest not installed.")
        sys.exit(1)

    con.header("Queued Ingestion")
    con.info(f"File: {ndjson_path}")

    # Resolve the ingest URI
    ingest_uri = cluster_uri.replace("https://", "https://ingest-")

    if auth_method == "device_code":
        con.info("Device code authentication — follow the prompt below.")
        credential = DeviceCodeCredential()
        credential.get_token("https://kusto.kusto.windows.net/.default")
        con.ok("Authenticated successfully")
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(ingest_uri, credential)
    elif auth_method == "interactive":
        credential = InteractiveBrowserCredential()
        con.info("Opening browser for authentication...")
        credential.get_token("https://kusto.kusto.windows.net/.default")
        con.ok("Authenticated successfully")
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(ingest_uri, credential)
    elif auth_method == "cli":
        credential = AzureCliCredential()
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(ingest_uri, credential)
    else:
        credential = DefaultAzureCredential()
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(ingest_uri, credential)

    ingestion_props = IngestionProperties(
        database=database,
        table=table,
        data_format=DataFormat.MULTIJSON,
        ingestion_mapping_reference=mapping,
        report_level=ReportLevel.FailuresAndSuccesses,
    )

    ingest_client = QueuedIngestClient(kcsb)

    con.info("Uploading file for ingestion...")
    ingest_client.ingest_from_file(ndjson_path, ingestion_properties=ingestion_props)
    con.ok("File submitted for queued ingestion")
    con.info("Queued ingestion processes asynchronously — data may take 2-5 minutes to appear.")
    con.info("Verify with: AdverseEvents | count")

    ingest_client.close()


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Load NDJSON data directly into Fabric Eventhouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Authentication methods:
  device_code   Terminal displays a code; sign in via any browser (default, most reliable)
  interactive   Browser popup (may be blocked by firewalls/popup blockers)
  cli           Uses existing 'az login' session
  default       DefaultAzureCredential chain

Examples:
  python load_to_eventhouse.py --file ../sample_data/adverse_events_portfolio.json
  python load_to_eventhouse.py --file ../sample_data/adverse_events_portfolio.json --clear
  python load_to_eventhouse.py --file ../sample_data/adverse_events_portfolio.json --auth interactive
  python load_to_eventhouse.py --file ../sample_data/adverse_events_portfolio.json --method queued
        """,
    )
    parser.add_argument("--file", type=str, required=True,
                        help="Path to NDJSON file")
    parser.add_argument("--cluster-uri", type=str, default="",
                        help="Eventhouse cluster URI (or reads from config.json)")
    parser.add_argument("--database", type=str, default="Drug-safety-signal-eh",
                        help="KQL database name")
    parser.add_argument("--table", type=str, default="AdverseEvents",
                        help="Target table name")
    parser.add_argument("--mapping", type=str, default="AdverseEvents_JsonMapping",
                        help="JSON mapping name")
    parser.add_argument("--method", type=str, choices=["streaming", "queued"],
                        default="streaming",
                        help="Ingestion method (default: streaming)")
    parser.add_argument("--auth", type=str, choices=["device_code", "interactive", "cli", "default"],
                        default="device_code",
                        help="Authentication method (default: device_code — most reliable)")
    parser.add_argument("--clear", action="store_true",
                        help="Clear existing data before ingestion")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Batch size for streaming ingestion (default: 50)")
    parser.add_argument("--config", type=str, default="../docs/config.json",
                        help="Path to config.json")
    args = parser.parse_args()

    # Resolve file path (try CWD first, then relative to script)
    file_path = Path(args.file)
    if not file_path.is_absolute() and not file_path.exists():
        file_path = (Path(__file__).parent / args.file).resolve()
    if not file_path.exists():
        con.err(f"File not found: {file_path}")
        sys.exit(1)

    # Load config for cluster URI if not provided
    cluster_uri = args.cluster_uri
    database = args.database
    if not cluster_uri:
        config_path = Path(args.config)
        if not config_path.is_absolute() and not config_path.exists():
            config_path = (Path(__file__).parent / args.config).resolve()
        cfg = load_config(str(config_path))
        cluster_uri = cfg.get("fabric_eventhouse", {}).get("cluster_uri", "")
        database = cfg.get("fabric_eventhouse", {}).get("database_name", database)

    if not cluster_uri or "<YOUR_" in cluster_uri:
        con.err("Eventhouse cluster URI not configured.")
        con.info("Provide via --cluster-uri or update config.json > fabric_eventhouse.cluster_uri")
        con.info("Find it in Fabric: Workspace > Eventhouse > Copy 'Query URI'")
        # Offer file upload as alternative
        con.header("Alternative: Upload via Fabric Portal")
        print(f"""
  Since the Kusto SDK connection is not configured, use the portal:

  1. Open Fabric Eventhouse > Drug-safety-signal-eh database
  2. Click "Get data" > "Local file"
  3. Browse to: {file_path}
  4. Target table: AdverseEvents (existing)
  5. Mapping: AdverseEvents_JsonMapping
  6. Click "Finish"
""")
        sys.exit(0)

    if args.method == "streaming":
        # Load events into memory
        events = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
        con.info(f"Loaded {len(events):,} events from {file_path.name}")

        ingest_via_streaming(
            cluster_uri=cluster_uri,
            database=database,
            table=args.table,
            mapping=args.mapping,
            events=events,
            batch_size=args.batch_size,
            auth_method=args.auth,
            clear_first=args.clear,
        )
    else:
        ingest_via_queued(
            cluster_uri=cluster_uri,
            database=database,
            table=args.table,
            mapping=args.mapping,
            ndjson_path=str(file_path),
            auth_method=args.auth,
        )

    # Post-ingestion verification queries
    con.header("Verify in Eventhouse Queryset")
    print(f"""
  Run these queries to confirm data is loaded:

    AdverseEvents | count
    AdverseEvents | take 5 | project ingest_timestamp, generic_name, primary_reaction, is_serious
    AdverseEvents | summarize count() by generic_name | order by count_ desc
    DetectSafetySignals(2.0, 3)
    PatientDemographics("aspirin")
""")


if __name__ == "__main__":
    main()
