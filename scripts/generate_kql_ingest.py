#!/usr/bin/env python3
"""
Drug Safety Signal Monitor — KQL Ingestion Script Generator
=============================================================
Converts an NDJSON file into a series of .set-or-append KQL commands
that can be pasted directly into the Fabric Eventhouse Queryset editor.

This is the zero-dependency fallback when SDK authentication fails.
No Azure packages needed — just Python 3.

Usage:
    python generate_kql_ingest.py --file ../sample_data/adverse_events_portfolio.json
    python generate_kql_ingest.py --file ../sample_data/adverse_events_portfolio.json --batch-size 100 --output ingest_data.kql

Then paste the output KQL into: Fabric > Eventhouse > Queryset > New query

Author: Pharmacovigilance Demo — Microsoft Fabric RTI
"""

from __future__ import annotations

import argparse
import json
import sys
import os
from pathlib import Path

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    os.environ.setdefault("PYTHONUTF8", "1")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

# Full 30-column schema matching the Eventhouse table
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
    ("event_id_1", "guid"),
    ("drug_characterization_1", "long"),
]


def build_kql_value(val, ctype: str) -> str:
    """Convert a Python value to a KQL literal."""
    if ctype == "bool":
        return "true" if val else "false"
    elif ctype == "int":
        return str(int(val)) if val else "0"
    elif ctype == "long":
        return str(int(val)) if val else "long(null)"
    elif ctype == "datetime":
        return f"datetime({val})" if val else "datetime(null)"
    elif ctype == "guid":
        return f"guid({val})" if val else "guid(null)"
    else:
        safe_val = str(val).replace("'", "\\'").replace("\n", " ").replace("\r", "")
        return f"'{safe_val}'"


def generate_batch_kql(table: str, events: list, batch_num: int) -> str:
    """Generate a .set-or-append KQL command for a batch of events."""
    col_defs = ", ".join(f"{n}:{t}" for n, t in TABLE_COLUMNS)

    rows = []
    for evt in events:
        values = []
        for name, ctype in TABLE_COLUMNS:
            val = evt.get(name, "")
            values.append(build_kql_value(val, ctype))
        rows.append(f"    {', '.join(values)}")

    kql = f"// ── Batch {batch_num} ({len(events)} events) ──\n"
    kql += f".set-or-append {table} <|\n"
    kql += f"  datatable({col_defs})\n"
    kql += f"  [\n"
    kql += ",\n".join(rows)
    kql += f"\n  ]\n"
    return kql


def main():
    parser = argparse.ArgumentParser(
        description="Generate KQL .set-or-append commands from NDJSON data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This generates KQL that can be pasted directly into the Fabric Queryset editor.
No Azure SDK or authentication needed — just copy and run in Fabric.

For large datasets, use --split to generate separate files per batch,
then run each file sequentially in the Queryset editor.

Examples:
  python generate_kql_ingest.py --file ../sample_data/adverse_events_portfolio.json
  python generate_kql_ingest.py --file data.json --batch-size 100 --output ingest.kql
  python generate_kql_ingest.py --file data.json --split --output-dir kql_batches/
        """,
    )
    parser.add_argument("--file", type=str, required=True,
                        help="Path to NDJSON file")
    parser.add_argument("--table", type=str, default="AdverseEvents",
                        help="Target table name (default: AdverseEvents)")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Events per .set-or-append command (default: 50)")
    parser.add_argument("--output", type=str, default="",
                        help="Output KQL file (default: print to stdout)")
    parser.add_argument("--split", action="store_true",
                        help="Split into separate files per batch (use with --output-dir)")
    parser.add_argument("--output-dir", type=str, default="kql_batches",
                        help="Directory for split output files (default: kql_batches/)")
    parser.add_argument("--clear", action="store_true",
                        help="Prepend a .drop extents command to clear existing data")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Maximum events to process (0 = all)")
    args = parser.parse_args()

    # Resolve file path
    file_path = Path(args.file)
    if not file_path.is_absolute() and not file_path.exists():
        file_path = (Path(__file__).parent / args.file).resolve()
    if not file_path.exists():
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    # Load events
    events = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
                if args.max_events and len(events) >= args.max_events:
                    break

    print(f"Loaded {len(events):,} events from {file_path.name}", file=sys.stderr)

    # Generate KQL
    total_batches = (len(events) + args.batch_size - 1) // args.batch_size

    if args.split:
        # Split into separate files
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        if args.clear:
            clear_file = out_dir / "00_clear_data.kql"
            with open(clear_file, "w", encoding="utf-8") as f:
                f.write(f"// Clear existing data from {args.table}\n")
                f.write(f".drop extents <| .show table {args.table} extents\n")
            print(f"  Created {clear_file}", file=sys.stderr)

        for i, batch_start in enumerate(range(0, len(events), args.batch_size)):
            batch = events[batch_start:batch_start + args.batch_size]
            kql = generate_batch_kql(args.table, batch, i + 1)

            filename = out_dir / f"batch_{i+1:04d}.kql"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(kql)

            pct = (i + 1) / total_batches * 100
            print(f"\r  Generated batch {i+1}/{total_batches} ({pct:.0f}%)", end="", file=sys.stderr, flush=True)

        print(f"\n\nGenerated {total_batches} batch files in {out_dir}/", file=sys.stderr)
        print(f"Run each file sequentially in the Fabric Queryset editor.", file=sys.stderr)

    else:
        # Single output
        output_parts = []

        # Header
        output_parts.append(f"// Drug Safety Signal Monitor — KQL Data Ingestion")
        output_parts.append(f"// Generated from {file_path.name} ({len(events):,} events)")
        output_parts.append(f"// Run this in: Fabric > Eventhouse > Queryset > New query")
        output_parts.append(f"// NOTE: Run each .set-or-append block separately (select + Run)")
        output_parts.append(f"")

        if args.clear:
            output_parts.append(f"// ── Clear existing data ──")
            output_parts.append(f".drop extents <| .show table {args.table} extents")
            output_parts.append(f"")

        for i, batch_start in enumerate(range(0, len(events), args.batch_size)):
            batch = events[batch_start:batch_start + args.batch_size]
            kql = generate_batch_kql(args.table, batch, i + 1)
            output_parts.append(kql)

            pct = (i + 1) / total_batches * 100
            print(f"\r  Generated batch {i+1}/{total_batches} ({pct:.0f}%)", end="", file=sys.stderr, flush=True)

        print(file=sys.stderr)

        full_kql = "\n".join(output_parts)

        if args.output:
            out_path = Path(args.output)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(full_kql)
            size_mb = out_path.stat().st_size / (1024 * 1024)
            print(f"Written to {out_path} ({size_mb:.1f} MB)", file=sys.stderr)
            print(f"\nTo use: Open in a text editor, copy batches into the Fabric Queryset editor, and run.", file=sys.stderr)
        else:
            print(full_kql)

    # Summary
    print(f"\nSummary:", file=sys.stderr)
    print(f"  Events:  {len(events):,}", file=sys.stderr)
    print(f"  Batches: {total_batches}", file=sys.stderr)
    print(f"  Batch size: {args.batch_size}", file=sys.stderr)


if __name__ == "__main__":
    main()
