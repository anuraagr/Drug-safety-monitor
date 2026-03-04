#!/usr/bin/env python3
"""
Drug Safety Signal Monitor — Deployment & Validation Script
=============================================================
Automates the end-to-end deployment and validation of the Pharmacovigilance
demo on Microsoft Fabric Real-Time Intelligence.

This script:
  1. Validates prerequisites (Python packages, config, API connectivity)
  2. Tests openFDA API with the configured drug filter
  3. Generates a sample event file for Eventstream wizard upload
  4. Produces a deployment checklist with pass/fail status
  5. Optionally runs a live ingestion to a configured Eventstream endpoint

Usage:
    python deploy_and_validate.py --config ../docs/config.json [--live]

Author: Pharmacovigilance Demo — Microsoft Fabric RTI
"""

from __future__ import annotations

import argparse
import datetime
import importlib
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("deploy_validate")


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

class CheckResult:
    """Captures the result of a single validation check."""
    def __init__(self, name: str, passed: bool, detail: str = ""):
        self.name = name
        self.passed = passed
        self.detail = detail

    def __str__(self):
        icon = "PASS" if self.passed else "FAIL"
        detail = f"  ({self.detail})" if self.detail else ""
        return f"  [{icon}] {self.name}{detail}"


def run_checks(checks: List[CheckResult]) -> bool:
    """Print all check results and return True if all passed."""
    print("\n" + "=" * 60)
    print("  DEPLOYMENT VALIDATION REPORT")
    print("=" * 60)
    all_passed = True
    for c in checks:
        print(str(c))
        if not c.passed:
            all_passed = False
    print("=" * 60)
    passed_count = sum(1 for c in checks if c.passed)
    print(f"  {passed_count}/{len(checks)} checks passed")
    if all_passed:
        print("  STATUS: READY FOR DEPLOYMENT")
    else:
        print("  STATUS: FIX FAILURES ABOVE BEFORE PROCEEDING")
    print("=" * 60 + "\n")
    return all_passed


# ---------------------------------------------------------------------------
# Check: Python packages
# ---------------------------------------------------------------------------

def check_packages() -> List[CheckResult]:
    """Verify required Python packages are installed."""
    results = []
    packages = {
        "requests": "HTTP client for openFDA API",
        "azure.eventhub": "Azure Event Hub SDK for Fabric Eventstream",
    }
    for pkg, desc in packages.items():
        try:
            importlib.import_module(pkg)
            results.append(CheckResult(f"Package: {pkg}", True, desc))
        except ImportError:
            results.append(CheckResult(f"Package: {pkg}", False, f"pip install {pkg.replace('.', '-')}"))
    return results


# ---------------------------------------------------------------------------
# Check: Configuration
# ---------------------------------------------------------------------------

def check_config(config_path: str) -> Tuple[List[CheckResult], Dict[str, Any] | None]:
    """Load and validate config.json."""
    results = []
    path = Path(config_path)

    if not path.exists():
        results.append(CheckResult("Config file exists", False, f"Not found: {config_path}"))
        return results, None

    results.append(CheckResult("Config file exists", True, str(path)))

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        config = {k: v for k, v in raw.items() if not k.startswith("//")}
        results.append(CheckResult("Config JSON valid", True))
    except json.JSONDecodeError as e:
        results.append(CheckResult("Config JSON valid", False, str(e)))
        return results, None

    # Check required sections
    for section in ["drug_filter", "openfda_api", "fabric_eventhub", "signal_detection", "streaming"]:
        if section in config:
            results.append(CheckResult(f"Config section: {section}", True))
        else:
            results.append(CheckResult(f"Config section: {section}", False, "Missing"))

    # Check drug filter
    drug = config.get("drug_filter", {})
    generic = drug.get("generic_name", "")
    if generic:
        results.append(CheckResult("Drug filter configured", True, f"generic_name={generic}"))
    else:
        results.append(CheckResult("Drug filter configured", False, "Set drug_filter.generic_name"))

    # Check Eventstream connection
    conn_str = config.get("fabric_eventhub", {}).get("connection_string", "")
    if conn_str and "<YOUR_" not in conn_str:
        results.append(CheckResult("Eventstream connection string", True, "Configured"))
    else:
        results.append(CheckResult("Eventstream connection string", False,
                                   "Update fabric_eventhub.connection_string in config.json"))

    return results, config


# ---------------------------------------------------------------------------
# Check: openFDA API connectivity
# ---------------------------------------------------------------------------

def check_openfda_api(config: Dict[str, Any]) -> List[CheckResult]:
    """Test connectivity to the openFDA FAERS API."""
    import requests

    results = []
    api_cfg = config["openfda_api"]
    drug_cfg = config["drug_filter"]
    base_url = api_cfg["base_url"]
    generic_name = drug_cfg.get("generic_name", "aspirin")
    date_start = api_cfg.get("date_range_start", "20200101")
    date_end = api_cfg.get("date_range_end", "20260101")

    search = f'receivedate:[{date_start} TO {date_end}] AND patient.drug.openfda.generic_name:"{generic_name}"'
    params = {"search": search, "limit": 1}
    api_key = api_cfg.get("api_key", "")
    if api_key:
        params["api_key"] = api_key

    try:
        resp = requests.get(base_url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            total = data.get("meta", {}).get("results", {}).get("total", 0)
            results.append(CheckResult("openFDA API reachable", True, f"HTTP 200"))
            results.append(CheckResult(
                f"FAERS records for '{generic_name}'",
                total > 0,
                f"{total:,} records found"
            ))
            if api_key:
                results.append(CheckResult("API key configured", True, "240 req/min limit"))
            else:
                results.append(CheckResult("API key configured", False,
                                           "Using 40 req/min limit. Register at open.fda.gov for 240 req/min"))
        else:
            results.append(CheckResult("openFDA API reachable", False, f"HTTP {resp.status_code}"))
    except Exception as e:
        results.append(CheckResult("openFDA API reachable", False, str(e)))

    return results


# ---------------------------------------------------------------------------
# Check: KQL & dashboard files
# ---------------------------------------------------------------------------

def check_artifact_files(base_dir: str) -> List[CheckResult]:
    """Verify all deployment artifact files exist."""
    results = []
    expected_files = {
        "scripts/setup_eventhouse.kql": "KQL schema & materialized views",
        "scripts/ingest_faers_stream.py": "Python ingestion script",
        "dashboards/signal_room_dashboard.kql": "RTI dashboard tile queries",
        "alerts/activator_rule.json": "Activator alert configuration",
        "docs/config.json": "Customer parameters",
        "docs/README.md": "Setup guide",
        "docs/DEMO_SCRIPT.md": "5-min walkthrough",
        "requirements.txt": "Python dependencies",
    }
    for rel_path, desc in expected_files.items():
        full_path = os.path.join(base_dir, rel_path)
        exists = os.path.isfile(full_path)
        results.append(CheckResult(f"File: {rel_path}", exists, desc if exists else "MISSING"))
    return results


# ---------------------------------------------------------------------------
# Generate sample events file for Eventstream wizard
# ---------------------------------------------------------------------------

def generate_sample_events(config: Dict[str, Any], output_path: str, count: int = 20) -> CheckResult:
    """
    Fetch a small batch of events from openFDA and write them as a JSON file.
    This file can be uploaded to the Fabric Eventstream wizard for schema
    detection and mapping configuration.
    """
    try:
        # Import the ingestion module
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        from ingest_faers_stream import OpenFDAClient, transform_event

        client = OpenFDAClient(config)
        data = client.fetch_page(skip=0, limit=count)
        results = data.get("results", [])

        if not results:
            return CheckResult("Generate sample events", False, "No results from openFDA")

        transformed = [transform_event(r, simulate_realtime=True) for r in results]

        # Write as newline-delimited JSON (NDJSON) — Eventstream's preferred format
        with open(output_path, "w", encoding="utf-8") as f:
            for event in transformed:
                f.write(json.dumps(event, default=str) + "\n")

        file_size = os.path.getsize(output_path)
        return CheckResult(
            "Generate sample events",
            True,
            f"{len(transformed)} events, {file_size:,} bytes → {output_path}"
        )
    except Exception as e:
        return CheckResult("Generate sample events", False, str(e))


# ---------------------------------------------------------------------------
# Generate PowerShell deployment commands
# ---------------------------------------------------------------------------

def print_deployment_commands(config: Dict[str, Any]):
    """Print the PowerShell/Azure CLI commands for resource provisioning."""
    drug = config.get("drug_filter", {}).get("generic_name", "aspirin")
    area = config.get("drug_filter", {}).get("therapeutic_area", "general")

    print("\n" + "=" * 60)
    print("  FABRIC DEPLOYMENT COMMANDS")
    print("=" * 60)
    print("""
# -------------------------------------------------------
# 1. FABRIC WORKSPACE (via Fabric Portal)
# -------------------------------------------------------
# Go to: https://app.fabric.microsoft.com
# → New workspace → Name: "Pharmacovigilance-{area}"
# → Assign Fabric capacity (Trial, F2, or higher)

# -------------------------------------------------------
# 2. CREATE EVENTHOUSE
# -------------------------------------------------------
# In workspace → New → Eventhouse
# Name: DrugSafetyEventhouse
# Database (auto-created): DrugSafetyDB
#
# Then open Query Editor and execute:
#   scripts/setup_eventhouse.kql  (all 10 steps)

# -------------------------------------------------------
# 3. CREATE EVENTSTREAM
# -------------------------------------------------------
# In workspace → New → Eventstream
# Name: FAERS-AdverseEvents-Stream
#
# Add SOURCE:
#   → Custom Endpoint
#   → Copy connection string → paste into config.json
#       fabric_eventhub.connection_string
#
# Add DESTINATION:
#   → Eventhouse
#   → Database: DrugSafetyDB
#   → Table: AdverseEvents
#   → Input format: JSON
#   → Mapping: AdverseEvents_JsonMapping

# -------------------------------------------------------
# 4. UPLOAD SAMPLE EVENTS (for Eventstream schema detection)
# -------------------------------------------------------
# Use the generated file: sample_events.json
# In Eventstream → Sources → Custom Endpoint → Upload sample

# -------------------------------------------------------
# 5. CREATE REAL-TIME DASHBOARD
# -------------------------------------------------------
# In workspace → New → Real-Time Dashboard
# Name: Signal Room Dashboard
# → Connect to DrugSafetyDB
# → Add tiles from: dashboards/signal_room_dashboard.kql

# -------------------------------------------------------
# 6. CREATE ACTIVATOR
# -------------------------------------------------------
# In workspace → New → Activator
# See: alerts/activator_rule.json → setup_instructions

# -------------------------------------------------------
# 7. RUN INGESTION
# -------------------------------------------------------""".format(area=area))

    print(f"""# Dry run (no Event Hub needed):
python scripts/ingest_faers_stream.py --config docs/config.json --dry-run --max-events 10

# Live ingestion (requires Eventstream connection string in config.json):
python scripts/ingest_faers_stream.py --config docs/config.json --max-events 500

# Full ingestion (all available records — may take hours):
python scripts/ingest_faers_stream.py --config docs/config.json --max-events 0
""")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Deploy & validate the Drug Safety Signal Monitor demo."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "docs", "config.json"),
        help="Path to config.json",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run live ingestion after validation (requires Eventstream connection).",
    )
    parser.add_argument(
        "--sample-count",
        type=int,
        default=20,
        help="Number of sample events to generate for Eventstream wizard (default: 20).",
    )
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.abspath(args.config)

    all_checks: List[CheckResult] = []

    # 1. Check Python packages
    print("\n[1/5] Checking Python packages...")
    all_checks.extend(check_packages())

    # 2. Check configuration
    print("[2/5] Validating configuration...")
    config_checks, config = check_config(config_path)
    all_checks.extend(config_checks)

    if not config:
        run_checks(all_checks)
        sys.exit(1)

    # 3. Check openFDA API
    print("[3/5] Testing openFDA API connectivity...")
    all_checks.extend(check_openfda_api(config))

    # 4. Check artifact files
    print("[4/5] Verifying deployment artifacts...")
    all_checks.extend(check_artifact_files(base_dir))

    # 5. Generate sample events
    print("[5/5] Generating sample events for Eventstream wizard...")
    sample_output = os.path.join(base_dir, "sample_events.json")
    all_checks.append(generate_sample_events(config, sample_output, count=args.sample_count))

    # Print report
    all_passed = run_checks(all_checks)

    # Print deployment commands
    print_deployment_commands(config)

    # Optionally run live ingestion
    if args.live:
        conn_str = config.get("fabric_eventhub", {}).get("connection_string", "")
        if "<YOUR_" in conn_str:
            logger.error("Cannot run live ingestion — Eventstream connection string not configured.")
            sys.exit(1)

        print("\n>>> Starting live ingestion to Fabric Eventstream...")
        from ingest_faers_stream import load_config, run_ingestion
        run_ingestion(config, dry_run=False, max_events=500)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
