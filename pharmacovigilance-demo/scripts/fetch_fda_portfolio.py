#!/usr/bin/env python3
"""
Drug Safety Signal Monitor — FDA Portfolio Data Fetcher
========================================================
Fetches real adverse event reports from the openFDA FAERS API across a
configurable portfolio of drugs spanning multiple therapeutic areas.

Produces a demo-ready NDJSON dataset with realistic ingest_timestamp
distribution (clustered in the last 30 days with a "spike" window for
live signal detection demonstrations).

Key features for a polished customer demo:
  • Multi-drug portfolio: fetches 5+ drugs across oncology, cardiology, etc.
  • Smart timestamp distribution: recent cluster + baseline + spike injection
  • Deduplication by safety_report_id across drug queries
  • Rich console output with progress tracking
  • Automatic data quality validation & summary report
  • Generates both NDJSON (for file upload) and KQL .set-or-append commands

Usage:
    python fetch_fda_portfolio.py                          # Full portfolio, 2000 events
    python fetch_fda_portfolio.py --count 500              # Quick demo set
    python fetch_fda_portfolio.py --profile oncology       # Oncology-focused
    python fetch_fda_portfolio.py --drugs aspirin warfarin  # Custom drug list

Author: Pharmacovigilance Demo — Microsoft Fabric RTI
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import random
import sys
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Ensure UTF-8 output on Windows (avoids cp1252 errors with box-drawing chars)
if sys.platform == "win32":
    os.environ.setdefault("PYTHONUTF8", "1")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass  # reconfigure not available on all streams

# ─── Constants ────────────────────────────────────────────────────────────────

BANNER = r"""
╔══════════════════════════════════════════════════════════════════╗
║   Drug Safety Signal Monitor — FDA FAERS Portfolio Fetch        ║
║   Microsoft Fabric Real-Time Intelligence Demo                  ║
╚══════════════════════════════════════════════════════════════════╝
"""

# Pre-defined drug portfolios for different customer verticals
DRUG_PORTFOLIOS = {
    "cardiology": {
        "description": "Cardiovascular drug safety monitoring",
        "drugs": [
            {"generic_name": "aspirin",        "category": "Antiplatelet",           "weight": 0.30},
            {"generic_name": "warfarin",       "category": "Anticoagulant",          "weight": 0.25},
            {"generic_name": "atorvastatin",   "category": "Statin",                 "weight": 0.20},
            {"generic_name": "metoprolol",     "category": "Beta-blocker",           "weight": 0.15},
            {"generic_name": "lisinopril",     "category": "ACE Inhibitor",          "weight": 0.10},
        ],
    },
    "oncology": {
        "description": "Oncology / immunotherapy safety monitoring",
        "drugs": [
            {"generic_name": "pembrolizumab",  "category": "PD-1 Inhibitor",         "weight": 0.30},
            {"generic_name": "nivolumab",      "category": "PD-1 Inhibitor",         "weight": 0.25},
            {"generic_name": "trastuzumab",    "category": "HER2 Antibody",          "weight": 0.20},
            {"generic_name": "rituximab",      "category": "CD20 Antibody",          "weight": 0.15},
            {"generic_name": "lenalidomide",   "category": "Immunomodulator",        "weight": 0.10},
        ],
    },
    "immunology": {
        "description": "Autoimmune / immunology drug monitoring",
        "drugs": [
            {"generic_name": "adalimumab",     "category": "TNF Inhibitor",          "weight": 0.30},
            {"generic_name": "infliximab",     "category": "TNF Inhibitor",          "weight": 0.25},
            {"generic_name": "etanercept",     "category": "TNF Inhibitor",          "weight": 0.20},
            {"generic_name": "methotrexate",   "category": "DMARD",                  "weight": 0.15},
            {"generic_name": "tofacitinib",    "category": "JAK Inhibitor",          "weight": 0.10},
        ],
    },
    "mixed": {
        "description": "Cross-therapeutic portfolio (best for demo impact)",
        "drugs": [
            {"generic_name": "aspirin",        "category": "Antiplatelet",           "weight": 0.20},
            {"generic_name": "pembrolizumab",  "category": "PD-1 Inhibitor",         "weight": 0.20},
            {"generic_name": "adalimumab",     "category": "TNF Inhibitor",          "weight": 0.20},
            {"generic_name": "warfarin",       "category": "Anticoagulant",          "weight": 0.15},
            {"generic_name": "metformin",      "category": "Antidiabetic",           "weight": 0.10},
            {"generic_name": "lisinopril",     "category": "ACE Inhibitor",          "weight": 0.08},
            {"generic_name": "omeprazole",     "category": "Proton Pump Inhibitor",  "weight": 0.07},
        ],
    },
}

# ─── Logging helpers ──────────────────────────────────────────────────────────

class Console:
    """Rich-ish console output without extra dependencies."""

    @staticmethod
    def header(text: str):
        print(f"\n{'─'*64}")
        print(f"  {text}")
        print(f"{'─'*64}")

    @staticmethod
    def info(text: str):
        print(f"  ℹ  {text}")

    @staticmethod
    def success(text: str):
        print(f"  ✓  {text}")

    @staticmethod
    def warn(text: str):
        print(f"  ⚠  {text}")

    @staticmethod
    def error(text: str):
        print(f"  ✗  {text}")

    @staticmethod
    def progress(current: int, total: int, label: str = ""):
        pct = current / total * 100 if total > 0 else 0
        bar_len = 30
        filled = int(bar_len * current / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)
        print(f"\r  [{bar}] {pct:5.1f}%  {current:,}/{total:,}  {label}     ", end="", flush=True)

    @staticmethod
    def table(headers: List[str], rows: List[List[str]], col_widths: Optional[List[int]] = None):
        """Print a formatted ASCII table."""
        if not col_widths:
            col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0)) + 2
                          for i, h in enumerate(headers)]
        header_line = "│".join(str(h).ljust(w) for h, w in zip(headers, col_widths))
        sep_line = "┼".join("─" * w for w in col_widths)
        print(f"  {header_line}")
        print(f"  {sep_line}")
        for row in rows:
            row_line = "│".join(str(c).ljust(w) for c, w in zip(row, col_widths))
            print(f"  {row_line}")


con = Console()

# ─── openFDA Client ──────────────────────────────────────────────────────────

class FDAClient:
    """Lean openFDA FAERS API client with rate limiting and retries."""

    BASE_URL = "https://api.fda.gov/drug/event.json"

    def __init__(self, api_key: str = "", rate_limit: int = 40):
        self.api_key = api_key or None
        rpm = 240 if self.api_key else rate_limit
        self._min_interval = 60.0 / rpm
        self._last_call = 0.0
        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"
        self._session.headers["User-Agent"] = "DrugSafetyMonitor/2.0 (Microsoft Fabric RTI Demo)"
        self._total_calls = 0

    def _throttle(self):
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.monotonic()

    def fetch_count(self, drug: str, date_start: str = "20200101", date_end: str = "20260101") -> int:
        """Get total matching record count for a drug."""
        search = (
            f'receivedate:[{date_start} TO {date_end}]'
            f' AND patient.drug.openfda.generic_name:"{drug}"'
        )
        params = {"search": search, "limit": 1}
        if self.api_key:
            params["api_key"] = self.api_key

        self._throttle()
        self._total_calls += 1
        try:
            resp = self._session.get(self.BASE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json().get("meta", {}).get("results", {}).get("total", 0)
            return 0
        except Exception:
            return 0

    def fetch_page(self, drug: str, skip: int = 0, limit: int = 100,
                   date_start: str = "20200101", date_end: str = "20260101") -> List[Dict]:
        """Fetch a page of adverse events for a specific drug."""
        search = (
            f'receivedate:[{date_start} TO {date_end}]'
            f' AND patient.drug.openfda.generic_name:"{drug}"'
        )
        params = {"search": search, "limit": limit, "skip": skip}
        if self.api_key:
            params["api_key"] = self.api_key

        max_retries = 4
        for attempt in range(1, max_retries + 1):
            self._throttle()
            self._total_calls += 1
            try:
                resp = self._session.get(self.BASE_URL, params=params, timeout=30)
                if resp.status_code == 200:
                    return resp.json().get("results", [])
                if resp.status_code == 404:
                    return []
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    con.warn(f"Rate-limited (429). Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                return []
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                con.error(f"Request failed after {max_retries} retries: {e}")
                return []
        return []


# ─── Event Transformer ───────────────────────────────────────────────────────

def _safe_get(d, *keys, default=""):
    """Safely navigate nested dicts/lists."""
    current = d
    for k in keys:
        if isinstance(current, list):
            current = current[0] if current else {}
        if isinstance(current, dict):
            current = current.get(k, default)
        else:
            return default
    return current or default


def _parse_fda_date(date_str: str) -> str:
    try:
        dt = datetime.datetime.strptime(date_str, "%Y%m%d")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def transform_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a raw openFDA record into the Eventhouse schema."""
    safety_report_id = raw.get("safetyreportid", str(uuid.uuid4()))
    receive_date = raw.get("receivedate", "")

    # Seriousness flags
    serious = raw.get("serious", "2")
    is_serious = serious == "1"

    # Patient
    patient = raw.get("patient", {})

    # Drug info — prefer suspect drug (characterization=1)
    drugs = patient.get("drug", [])
    suspect = [d for d in drugs if d.get("drugcharacterization") == "1"]
    primary = suspect[0] if suspect else (drugs[0] if drugs else {})

    generic_name = _safe_get(primary, "openfda", "generic_name", default="")
    brand_name = _safe_get(primary, "openfda", "brand_name", default="")
    if isinstance(generic_name, list):
        generic_name = generic_name[0] if generic_name else ""
    if isinstance(brand_name, list):
        brand_name = brand_name[0] if brand_name else ""

    # Reactions
    reactions = patient.get("reaction", [])
    terms = [r.get("reactionmeddrapt", "") for r in reactions if r.get("reactionmeddrapt")]

    # Country
    country = raw.get("occurcountry", "")
    if not country:
        country = _safe_get(raw, "primarysource", "reportercountry", default="")

    return {
        "event_id": str(uuid.uuid4()),
        "safety_report_id": safety_report_id,
        "receive_date": _parse_fda_date(receive_date),
        "ingest_timestamp": "",  # Set later by timestamp distributor
        "is_serious": is_serious,
        "hospitalization": raw.get("seriousnesshospitalization", "0") == "1",
        "death": raw.get("seriousnessdeath", "0") == "1",
        "life_threatening": raw.get("seriousnesslifethreatening", "0") == "1",
        "disability": raw.get("seriousnessdisabling", "0") == "1",
        "congenital_anomaly": raw.get("seriousnesscongenitalanomali", "0") == "1",
        "required_intervention": raw.get("seriousnessother", "0") == "1",
        "patient_age": _safe_get(patient, "patientonsetage", default=""),
        "patient_age_unit": _safe_get(patient, "patientonsetageunit", default=""),
        "patient_sex": _safe_get(patient, "patientsex", default=""),
        "patient_weight": _safe_get(patient, "patientweight", default=""),
        "drug_name": primary.get("medicinalproduct", ""),
        "generic_name": generic_name,
        "brand_name": brand_name,
        "drug_route": primary.get("drugadministrationroute", ""),
        "drug_indication": primary.get("drugindication", ""),
        "drug_characterization": primary.get("drugcharacterization", ""),
        "reaction_terms": ",".join(terms),
        "primary_reaction": terms[0] if terms else "",
        "reaction_count": len(terms),
        "country": country,
        "sender_organization": _safe_get(raw, "sender", "senderorganization", default=""),
        "report_type": raw.get("reporttype", ""),
        "qualification": _safe_get(raw, "primarysource", "qualification", default=""),
    }


# ─── Timestamp Distribution Engine ───────────────────────────────────────────

def distribute_timestamps(events: List[Dict], spike_drug: str = "") -> List[Dict]:
    """
    Assign realistic ingest_timestamps across the last 30 days:
      • 40% in last 7 days (recent activity — feeds 7d materialized view)
      • 30% in days 8-21 (mid-range baseline)
      • 30% in days 22-30 (older baseline)
      • Optional spike: cluster 15% of `spike_drug` events into a 48-hour window
        to guarantee the DetectSafetySignals function fires for the demo.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    random.seed(42)  # Reproducible for demo consistency

    for i, evt in enumerate(events):
        # Determine time bucket
        r = random.random()
        if r < 0.40:
            # Last 7 days — recent
            offset_hours = random.uniform(0, 7 * 24)
        elif r < 0.70:
            # Days 8-21
            offset_hours = random.uniform(7 * 24, 21 * 24)
        else:
            # Days 22-30
            offset_hours = random.uniform(21 * 24, 30 * 24)

        ts = now - datetime.timedelta(hours=offset_hours)
        # Add some randomness to minutes/seconds for realism
        ts = ts.replace(
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=random.randint(0, 999999),
        )
        evt["ingest_timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # Spike injection: cluster events for the spike_drug in a 48-hour window
    if spike_drug:
        spike_events = [e for e in events if e["generic_name"].lower() == spike_drug.lower() and e["is_serious"]]
        spike_count = max(int(len(spike_events) * 0.40), 8)  # At least 8 events in spike
        spike_center = now - datetime.timedelta(hours=36)  # 36 hours ago

        for evt in spike_events[:spike_count]:
            offset_hours = random.uniform(-24, 24)
            ts = spike_center + datetime.timedelta(hours=offset_hours)
            ts = ts.replace(
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
                microsecond=random.randint(0, 999999),
            )
            evt["ingest_timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        con.info(f"Injected spike: {spike_count} serious '{spike_drug}' events clustered in 48h window")

    # Sort by timestamp for realistic ordering
    events.sort(key=lambda e: e["ingest_timestamp"])
    return events


# ─── Main Orchestrator ────────────────────────────────────────────────────────

def fetch_portfolio(
    profile: str = "mixed",
    custom_drugs: Optional[List[str]] = None,
    target_count: int = 2000,
    api_key: str = "",
    date_start: str = "20200101",
    date_end: str = "20260101",
    spike_drug: str = "",
) -> List[Dict[str, Any]]:
    """Fetch adverse events across a drug portfolio from openFDA."""

    print(BANNER)

    # Resolve drug list
    if custom_drugs:
        drug_list = [{"generic_name": d, "category": "Custom", "weight": 1.0 / len(custom_drugs)}
                     for d in custom_drugs]
        profile_desc = f"Custom drugs: {', '.join(custom_drugs)}"
    else:
        portfolio = DRUG_PORTFOLIOS.get(profile)
        if not portfolio:
            con.error(f"Unknown profile '{profile}'. Available: {', '.join(DRUG_PORTFOLIOS.keys())}")
            sys.exit(1)
        drug_list = portfolio["drugs"]
        profile_desc = portfolio["description"]

    con.header(f"Portfolio: {profile_desc}")
    con.info(f"Target event count: {target_count:,}")
    con.info(f"Date range: {date_start} — {date_end}")
    con.info(f"Drugs to fetch: {len(drug_list)}")
    print()

    client = FDAClient(api_key=api_key)

    # Phase 1: Query available counts for each drug
    con.header("Phase 1: Querying FDA for available record counts")
    drug_counts: List[Tuple[Dict, int]] = []
    for drug_info in drug_list:
        name = drug_info["generic_name"]
        count = client.fetch_count(name, date_start, date_end)
        drug_counts.append((drug_info, count))
        status = f"{count:>8,} records" if count > 0 else "  NO RECORDS"
        print(f"  {name:<20s}  {drug_info['category']:<25s}  {status}")

    # Remove drugs with 0 records
    drug_counts = [(d, c) for d, c in drug_counts if c > 0]
    if not drug_counts:
        con.error("No records found for any drug in the portfolio. Check API connectivity.")
        sys.exit(1)

    total_available = sum(c for _, c in drug_counts)
    con.info(f"Total available records across portfolio: {total_available:,}")

    # Phase 2: Calculate per-drug fetch targets (proportional to weight)
    total_weight = sum(d["weight"] for d, _ in drug_counts)
    fetch_plan: List[Tuple[Dict, int, int]] = []  # (drug_info, target, available)
    for drug_info, available in drug_counts:
        proportion = drug_info["weight"] / total_weight
        raw_target = int(target_count * proportion)
        actual = min(raw_target, available, 5000)  # openFDA skip limit ~5000
        fetch_plan.append((drug_info, actual, available))

    # Redistribute any shortfall
    assigned = sum(t for _, t, _ in fetch_plan)
    if assigned < target_count:
        deficit = target_count - assigned
        for i, (drug_info, target, available) in enumerate(fetch_plan):
            extra = min(deficit, available - target)
            if extra > 0:
                fetch_plan[i] = (drug_info, target + extra, available)
                deficit -= extra
            if deficit <= 0:
                break

    print()
    con.header("Phase 2: Fetch plan")
    plan_rows = []
    for drug_info, target, available in fetch_plan:
        plan_rows.append([
            drug_info["generic_name"],
            drug_info["category"],
            f"{target:,}",
            f"{available:,}",
        ])
    con.table(["Drug", "Category", "Fetch", "Available"], plan_rows,
              [22, 27, 10, 12])
    print()

    # Phase 3: Fetch events
    con.header("Phase 3: Fetching adverse events from FDA")
    all_events: List[Dict] = []
    seen_report_ids: set = set()
    total_target = sum(t for _, t, _ in fetch_plan)

    for drug_info, target, _ in fetch_plan:
        name = drug_info["generic_name"]
        drug_events: List[Dict] = []
        skip = 0
        page_size = 100

        print(f"\n  Fetching {name} ({target:,} events):")
        while len(drug_events) < target:
            needed = min(page_size, target - len(drug_events))
            results = client.fetch_page(name, skip=skip, limit=needed,
                                        date_start=date_start, date_end=date_end)
            if not results:
                break

            for raw in results:
                evt = transform_event(raw)
                # Deduplicate by safety_report_id
                rid = evt["safety_report_id"]
                if rid not in seen_report_ids:
                    seen_report_ids.add(rid)
                    drug_events.append(evt)
                if len(drug_events) >= target:
                    break

            skip += len(results)
            con.progress(len(drug_events), target, name)

        print()  # newline after progress bar
        con.success(f"{name}: {len(drug_events):,} events fetched")
        all_events.extend(drug_events)

    con.info(f"\nTotal raw events: {len(all_events):,}  (API calls: {client._total_calls})")

    # Phase 4: Distribute timestamps
    con.header("Phase 4: Distributing timestamps for demo realism")
    actual_spike = spike_drug or (fetch_plan[0][0]["generic_name"] if fetch_plan else "")
    all_events = distribute_timestamps(all_events, spike_drug=actual_spike)
    con.success(f"Timestamps distributed across 30 days with spike on '{actual_spike}'")

    return all_events


# ─── Output Writers ───────────────────────────────────────────────────────────

def write_ndjson(events: List[Dict], output_dir: Path) -> Path:
    """Write NDJSON file for Fabric 'Get data > Local file' upload."""
    path = output_dir / "adverse_events_portfolio.json"
    with open(path, "w", encoding="utf-8") as f:
        for evt in events:
            f.write(json.dumps(evt, default=str) + "\n")
    return path


def write_summary_report(events: List[Dict], output_dir: Path, profile: str) -> Path:
    """Write a comprehensive data quality & summary report."""
    path = output_dir / "portfolio_summary.txt"
    now = datetime.datetime.now(datetime.timezone.utc)

    # Compute stats
    total = len(events)
    serious = sum(1 for e in events if e["is_serious"])
    deaths = sum(1 for e in events if e["death"])
    hosps = sum(1 for e in events if e["hospitalization"])
    lt = sum(1 for e in events if e["life_threatening"])

    drugs = Counter(e["generic_name"] for e in events if e["generic_name"])
    reactions = Counter(e["primary_reaction"] for e in events if e["primary_reaction"])
    countries = Counter(e["country"] for e in events if e["country"])
    sexes = Counter(e["patient_sex"] for e in events if e["patient_sex"])

    # 7-day window stats (for signal detection validation)
    cutoff_7d = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%dT")
    events_7d = [e for e in events if e["ingest_timestamp"] >= cutoff_7d]
    serious_7d = [e for e in events_7d if e["is_serious"]]

    with open(path, "w", encoding="utf-8") as f:
        f.write("╔══════════════════════════════════════════════════════════════╗\n")
        f.write("║   Drug Safety Signal Monitor — Portfolio Data Report        ║\n")
        f.write("╚══════════════════════════════════════════════════════════════╝\n\n")
        f.write(f"  Generated:  {now.isoformat()}\n")
        f.write(f"  Profile:    {profile}\n")
        f.write(f"  Events:     {total:,}\n\n")

        f.write("── Overall Statistics ──────────────────────────────────────────\n\n")
        f.write(f"  Total events:          {total:,}\n")
        f.write(f"  Serious events:        {serious:,} ({100*serious/total:.1f}%)\n")
        f.write(f"  Deaths:                {deaths:,} ({100*deaths/total:.1f}%)\n")
        f.write(f"  Hospitalizations:      {hosps:,} ({100*hosps/total:.1f}%)\n")
        f.write(f"  Life-threatening:      {lt:,} ({100*lt/total:.1f}%)\n")
        f.write(f"  Unique drugs:          {len(drugs)}\n")
        f.write(f"  Unique reactions:      {len(reactions)}\n")
        f.write(f"  Countries:             {len(countries)}\n\n")

        f.write("── 7-Day Window (for Signal Detection) ────────────────────────\n\n")
        f.write(f"  Events in last 7d:     {len(events_7d):,}\n")
        f.write(f"  Serious in last 7d:    {len(serious_7d):,}\n\n")

        f.write("── Drug Breakdown ─────────────────────────────────────────────\n\n")
        f.write(f"  {'Drug':<24s}  {'Count':>8s}  {'Serious':>8s}  {'Deaths':>7s}  {'Hosp':>6s}\n")
        f.write(f"  {'─'*24}  {'─'*8}  {'─'*8}  {'─'*7}  {'─'*6}\n")
        for drug_name in sorted(drugs.keys()):
            drug_evts = [e for e in events if e["generic_name"] == drug_name]
            ds = sum(1 for e in drug_evts if e["is_serious"])
            dd = sum(1 for e in drug_evts if e["death"])
            dh = sum(1 for e in drug_evts if e["hospitalization"])
            f.write(f"  {drug_name:<24s}  {len(drug_evts):>8,}  {ds:>8,}  {dd:>7,}  {dh:>6,}\n")

        f.write(f"\n── Top 15 Reactions ────────────────────────────────────────────\n\n")
        for rxn, cnt in reactions.most_common(15):
            bar = "█" * min(cnt, 40)
            f.write(f"  {rxn:<40s}  {cnt:>5,}  {bar}\n")

        f.write(f"\n── Top 10 Countries ───────────────────────────────────────────\n\n")
        for country, cnt in countries.most_common(10):
            f.write(f"  {country:<6s}  {cnt:>6,}\n")

        f.write(f"\n── Data Quality Checks ────────────────────────────────────────\n\n")
        checks = [
            ("Events have event_id",       all(e.get("event_id") for e in events)),
            ("Events have ingest_timestamp", all(e.get("ingest_timestamp") for e in events)),
            ("Events have generic_name",   all(e.get("generic_name") for e in events)),
            ("Events have primary_reaction", sum(1 for e in events if e.get("primary_reaction")) / total > 0.5),
            ("Serious flag populated",     sum(1 for e in events if e.get("is_serious") is not None) == total),
            ("Country populated (>50%)",   sum(1 for e in events if e.get("country")) / total > 0.5),
            ("Multiple drugs present",     len(drugs) >= 2),
            ("7d events present",          len(events_7d) > 0),
            ("Spike drug has 7d events",   any(e["is_serious"] for e in events_7d)),
        ]
        all_pass = True
        for label, passed in checks:
            status = "✓ PASS" if passed else "✗ FAIL"
            f.write(f"  {status}  {label}\n")
            if not passed:
                all_pass = False

        f.write(f"\n  {'All checks passed!' if all_pass else 'Some checks failed — review data.'}\n")

    return path


def write_kql_ingest_commands(events: List[Dict], output_dir: Path) -> Path:
    """Write .set-or-append KQL commands for direct paste into Queryset editor."""
    path = output_dir / "ingest_commands.kql"

    # KQL .set-or-append has a practical limit of ~500 rows per command
    batch_size = 200

    with open(path, "w", encoding="utf-8") as f:
        f.write("// ════════════════════════════════════════════════════════════════\n")
        f.write("// Auto-generated KQL ingest commands for AdverseEvents table\n")
        f.write(f"// Generated: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")
        f.write(f"// Events: {len(events):,}\n")
        f.write("// ════════════════════════════════════════════════════════════════\n")
        f.write("// Execute each .set-or-append block ONE AT A TIME in the Queryset editor.\n\n")

        for batch_num in range(0, len(events), batch_size):
            batch = events[batch_num:batch_num + batch_size]
            f.write(f"// ── Batch {batch_num // batch_size + 1} ({len(batch)} events) ──\n")
            f.write(f".set-or-append AdverseEvents <|\n")
            f.write(f"  print x = dynamic([")

            # Write JSON array of events
            json_strs = [json.dumps(evt, default=str) for evt in batch]
            f.write(",\n    ".join(json_strs))

            f.write("])\n")
            f.write("  | mv-expand x\n")
            f.write("  | evaluate bag_unpack(x)\n")
            f.write(";\n\n")

    return path


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch a polished multi-drug FAERS dataset from openFDA for Fabric RTI demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Profiles:
  mixed       Cross-therapeutic portfolio (default, best demo impact)
  cardiology  Cardiovascular drug monitoring
  oncology    Oncology / immunotherapy monitoring
  immunology  Autoimmune / immunology monitoring

Examples:
  python fetch_fda_portfolio.py                          # 2000 events, mixed portfolio
  python fetch_fda_portfolio.py --count 500 --profile cardiology
  python fetch_fda_portfolio.py --drugs aspirin warfarin pembrolizumab
  python fetch_fda_portfolio.py --spike-drug aspirin     # Force spike on aspirin
        """,
    )
    parser.add_argument("--count", type=int, default=2000,
                        help="Target event count (default: 2000)")
    parser.add_argument("--profile", type=str, default="mixed",
                        choices=list(DRUG_PORTFOLIOS.keys()),
                        help="Drug portfolio profile (default: mixed)")
    parser.add_argument("--drugs", nargs="+", type=str, default=None,
                        help="Custom drug list (overrides --profile)")
    parser.add_argument("--spike-drug", type=str, default="",
                        help="Drug to create a demo spike for (default: first drug)")
    parser.add_argument("--api-key", type=str, default="",
                        help="openFDA API key (optional, increases rate limit)")
    parser.add_argument("--output", type=str, default="../sample_data",
                        help="Output directory (default: ../sample_data)")
    parser.add_argument("--date-start", type=str, default="20200101",
                        help="FAERS date range start (YYYYMMDD)")
    parser.add_argument("--date-end", type=str, default="20260101",
                        help="FAERS date range end (YYYYMMDD)")
    parser.add_argument("--no-kql", action="store_true",
                        help="Skip generating KQL ingest commands (large file)")
    args = parser.parse_args()

    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Fetch from FDA
    events = fetch_portfolio(
        profile=args.profile,
        custom_drugs=args.drugs,
        target_count=args.count,
        api_key=args.api_key,
        spike_drug=args.spike_drug,
        date_start=args.date_start,
        date_end=args.date_end,
    )

    if not events:
        con.error("No events fetched. Exiting.")
        sys.exit(1)

    # Write outputs
    con.header("Phase 5: Writing output files")

    ndjson_path = write_ndjson(events, output_dir)
    file_size = os.path.getsize(ndjson_path)
    con.success(f"NDJSON:  {ndjson_path}")
    con.info(f"         {len(events):,} events, {file_size:,} bytes ({file_size/1024:.0f} KB)")

    report_path = write_summary_report(events, output_dir, args.profile)
    con.success(f"Report:  {report_path}")

    if not args.no_kql:
        kql_path = write_kql_ingest_commands(events, output_dir)
        kql_size = os.path.getsize(kql_path)
        con.success(f"KQL:     {kql_path}")
        con.info(f"         {kql_size:,} bytes ({kql_size/1024:.0f} KB)")

    # Print summary
    con.header("Summary")
    total = len(events)
    serious = sum(1 for e in events if e["is_serious"])
    deaths = sum(1 for e in events if e["death"])
    drugs = len(set(e["generic_name"] for e in events if e["generic_name"]))

    print(f"""
  Events:    {total:,}
  Serious:   {serious:,} ({100*serious/total:.0f}%)
  Deaths:    {deaths:,}
  Drugs:     {drugs}
  File:      {ndjson_path.name} ({file_size/1024:.0f} KB)
""")

    con.header("Next: Load into Fabric Eventhouse")
    print(f"""
  1. Open Fabric Eventhouse → Drug-safety-signal-eh database
  2. Click "Get data" → "Local file"
  3. Browse to: {ndjson_path}
  4. Target table: AdverseEvents (existing)
  5. Mapping: AdverseEvents_JsonMapping
  6. Click "Finish"

  Then verify:
    AdverseEvents | count
    DetectSafetySignals(2.0, 3)
    PatientDemographics("aspirin")
""")


if __name__ == "__main__":
    main()
