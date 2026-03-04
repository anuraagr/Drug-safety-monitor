#!/usr/bin/env python3
"""
Drug Safety Signal Monitor — Eventhouse Schema Deployer
=========================================================
Connects to Fabric Eventhouse via Kusto SDK and deploys all KQL objects:

  • Staging table + ingestion mapping (raw Eventstream landing zone)
  • Update policy (auto-enrichment from staging → main table)
  • Quarantine table (bad records)
  • Materialized views (4 pre-aggregated analytics views)
  • Stored functions (5 signal detection & analytics functions)
  • Retention, caching, and streaming policies

This mimics a production pharmacovigilance pipeline:

    Eventstream  →  AdverseEvents_Raw (staging)
                        │
                        ├─ Update Policy → AdverseEvents (enriched)
                        │                     │
                        │                     ├─ SeriousEvents_7Day (mat. view)
                        │                     ├─ SeriousEvents_30Day (mat. view)
                        │                     ├─ DailyEventSummary (mat. view)
                        │                     └─ EventsByCountry (mat. view)
                        │
                        └─ (errors) → AdverseEvents_Quarantine

Prerequisites:
    pip install azure-kusto-data azure-identity

Usage:
    python deploy_to_eventhouse.py
    python deploy_to_eventhouse.py --skip-staging     # skip staging table / update policy
    python deploy_to_eventhouse.py --functions-only    # only deploy stored functions
    python deploy_to_eventhouse.py --verify-only       # run verification queries only
    python deploy_to_eventhouse.py --dry-run           # print KQL without executing

Author: Pharmacovigilance Demo — Microsoft Fabric RTI
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    os.environ.setdefault("PYTHONUTF8", "1")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass


# ─── Console helpers ──────────────────────────────────────────────────────────

class Console:
    @staticmethod
    def header(text: str):
        print(f"\n{'═'*64}")
        print(f"  {text}")
        print(f"{'═'*64}")

    @staticmethod
    def step(num: int, total: int, text: str):
        print(f"\n  [{num}/{total}] {text}")
        print(f"  {'─'*56}")

    @staticmethod
    def info(t: str):   print(f"    ℹ  {t}")
    @staticmethod
    def ok(t: str):     print(f"    ✓  {t}")
    @staticmethod
    def warn(t: str):   print(f"    ⚠  {t}")
    @staticmethod
    def err(t: str):    print(f"    ✗  {t}")
    @staticmethod
    def kql(t: str):    print(f"    ▸  {t[:100]}{'…' if len(t) > 100 else ''}")
    @staticmethod
    def skip(t: str):   print(f"    ○  {t} (skipped)")

con = Console()


# ─── Configuration ────────────────────────────────────────────────────────────

DEFAULT_CLUSTER_URI = "https://trd-yxvwsau7zstjcxcq7h.z0.kusto.fabric.microsoft.com"
DEFAULT_DATABASE    = "Drug-safety-signal-eh"
DEFAULT_TABLE       = "AdverseEvents"


def load_config(config_path: str) -> Dict[str, Any]:
    p = Path(config_path)
    if p.exists():
        with open(p) as f:
            raw = json.load(f)
        return {k: v for k, v in raw.items() if not k.startswith("//")}
    return {}


# ─── KQL Command Definitions ─────────────────────────────────────────────────
# Each step is a (name, kql_command) tuple.  The deployer runs them in order.

def get_staging_commands() -> List[Tuple[str, str]]:
    """Staging table, quarantine table, ingestion mapping, update policy."""
    return [
        # ── Staging table ─────────────────────────────────────────────
        ("Create staging table (AdverseEvents_Raw)", """\
.create-merge table AdverseEvents_Raw (
    raw_event_id:           string,
    safety_report_id:       string,
    receive_date:           string,
    is_serious:             string,
    hospitalization:        string,
    death:                  string,
    life_threatening:       string,
    disability:             string,
    congenital_anomaly:     string,
    required_intervention:  string,
    patient_age:            string,
    patient_age_unit:       string,
    patient_sex:            string,
    patient_weight:         string,
    drug_name:              string,
    generic_name:           string,
    brand_name:             string,
    drug_route:             string,
    drug_indication:        string,
    drug_characterization:  string,
    reaction_terms:         string,
    primary_reaction:       string,
    reaction_count:         string,
    country:                string,
    sender_organization:    string,
    report_type:            string,
    qualification:          string,
    raw_ingest_time:        datetime
)"""),

        # ── Quarantine table ──────────────────────────────────────────
        ("Create quarantine table (AdverseEvents_Quarantine)", """\
.create-merge table AdverseEvents_Quarantine (
    raw_event_id:       string,
    safety_report_id:   string,
    generic_name:       string,
    error_reason:       string,
    raw_payload:        string,
    quarantine_time:    datetime
)"""),

        # ── Staging ingestion mapping ─────────────────────────────────
        ("Create staging JSON mapping (AdverseEvents_Raw_JsonMapping)", """\
.create-or-alter table AdverseEvents_Raw ingestion json mapping 'AdverseEvents_Raw_JsonMapping'
```
[
    {"column": "raw_event_id",          "path": "$.event_id",              "datatype": "string"},
    {"column": "safety_report_id",      "path": "$.safety_report_id",      "datatype": "string"},
    {"column": "receive_date",          "path": "$.receive_date",          "datatype": "string"},
    {"column": "is_serious",            "path": "$.is_serious",            "datatype": "string"},
    {"column": "hospitalization",       "path": "$.hospitalization",       "datatype": "string"},
    {"column": "death",                 "path": "$.death",                 "datatype": "string"},
    {"column": "life_threatening",      "path": "$.life_threatening",      "datatype": "string"},
    {"column": "disability",            "path": "$.disability",            "datatype": "string"},
    {"column": "congenital_anomaly",    "path": "$.congenital_anomaly",    "datatype": "string"},
    {"column": "required_intervention", "path": "$.required_intervention", "datatype": "string"},
    {"column": "patient_age",           "path": "$.patient_age",           "datatype": "string"},
    {"column": "patient_age_unit",      "path": "$.patient_age_unit",      "datatype": "string"},
    {"column": "patient_sex",           "path": "$.patient_sex",           "datatype": "string"},
    {"column": "patient_weight",        "path": "$.patient_weight",        "datatype": "string"},
    {"column": "drug_name",             "path": "$.drug_name",             "datatype": "string"},
    {"column": "generic_name",          "path": "$.generic_name",          "datatype": "string"},
    {"column": "brand_name",            "path": "$.brand_name",            "datatype": "string"},
    {"column": "drug_route",            "path": "$.drug_route",            "datatype": "string"},
    {"column": "drug_indication",       "path": "$.drug_indication",       "datatype": "string"},
    {"column": "drug_characterization", "path": "$.drug_characterization", "datatype": "string"},
    {"column": "reaction_terms",        "path": "$.reaction_terms",        "datatype": "string"},
    {"column": "primary_reaction",      "path": "$.primary_reaction",      "datatype": "string"},
    {"column": "reaction_count",        "path": "$.reaction_count",        "datatype": "string"},
    {"column": "country",               "path": "$.country",               "datatype": "string"},
    {"column": "sender_organization",   "path": "$.sender_organization",   "datatype": "string"},
    {"column": "report_type",           "path": "$.report_type",           "datatype": "string"},
    {"column": "qualification",         "path": "$.qualification",         "datatype": "string"}
]
```"""),

        # ── Staging streaming ingestion policy ────────────────────────
        ("Enable streaming ingestion on staging table", """\
.alter table AdverseEvents_Raw policy streamingingestion enable"""),

        # ── Staging retention policy (short-lived) ────────────────────
        ("Set staging table retention (7d — ephemeral landing zone)", """\
.alter table AdverseEvents_Raw policy retention
```
{
    "SoftDeletePeriod": "7.00:00:00",
    "Recoverability": "Disabled"
}
```"""),

        # ── Transformation function for update policy ─────────────────
        ("Create transformation function (TransformRawAdverseEvent)", """\
.create-or-alter function with (folder="Pipeline", docstring="Transforms raw staging records into enriched AdverseEvents rows")
TransformRawAdverseEvent()
{
    AdverseEvents_Raw
    | extend
        event_id            = coalesce(raw_event_id, new_guid()),
        safety_report_id    = safety_report_id,
        receive_date        = todatetime(receive_date),
        ingest_timestamp    = coalesce(raw_ingest_time, now()),
        is_serious          = tobool(is_serious),
        hospitalization     = tobool(hospitalization),
        death               = tobool(death),
        life_threatening    = tobool(life_threatening),
        disability          = tobool(disability),
        congenital_anomaly  = tobool(congenital_anomaly),
        required_intervention = tobool(required_intervention),
        patient_age         = patient_age,
        patient_age_unit    = patient_age_unit,
        patient_sex         = patient_sex,
        patient_weight      = patient_weight,
        drug_name           = drug_name,
        generic_name        = tolower(trim(' ', generic_name)),
        brand_name          = brand_name,
        drug_route          = drug_route,
        drug_indication     = drug_indication,
        drug_characterization = drug_characterization,
        reaction_terms      = reaction_terms,
        primary_reaction    = trim(' ', primary_reaction),
        reaction_count      = toint(reaction_count),
        country             = toupper(trim(' ', country)),
        sender_organization = sender_organization,
        report_type         = report_type,
        qualification       = qualification
    | project
        event_id, safety_report_id, receive_date, ingest_timestamp,
        is_serious, hospitalization, death, life_threatening,
        disability, congenital_anomaly, required_intervention,
        patient_age, patient_age_unit, patient_sex, patient_weight,
        drug_name, generic_name, brand_name, drug_route,
        drug_indication, drug_characterization, reaction_terms,
        primary_reaction, reaction_count, country,
        sender_organization, report_type, qualification
}"""),

        # ── Update policy on AdverseEvents (pulls from staging) ───────
        ("Set update policy on AdverseEvents (auto-transform from staging)", """\
.alter table AdverseEvents policy update
```
[
    {
        "IsEnabled": true,
        "Source": "AdverseEvents_Raw",
        "Query": "TransformRawAdverseEvent()",
        "IsTransactional": false,
        "PropagateIngestionProperties": true,
        "ManagedIdentity": null
    }
]
```"""),
    ]


def get_policy_commands() -> List[Tuple[str, str]]:
    """Retention, caching, and streaming policies for the main table."""
    return [
        ("Set retention policy (365d) on AdverseEvents", """\
.alter table AdverseEvents policy retention
```
{
    "SoftDeletePeriod": "365.00:00:00",
    "Recoverability": "Enabled"
}
```"""),

        ("Set caching policy (90d hot) on AdverseEvents", """\
.alter table AdverseEvents policy caching
    hot = 90d"""),

        ("Enable streaming ingestion on AdverseEvents", """\
.alter table AdverseEvents policy streamingingestion enable"""),
    ]


def get_ingestion_mapping_commands() -> List[Tuple[str, str]]:
    """Ingestion mappings for the main table (JSON + CSV)."""
    return [
        ("Create/update JSON mapping (AdverseEvents_JsonMapping)", """\
.create-or-alter table AdverseEvents ingestion json mapping 'AdverseEvents_JsonMapping'
```
[
    {"column": "event_id",              "path": "$.event_id",              "datatype": "string"},
    {"column": "safety_report_id",      "path": "$.safety_report_id",      "datatype": "string"},
    {"column": "receive_date",          "path": "$.receive_date",          "datatype": "datetime"},
    {"column": "ingest_timestamp",      "path": "$.ingest_timestamp",      "datatype": "datetime"},
    {"column": "is_serious",            "path": "$.is_serious",            "datatype": "bool"},
    {"column": "hospitalization",       "path": "$.hospitalization",       "datatype": "bool"},
    {"column": "death",                 "path": "$.death",                 "datatype": "bool"},
    {"column": "life_threatening",      "path": "$.life_threatening",      "datatype": "bool"},
    {"column": "disability",            "path": "$.disability",            "datatype": "bool"},
    {"column": "congenital_anomaly",    "path": "$.congenital_anomaly",    "datatype": "bool"},
    {"column": "required_intervention", "path": "$.required_intervention", "datatype": "bool"},
    {"column": "patient_age",           "path": "$.patient_age",           "datatype": "string"},
    {"column": "patient_age_unit",      "path": "$.patient_age_unit",      "datatype": "string"},
    {"column": "patient_sex",           "path": "$.patient_sex",           "datatype": "string"},
    {"column": "patient_weight",        "path": "$.patient_weight",        "datatype": "string"},
    {"column": "drug_name",             "path": "$.drug_name",             "datatype": "string"},
    {"column": "generic_name",          "path": "$.generic_name",          "datatype": "string"},
    {"column": "brand_name",            "path": "$.brand_name",            "datatype": "string"},
    {"column": "drug_route",            "path": "$.drug_route",            "datatype": "string"},
    {"column": "drug_indication",       "path": "$.drug_indication",       "datatype": "string"},
    {"column": "drug_characterization", "path": "$.drug_characterization", "datatype": "string"},
    {"column": "reaction_terms",        "path": "$.reaction_terms",        "datatype": "string"},
    {"column": "primary_reaction",      "path": "$.primary_reaction",      "datatype": "string"},
    {"column": "reaction_count",        "path": "$.reaction_count",        "datatype": "int"},
    {"column": "country",               "path": "$.country",               "datatype": "string"},
    {"column": "sender_organization",   "path": "$.sender_organization",   "datatype": "string"},
    {"column": "report_type",           "path": "$.report_type",           "datatype": "string"},
    {"column": "qualification",         "path": "$.qualification",         "datatype": "string"}
]
```"""),

        ("Create CSV mapping (AdverseEvents_CsvMapping)", """\
.create-or-alter table AdverseEvents ingestion csv mapping 'AdverseEvents_CsvMapping'
```
[
    {"column": "event_id",              "ordinal": 0,  "datatype": "string"},
    {"column": "safety_report_id",      "ordinal": 1,  "datatype": "string"},
    {"column": "receive_date",          "ordinal": 2,  "datatype": "datetime"},
    {"column": "ingest_timestamp",      "ordinal": 3,  "datatype": "datetime"},
    {"column": "is_serious",            "ordinal": 4,  "datatype": "bool"},
    {"column": "hospitalization",       "ordinal": 5,  "datatype": "bool"},
    {"column": "death",                 "ordinal": 6,  "datatype": "bool"},
    {"column": "life_threatening",      "ordinal": 7,  "datatype": "bool"},
    {"column": "disability",            "ordinal": 8,  "datatype": "bool"},
    {"column": "congenital_anomaly",    "ordinal": 9,  "datatype": "bool"},
    {"column": "required_intervention", "ordinal": 10, "datatype": "bool"},
    {"column": "patient_age",           "ordinal": 11, "datatype": "string"},
    {"column": "patient_age_unit",      "ordinal": 12, "datatype": "string"},
    {"column": "patient_sex",           "ordinal": 13, "datatype": "string"},
    {"column": "patient_weight",        "ordinal": 14, "datatype": "string"},
    {"column": "drug_name",             "ordinal": 15, "datatype": "string"},
    {"column": "generic_name",          "ordinal": 16, "datatype": "string"},
    {"column": "brand_name",            "ordinal": 17, "datatype": "string"},
    {"column": "drug_route",            "ordinal": 18, "datatype": "string"},
    {"column": "drug_indication",       "ordinal": 19, "datatype": "string"},
    {"column": "drug_characterization", "ordinal": 20, "datatype": "string"},
    {"column": "reaction_terms",        "ordinal": 21, "datatype": "string"},
    {"column": "primary_reaction",      "ordinal": 22, "datatype": "string"},
    {"column": "reaction_count",        "ordinal": 23, "datatype": "int"},
    {"column": "country",               "ordinal": 24, "datatype": "string"},
    {"column": "sender_organization",   "ordinal": 25, "datatype": "string"},
    {"column": "report_type",           "ordinal": 26, "datatype": "string"},
    {"column": "qualification",         "ordinal": 27, "datatype": "string"}
]
```"""),
    ]


def get_materialized_view_commands() -> List[Tuple[str, str]]:
    """Materialized views for pre-aggregated analytics."""
    return [
        ("Create/update materialized view: SeriousEvents_7Day", """\
.create-or-alter materialized-view with (backfill=true) SeriousEvents_7Day on table AdverseEvents
{
    AdverseEvents
    | where is_serious == true
    | where ingest_timestamp > ago(7d)
    | summarize
        event_count            = count(),
        hospitalization_count  = countif(hospitalization),
        death_count            = countif(death),
        life_threatening_count = countif(life_threatening),
        latest_event           = max(ingest_timestamp),
        earliest_event         = min(ingest_timestamp),
        distinct_reports       = dcount(safety_report_id)
      by generic_name, primary_reaction
}"""),

        ("Create/update materialized view: SeriousEvents_30Day", """\
.create-or-alter materialized-view with (backfill=true) SeriousEvents_30Day on table AdverseEvents
{
    AdverseEvents
    | where is_serious == true
    | where ingest_timestamp > ago(30d)
    | summarize
        event_count            = count(),
        hospitalization_count  = countif(hospitalization),
        death_count            = countif(death),
        life_threatening_count = countif(life_threatening),
        latest_event           = max(ingest_timestamp),
        earliest_event         = min(ingest_timestamp),
        distinct_reports       = dcount(safety_report_id)
      by generic_name, primary_reaction
}"""),

        ("Create/update materialized view: DailyEventSummary", """\
.create-or-alter materialized-view with (backfill=true) DailyEventSummary on table AdverseEvents
{
    AdverseEvents
    | summarize
        total_events        = count(),
        serious_events      = countif(is_serious),
        hospitalization_count = countif(hospitalization),
        death_count         = countif(death),
        distinct_drugs      = dcount(generic_name),
        distinct_reactions  = dcount(primary_reaction)
      by event_date = bin(ingest_timestamp, 1d)
}"""),

        ("Create/update materialized view: EventsByCountry", """\
.create-or-alter materialized-view with (backfill=true) EventsByCountry on table AdverseEvents
{
    AdverseEvents
    | where isnotempty(country)
    | summarize
        total_events    = count(),
        serious_events  = countif(is_serious),
        death_count     = countif(death)
      by country
}"""),
    ]


def get_function_commands() -> List[Tuple[str, str]]:
    """All 5 stored functions."""
    return [
        # ── Function 1: DetectSafetySignals ───────────────────────────
        ("Create/update function: DetectSafetySignals", """\
.create-or-alter function with (folder="SignalDetection", docstring="Detect drug+reaction safety signal spikes")
DetectSafetySignals(spike_multiplier: real = 2.0, min_event_count: int = 5)
{
    let recent =
        AdverseEvents
        | where is_serious == true
        | where ingest_timestamp > ago(7d)
        | summarize
            event_count = count(),
            hospitalization_count = countif(hospitalization),
            death_count = countif(death),
            life_threatening_count = countif(life_threatening),
            latest_event = max(ingest_timestamp),
            distinct_reports = dcount(safety_report_id)
          by generic_name, primary_reaction
        | project generic_name, primary_reaction, recent_count = event_count,
                  recent_hosp = hospitalization_count, recent_death = death_count,
                  latest_event;
    let baseline =
        AdverseEvents
        | where is_serious == true
        | where ingest_timestamp > ago(30d)
        | summarize
            event_count = count(),
            hospitalization_count = countif(hospitalization),
            death_count = countif(death)
          by generic_name, primary_reaction
        | project generic_name, primary_reaction, baseline_count = event_count,
                  baseline_hosp = hospitalization_count, baseline_death = death_count;
    recent
    | join kind=inner baseline on generic_name, primary_reaction
    | extend expected_7d = (todouble(baseline_count) / 30.0) * 7.0
    | extend spike_ratio = iff(expected_7d > 0, todouble(recent_count) / expected_7d, 0.0)
    | where recent_count >= min_event_count
    | where spike_ratio >= spike_multiplier
    | project
        generic_name,
        primary_reaction,
        recent_7d_count      = recent_count,
        baseline_30d_count   = baseline_count,
        expected_7d_count    = round(expected_7d, 1),
        spike_ratio          = round(spike_ratio, 2),
        recent_deaths        = recent_death,
        recent_hospitalizations = recent_hosp,
        latest_event,
        signal_severity      = case(
            recent_death > 0, "CRITICAL",
            recent_hosp > 0,  "HIGH",
            "MEDIUM"
        )
    | order by spike_ratio desc, recent_7d_count desc
}"""),

        # ── Function 2: PatientDemographics ───────────────────────────
        ("Create/update function: PatientDemographics", """\
.create-or-alter function with (folder="Analytics", docstring="Patient demographics for a given drug")
PatientDemographics(drug_generic_name: string)
{
    AdverseEvents
    | where generic_name =~ drug_generic_name
    | where ingest_timestamp > ago(90d)
    | extend age_numeric = todouble(patient_age)
    | extend age_group = case(
        age_numeric <= 17,  "Pediatric (0-17)",
        age_numeric <= 44,  "Adult (18-44)",
        age_numeric <= 64,  "Middle-aged (45-64)",
        age_numeric <= 84,  "Elderly (65-84)",
        age_numeric > 84,   "Very elderly (85+)",
        "Unknown"
    )
    | extend sex_label = case(
        patient_sex == "1", "Male",
        patient_sex == "2", "Female",
        "Unknown"
    )
    | summarize event_count = count(), serious_count = countif(is_serious)
      by age_group, sex_label
    | order by age_group asc, sex_label asc
}"""),

        # ── Function 3: DrugComparisonReport ──────────────────────────
        ("Create/update function: DrugComparisonReport", """\
.create-or-alter function with (folder="Analytics", docstring="Head-to-head safety comparison of two drugs")
DrugComparisonReport(drug_a: string, drug_b: string)
{
    let drugs = pack_array(drug_a, drug_b);
    AdverseEvents
    | where ingest_timestamp > ago(90d)
    | where generic_name in~ (drugs)
    | summarize
        Total_Events       = count(),
        Serious            = countif(is_serious),
        Deaths             = countif(death),
        Hospitalizations   = countif(hospitalization),
        Life_Threatening   = countif(life_threatening),
        Disabilities       = countif(disability),
        Unique_Reactions   = dcount(primary_reaction),
        Countries          = dcount(country),
        Physician_Reports  = countif(qualification == "1"),
        Consumer_Reports   = countif(qualification == "5"),
        Latest_Report      = max(ingest_timestamp),
        Avg_Age            = round(avgif(todouble(patient_age), patient_age_unit == "801" and isnotempty(patient_age)), 1)
      by Drug = generic_name
    | extend
        Serious_Rate  = strcat(tostring(round(100.0 * Serious / Total_Events, 1)), "%"),
        Death_Rate    = strcat(tostring(round(100.0 * Deaths / Total_Events, 2)), "%"),
        Physician_Pct = strcat(tostring(round(100.0 * Physician_Reports / Total_Events, 0)), "%")
    | project
        Drug, Total_Events, Serious, Serious_Rate, Deaths, Death_Rate,
        Hospitalizations, Life_Threatening, Unique_Reactions, Countries,
        Physician_Pct, Avg_Age, Latest_Report
    | order by Drug asc
}"""),

        # ── Function 4: ReactionTimeline ──────────────────────────────
        ("Create/update function: ReactionTimeline", """\
.create-or-alter function with (folder="Analytics", docstring="Daily reaction counts for a drug over a configurable window")
ReactionTimeline(drug_generic_name: string, window: timespan = 30d)
{
    AdverseEvents
    | where ingest_timestamp > ago(window)
    | where generic_name =~ drug_generic_name
    | where isnotempty(primary_reaction)
    | summarize Events = count()
      by Reaction = primary_reaction, Day = bin(ingest_timestamp, 1d)
    | order by Day asc, Events desc
}"""),

        # ── Function 5: RiskScoreCard ─────────────────────────────────
        ("Create/update function: RiskScoreCard", """\
.create-or-alter function with (folder="SignalDetection", docstring="Composite pharmacovigilance risk score for a drug")
RiskScoreCard(drug_generic_name: string)
{
    let stats = AdverseEvents
        | where ingest_timestamp > ago(90d)
        | where generic_name =~ drug_generic_name
        | summarize
            total         = count(),
            serious       = countif(is_serious),
            deaths        = countif(death),
            hosp          = countif(hospitalization),
            lt            = countif(life_threatening),
            reactions     = dcount(primary_reaction),
            countries     = dcount(country),
            latest        = max(ingest_timestamp),
            recent_7d     = countif(ingest_timestamp > ago(7d)),
            recent_30d    = countif(ingest_timestamp > ago(30d));
    let signals = DetectSafetySignals(2.0, 3)
        | where generic_name =~ drug_generic_name
        | summarize
            active_signals = count(),
            max_spike      = max(spike_ratio);
    stats
    | extend
        seriousness_score = round(todouble(serious) / total * 100.0, 1),
        mortality_score   = round(iff(total > 0, todouble(deaths) / total * 1000.0, 0.0), 1),
        velocity_score    = round(iff(recent_30d > 0, todouble(recent_7d) / recent_30d * 100.0 * (30.0/7.0), 0.0), 1),
        breadth_score     = round(todouble(reactions) * 2.0, 1)
    | extend
        mortality_score = iff(mortality_score > 100, 100.0, mortality_score),
        velocity_score  = iff(velocity_score > 100, 100.0, velocity_score),
        breadth_score   = iff(breadth_score > 100, 100.0, breadth_score)
    | extend
        composite_risk = round(
            seriousness_score * 0.30 +
            mortality_score   * 0.30 +
            velocity_score    * 0.25 +
            breadth_score     * 0.15
        , 1)
    | extend
        risk_level = case(
            composite_risk >= 70, "CRITICAL",
            composite_risk >= 50, "HIGH",
            composite_risk >= 30, "ELEVATED",
            "NORMAL"
        )
    | project
        Drug             = drug_generic_name,
        Risk_Level       = risk_level,
        Composite_Score  = composite_risk,
        Seriousness_Pct  = seriousness_score,
        Mortality_Per1k  = mortality_score,
        Velocity_Score   = velocity_score,
        Reaction_Breadth = breadth_score,
        Total_Events_90d = total,
        Deaths           = deaths,
        Hospitalizations = hosp,
        Active_Reactions = reactions,
        Countries        = countries,
        Latest_Report    = latest
    | take 1
}"""),
    ]


def get_verification_queries() -> List[Tuple[str, str]]:
    """Queries to verify the deployment succeeded."""
    return [
        ("Row count", "AdverseEvents | count"),
        ("Table schema", ".show table AdverseEvents schema as json | project Schema"),
        ("Ingestion mappings", ".show table AdverseEvents ingestion mappings | project Name, Kind"),
        ("Staging table exists", ".show tables | where TableName == 'AdverseEvents_Raw' | project TableName"),
        ("Quarantine table exists", ".show tables | where TableName == 'AdverseEvents_Quarantine' | project TableName"),
        ("Update policies", ".show table AdverseEvents policy update | project IsEnabled, Source, Query"),
        ("Materialized views", ".show materialized-views | project Name, MaterializedTo, IsHealthy"),
        ("Stored functions", ".show functions | project Name, Folder, DocString"),
        ("Retention policy", ".show table AdverseEvents policy retention | project SoftDeletePeriod"),
        ("Caching policy", ".show table AdverseEvents policy caching | project DataHotSpan"),
        ("Streaming policy", ".show table AdverseEvents policy streamingingestion | project IsEnabled"),
        ("Top 5 drugs", "AdverseEvents | summarize c=count() by generic_name | top 5 by c desc"),
        ("Signal check", "DetectSafetySignals(2.0, 3) | count"),
    ]


# ─── Deployment Engine ────────────────────────────────────────────────────────

def execute_kql(client, database: str, command: str, step_name: str,
                dry_run: bool = False) -> Tuple[bool, str]:
    """Execute a single KQL management command. Returns (success, detail)."""
    if dry_run:
        con.kql(command.split("\n")[0])
        return True, "dry-run"

    try:
        result = client.execute(database, command)
        # Try to extract something useful from the result
        detail = ""
        if result.primary_results and len(result.primary_results) > 0:
            rows = result.primary_results[0]
            if len(rows) > 0:
                first_row = rows[0]
                detail = str(dict(first_row))[:120] if hasattr(first_row, '__iter__') else str(first_row)[:120]
        return True, detail
    except Exception as e:
        error_msg = str(e)
        # Some expected "errors" aren't really errors
        if "already exists" in error_msg.lower():
            return True, "already exists (OK)"
        if "identical" in error_msg.lower():
            return True, "no changes needed"
        return False, error_msg[:200]


def deploy(
    cluster_uri: str,
    database: str,
    dry_run: bool = False,
    skip_staging: bool = False,
    functions_only: bool = False,
    verify_only: bool = False,
    auth_method: str = "interactive",
):
    """Main deployment orchestrator."""

    # ── Authenticate ──────────────────────────────────────────────
    con.header("Drug Safety Signal Monitor — Eventhouse Deployer")
    con.info(f"Cluster:   {cluster_uri}")
    con.info(f"Database:  {database}")
    con.info(f"Mode:      {'DRY RUN' if dry_run else 'VERIFY ONLY' if verify_only else 'LIVE DEPLOY'}")

    if dry_run:
        con.info("Dry run — KQL commands will be printed but NOT executed.")
        client = None
    else:
        try:
            from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
            from azure.identity import InteractiveBrowserCredential, AzureCliCredential, DefaultAzureCredential
        except ImportError:
            con.err("Missing packages. Run:  pip install azure-kusto-data azure-identity")
            sys.exit(1)

        con.info(f"Auth:      {auth_method}")
        if auth_method == "interactive":
            credential = InteractiveBrowserCredential()
            con.info("Opening browser for authentication...")
            credential.get_token("https://kusto.kusto.windows.net/.default")
            con.ok("Authenticated via interactive browser")
        elif auth_method == "cli":
            credential = AzureCliCredential()
        else:
            credential = DefaultAzureCredential()

        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster_uri, credential)
        client = KustoClient(kcsb)

    # ── Build command plan ────────────────────────────────────────
    plan: List[Tuple[str, str]] = []

    if verify_only:
        plan = get_verification_queries()
    elif functions_only:
        plan = get_function_commands()
    else:
        # Full deployment order:
        #  1. Ingestion mappings (main table)
        #  2. Policies (retention, caching, streaming)
        #  3. Staging table + quarantine + update policy
        #  4. Materialized views
        #  5. Stored functions
        plan.extend(get_ingestion_mapping_commands())
        plan.extend(get_policy_commands())
        if not skip_staging:
            plan.extend(get_staging_commands())
        plan.extend(get_materialized_view_commands())
        plan.extend(get_function_commands())

    total_steps = len(plan)
    succeeded = 0
    failed = 0
    skipped = 0

    for i, (name, kql) in enumerate(plan, 1):
        con.step(i, total_steps, name)

        if client is None and not dry_run:
            con.skip(name)
            skipped += 1
            continue

        success, detail = execute_kql(client, database, kql, name, dry_run=dry_run)

        if success:
            con.ok(f"{name}")
            if detail and detail != "dry-run":
                con.info(detail)
            succeeded += 1
        else:
            con.err(f"{name}")
            con.info(f"Error: {detail}")
            failed += 1

    # ── Run verification queries (always, unless dry-run) ─────────
    if not dry_run and not verify_only and client is not None:
        con.header("Post-Deployment Verification")
        verify_plan = get_verification_queries()
        for i, (name, kql) in enumerate(verify_plan, 1):
            con.step(i, len(verify_plan), f"Verify: {name}")
            success, detail = execute_kql(client, database, kql, name)
            if success:
                con.ok(detail if detail else "OK")
            else:
                con.warn(detail)

    # ── Summary ───────────────────────────────────────────────────
    con.header("Deployment Summary")
    con.info(f"Total steps:  {total_steps}")
    con.ok(f"Succeeded:    {succeeded}")
    if failed:
        con.err(f"Failed:       {failed}")
    if skipped:
        con.warn(f"Skipped:      {skipped}")

    if failed == 0:
        con.ok("All steps completed successfully!")
        print()
        con.info("Next steps:")
        con.info("  1. Open Fabric portal → Eventhouse → Query Editor")
        con.info("  2. Run:  DetectSafetySignals(2.0, 3)")
        con.info("  3. Run:  RiskScoreCard(\"aspirin\")")
        con.info("  4. Run:  DrugComparisonReport(\"aspirin\", \"pembrolizumab\")")
        con.info("  5. Build Real-Time Dashboard with 14 tiles (see DASHBOARD_LAYOUT.md)")
        if not skip_staging:
            print()
            con.info("Pipeline architecture (staging → enrichment → analytics):")
            con.info("  Eventstream → AdverseEvents_Raw (staging, 7d retention)")
            con.info("                    ↓  TransformRawAdverseEvent() [update policy]")
            con.info("                AdverseEvents (enriched, 365d retention)")
            con.info("                    ↓")
            con.info("                Materialized Views → Dashboard Tiles")
    else:
        con.warn(f"{failed} steps failed. Review errors above and re-run.")

    if client is not None:
        client.close()

    return failed == 0


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Deploy KQL objects to Fabric Eventhouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python deploy_to_eventhouse.py                    # Full deployment
  python deploy_to_eventhouse.py --dry-run          # Preview KQL without executing
  python deploy_to_eventhouse.py --functions-only   # Deploy only stored functions
  python deploy_to_eventhouse.py --skip-staging     # Skip staging table setup
  python deploy_to_eventhouse.py --verify-only      # Only run verification queries

Pipeline architecture:
  Eventstream → AdverseEvents_Raw (staging) → [update policy] → AdverseEvents
                                                                    ↓
                                                             Materialized Views
                                                                    ↓
                                                             Dashboard Tiles
""")
    parser.add_argument("--cluster-uri", type=str, default="",
                        help="Eventhouse Query URI (default: from config.json)")
    parser.add_argument("--database", type=str, default="",
                        help="Database name (default: from config.json)")
    parser.add_argument("--auth", type=str, choices=["interactive", "cli", "default"],
                        default="interactive", help="Auth method (default: interactive)")
    parser.add_argument("--config", type=str,
                        default=str(Path(__file__).parent / ".." / "docs" / "config.json"),
                        help="Path to config.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print KQL commands without executing")
    parser.add_argument("--skip-staging", action="store_true",
                        help="Skip staging table and update policy creation")
    parser.add_argument("--functions-only", action="store_true",
                        help="Only deploy stored functions")
    parser.add_argument("--verify-only", action="store_true",
                        help="Only run verification queries")
    args = parser.parse_args()

    # Resolve cluster URI and database from config
    cluster_uri = args.cluster_uri
    database = args.database

    if not cluster_uri or not database:
        cfg = load_config(str(Path(args.config).resolve()))
        eh_cfg = cfg.get("fabric_eventhouse", {})
        if not cluster_uri:
            cluster_uri = eh_cfg.get("cluster_uri", DEFAULT_CLUSTER_URI)
        if not database:
            database = eh_cfg.get("database_name", DEFAULT_DATABASE)

    if not cluster_uri or "<YOUR_" in cluster_uri:
        con.err("Eventhouse cluster URI not configured.")
        con.info("Provide via --cluster-uri or update config.json")
        sys.exit(1)

    success = deploy(
        cluster_uri=cluster_uri,
        database=database,
        dry_run=args.dry_run,
        skip_staging=args.skip_staging,
        functions_only=args.functions_only,
        verify_only=args.verify_only,
        auth_method=args.auth,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
