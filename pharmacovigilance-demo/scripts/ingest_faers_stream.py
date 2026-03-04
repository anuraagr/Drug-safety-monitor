#!/usr/bin/env python3
"""
Drug Safety Signal Monitor — openFDA FAERS Ingestion Script
============================================================
Fetches adverse event reports from the openFDA FAERS API, transforms them into
a streaming-friendly JSON schema, and publishes batches to a Microsoft Fabric
Eventstream (Event Hub compatible) endpoint.

Key behaviors:
  • Parameterizable drug filter via config.json (generic_name / brand_name).
  • Respects openFDA rate limits (40 req/min without key, 240 with key).
  • Each Event Hub message payload is kept under 1 MB (Fabric Eventstream limit).
  • In "simulate_realtime" mode, historical records are replayed with fresh
    ISO 8601 timestamps so Fabric treats them as live streaming data.
  • Comprehensive logging, retry logic, and graceful shutdown (Ctrl+C).

Usage:
    python ingest_faers_stream.py --config ../docs/config.json [--dry-run]

Requires:
    pip install azure-eventhub requests

Author: Pharmacovigilance Demo — Microsoft Fabric RTI
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import signal
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Conditional import — allow dry-run mode without azure-eventhub installed
# ---------------------------------------------------------------------------
try:
    from azure.eventhub import EventData, EventHubProducerClient
    EVENTHUB_AVAILABLE = True
except ImportError:
    EVENTHUB_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("faers_ingest")

# ---------------------------------------------------------------------------
# Graceful shutdown flag
# ---------------------------------------------------------------------------
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle Ctrl+C for graceful shutdown."""
    global _shutdown_requested
    logger.warning("Shutdown requested (signal %s). Finishing current batch…", signum)
    _shutdown_requested = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------------------------------------------------------------------------
# Configuration loader
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> Dict[str, Any]:
    """Load and validate the config.json file."""
    path = Path(config_path)
    if not path.exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as fh:
        # Strip comment keys (keys starting with "//")
        raw = json.load(fh)
    config = {k: v for k, v in raw.items() if not k.startswith("//")}
    logger.info("Loaded configuration from %s", config_path)
    return config

# ---------------------------------------------------------------------------
# openFDA API client
# ---------------------------------------------------------------------------

class OpenFDAClient:
    """Thin wrapper around the openFDA Drug Adverse Events API."""

    def __init__(self, config: Dict[str, Any]):
        api_cfg = config["openfda_api"]
        self.base_url: str = api_cfg["base_url"]
        self.api_key: Optional[str] = api_cfg.get("api_key") or None
        self.max_per_request: int = api_cfg.get("max_records_per_request", 100)
        self.date_start: str = api_cfg.get("date_range_start", "20200101")
        self.date_end: str = api_cfg.get("date_range_end", "20260101")

        # Rate-limit management
        rpm = (
            api_cfg.get("rate_limit_with_key_per_minute", 240)
            if self.api_key
            else api_cfg.get("rate_limit_per_minute", 40)
        )
        self._min_interval = 60.0 / rpm  # seconds between calls
        self._last_call_ts: float = 0.0

        # Drug filter
        drug_cfg = config["drug_filter"]
        self.generic_name: str = drug_cfg.get("generic_name", "")
        self.brand_name: str = drug_cfg.get("brand_name", "")

        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    # ---- internal helpers ----

    def _build_search_clause(self) -> str:
        """
        Build the openFDA 'search' query parameter.

        Uses spaces between operators (AND, TO) — the requests library
        URL-encodes spaces as '+', which is exactly what the openFDA
        Elasticsearch backend expects. Using literal '+' would cause
        double-encoding (%2B) and a parse error.
        """
        clauses: List[str] = []
        clauses.append(
            f"receivedate:[{self.date_start} TO {self.date_end}]"
        )
        if self.generic_name:
            clauses.append(
                f'patient.drug.openfda.generic_name:"{self.generic_name}"'
            )
        if self.brand_name:
            clauses.append(
                f'patient.drug.openfda.brand_name:"{self.brand_name}"'
            )
        return " AND ".join(clauses)

    def _throttle(self):
        """Enforce per-minute rate limit between successive API calls."""
        elapsed = time.monotonic() - self._last_call_ts
        if elapsed < self._min_interval:
            sleep_for = self._min_interval - elapsed
            logger.debug("Rate-limit throttle: sleeping %.2f s", sleep_for)
            time.sleep(sleep_for)
        self._last_call_ts = time.monotonic()

    # ---- public interface ----

    def fetch_page(self, skip: int = 0, limit: int | None = None) -> Dict[str, Any]:
        """
        Fetch a single page of adverse events from openFDA.

        Returns the raw JSON response dict, or raises on unrecoverable error.
        Implements retry with exponential back-off for transient errors / 429s.
        """
        limit = limit or self.max_per_request
        params: Dict[str, Any] = {
            "search": self._build_search_clause(),
            "limit": limit,
            "skip": skip,
        }
        if self.api_key:
            params["api_key"] = self.api_key

        max_retries = 5
        for attempt in range(1, max_retries + 1):
            self._throttle()
            try:
                resp = self._session.get(self.base_url, params=params, timeout=30)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429:
                    # openFDA rate-limit exceeded — back off
                    backoff = 2 ** attempt
                    logger.warning(
                        "HTTP 429 — rate-limited. Retrying in %d s (attempt %d/%d)",
                        backoff, attempt, max_retries,
                    )
                    time.sleep(backoff)
                    continue

                if resp.status_code == 404:
                    # No results for the query (e.g., skip past total)
                    logger.info("HTTP 404 — no more results at skip=%d", skip)
                    return {"results": []}

                # Other HTTP errors
                logger.error(
                    "HTTP %d from openFDA (attempt %d/%d): %s",
                    resp.status_code, attempt, max_retries, resp.text[:500],
                )
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                resp.raise_for_status()

            except requests.exceptions.RequestException as exc:
                logger.error(
                    "Request exception (attempt %d/%d): %s",
                    attempt, max_retries, exc,
                )
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                raise

        return {"results": []}

    def fetch_total_count(self) -> int:
        """Return the total number of matching adverse event records."""
        data = self.fetch_page(skip=0, limit=1)
        total = data.get("meta", {}).get("results", {}).get("total", 0)
        logger.info("Total matching FAERS records: %d", total)
        return total


# ---------------------------------------------------------------------------
# Event transformer — openFDA ➜ Eventhouse-friendly JSON
# ---------------------------------------------------------------------------

def _safe_get(d: dict, *keys, default=""):
    """Safely traverse nested dicts / lists."""
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
    """
    Convert openFDA date string (YYYYMMDD) to ISO 8601.
    Falls back to current UTC time if parsing fails.
    """
    try:
        dt = datetime.datetime.strptime(date_str, "%Y%m%d")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def transform_event(raw: Dict[str, Any], simulate_realtime: bool = False) -> Dict[str, Any]:
    """
    Transform a single openFDA adverse event to the Eventhouse schema.

    When `simulate_realtime` is True, the ingest_timestamp is set to *now*
    so Fabric Eventstream treats each event as a live-arriving record.
    """
    # --- Identification ---
    safety_report_id = raw.get("safetyreportid", str(uuid.uuid4()))
    receive_date = raw.get("receivedate", "")

    # --- Seriousness flags ---
    serious = raw.get("serious", "2")  # "1" = serious, "2" = not serious
    serious_flags = {
        "is_serious": serious == "1",
        "hospitalization": raw.get("seriousnesshospitalization", "0") == "1",
        "death": raw.get("seriousnessdeath", "0") == "1",
        "life_threatening": raw.get("seriousnesslifethreatening", "0") == "1",
        "disability": raw.get("seriousnessdisabling", "0") == "1",
        "congenital_anomaly": raw.get("seriousnesscongenitalanomali", "0") == "1",
        "required_intervention": raw.get("seriousnessother", "0") == "1",
    }

    # --- Patient demographics ---
    patient = raw.get("patient", {})
    patient_info = {
        "age": _safe_get(patient, "patientonsetage", default=""),
        "age_unit": _safe_get(patient, "patientonsetageunit", default=""),
        "sex": _safe_get(patient, "patientsex", default=""),
        "weight": _safe_get(patient, "patientweight", default=""),
    }

    # --- Drug information (primary suspect drug) ---
    drugs = patient.get("drug", [])
    suspect_drugs = [d for d in drugs if d.get("drugcharacterization") == "1"]
    primary_drug = suspect_drugs[0] if suspect_drugs else (drugs[0] if drugs else {})

    drug_info = {
        "drug_name": primary_drug.get("medicinalproduct", ""),
        "generic_name": _safe_get(primary_drug, "openfda", "generic_name", default=""),
        "brand_name": _safe_get(primary_drug, "openfda", "brand_name", default=""),
        "route": primary_drug.get("drugadministrationroute", ""),
        "indication": primary_drug.get("drugindication", ""),
        "characterization": primary_drug.get("drugcharacterization", ""),
    }
    # Flatten list values from openfda
    if isinstance(drug_info["generic_name"], list):
        drug_info["generic_name"] = drug_info["generic_name"][0] if drug_info["generic_name"] else ""
    if isinstance(drug_info["brand_name"], list):
        drug_info["brand_name"] = drug_info["brand_name"][0] if drug_info["brand_name"] else ""

    # --- Reactions ---
    reactions = patient.get("reaction", [])
    reaction_terms = [r.get("reactionmeddrapt", "") for r in reactions if r.get("reactionmeddrapt")]

    # --- Geographic info ---
    country = raw.get("occurcountry", raw.get("primarysource", {}).get("reportercountry", ""))
    sender_country = raw.get("sender", {}).get("senderorganization", "")

    # --- Timestamps ---
    ingest_timestamp = (
        datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if simulate_realtime
        else _parse_fda_date(receive_date)
    )

    return {
        "event_id": str(uuid.uuid4()),
        "safety_report_id": safety_report_id,
        "receive_date": _parse_fda_date(receive_date),
        "ingest_timestamp": ingest_timestamp,
        "is_serious": serious_flags["is_serious"],
        "hospitalization": serious_flags["hospitalization"],
        "death": serious_flags["death"],
        "life_threatening": serious_flags["life_threatening"],
        "disability": serious_flags["disability"],
        "congenital_anomaly": serious_flags["congenital_anomaly"],
        "required_intervention": serious_flags["required_intervention"],
        "patient_age": patient_info["age"],
        "patient_age_unit": patient_info["age_unit"],
        "patient_sex": patient_info["sex"],
        "patient_weight": patient_info["weight"],
        "drug_name": drug_info["drug_name"],
        "generic_name": drug_info["generic_name"],
        "brand_name": drug_info["brand_name"],
        "drug_route": drug_info["route"],
        "drug_indication": drug_info["indication"],
        "drug_characterization": drug_info["characterization"],
        "reaction_terms": ",".join(reaction_terms),
        "primary_reaction": reaction_terms[0] if reaction_terms else "",
        "reaction_count": len(reaction_terms),
        "country": country,
        "sender_organization": sender_country,
        "report_type": raw.get("reporttype", ""),
        "qualification": _safe_get(raw, "primarysource", "qualification", default=""),
    }


# ---------------------------------------------------------------------------
# Event Hub publisher
# ---------------------------------------------------------------------------

class EventPublisher:
    """Publishes transformed adverse events to Fabric Eventstream via Event Hub SDK."""

    def __init__(self, config: Dict[str, Any], dry_run: bool = False):
        self.dry_run = dry_run
        self.max_payload = config["streaming"]["max_payload_bytes"]
        self._producer: Optional[EventHubProducerClient] = None

        if not dry_run:
            if not EVENTHUB_AVAILABLE:
                logger.error(
                    "azure-eventhub package is not installed. "
                    "Install with: pip install azure-eventhub"
                )
                sys.exit(1)

            conn_str = config["fabric_eventhub"]["connection_string"]
            eh_name = config["fabric_eventhub"]["eventhub_name"]

            if "<YOUR_" in conn_str:
                logger.error(
                    "Eventstream connection string not configured in config.json. "
                    "Update fabric_eventhub.connection_string with your Fabric "
                    "Eventstream Custom Endpoint connection string."
                )
                sys.exit(1)

            self._producer = EventHubProducerClient.from_connection_string(
                conn_str=conn_str,
                eventhub_name=eh_name,
            )
            logger.info("EventHubProducerClient initialized for '%s'", eh_name)

    def send_batch(self, events: List[Dict[str, Any]]) -> int:
        """
        Send a list of transformed event dicts to Event Hub.

        Automatically splits into multiple batches if the combined payload
        exceeds the Fabric Eventstream 1 MB limit.

        Returns the number of events successfully sent.
        """
        if not events:
            return 0

        if self.dry_run:
            # In dry-run mode, just log the first event as a sample
            logger.info("[DRY RUN] Would send %d events. Sample:", len(events))
            print(json.dumps(events[0], indent=2, default=str))
            return len(events)

        sent = 0
        batch = self._producer.create_batch()

        for event in events:
            payload = json.dumps(event, default=str).encode("utf-8")

            # Fabric constraint: individual event must be < 1 MB
            if len(payload) > self.max_payload:
                logger.warning(
                    "Single event %s exceeds %d bytes — skipping.",
                    event.get("event_id", "?"), self.max_payload,
                )
                continue

            try:
                batch.add(EventData(payload))
            except ValueError:
                # Batch is full (size limit reached) — send what we have
                self._producer.send_batch(batch)
                sent += batch.size_in_bytes  # approximate
                logger.debug("Sent intermediate batch (%d bytes)", batch.size_in_bytes)
                batch = self._producer.create_batch()
                batch.add(EventData(payload))

        # Send remaining events in the batch
        if batch:
            self._producer.send_batch(batch)

        sent = len(events)
        logger.info("Sent batch of %d events to Eventstream", sent)
        return sent

    def close(self):
        """Cleanly close the Event Hub producer."""
        if self._producer:
            self._producer.close()
            logger.info("EventHubProducerClient closed.")


# ---------------------------------------------------------------------------
# Main ingestion loop
# ---------------------------------------------------------------------------

def run_ingestion(config: Dict[str, Any], dry_run: bool = False, max_events: int = 0):
    """
    Main loop: fetch → transform → publish, in configurable batches.

    Parameters
    ----------
    config : dict
        Parsed config.json.
    dry_run : bool
        If True, events are printed to stdout instead of sent to Event Hub.
    max_events : int
        Maximum total events to ingest (0 = unlimited / until exhausted).
    """
    client = OpenFDAClient(config)
    publisher = EventPublisher(config, dry_run=dry_run)
    stream_cfg = config["streaming"]
    batch_size = stream_cfg.get("batch_size", 50)
    batch_interval = stream_cfg.get("batch_interval_seconds", 5)
    simulate = stream_cfg.get("simulate_realtime", True)

    total_available = client.fetch_total_count()
    if total_available == 0:
        logger.warning("No matching FAERS records found. Check drug_filter in config.json.")
        return

    target = min(max_events, total_available) if max_events > 0 else total_available
    logger.info(
        "Starting ingestion: target=%d events, batch_size=%d, interval=%ds, simulate_realtime=%s",
        target, batch_size, batch_interval, simulate,
    )

    total_sent = 0
    skip = 0

    try:
        while total_sent < target and not _shutdown_requested:
            remaining = target - total_sent
            fetch_limit = min(batch_size, remaining)

            logger.info(
                "Fetching page: skip=%d, limit=%d (progress: %d/%d)",
                skip, fetch_limit, total_sent, target,
            )

            data = client.fetch_page(skip=skip, limit=fetch_limit)
            results = data.get("results", [])

            if not results:
                logger.info("No more results from API. Ingestion complete.")
                break

            # Transform each raw event
            transformed = [transform_event(r, simulate_realtime=simulate) for r in results]

            # Publish batch
            sent = publisher.send_batch(transformed)
            total_sent += sent
            skip += len(results)

            # Inter-batch delay (simulates streaming cadence)
            if total_sent < target and not _shutdown_requested:
                logger.debug("Sleeping %d s before next batch…", batch_interval)
                time.sleep(batch_interval)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    finally:
        publisher.close()

    logger.info("Ingestion finished. Total events sent: %d", total_sent)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingest openFDA FAERS adverse events into Microsoft Fabric Eventstream."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "..", "docs", "config.json"),
        help="Path to config.json (default: ../docs/config.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print transformed events to stdout instead of sending to Event Hub.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=500,
        help="Maximum number of events to ingest (0 = unlimited). Default: 500 for demo.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = load_config(args.config)
    run_ingestion(config, dry_run=args.dry_run, max_events=args.max_events)


if __name__ == "__main__":
    main()
