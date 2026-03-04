#!/usr/bin/env python3
"""
Drug Safety Signal Monitor — Live Demo Simulator
==================================================
Replays adverse events from a pre-fetched NDJSON file as if they are arriving
in real-time. Designed to run DURING the customer demo to create a compelling
"events flowing in" experience.

Two modes:
  A. Console-only:  Rich live display of events arriving (no Fabric connection)
  B. Eventstream:   Publishes to Fabric Eventstream (requires connection string)

The console output is designed to be screen-shared during demos — it shows a
live ticker of adverse events with color-coded severity and running statistics.

Usage:
    # Console display only (great for screen sharing)
    python live_demo.py --source ../sample_data/adverse_events_portfolio.json

    # With Fabric Eventstream publishing
    python live_demo.py --source ../sample_data/adverse_events_portfolio.json --publish

    # Faster replay for quick demos
    python live_demo.py --source ../sample_data/adverse_events_portfolio.json --speed 5

Author: Pharmacovigilance Demo — Microsoft Fabric RTI
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import signal
import sys
import time
import uuid
from collections import Counter, defaultdict
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
try:
    from azure.eventhub import EventData, EventHubProducerClient
    EVENTHUB_OK = True
except ImportError:
    EVENTHUB_OK = False

# ─── Graceful shutdown ────────────────────────────────────────────────────────
_stop = False

def _handler(sig, frame):
    global _stop
    _stop = True

signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)

# ─── ANSI helpers (Windows Terminal / modern terminals support) ────────────────

class C:
    """ANSI color codes. Falls back to no-op on unsupported terminals."""
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    GREEN  = "\033[92m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"
    BLUE   = "\033[94m"
    MAGENTA = "\033[95m"
    BG_RED = "\033[41m"
    BG_YELLOW = "\033[43m"
    BG_GREEN = "\033[42m"


BANNER = f"""{C.CYAN}{C.BOLD}
╔═══════════════════════════════════════════════════════════════════════╗
║                                                                       ║
║   ██████╗ ██████╗ ██╗   ██╗ ██████╗     ███████╗ █████╗ ███████╗    ║
║   ██╔══██╗██╔══██╗██║   ██║██╔════╝     ██╔════╝██╔══██╗██╔════╝    ║
║   ██║  ██║██████╔╝██║   ██║██║  ███╗    ███████╗███████║█████╗      ║
║   ██║  ██║██╔══██╗██║   ██║██║   ██║    ╚════██║██╔══██║██╔══╝      ║
║   ██████╔╝██║  ██║╚██████╔╝╚██████╔╝    ███████║██║  ██║██║         ║
║   ╚═════╝ ╚═╝  ╚═╝ ╚═════╝  ╚═════╝     ╚══════╝╚═╝  ╚═╝╚═╝      ║
║                                                                       ║
║   Drug Safety Signal Monitor — LIVE                                   ║
║   Microsoft Fabric Real-Time Intelligence                             ║
║                                                                       ║
╚═══════════════════════════════════════════════════════════════════════╝
{C.RESET}"""


SEVERITY_DISPLAY = {
    "DEATH":            f"{C.BG_RED}{C.WHITE}{C.BOLD}  DEATH  {C.RESET}",
    "HOSPITALIZATION":  f"{C.RED}{C.BOLD}  HOSP   {C.RESET}",
    "LIFE-THREATENING": f"{C.YELLOW}{C.BOLD}  L/T    {C.RESET}",
    "SERIOUS":          f"{C.YELLOW}  SERIOUS{C.RESET}",
    "NON-SERIOUS":      f"{C.GREEN}  ROUTINE{C.RESET}",
}


def severity_label(evt: Dict) -> str:
    """Determine display severity from event flags."""
    if evt.get("death"):
        return "DEATH"
    if evt.get("hospitalization"):
        return "HOSPITALIZATION"
    if evt.get("life_threatening"):
        return "LIFE-THREATENING"
    if evt.get("is_serious"):
        return "SERIOUS"
    return "NON-SERIOUS"


def sex_label(code: str) -> str:
    return {"1": "M", "2": "F"}.get(code, "?")


class LiveStats:
    """Running statistics displayed in the header during the demo."""

    def __init__(self):
        self.total = 0
        self.serious = 0
        self.deaths = 0
        self.hospitalizations = 0
        self.drugs = Counter()
        self.reactions = Counter()
        self.countries = Counter()
        self.start_time = time.monotonic()
        self.last_10_severities: List[str] = []

    def update(self, evt: Dict):
        self.total += 1
        if evt.get("is_serious"):
            self.serious += 1
        if evt.get("death"):
            self.deaths += 1
        if evt.get("hospitalization"):
            self.hospitalizations += 1
        drug = evt.get("generic_name", "unknown")
        self.drugs[drug] += 1
        rxn = evt.get("primary_reaction", "")
        if rxn:
            self.reactions[rxn] += 1
        country = evt.get("country", "")
        if country:
            self.countries[country] += 1
        sev = severity_label(evt)
        self.last_10_severities.append(sev)
        if len(self.last_10_severities) > 10:
            self.last_10_severities.pop(0)

    def elapsed(self) -> str:
        secs = int(time.monotonic() - self.start_time)
        m, s = divmod(secs, 60)
        return f"{m:02d}:{s:02d}"

    def rate(self) -> float:
        elapsed = time.monotonic() - self.start_time
        return self.total / elapsed * 60 if elapsed > 0 else 0

    def render_header(self) -> str:
        """Render the live statistics bar."""
        serious_pct = (100 * self.serious / self.total) if self.total > 0 else 0
        lines = [
            f"{C.DIM}{'─'*72}{C.RESET}",
            f"  {C.BOLD}LIVE{C.RESET}  "
            f"Events: {C.CYAN}{self.total:,}{C.RESET}  │  "
            f"Rate: {C.CYAN}{self.rate():.0f}/min{C.RESET}  │  "
            f"Serious: {C.YELLOW}{self.serious:,}{C.RESET} ({serious_pct:.0f}%)  │  "
            f"Deaths: {C.RED}{self.deaths}{C.RESET}  │  "
            f"Elapsed: {self.elapsed()}",
            f"  Drugs: {C.WHITE}{len(self.drugs)}{C.RESET}  │  "
            f"Countries: {C.WHITE}{len(self.countries)}{C.RESET}  │  "
            f"Top drug: {C.BOLD}{self.drugs.most_common(1)[0][0] if self.drugs else '—'}{C.RESET}  │  "
            f"Top reaction: {C.BOLD}{self.reactions.most_common(1)[0][0][:25] if self.reactions else '—'}{C.RESET}",
            f"{C.DIM}{'─'*72}{C.RESET}",
        ]
        return "\n".join(lines)


def render_event_line(evt: Dict, index: int) -> str:
    """Render a single event as a formatted console line."""
    sev = severity_label(evt)
    sev_display = SEVERITY_DISPLAY.get(sev, sev)
    drug = (evt.get("generic_name", "?") or "?")[:18]
    reaction = (evt.get("primary_reaction", "—") or "—")[:30]
    country = (evt.get("country", "??") or "??")[:3]
    sex = sex_label(evt.get("patient_sex", ""))
    age = evt.get("patient_age", "?")
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")

    return (
        f"  {C.DIM}{ts}{C.RESET}  "
        f"{sev_display}  "
        f"{C.WHITE}{drug:<18s}{C.RESET}  "
        f"{reaction:<30s}  "
        f"{country}  "
        f"{sex}/{age}"
    )


# ─── Event Hub Publisher ──────────────────────────────────────────────────────

class DemoPublisher:
    """Publishes events to Fabric Eventstream with batching."""

    def __init__(self, conn_str: str, eh_name: str):
        if not EVENTHUB_OK:
            print(f"{C.RED}ERROR: azure-eventhub not installed. Run: pip install azure-eventhub{C.RESET}")
            sys.exit(1)
        self._producer = EventHubProducerClient.from_connection_string(
            conn_str=conn_str, eventhub_name=eh_name)
        self._batch = self._producer.create_batch()
        self._count = 0

    def add(self, evt: Dict):
        """Add an event to the current batch; auto-flushes when full."""
        payload = json.dumps(evt, default=str).encode("utf-8")
        try:
            self._batch.add(EventData(payload))
            self._count += 1
        except ValueError:
            self.flush()
            self._batch = self._producer.create_batch()
            self._batch.add(EventData(payload))
            self._count += 1

    def flush(self):
        if self._count > 0:
            self._producer.send_batch(self._batch)
            self._batch = self._producer.create_batch()
            self._count = 0

    def close(self):
        self.flush()
        self._producer.close()


# ─── Main Loop ────────────────────────────────────────────────────────────────

def run_demo(
    source_path: str,
    speed: float = 1.0,
    publish: bool = False,
    conn_str: str = "",
    eh_name: str = "",
    max_events: int = 0,
    compact: bool = False,
):
    """Run the live demo replay."""

    print(BANNER)

    # Load events
    events: List[Dict] = []
    print(f"  Loading events from {source_path}...")
    with open(source_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    if max_events > 0:
        events = events[:max_events]

    print(f"  Loaded {len(events):,} events")

    # Setup publisher if requested
    publisher: Optional[DemoPublisher] = None
    if publish:
        if not conn_str or "<YOUR_" in conn_str:
            print(f"\n{C.RED}  ERROR: Provide a valid Eventstream connection string.{C.RESET}")
            print(f"  Use: --conn-str 'Endpoint=sb://...'")
            print(f"  Or update config.json > fabric_eventhub.connection_string\n")
            sys.exit(1)
        print(f"  {C.GREEN}Connecting to Fabric Eventstream...{C.RESET}")
        publisher = DemoPublisher(conn_str, eh_name)
        print(f"  {C.GREEN}Connected! Events will be published live.{C.RESET}")

    # Calculate delay between events
    base_delay = 0.5 / speed  # At speed=1, ~2 events/sec; at speed=5, ~10/sec

    # Live stats
    stats = LiveStats()

    # Header
    mode = f"{C.GREEN}PUBLISHING to Eventstream{C.RESET}" if publish else f"{C.YELLOW}DISPLAY ONLY{C.RESET}"
    print(f"\n  Mode: {mode}")
    print(f"  Speed: {speed}x ({1/base_delay:.1f} events/sec)")
    print(f"  Press Ctrl+C to stop\n")

    # Column headers
    if not compact:
        print(f"  {C.DIM}{'TIME':<10s}  {'SEVERITY':<10s}  {'DRUG':<18s}  {'REACTION':<30s}  {'CTY':3s}  {'PAT'}{C.RESET}")
        print(f"  {C.DIM}{'─'*10}  {'─'*10}  {'─'*18}  {'─'*30}  {'─'*3}  {'─'*5}{C.RESET}")

    try:
        for i, evt in enumerate(events):
            if _stop:
                break

            # Inject fresh timestamp
            now = datetime.datetime.now(datetime.timezone.utc)
            evt["ingest_timestamp"] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            evt["event_id"] = str(uuid.uuid4())  # Fresh UUID

            # Update stats
            stats.update(evt)

            # Display
            if compact:
                # Compact mode: just stats header every 25 events
                if i % 25 == 0:
                    print(f"\033[2J\033[H")  # Clear screen
                    print(BANNER)
                    print(stats.render_header())
                    print(f"\n  {C.DIM}{'TIME':<10s}  {'SEVERITY':<10s}  {'DRUG':<18s}  {'REACTION':<30s}  {'CTY':3s}  {'PAT'}{C.RESET}")
                    print(f"  {C.DIM}{'─'*10}  {'─'*10}  {'─'*18}  {'─'*30}  {'─'*3}  {'─'*5}{C.RESET}")
            else:
                # Show stats header periodically
                if i > 0 and i % 20 == 0:
                    print()
                    print(stats.render_header())
                    print()

            print(render_event_line(evt, i))

            # Publish to Eventstream
            if publisher:
                publisher.add(evt)
                # Flush every 10 events for low-latency demo
                if (i + 1) % 10 == 0:
                    publisher.flush()

            # Pacing
            time.sleep(base_delay)

    except KeyboardInterrupt:
        pass

    # Final summary
    if publisher:
        publisher.close()

    print(f"\n\n{stats.render_header()}")
    print(f"\n  {C.GREEN}{C.BOLD}Demo complete!{C.RESET}")
    print(f"  Replayed {stats.total:,} events in {stats.elapsed()}")
    if publisher:
        print(f"  All events published to Fabric Eventstream")
    print(f"\n  Run this in Eventhouse to verify:")
    print(f"    AdverseEvents | where ingest_timestamp > ago(1h) | count")
    print(f"    DetectSafetySignals(2.0, 3)")
    print()


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Live demo simulator — replay FDA adverse events with real-time display",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", type=str, required=True,
                        help="Path to NDJSON file (from fetch_fda_portfolio.py)")
    parser.add_argument("--speed", type=float, default=1.0,
                        help="Replay speed multiplier (default: 1.0, try 3-5 for demos)")
    parser.add_argument("--publish", action="store_true",
                        help="Publish events to Fabric Eventstream (requires --conn-str)")
    parser.add_argument("--conn-str", type=str, default="",
                        help="Fabric Eventstream connection string")
    parser.add_argument("--eh-name", type=str, default="faers-adverse-events",
                        help="Event Hub name (default: faers-adverse-events)")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Max events to replay (0 = all)")
    parser.add_argument("--compact", action="store_true",
                        help="Compact mode: clear screen and refresh display")
    parser.add_argument("--config", type=str, default="",
                        help="Path to config.json (reads Eventstream connection from there)")
    args = parser.parse_args()

    # Resolve source path (try CWD first, then relative to script)
    source = Path(args.source)
    if not source.is_absolute():
        if not source.exists():
            source = (Path(__file__).parent / args.source).resolve()
    if not source.exists():
        print(f"{C.RED}ERROR: Source file not found: {source}{C.RESET}")
        sys.exit(1)

    # Load connection string from config if provided
    conn_str = args.conn_str
    eh_name = args.eh_name
    if args.config and not conn_str:
        cfg_path = Path(args.config)
        if not cfg_path.is_absolute() and not cfg_path.exists():
            cfg_path = (Path(__file__).parent / cfg_path).resolve()
        if cfg_path.exists():
            with open(cfg_path) as f:
                cfg = json.load(f)
            conn_str = cfg.get("fabric_eventhub", {}).get("connection_string", "")
            eh_name = cfg.get("fabric_eventhub", {}).get("eventhub_name", eh_name)

    run_demo(
        source_path=str(source),
        speed=args.speed,
        publish=args.publish,
        conn_str=conn_str,
        eh_name=eh_name,
        max_events=args.max_events,
        compact=args.compact,
    )


if __name__ == "__main__":
    main()
