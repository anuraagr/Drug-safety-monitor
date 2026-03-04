# Demo Script: Drug Safety Signal Monitor

**Duration:** 10 minutes | **Audience:** Life Sciences IT, Pharmacovigilance, Data Platform teams

---

## Pre-Demo Setup (done before the meeting)

1. ✅ Ran `fetch_fda_portfolio.py --count 2000 --profile mixed --spike-drug aspirin`
2. ✅ Loaded data into Eventhouse (portal upload or `load_to_eventhouse.py`)
3. ✅ Verified: `AdverseEvents | count` returns ~2,000
4. ✅ Verified: `DetectSafetySignals(2.0, 3)` returns aspirin spike rows
5. ✅ Dashboard tiles rendering with data
6. ✅ `live_demo.py` tested and ready to run
7. ✅ Browser tabs pre-loaded:
   - Tab 1: Fabric Eventstream (or your terminal running `live_demo.py`)
   - Tab 2: Eventhouse query editor
   - Tab 3: Real-Time Dashboard ("Signal Room")
   - Tab 4: Teams channel (for Activator alert, if configured)

---

## Opening (30 seconds)

> *"Pharmacovigilance — monitoring drug safety after market approval — is one of the most regulated and time-critical workflows in life sciences. Every pharma and biotech company must detect adverse event signals and report them to regulators within strict timelines.*
>
> *Today I'll show you how Microsoft Fabric Real-Time Intelligence transforms this from a batch process into a live, streaming operation — using real FDA data across a portfolio of drugs."*

---

## Act 1: Real FDA Data (60 seconds)

**Show:** Terminal with `live_demo.py` running (or ready to start)

> *"We're connected to the FDA's Adverse Event Reporting System — FAERS — through the openFDA API. This is the same data FDA safety reviewers use. It's public, it's real, and it covers millions of adverse event reports."*

**Action:** Start the live demo replay:

```bash
python scripts/live_demo.py --source sample_data/fda_portfolio_mixed.ndjson --speed 3
```

> *"Watch the console — each line is a real adverse event report. The color coding tells the story instantly:*
> - *Red background means a patient death*
> - *Red text is a hospitalization*
> - *Yellow is life-threatening or other serious outcome*
> - *Green is a routine report*
>
> *Notice we're monitoring seven drugs simultaneously — aspirin, metformin, atorvastatin, omeprazole, and more. This is a portfolio view, not just one drug."*

**Point out:**
- Running statistics updating in real-time (serious rate %, deaths, event count)
- Multiple drugs appearing in the feed
- Events carrying MedDRA-coded reaction terms, patient demographics, geographic origin

---

## Act 2: Streaming into Eventhouse (45 seconds)

**Show:** Fabric portal → Eventhouse query editor

> *"These events flow into Fabric Eventhouse — a high-performance KQL database purpose-built for streaming analytics. The schema captures everything a safety officer needs: seriousness outcome flags, patient demographics, drug information, and standardized reaction terms."*

**Action:** Run in query editor:

```kql
AdverseEvents
| where ingest_timestamp > ago(1h)
| project ingest_timestamp, generic_name, reaction_preferred_term,
          is_serious, serious_death, serious_hospitalization, patient_country
| order by ingest_timestamp desc
| take 10
```

**Point out:**
- Events arriving with recent timestamps
- Multiple drugs in the `generic_name` column
- Boolean seriousness flags parsed directly from FDA data
- Materialized views aggregating automatically in the background

---

## Act 3: Signal Detection — The "Aha" Moment (90 seconds)

**Show:** Eventhouse query editor

> *"This is where Fabric's real-time analytics really shine. We have a KQL function that compares the last 7 days of serious events against 30-day historical baselines. When a drug+reaction combination exceeds 2× the expected count, it's flagged as a potential safety signal."*

**Action:** Run the signal detection function:

```kql
DetectSafetySignals(2.0, 3)
```

> *"Look at these results. Aspirin is showing a spike — the event count for certain reactions is [X]× above the historical baseline. That's automatically classified as [CRITICAL/HIGH] severity because [it includes deaths / hospitalizations].*
>
> *This is exactly the kind of signal that would take a pharmacovigilance team days to detect in a batch process. Here it appears in seconds."*

**Point out:**
- `spike_ratio` column — how many times above baseline (e.g., "3.2× above normal")
- `severity` classification: CRITICAL (deaths), HIGH (hospitalizations), MEDIUM
- Multiple drug+reaction pairs being monitored simultaneously
- Sub-second query performance on materialized views
- Thresholds are configurable — lower for rare diseases, higher for common drugs

---

## Act 4: The Signal Room Dashboard (90 seconds)

**Show:** Switch to Real-Time Dashboard tab

> *"Safety officers don't run KQL queries. This is their Signal Room — a real-time dashboard that refreshes every 30 seconds and gives them everything at a glance."*

**Walk through tiles systematically:**

### Top row — Status at a glance
| Tile | Talk Track |
|------|-----------|
| **Command Center** | *"Eight key metrics in one view: [X] total events, [Y]% serious rate, [Z] deaths in the last 24 hours. This is the ops heartbeat."* |
| **Arrival Rate** | *"Stacked area chart — the red band is serious events, green is routine. You can immediately see if the serious proportion is growing."* |

### Middle row — Signal detection
| Tile | Talk Track |
|------|-----------|
| **Signal Alerts** | *"This is the most important tile. See the 🔴 CRITICAL flag on aspirin? That's the same spike we saw in the query — 3.2× above baseline. One click drills into the case narratives."* |
| **Portfolio Heatmap** | *"Every drug in the portfolio, ranked by risk level. Green means stable, yellow means elevated, red means action needed. Notice aspirin is flagged."* |

### Bottom row — Deep analytics
| Tile | Talk Track |
|------|-----------|
| **Top Reactions** | *"The 10 most frequent serious reactions, stacked by outcome type. Deaths and life-threatening conditions are immediately visible."* |
| **Geographic Map** | *"Country-level distribution. The US dominates reporting — that's FDA home-country bias. But watch for emerging signals from Japan or the EU."* |
| **Trend by Drug** | *"30-day trend lines for every drug. Aspirin shows that spike clustering — see the sharp uptick? That's what triggered the signal."* |

> *"Every tile auto-refreshes. The safety team opens this in the morning and it's already caught up to real-time."*

---

## Act 5: Ad-Hoc Analytics (60 seconds)

**Show:** Switch back to Eventhouse query editor

> *"The dashboard gives the operational view. But when a safety officer wants to drill deeper, they can run ad-hoc KQL queries right from the same Fabric workspace."*

**Action:** Run the drug comparison function:

```kql
DrugComparisonReport("aspirin", "pembrolizumab")
```

> *"Side-by-side comparison: aspirin versus pembrolizumab. Notice the seriousness rate difference, the death rates, and the number of unique reactions. This is the kind of question that comes up in every safety board meeting — answered in under a second."*

**Action:** Run the risk score:

```kql
RiskScoreCard("aspirin")
```

> *"The composite risk score combines four dimensions — seriousness rate, mortality, reporting velocity, and reaction breadth — into a single 0-to-100 score. Aspirin scores [X], classified as [CRITICAL/HIGH/ELEVATED]. This gives executives a single number for decision-making."*

**Point out:**
- Both queries run against the same Eventhouse data in sub-second time
- Functions are parameterized — swap drug names for any drug in the portfolio
- Results can feed directly into regulatory submission documents

---

## Act 6: Alert Escalation (45 seconds)

**Show:** Teams channel with Activator alert (if configured) — or show the Activator configuration

> *"When a signal fires, it doesn't just light up a dashboard. Fabric Activator pushes an alert directly to the pharmacovigilance team's Teams channel — and backup email."*

**Show the adaptive card (or describe it):**

> *"The alert card includes everything for triage: drug name, reaction term, event count versus baseline, spike ratio, and action buttons. 'Review Case Narratives' deep-links to the Eventhouse query. 'Log in Safety Database' connects to their case management system. No context-switching."*

**Point out:**
- Structured adaptive card (not a plain notification)
- 60-minute cooldown prevents alert fatigue
- Email backup ensures nothing falls through
- Action buttons enable one-click triage

---

## Closing (60 seconds)

> *"Let me recap what we just saw:*
>
> 1. **Real FDA data** — not synthetic — from the openFDA Adverse Event Reporting System, covering a multi-drug portfolio
> 2. **Streaming ingestion** into Fabric Eventhouse with sub-second query performance
> 3. **Automated signal detection** comparing real-time events against 30-day baselines
> 4. **A 14-tile Signal Room dashboard** purpose-built for safety officers — from operational heartbeat to case-level drilldown
> 5. **Ad-hoc analytics** with head-to-head drug comparison and composite risk scoring
> 6. **Alert escalation** to Teams and email with drill-down capabilities
>
> *This is an end-to-end regulatory workflow running entirely in Microsoft Fabric. No Spark clusters, no external ETL pipelines, no third-party tools to maintain.*
>
> *And it's fully customizable. We can re-point this at your drug portfolio in minutes — switch the profile to your therapeutic area, adjust sensitivity thresholds for your risk tolerance, and brand the dashboard for your safety operations team.*
>
> *This is what real-time pharmacovigilance looks like on Microsoft Fabric. Questions?"*

---

## Live Demo Commands (Quick Reference)

```bash
# Fetch data (do before the demo)
python scripts/fetch_fda_portfolio.py --count 2000 --profile mixed --spike-drug aspirin

# Load into Eventhouse (do before the demo)
python scripts/load_to_eventhouse.py --file sample_data/adverse_events_portfolio.json --auth interactive

# Deploy new KQL functions (do before the demo, in Eventhouse query editor)
# Run STEP13, STEP14, STEP15 from scripts/setup_steps/

# Live replay during demo (run this on-screen)
python scripts/live_demo.py --source sample_data/adverse_events_portfolio.json --speed 3

# Signal detection query (run in Eventhouse during demo)
DetectSafetySignals(2.0, 3)

# Drug comparison (run during Q&A)
DrugComparisonReport("aspirin", "pembrolizumab")

# Risk score (run during Q&A)
RiskScoreCard("aspirin")

# Reaction timeline (run if time permits)
ReactionTimeline("aspirin", 30d) | render linechart
```

---

## Backup: Common Questions & Answers

### "Is this real FDA data?"
Yes. openFDA provides public access to the FDA Adverse Event Reporting System (FAERS). This is the same data FDA reviewers use, with personal identifiers removed.

### "Can we use our own internal safety data?"
Absolutely. Replace the openFDA fetcher with a connector to your safety database (Argus, ArisG, Oracle AERS). The Eventhouse schema and signal detection logic remain identical.

### "How does this compare to Empirica Signal or traditional PV tools?"
Traditional tools run weekly or monthly batch analyses. This detects signals in near-real-time as events arrive. It's complementary: use Fabric for early warning, and validated tools for regulatory submissions.

### "Can we scale beyond seven drugs?"
The demo uses seven drugs, but there's no architectural limit. Remove the drug filter entirely and the system monitors everything that comes through FAERS. Materialized views group by `generic_name`, so scaling is built in.

### "What about MedDRA coding?"
openFDA data includes MedDRA Preferred Terms (PT) in the reaction fields. For internal data, you'd add your MedDRA dictionary lookup. The schema supports PT and LLT codes.

### "What Fabric SKU do we need?"
The demo runs on Fabric Trial or F2. For production with high-volume streaming, F4 or higher is recommended.

### "How fast does it detect a signal?"
From event ingestion to signal flag: seconds. The `DetectSafetySignals` function runs against materialized views that auto-update incrementally. Dashboard refresh is 30 seconds. Activator evaluates every 60 seconds.

---

## Adapting for Specific Customers

### Oncology demo
```bash
python scripts/fetch_fda_portfolio.py --profile oncology --spike-drug pembrolizumab --count 1500
```
Talk track adjustment: *"Monitoring checkpoint inhibitors and targeted therapies — pembrolizumab, nivolumab, trastuzumab — where immune-related adverse events require rapid detection."*

### Cardiology demo
```bash
python scripts/fetch_fda_portfolio.py --profile cardiology --spike-drug warfarin --count 1500
```
Talk track adjustment: *"Anticoagulants and statins — high-volume drugs where signal-to-noise ratio is the challenge. The 2× threshold filters out baseline noise while catching genuine spikes."*

### Rare disease demo
```bash
python scripts/fetch_fda_portfolio.py --drugs nusinersen ivacaftor --spike-drug nusinersen --count 300
```
Talk track adjustment: *"For rare diseases, event volumes are lower, so we reduce the minimum event threshold. Even three clustered events in a week can be clinically significant."*

Config change for rare disease sensitivity:
```json
"signal_detection": {
    "spike_threshold_multiplier": 1.5,
    "minimum_event_count": 2
}
```
