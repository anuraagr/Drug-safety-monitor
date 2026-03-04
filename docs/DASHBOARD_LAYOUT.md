# Signal Room — Dashboard Layout Guide

**14-tile layout for Fabric Real-Time Intelligence Dashboard**

Use this guide when building the dashboard in Fabric. The layout is organized in four rows that tell a story: **status → detection → analytics → context**.

---

## Recommended Grid Layout

```
╔══════════════════════════════════════════════════════════════════════════╗
║                    ROW 1: OPERATIONAL STATUS                            ║
╠══════════════════════╦═══════════════════════╦══════════════════════════╣
║   TILE 1             ║   TILE 2              ║   TILE 14               ║
║   Command Center     ║   Arrival Rate        ║   Signal Timeline       ║
║   (Stat cards)       ║   (Stacked area 24h)  ║   (Annotated line)      ║
║   4 × 2              ║   4 × 2               ║   4 × 2                 ║
╠══════════════════════╩═══════════════════════╩══════════════════════════╣
║                    ROW 2: SIGNAL DETECTION (the "aha")                  ║
╠══════════════════════════════════════════╦═══════════════════════════════╣
║   TILE 3                                ║   TILE 4                      ║
║   Signal Alerts           ⭐ HERO TILE  ║   Portfolio Heatmap           ║
║   (Severity table)                      ║   (Risk-level table)          ║
║   6 × 3                                ║   6 × 3                       ║
╠════════════════╦════════════════╦════════╩═══════╦══════════════════════╣
║                ROW 3: DEEP ANALYTICS                                    ║
╠════════════════╦════════════════╦════════════════╦══════════════════════╣
║   TILE 5       ║   TILE 7       ║   TILE 13      ║   TILE 11           ║
║   Top 10       ║   Trend by     ║   Drug×Reaction ║  Outcome Funnel    ║
║   Reactions    ║   Drug (30d)   ║   Cross-tab     ║  (Severity cascade)║
║   (Stacked bar)║   (Multi-line) ║   (Heatmap tbl) ║  (Bar chart)       ║
║   3 × 2        ║   3 × 2        ║   3 × 2         ║  3 × 2             ║
╠════════════════╩════════════════╩════════════════╩══════════════════════╣
║                ROW 4: CONTEXT & DEMOGRAPHICS                            ║
╠═══════════╦═══════════╦════════════╦══════════════╦═════════════════════╣
║   TILE 6  ║  TILE 8   ║  TILE 10   ║  TILE 12     ║  TILE 9            ║
║   Geo Map ║  Sex Dist ║  Age Dist  ║  Reporter    ║  Case Drilldown    ║
║   (Map)   ║  (Pie)    ║  (Bar)     ║  Quality     ║  (Scrollable tbl)  ║
║   3 × 2   ║  2 × 2    ║  2 × 2     ║  (Donut)     ║  3 × 3             ║
║           ║           ║            ║  2 × 2       ║                     ║
╚═══════════╩═══════════╩════════════╩══════════════╩═════════════════════╝
```

---

## Row-by-Row Story

### Row 1 — "How are we doing right now?"
| Tile | Type | Purpose | Auto-Refresh |
|------|------|---------|-------------|
| **TILE 1: Command Center** | Stat cards | 8 KPIs at a glance: total events, serious count, deaths, hospitalizations, life-threatening, drugs monitored, countries, serious rate % | 30s |
| **TILE 2: Arrival Rate** | Stacked area | Serious vs non-serious events per hour over 24h — shows throughput and serious proportion trends | 30s |
| **TILE 14: Signal Timeline** | Annotated line | Daily serious events vs 7-day rolling average with spike markers — the "story arc" showing the signal building over time | 60s |

> **Demo tip:** Start here. "This is the ops heartbeat — 8 KPIs refreshing every 30 seconds. Notice the serious rate is [X]% and climbing."

### Row 2 — "What signals are firing?" (THE HERO SECTION)
| Tile | Type | Purpose | Auto-Refresh |
|------|------|---------|-------------|
| **TILE 3: Signal Alerts** ⭐ | Severity table | Drug+reaction pairs exceeding 2× baseline — with 🔴 CRITICAL / 🟠 HIGH / 🟡 ELEVATED / 🔵 MEDIUM badges, spike ratios, death counts | 30s |
| **TILE 4: Portfolio Heatmap** | Risk table | Every drug ranked by risk level with serious rate and death count — portfolio-level triage | 60s |

> **Demo tip:** This is the "aha" moment. Point to aspirin's 🔴 CRITICAL flag. "This spike was detected in seconds — traditional PV tools would take days."

### Row 3 — "What's driving the signals?"
| Tile | Type | Purpose | Auto-Refresh |
|------|------|---------|-------------|
| **TILE 5: Top 10 Reactions** | Stacked bar | Most frequent serious reactions broken down by outcome type (deaths, hospitalizations, life-threatening) | 60s |
| **TILE 7: Trend by Drug** | Multi-line | 30-day daily event counts per drug — one line each — shows aspirin's spike clustering | 60s |
| **TILE 13: Drug×Reaction Cross-tab** | Heatmap table | Pivot of top 8 drugs vs top 8 reactions — immediately reveals hotspot combinations | 60s |
| **TILE 11: Outcome Funnel** | Bar chart | Total → Serious → Hospitalization → Life-Threatening → Death cascade — executive severity overview | 60s |

> **Demo tip:** "The cross-tab is what safety officers use daily — which drug+reaction pairs are most active. The funnel shows that [X]% of serious events involve hospitalizations."

### Row 4 — "Who is affected and where?"
| Tile | Type | Purpose | Auto-Refresh |
|------|------|---------|-------------|
| **TILE 6: Geographic Map** | Map | Country-level event distribution with serious rate — shows US reporting dominance and emerging international signals | 60s |
| **TILE 8: Sex Distribution** | Pie chart | Male/Female/Unknown patient breakdown | 120s |
| **TILE 10: Age Distribution** | Bar chart | Age bands (Pediatric → Elderly) stacked by seriousness — highlights elderly over-representation | 120s |
| **TILE 12: Reporter Quality** | Donut chart | Physician vs Consumer vs Pharmacist reports — data quality indicator | 120s |
| **TILE 9: Case Drilldown** | Table | 50 most recent serious cases with outcome icons, drug, reaction, demographics — the detailed audit trail | 30s |

> **Demo tip:** "Notice 65+ elderly patients are over-represented in serious outcomes. And [X]% of reports come from physicians — this is high-quality clinical data, not just consumer noise."

---

## Dashboard Configuration Checklist

### Global Settings
- [ ] Auto-refresh: **30 seconds**
- [ ] Default time range: **Last 24 hours** (tiles override as needed)
- [ ] Theme: **Dark** (better for presentations) or **Light** (for documentation)

### Per-Tile Setup Notes

| Tile | Visualization | Special Configuration |
|------|--------------|----------------------|
| TILE 1 | Stat tile | Use "Multi-stat" — map each column to a separate stat card |
| TILE 3 | Table | Add **conditional formatting**: Severity column → Red for CRITICAL, Orange for HIGH, Yellow for ELEVATED |
| TILE 4 | Table | Enable **data bars** on the "Serious" column |
| TILE 6 | Map | Set Location = `Country`, Size = `Events`, Color = `Serious_Pct` |
| TILE 9 | Table | Enable **row limit: 50**, compact row height, scrollable |
| TILE 13 | Table | Apply **heatmap conditional formatting** to all numeric columns |
| TILE 14 | Line chart | Use dual-axis: Daily on left, Avg on right. Set "Is_Anomaly" as annotation |

### Parameters (Optional, for interactive demos)

Add these dashboard parameters for live drill-down:

| Parameter | Type | Default | Used By |
|-----------|------|---------|---------|
| `@TimeRange` | Timespan | `24h` | TILE 1, 2, 9 |
| `@DrugFilter` | String (dropdown from `generic_name`) | All | TILE 5, 7, 9 |
| `@SpikeThreshold` | Real | `2.0` | TILE 3 |

To parameterize TILE 3, replace the hardcoded `let spike_multiplier = 2.0;` with:
```kql
let spike_multiplier = iff(isempty(@SpikeThreshold), 2.0, todouble(@SpikeThreshold));
```

---

## Quick KQL Functions for Live Demo

During the demo, switch to the Eventhouse query editor and run these interactively:

```kql
// "Let me show you what's behind that aspirin signal"
DetectSafetySignals(2.0, 3)

// "Now let's compare aspirin head-to-head with pembrolizumab"
DrugComparisonReport("aspirin", "pembrolizumab")

// "Here's how aspirin's reactions are trending over time"
ReactionTimeline("aspirin", 30d) | render linechart

// "And the composite risk score — combining seriousness, mortality, velocity..."
RiskScoreCard("aspirin")

// "For the safety team, here's the patient demographics"
PatientDemographics("aspirin")
```

---

## Presentation Tips

1. **Start at Row 1** — establish the operational cadence ("events are flowing in")
2. **Dwell on Row 2** — this is where the "aha" happens. Let the audience absorb the signal table.
3. **Click through Row 3** — show analytical depth. The cross-tab is a crowd-pleaser.
4. **End at Row 4** — demographics + case drilldown grounds it in real patients.
5. **Switch to query editor** — run `DetectSafetySignals(2.0, 3)` live to show the KQL behind the tile.
6. **If time allows** — run `DrugComparisonReport` or `RiskScoreCard` for ad-hoc Q&A.

> **Pro tip:** Keep the live_demo.py console visible on a second screen during the dashboard walkthrough. The audience sees events flowing in real-time on one screen while you show the dashboard detecting signals on the other.
