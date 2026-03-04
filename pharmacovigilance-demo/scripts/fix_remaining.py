"""
Fix the 2 remaining KQL objects that failed during initial deployment:
1. TransformRawAdverseEvent — coalesce(string, guid) type mismatch → use tostring(new_guid())
2. RiskScoreCard — 'latest' reserved keyword → renamed to 'latest_report'
"""
import sys
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.identity import InteractiveBrowserCredential, SharedTokenCacheCredential, DeviceCodeCredential, ChainedTokenCredential

CLUSTER = "https://trd-yxvwsau7zstjcxcq7h.z0.kusto.fabric.microsoft.com"
DATABASE = "Drug-safety-signal-eh"

TRANSFORM_FN = """
.create-or-alter function with (folder="Pipeline", docstring="Transforms raw staging records into enriched AdverseEvents rows")
TransformRawAdverseEvent()
{
    AdverseEvents_Raw
    | extend
        event_id            = coalesce(raw_event_id, tostring(new_guid())),
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
        generic_name        = tolower(trim(" ", generic_name)),
        brand_name          = brand_name,
        drug_route          = drug_route,
        drug_indication     = drug_indication,
        drug_characterization = drug_characterization,
        reaction_terms      = reaction_terms,
        primary_reaction    = trim(" ", primary_reaction),
        reaction_count      = toint(reaction_count),
        country             = toupper(trim(" ", country)),
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
}
"""

RISK_SCORE_FN = """
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
            latest_report = max(ingest_timestamp),
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
        Latest_Report    = latest_report
    | take 1
}
"""

UPDATE_POLICY = """
.alter table AdverseEvents policy update
@'[{"IsEnabled": true, "Source": "AdverseEvents_Raw", "Query": "TransformRawAdverseEvent()", "IsTransactional": true, "PropagateIngestionProperties": true}]'
"""


def run():
    import io
    log = open("fix_output.txt", "w", encoding="utf-8")
    
    def log_print(msg=""):
        print(msg, flush=True)
        log.write(msg + "\n")
        log.flush()

    log_print("Authenticating (trying cached token, then device code)...")
    cred = ChainedTokenCredential(
        SharedTokenCacheCredential(),
        DeviceCodeCredential(),
    )
    cred.get_token("https://kusto.kusto.windows.net/.default")
    log_print("Auth token acquired.")
    kcsb = KustoConnectionStringBuilder.with_azure_token_credential(CLUSTER, cred)
    client = KustoClient(kcsb)

    steps = [
        ("TransformRawAdverseEvent function", TRANSFORM_FN),
        ("RiskScoreCard function", RISK_SCORE_FN),
        ("Update policy (re-apply)", UPDATE_POLICY),
    ]

    for name, kql in steps:
        log_print(f"\n{'='*60}")
        log_print(f"Deploying: {name}")
        log_print(f"{'='*60}")
        try:
            client.execute(DATABASE, kql.strip())
            log_print(f"  SUCCESS")
        except Exception as e:
            log_print(f"  ERROR: {e}")

    # Verify all functions
    log_print(f"\n{'='*60}")
    log_print("Verifying all functions...")
    log_print(f"{'='*60}")
    r = client.execute(DATABASE, ".show functions | project Name, Folder")
    for row in r.primary_results[0]:
        log_print(f"  {row[0]:35s}  [{row[1]}]")

    # Test RiskScoreCard
    log_print(f"\n{'='*60}")
    log_print("Testing: RiskScoreCard('aspirin')")
    log_print(f"{'='*60}")
    try:
        r = client.execute(DATABASE, 'RiskScoreCard("aspirin")')
        row = r.primary_results[0][0]
        log_print(f"  Drug      = {row[0]}")
        log_print(f"  Risk      = {row[1]}")
        log_print(f"  Score     = {row[2]}")
        log_print(f"  Events90d = {row[7]}")
        log_print(f"  Deaths    = {row[8]}")
        log_print(f"  Hosp      = {row[9]}")
    except Exception as e:
        log_print(f"  ERROR: {e}")

    # Test TransformRawAdverseEvent (just check it compiles / returns schema)
    log_print(f"\n{'='*60}")
    log_print("Testing: TransformRawAdverseEvent() | getschema")
    log_print(f"{'='*60}")
    try:
        r = client.execute(DATABASE, "TransformRawAdverseEvent() | getschema")
        for row in r.primary_results[0]:
            log_print(f"  {row[0]:30s} {row[1]}")
    except Exception as e:
        log_print(f"  ERROR: {e}")

    # Show update policy details
    log_print(f"\n{'='*60}")
    log_print("Verifying: update policy on AdverseEvents")
    log_print(f"{'='*60}")
    try:
        r = client.execute(DATABASE, ".show table AdverseEvents policy update")
        row = r.primary_results[0][0]
        cols = r.primary_results[0].columns
        for i, col in enumerate(cols):
            val = str(row[i])[:120]
            log_print(f"  {col.column_name:25s} = {val}")
    except Exception as e:
        log_print(f"  ERROR: {e}")

    client.close()
    log_print("\nAll fixes applied.")
    log.close()


if __name__ == "__main__":
    run()
