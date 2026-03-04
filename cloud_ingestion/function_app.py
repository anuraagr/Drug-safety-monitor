import logging
import datetime
import json
import os
import uuid
import requests
import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData

# Configuration
OPENFDA_URL = "https://api.fda.gov/drug/event.json"
EVENTHUB_CONNECTION_STRING = os.environ.get("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME")
# Filter for demo purposes (can be updated via app settings)
SEARCH_TERM = os.environ.get("DRUG_FILTER", "sensit") 

app = func.FunctionApp()

@app.schedule(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def faers_ingest_timer(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('FAERS Ingestion function started.')
    
    if not EVENTHUB_CONNECTION_STRING:
        logging.error("EVENTHUB_CONNECTION_STRING not found in environment variables.")
        return

    # 1. Fetch Data from openFDA
    # We fetch a small batch of recent/random events to simulate activity
    params = {
        "search":  f"patient.drug.openfda.generic_name:{SEARCH_TERM} OR patient.drug.openfda.brand_name:{SEARCH_TERM}",
        "limit": 20,
        "skip": datetime.datetime.now().minute * 20 # Rotate data based on minute to simulate variety
    }
    
    try:
        response = requests.get(OPENFDA_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        
        if not results:
            logging.warning("No results found from openFDA.")
            return

        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENTHUB_CONNECTION_STRING,
            eventhub_name=EVENTHUB_NAME
        )
        
        with producer:
            batch = producer.create_batch()
            
            for raw in results:
                # --- Transformation Logic (Ported from ingest_faers_stream.py) ---
                
                # Helpers
                def _safe_get(d, *keys, default=""):
                    current = d
                    for k in keys:
                        if isinstance(current, list):
                            current = current[0] if current else {}
                        if isinstance(current, dict):
                            current = current.get(k, default)
                        else:
                            return default
                    return current or default

                # Identification
                safety_report_id = raw.get("safetyreportid", str(uuid.uuid4()))
                receive_date_str = raw.get("receivedate", "")
                try:
                    receive_date = datetime.datetime.strptime(receive_date_str, "%Y%m%d").strftime("%Y-%m-%dT%H:%M:%SZ")
                except (ValueError, TypeError):
                    receive_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                # Timestamps - simulate realtime arrival
                ingest_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

                # Seriousness
                serious = raw.get("serious", "2")
                serious_flags = {
                    "is_serious": serious == "1",
                    "hospitalization": raw.get("seriousnesshospitalization", "0") == "1",
                    "death": raw.get("seriousnessdeath", "0") == "1",
                    "life_threatening": raw.get("seriousnesslifethreatening", "0") == "1",
                    "disability": raw.get("seriousnessdisabling", "0") == "1",
                    "congenital_anomaly": raw.get("seriousnesscongenitalanomali", "0") == "1",
                    "required_intervention": raw.get("seriousnessother", "0") == "1",
                }

                # Patient
                patient = raw.get("patient", {})
                patient_info = {
                    "age": _safe_get(patient, "patientonsetage", default=""),
                    "age_unit": _safe_get(patient, "patientonsetageunit", default=""),
                    "sex": _safe_get(patient, "patientsex", default=""),
                    "weight": _safe_get(patient, "patientweight", default=""),
                }

                # Drugs
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
                # Fix lists
                if isinstance(drug_info["generic_name"], list):
                    drug_info["generic_name"] = drug_info["generic_name"][0] if drug_info["generic_name"] else ""
                if isinstance(drug_info["brand_name"], list):
                    drug_info["brand_name"] = drug_info["brand_name"][0] if drug_info["brand_name"] else ""

                # Reactions
                reactions = patient.get("reaction", [])
                reaction_terms = [r.get("reactionmeddrapt", "") for r in reactions if r.get("reactionmeddrapt")]

                # Geo
                country = raw.get("occurcountry", raw.get("primarysource", {}).get("reportercountry", ""))
                sender_country = raw.get("sender", {}).get("senderorganization", "")

                # Final Event Object
                event = {
                    "event_id": str(uuid.uuid4()),
                    "safety_report_id": safety_report_id,
                    "receive_date": receive_date,
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
                
                json_payload = json.dumps(event)
                try:
                    batch.add(EventData(json_payload))
                except ValueError:
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(EventData(json_payload))
            
            if len(batch) > 0:
                producer.send_batch(batch)

                
        logging.info(f"Successfully published {len(results)} events to Event Hub.")

    except Exception as e:
        logging.error(f"Error during ingestion: {str(e)}")

