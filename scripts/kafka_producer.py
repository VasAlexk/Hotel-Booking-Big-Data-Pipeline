import pandas as pd 
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# ========== CONFIG ==========
CSV_FILE = "hotel_clickstream.csv"
TOPIC = "SDMD-1097464-final"  # νέο τελικό topic
BROKER = "150.140.142.67:9094"
INTERVAL = 5  # δευτερόλεπτα

# ========== ΦΟΡΤΩΣΗ ΔΕΔΟΜΕΝΩΝ ==========
print("Φόρτωση CSV...")
df = pd.read_csv(CSV_FILE, parse_dates=["timestamp"], keep_default_na=False)
df = df.sort_values("timestamp").reset_index(drop=True)

# Υπολογισμός Τ1 και μετατόπισης ΔΤ
t1 = df["timestamp"].iloc[0]
t2 = datetime.now()
delta = t2 - t1
df["adjusted_timestamp"] = df["timestamp"] + delta

print(f"Αποστολή {len(df)} εγγραφών προς Kafka...")
print(f"T1 = {t1}, T2 = {t2}, ΔT = {delta}")

# ========== ΡΥΘΜΙΣΗ KAFKA PRODUCER ==========
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# ========== STREAMING LOOP ==========
start_time = datetime.now()
while not df.empty:
    now = datetime.now()
    elapsed = now - start_time
    current_virtual_time = t2 + elapsed

    ready = df[df["adjusted_timestamp"] <= current_virtual_time]
    df = df[df["adjusted_timestamp"] > current_virtual_time]

    for _, row in ready.iterrows():
        event = row.drop("timestamp").to_dict()
        event["timestamp"] = row["adjusted_timestamp"].isoformat()

        # Καθαρισμός
        for k, v in event.items():
            if isinstance(v, (datetime, pd.Timestamp)):
                event[k] = v.isoformat()
            elif pd.isna(v) or v == "":
                event[k] = None

        # Χωρίς φίλτρο πλέον, στέλνεις τα πάντα
        producer.send(TOPIC, value=event)
        print(f"Sent: {event}")

    time.sleep(INTERVAL)

producer.flush()
producer.close()
print("Ολοκληρώθηκε η αποστολή.")
