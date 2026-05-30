"""
02_stream_to_bronze.py
-----------------------
hotel_raw_stream.csv dosyasini okur ve satirlari
Fabric Eventstream'e (Azure Event Hub protokolu) gonderir.

- Her satiri JSON'a cevirir
- Batch halinde (BATCH_SIZE) Fabric'e yollar
- Gonderilen satirlari local JSONL dosyasina da yazar

Kullanim:
    python stream_to_bronze.py

Ortam degiskenleri (.env):
    FABRIC_CONNECTION_STRING   Fabric custom endpoint connection string
    BATCH_SIZE                 Kac satirda bir gonderilsin (default: 50)
    INTERVAL_SEC               Batch'ler arasi bekleme suresi (default: 0.5)
"""

import os
import json
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
CONNECTION_STRING = os.getenv("FABRIC_CONNECTION_STRING", "")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "50"))
INTERVAL_SEC      = float(os.getenv("INTERVAL_SEC", "0.5"))
INPUT_FILE        = os.path.join("data", "hotel_raw_stream.csv")
OUTPUT_JSONL      = os.path.join("data", "hotel_raw_stream.jsonl")

# ── Validasyon ────────────────────────────────────────────────────────────────
if not CONNECTION_STRING:
    log.error("❌ FABRIC_CONNECTION_STRING .env dosyasinda tanimli degil!")
    exit(1)

if not os.path.exists(INPUT_FILE):
    log.error("❌ %s bulunamadi! Once onbronze_ingest_batch.py calistirin.", INPUT_FILE)
    exit(1)

# ── CSV oku ───────────────────────────────────────────────────────────────────
log.info("📖 Okunuyor: %s", INPUT_FILE)
df = pd.read_csv(INPUT_FILE, low_memory=False)
total_rows = len(df)
log.info("✅ Toplam satir: %s", f"{total_rows:,}")

# NaN degerlerini None'a cevir (JSON uyumlulugu icin)
df = df.where(pd.notnull(df), None)

# ── Fabric'e gonder ───────────────────────────────────────────────────────────
log.info("🚀 Fabric Eventstream'e gonderiliyor | batch=%d | interval=%.1fs",
         BATCH_SIZE, INTERVAL_SEC)

producer = EventHubProducerClient.from_connection_string(CONNECTION_STRING)
sent_total = 0

try:
    with producer:
        with open(OUTPUT_JSONL, "w", encoding="utf-8") as jsonl_file:
            # Satirlari batch'lere bol
            for start in range(0, total_rows, BATCH_SIZE):
                chunk = df.iloc[start: start + BATCH_SIZE]

                # Event Hub batch olustur
                event_batch = producer.create_batch()

                for _, row in chunk.iterrows():
                    record = row.to_dict()
                    # event_type ekle (stream oldugunu belirt)
                    record["event_source"] = "batch_stream"
                    json_str = json.dumps(record, ensure_ascii=False, default=str)
                    event_batch.add(EventData(json_str))
                    jsonl_file.write(json_str + "\n")

                # Fabric'e gonder
                producer.send_batch(event_batch)
                sent_total += len(chunk)

                pct = (sent_total / total_rows) * 100
                log.info("📦 Gonderildi: %s / %s satir (%.1f%%)",
                         f"{sent_total:,}", f"{total_rows:,}", pct)

                time.sleep(INTERVAL_SEC)

except KeyboardInterrupt:
    log.info("🛑 Durduruldu. Gonderilen: %s satir", f"{sent_total:,}")

log.info("🎉 Tamamlandi! Toplam gonderilen: %s satir", f"{sent_total:,}")
log.info("   JSONL kayit: %s", OUTPUT_JSONL)
log.info("   Siradaki adim: Silver transformations notebook")