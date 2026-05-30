"""
stream_producer.py
------------------
Hotel booking & event data producer → Fabric Eventstream (Azure Event Hub protocol)

Environment variables (.env):
  FABRIC_CONNECTION_STRING   Full Event Hub connection string from Fabric
  INTERVAL_SEC               Seconds between events (default: 1.0)
  BATCH_SIZE                 Events per batch (default: 10)
"""

import os
import json
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

load_dotenv()

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
CONNECTION_STRING = os.getenv("FABRIC_CONNECTION_STRING", "")
INTERVAL_SEC      = float(os.getenv("INTERVAL_SEC", "1.0"))
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "10"))
OUTPUT_FILE       = "hotel_raw_stream.jsonl"

# ── Seed data ─────────────────────────────────────────────────────────────────
HOTELS = [
    {"hotel_id": "HTL001", "name": "Grand Bosphorus",  "city": "Istanbul",  "stars": 5},
    {"hotel_id": "HTL002", "name": "Tulip City Hotel", "city": "Amsterdam", "stars": 4},
    {"hotel_id": "HTL003", "name": "Aegean Pearl",     "city": "Izmir",     "stars": 4},
    {"hotel_id": "HTL004", "name": "Marble Palace",    "city": "Ankara",    "stars": 5},
    {"hotel_id": "HTL005", "name": "Harbour Suites",   "city": "Rotterdam", "stars": 3},
]

ROOM_TYPES    = ["Standard", "Deluxe", "Suite", "Executive", "Family"]
CHANNELS      = ["web", "mobile_app", "ota_booking", "walk_in", "corporate"]
EVENT_TYPES   = ["booking", "cancellation", "check_in", "check_out", "room_service"]
NATIONALITIES = ["TR", "NL", "DE", "FR", "GB", "US", "JP", "AE", "IT", "ES"]
ROOM_SERVICE_ITEMS = [
    "Breakfast in bed", "Extra towels", "Mini bar restock",
    "Late checkout request", "Airport transfer", "Spa appointment",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _future_date(min_days=1, max_days=180):
    d = datetime.now(timezone.utc) + timedelta(days=random.randint(min_days, max_days))
    return d.strftime("%Y-%m-%d")

def _price(room_type):
    base = {"Standard": 80, "Deluxe": 130, "Suite": 250, "Executive": 200, "Family": 160}[room_type]
    return round(base * random.uniform(0.85, 1.40), 2)

# ── Event generators ──────────────────────────────────────────────────────────
def make_booking(hotel):
    rt = random.choice(ROOM_TYPES)
    n  = random.randint(1, 14)
    p  = _price(rt)
    return {
        "event_type": "booking", "event_id": f"EVT-{random.randint(100000,999999)}",
        "event_ts": _now_iso(), "hotel_id": hotel["hotel_id"], "hotel_name": hotel["name"],
        "city": hotel["city"], "stars": hotel["stars"],
        "booking_id": f"BK{random.randint(1000000,9999999)}",
        "guest_id": f"G{random.randint(10000,99999)}", "nationality": random.choice(NATIONALITIES),
        "room_type": rt, "channel": random.choice(CHANNELS),
        "check_in_date": _future_date(), "nights": n,
        "price_per_night": p, "total_price": round(p * n, 2), "currency": "EUR",
        "adults": random.randint(1, 3), "children": random.randint(0, 2),
    }

def make_cancellation(hotel):
    return {
        "event_type": "cancellation", "event_id": f"EVT-{random.randint(100000,999999)}",
        "event_ts": _now_iso(), "hotel_id": hotel["hotel_id"], "hotel_name": hotel["name"],
        "city": hotel["city"], "booking_id": f"BK{random.randint(1000000,9999999)}",
        "guest_id": f"G{random.randint(10000,99999)}",
        "reason": random.choice(["change of plans", "found cheaper", "emergency", "duplicate"]),
        "refund_pct": random.choice([0, 50, 100]),
    }

def make_checkin(hotel):
    return {
        "event_type": "check_in", "event_id": f"EVT-{random.randint(100000,999999)}",
        "event_ts": _now_iso(), "hotel_id": hotel["hotel_id"], "hotel_name": hotel["name"],
        "city": hotel["city"], "booking_id": f"BK{random.randint(1000000,9999999)}",
        "guest_id": f"G{random.randint(10000,99999)}",
        "room_number": f"{random.randint(1,10)}{random.randint(10,30)}",
        "room_type": random.choice(ROOM_TYPES), "early_checkin": random.choice([True, False]),
    }

def make_checkout(hotel):
    return {
        "event_type": "check_out", "event_id": f"EVT-{random.randint(100000,999999)}",
        "event_ts": _now_iso(), "hotel_id": hotel["hotel_id"], "hotel_name": hotel["name"],
        "city": hotel["city"], "booking_id": f"BK{random.randint(1000000,9999999)}",
        "guest_id": f"G{random.randint(10000,99999)}",
        "satisfaction_score": random.randint(1, 10),
        "minibar_charged": round(random.uniform(0, 80), 2),
        "late_checkout": random.choice([True, False]),
    }

def make_room_service(hotel):
    qty = random.randint(1, 3)
    return {
        "event_type": "room_service", "event_id": f"EVT-{random.randint(100000,999999)}",
        "event_ts": _now_iso(), "hotel_id": hotel["hotel_id"], "hotel_name": hotel["name"],
        "city": hotel["city"], "guest_id": f"G{random.randint(10000,99999)}",
        "room_number": f"{random.randint(1,10)}{random.randint(10,30)}",
        "item": random.choice(ROOM_SERVICE_ITEMS),
        "quantity": qty, "amount": round(random.uniform(5, 60) * qty, 2), "currency": "EUR",
    }

FACTORY = {
    "booking": make_booking, "cancellation": make_cancellation,
    "check_in": make_checkin, "check_out": make_checkout, "room_service": make_room_service,
}
WEIGHTS = [0.45, 0.15, 0.15, 0.15, 0.10]

def generate_event():
    hotel = random.choice(HOTELS)
    etype = random.choices(EVENT_TYPES, weights=WEIGHTS, k=1)[0]
    return FACTORY[etype](hotel)

# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    if not CONNECTION_STRING:
        log.error("❌ FABRIC_CONNECTION_STRING not set in .env!")
        return

    log.info("🏨 Hotel Stream Producer started → Fabric Eventstream")
    log.info("   interval=%.1fs | batch=%d", INTERVAL_SEC, BATCH_SIZE)

    producer = EventHubProducerClient.from_connection_string(CONNECTION_STRING)
    total = 0

    try:
        with producer:
            batch_events = []
            while True:
                event = generate_event()
                batch_events.append(event)
                total += 1

                log.info("[%s] %s | %s | €%.2f",
                         event["event_type"].upper(),
                         event.get("hotel_name", ""),
                         event.get("city", ""),
                         event.get("total_price", event.get("amount", 0.0)))

                if len(batch_events) >= BATCH_SIZE:
                    # Write to local file
                    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                        for e in batch_events:
                            f.write(json.dumps(e, ensure_ascii=False) + "\n")

                    # Send to Fabric
                    event_data_batch = producer.create_batch()
                    for e in batch_events:
                        event_data_batch.add(EventData(json.dumps(e, ensure_ascii=False)))
                    producer.send_batch(event_data_batch)
                    log.info("📦 Batch of %d sent to Fabric | total: %d", len(batch_events), total)
                    batch_events.clear()

                time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("🛑 Stopped. Total events produced: %d", total)

if __name__ == "__main__":
    run()