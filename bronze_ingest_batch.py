"""
01_bronze_ingest_batch.py
--------------------------
1. booking_dirty.csv dosyasini okur
2. %70 batch / %30 stream olarak ikiye boler
3. hotel_raw_batch.csv  → data/ klasorune kaydeder
4. hotel_raw_stream.csv → data/ klasorune kaydeder

Kullanim:
    python bronze_ingest_batch.py
"""

import os
import pandas as pd
from dotenv import load_dotenv
import requests
import json
from datetime import date

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE      = "booking_dirty.csv"
OUTPUT_DIR      = "data"
BATCH_FILE      = os.path.join(OUTPUT_DIR, "hotel_raw_batch.csv")
STREAM_FILE     = os.path.join(OUTPUT_DIR, "hotel_raw_stream.csv")
BATCH_RATIO     = 0.70   # %70 batch
RANDOM_SEED     = 42

# ── Setup ─────────────────────────────────────────────────────────────────────
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── 1. CSV oku ────────────────────────────────────────────────────────────────
print(f"📖 Okunuyor: {INPUT_FILE} ...")
df = pd.read_csv(INPUT_FILE, low_memory=False)
print(f"✅ Toplam satir: {len(df):,}")
print(f"   Kolonlar    : {list(df.columns)}\n")

# ── 2. Temel bilgi ────────────────────────────────────────────────────────────
print("📊 NULL degerleri:")
null_counts = df.isnull().sum()
print(null_counts[null_counts > 0].to_string())
print()

print("📊 Veri tipleri:")
print(df.dtypes.to_string())
print()

# ── 3. Split ─────────────────────────────────────────────────────────────────
batch_df  = df.sample(frac=BATCH_RATIO,  random_state=RANDOM_SEED)
stream_df = df.drop(batch_df.index)

print(f"✂️  Bolunuyor:")
print(f"   Batch  (%70) : {len(batch_df):,} satir  → {BATCH_FILE}")
print(f"   Stream (%30) : {len(stream_df):,} satir → {STREAM_FILE}")
print()

# ── 4. Kaydet ────────────────────────────────────────────────────────────────
print("💾 Kaydediliyor...")
batch_df.to_csv(BATCH_FILE,   index=False, encoding="utf-8")
stream_df.to_csv(STREAM_FILE, index=False, encoding="utf-8")

print(f"✅ {BATCH_FILE}  kaydedildi  ({os.path.getsize(BATCH_FILE)  / 1e6:.1f} MB)")
print(f"✅ {STREAM_FILE} kaydedildi ({os.path.getsize(STREAM_FILE) / 1e6:.1f} MB)")
print()
print("🎉 Tamamlandi! Siradaki adim: 02_stream_to_bronze.py")

# ── 5. Weather API ────────────────────────────────────────────────────────────
print("🌤️  Hava durumu verisi çekiliyor...")

cities = {
    "Amsterdam": {"lat": 52.37, "lon": 4.89},
    "London":    {"lat": 51.51, "lon": -0.13},
    "Paris":     {"lat": 48.85, "lon": 2.35},
    "Berlin":    {"lat": 52.52, "lon": 13.40},
}

weather_rows = []
for city, coords in cities.items():
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude":  coords["lat"],
            "longitude": coords["lon"],
            "daily":     "temperature_2m_max,precipitation_sum",
            "timezone":  "Europe/Amsterdam",
            "forecast_days": 1
        }
    ).json()
    weather_rows.append({
        "date":        date.today().isoformat(),
        "city":        city,
        "temp_max_c":  resp["daily"]["temperature_2m_max"][0],
        "precip_mm":   resp["daily"]["precipitation_sum"][0],
    })

weather_df = pd.DataFrame(weather_rows)
weather_path = os.path.join(OUTPUT_DIR, "weather_api.csv")
weather_df.to_csv(weather_path, index=False)
print(weather_df)
print(f"✅ {weather_path} kaydedildi")
print()

# ── 6. Currency API ───────────────────────────────────────────────────────────
print("💱 Döviz kuru çekiliyor...")

resp = requests.get("https://api.frankfurter.app/latest?from=EUR&to=USD,GBP,TRY").json()
currency_rows = []
for target, rate in resp["rates"].items():
    currency_rows.append({
        "date":      resp["date"],
        "from":      "EUR",
        "to":        target,
        "rate":      rate,
    })

currency_df = pd.DataFrame(currency_rows)
currency_path = os.path.join(OUTPUT_DIR, "currency_api.csv")
currency_df.to_csv(currency_path, index=False)
print(currency_df)
print(f"✅ {currency_path} kaydedildi")
print()

print("🎉 Tamamlandı! data/ klasöründe 4 dosya hazır.")

