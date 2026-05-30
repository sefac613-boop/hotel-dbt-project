# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b9dbacf7-62f0-4c7a-ac5f-1075b6f945e0",
# META       "default_lakehouse_name": "bronze_booking",
# META       "default_lakehouse_workspace_id": "0999e6f2-dc46-4be3-bdc3-ec78ffa14baf",
# META       "known_lakehouses": [
# META         {
# META           "id": "b9dbacf7-62f0-4c7a-ac5f-1075b6f945e0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_bronze = spark.sql("SELECT * FROM bronze_booking.dbo.bronze_booking_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.sql("SELECT * FROM bronze_booking.dbo.bronze_booking_table")
print(df_bronze.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.sql("SELECT * FROM bronze_booking.dbo.bronze_booking_table")
print(df_bronze.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_bookings = spark.sql("SELECT * FROM bronze_booking.dbo.silver_bookings")
print(df_silver_bookings.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adım 1 — Kaynakları oku
df_bookings = spark.sql("SELECT * FROM bronze_booking.dbo.silver_bookings")
df_events = spark.sql("SELECT * FROM bronze_booking.dbo.bronze_booking_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================
# Adım 2 — Left Join Metodu ve Step 5 Temizlik Standartları
# ==========================================
from pyspark.sql.functions import col, trim, lower, upper, regexp_replace, to_date, when

# --- 1. KAYNAKLARI DOĞRU TABLOLARDAN OKUMA ---
df_bookings = spark.sql("SELECT * FROM bronze_booking.dbo.silver_bookings")
df_events = spark.sql("SELECT * FROM bronze_booking.dbo.silver_events")

# --- 2. ID YAPILARINI VE ÖN EKLERİ TEMİZLEME (Step 5) ---
df_bookings_fixed = df_bookings.withColumn(
    "booking_id", upper(trim(regexp_replace(col("booking_id"), "^!!", "")))
).withColumn(
    "hotel_id", upper(trim(regexp_replace(col("hotel_id"), "^!!", "")))
)

df_events_fixed = df_events.withColumn(
    "booking_id", upper(trim(regexp_replace(col("booking_id"), "^!!", "")))
)

# Tarih formatı standardizasyonu
df_bookings_fixed = df_bookings_fixed \
    .withColumn("booking_date",  to_date(col("booking_date"))) \
    .withColumn("checkin_date",  to_date(col("checkin_date"))) \
    .withColumn("checkout_date", to_date(col("checkout_date")))

# --- 3. KRİTİK ALANLAR İÇİN NULL VE KİRLİLİK FİLTRESİ (Step 5) ---
df_bookings_clean = df_bookings_fixed.filter(
    # Anahtar kolonlarda NULL veya anlamsız değer barındıran satırları eliyoruz
    (col("booking_id").isNotNull()) & (~trim(col("booking_id")).isin(["--", "???", ""])) &
    (col("hotel_id").isNotNull()) & (~trim(col("hotel_id")).isin(["--", "???", ""])) &
    (col("customer_id").isNotNull()) &
    (col("booking_date").isNotNull()) &
    (col("checkin_date").isNotNull()) &
    (col("checkout_date").isNotNull()) &
    (col("payment_status").isNotNull()) & (~trim(lower(col("payment_status"))).isin(["null", ""])) &
    # Outlier analizi — total_price sınırları
    (col("total_price") > 0) & (col("total_price") < 50000) &
    # Metinsel kirlilik filtreleri
    (~col("room_type").rlike("!!")) &
    (~trim(lower(col("room_type"))).isin(["--", "???", ""])) &
    (~col("booking_channel").rlike("!!")) &
    (~trim(lower(col("booking_channel"))).isin(["--", "???", "___", ""]))
)

# --- 4. FACT_BOOKING MODELİ (Left Join Metodu) ---
df_fact_booking = df_bookings_clean.alias("b").join(
    df_events_fixed.alias("e"), 
    on="booking_id", 
    how="left"
).select(
    "booking_id", 
    col("b.hotel_id").alias("hotel_id"), 
    "customer_id", "booking_date", "checkin_date", "checkout_date", 
    "nights", "room_type", "booking_channel", "total_price", 
    "is_cancelled", "cancellation_reason", "payment_status",
    "satisfaction_score", "minibar_charged", "late_checkout"
).dropDuplicates(["booking_id"])

# --- 5. DIM_CITY MODELİ ---
df_dim_city = df_bookings_clean.filter(
    (col("city").isNotNull()) & (~trim(col("city")).isin(["--", "???", "___", ""])) &
    (col("country").isNotNull()) & (~trim(col("country")).isin(["--", "???", ""])) &
    (col("hotel_type").isNotNull()) & (~trim(col("hotel_type")).isin(["--", "???", ""])) &
    (col("star_rating").isNotNull())
).select(
    "hotel_id", "hotel_name", "city", "country", "hotel_type", "star_rating"
).withColumn(
    "country", regexp_replace(col("country"), "^!!", "")
).dropDuplicates(["hotel_id"])

# --- 6. KPI_REVENUE MODELİ ---
df_kpi_revenue = df_bookings_clean.select(
    "booking_id", "total_price", "booking_date", "booking_channel", "is_cancelled"
).dropDuplicates(["booking_id"])

# --- 7. TABLOLARI DELTA FORMATINDA YAZMA ---
print("Gold modelleri Left Join yöntemiyle OneLake'e kaydediliyor...")

spark.sql("DROP TABLE IF EXISTS dbo.fact_booking")
spark.sql("DROP TABLE IF EXISTS dbo.dim_city")
spark.sql("DROP TABLE IF EXISTS dbo.kpi_revenue")

df_fact_booking.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.fact_booking")
df_dim_city.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.dim_city")
df_kpi_revenue.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.kpi_revenue")

print("✅ fact_booking, dim_city ve kpi_revenue tabloları başarıyla oluşturuldu!")
print(f"   fact_booking satır sayısı : {df_fact_booking.count():,}")
print(f"   dim_city satır sayısı     : {df_dim_city.count():,}")
print(f"   kpi_revenue satır sayısı  : {df_kpi_revenue.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# İki tablodaki ID formatlarını karşılaştırmak için ilk 5 satırı gösterelim
print("--- BOOKINGS TABLOSUNDAKİ ID YAPISI ---")
spark.sql("SELECT booking_id FROM bronze_booking.dbo.silver_bookings LIMIT 5").show()

print("--- EVENTS TABLOSUNDAKİ ID YAPISI ---")
spark.sql("SELECT booking_id FROM bronze_booking.dbo.silver_events LIMIT 5").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
