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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

df_bookings = spark.sql("SELECT * FROM silver_bookings")
df_events = spark.sql("SELECT * FROM silver_events")

print(f"Bookings: {df_bookings.count()}")
print(f"Events: {df_events.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum, avg, count, round, countDistinct,when

# Önce fiyat dağılımına bakalım
print("=== TOTAL PRICE DAĞILIMI ===")
df_bookings.select("total_price").summary("min", "25%", "50%", "75%", "max", "mean").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bookings_clean = df_bookings.filter(
    (col("total_price") > 0) &          # negatif fiyat yok
    (col("total_price") < 9999) &        # outlier yok
    (col("hotel_name").isNotNull()) &    # null hotel_name yok
    (~col("hotel_name").isin(["___", "--", "???"])) # kirli değer yok
)

gold_hotel_revenue = df_bookings_clean \
    .groupBy("hotel_id", "hotel_name", "city", "country", "hotel_type", "star_rating") \
    .agg(
        count("booking_id").alias("total_bookings"),
        round(sum("total_price"), 2).alias("total_revenue"),
        round(avg("total_price"), 2).alias("avg_booking_value"),
        round(avg("nights"), 2).alias("avg_nights"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .orderBy("total_revenue", ascending=False)

gold_hotel_revenue.show(5)
gold_hotel_revenue.write.format("delta").mode("overwrite").saveAsTable("gold_hotel_revenue")
print(f"gold_hotel_revenue ✅ — {gold_hotel_revenue.count()} otel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_customer_segments = df_bookings_clean \
    .filter(
        col("customer_id").isNotNull() &
        col("full_name").isNotNull() &
        (~col("full_name").isin(["--", "___", "???", "!!"]))
    ) \
    .groupBy("customer_id", "full_name", "loyalty_level_customer", "gender", "country_customer") \
    .agg(
        count("booking_id").alias("total_bookings"),
        round(sum("total_price"), 2).alias("total_spent"),
        round(avg("total_price"), 2).alias("avg_spent_per_booking"),
        round(avg("review_rating"), 2).alias("avg_review_rating")
    ) \
    .orderBy("total_spent", ascending=False)

gold_customer_segments.show(5)
gold_customer_segments.write.format("delta").mode("overwrite").saveAsTable("gold_customer_segments")
print(f"gold_customer_segments ✅ — {gold_customer_segments.count()} müşteri")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_cancellation = df_bookings_clean \
    .groupBy("hotel_id", "hotel_name", "city", "booking_channel", "room_type") \
    .agg(
        count("booking_id").alias("total_bookings"),
        count(when(col("is_cancelled") == "true", 1)).alias("total_cancelled"),
        round(
            count(when(col("is_cancelled") == "true", 1)) / count("booking_id") * 100, 2
        ).alias("cancellation_rate_pct")
    ) \
    .orderBy("cancellation_rate_pct", ascending=False)

gold_cancellation.show(5)
gold_cancellation.write.format("delta").mode("overwrite").saveAsTable("gold_cancellation_analysis")
print(f"gold_cancellation_analysis ✅ — {gold_cancellation.count()} kayıt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum, avg, count, round, countDistinct, when

df_bookings_clean = df_bookings.filter(
    (col("total_price") > 0) &
    (col("total_price") < 9999) &
    (col("hotel_name").isNotNull()) &
    (~col("hotel_name").isin(["___", "--", "???"])) &
    (~col("hotel_name").rlike("^!!"))
)

print(f"Temiz kayıt: {df_bookings_clean.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_customer_segments = df_bookings_clean \
    .filter(
        col("customer_id").isNotNull() &
        col("full_name").isNotNull() &
        (~col("full_name").isin(["--", "___", "???", "!!"]))
    ) \
    .groupBy("customer_id", "full_name", "loyalty_level_customer", "gender", "country_customer") \
    .agg(
        count("booking_id").alias("total_bookings"),
        round(sum("total_price"), 2).alias("total_spent"),
        round(avg("total_price"), 2).alias("avg_spent_per_booking"),
        round(avg("review_rating"), 2).alias("avg_review_rating")
    ) \
    .orderBy("total_spent", ascending=False)

gold_customer_segments.show(5)
gold_customer_segments.write.format("delta").mode("overwrite").saveAsTable("gold_customer_segments")
print(f"gold_customer_segments ✅ — {gold_customer_segments.count()} müşteri")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_channel = df_bookings_clean \
    .filter(col("booking_channel").isNotNull()) \
    .groupBy("booking_channel", "booking_source", "payment_method") \
    .agg(
        count("booking_id").alias("total_bookings"),
        round(sum("total_price"), 2).alias("total_revenue"),
        round(avg("total_price"), 2).alias("avg_revenue"),
        round(avg("discount_amount"), 2).alias("avg_discount"),
        count(when(col("is_cancelled") == "true", 1)).alias("cancellations")
    ) \
    .orderBy("total_revenue", ascending=False)

gold_channel.show(5)
gold_channel.write.format("delta").mode("overwrite").saveAsTable("gold_channel_performance")
print(f"gold_channel_performance ✅ — {gold_channel.count()} kayıt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_reviews = df_bookings_clean \
    .filter(col("review_id").isNotNull()) \
    .groupBy("hotel_id", "hotel_name", "city", "hotel_type", "star_rating") \
    .agg(
        count("review_id").alias("total_reviews"),
        round(avg("review_rating"), 2).alias("avg_review_rating"),
        count(when(col("review_rating") >= 4, 1)).alias("positive_reviews"),
        count(when(col("review_rating") <= 2, 1)).alias("negative_reviews"),
        round(avg("helpful_votes"), 2).alias("avg_helpful_votes")
    ) \
    .orderBy("avg_review_rating", ascending=False)

gold_reviews.show(5)
gold_reviews.write.format("delta").mode("overwrite").saveAsTable("gold_review_analysis")
print(f"gold_review_analysis ✅ — {gold_reviews.count()} otel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bookings_clean = df_bookings.filter(
    (col("total_price") > 0) &
    (col("total_price") < 9999) &
    (col("hotel_name").isNotNull()) &
    (~col("hotel_name").isin(["___", "--", "???"])) &
    (~col("hotel_name").rlike("^!!")) &
    (col("city").isNotNull()) &
    (~col("city").isin(["___", "--", "???"])) &
    (~col("city").rlike("^!!")) &
    (~col("city").rlike("\\.\\.\\."))   # "Fes..." gibi değerler
)

print(f"Temiz kayıt: {df_bookings_clean.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_reviews = df_bookings_clean \
    .filter(
        col("review_id").isNotNull() &
        col("review_rating").isNotNull() &
        (col("review_rating") >= 1) &
        (col("review_rating") <= 10)
    ) \
    .groupBy("hotel_id", "hotel_name", "city", "hotel_type", "star_rating") \
    .agg(
        count("review_id").alias("total_reviews"),
        round(avg("review_rating"), 2).alias("avg_review_rating"),
        count(when(col("review_rating") >= 4, 1)).alias("positive_reviews"),
        count(when(col("review_rating") <= 2, 1)).alias("negative_reviews"),
        round(avg("helpful_votes"), 2).alias("avg_helpful_votes")
    ) \
    .orderBy("avg_review_rating", ascending=False)

gold_reviews.show(5)
gold_reviews.write.format("delta").mode("overwrite").saveAsTable("gold_review_analysis")
print(f"gold_review_analysis ✅ — {gold_reviews.count()} otel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# Önce kirli değerleri görelim
print("=== KİRLİ HOTEL_ID'LER ===")
df_bookings_clean.filter(
    col("hotel_id").isNull() |
    col("hotel_id").isin(["--", "___", "???"]) |
    col("hotel_id").rlike("^!!")
).select("hotel_id").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS gold_hotel_revenue")
spark.sql("DROP TABLE IF EXISTS gold_cancellation_analysis")
spark.sql("DROP TABLE IF EXISTS gold_channel_performance")
spark.sql("DROP TABLE IF EXISTS gold_customer_segments")
spark.sql("DROP TABLE IF EXISTS gold_review_analysis")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
