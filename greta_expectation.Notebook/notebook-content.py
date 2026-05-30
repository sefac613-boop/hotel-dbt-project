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
# META     "environment": {
# META       "environmentId": "c82676e2-481c-9032-4b11-ff194b479822",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import great_expectations as gx
import pandas as pd

# Silver tablosunu oku
df = spark.sql("SELECT * FROM bronze_booking.dbo.fact_booking").toPandas()

print(f"✅ Tablo yüklendi: {len(df):,} satır, {len(df.columns)} kolon")
print(f"   Kolonlar: {list(df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations
print(great_expectations.__version__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations as gx
from great_expectations.expectations.expectation import ExpectationConfiguration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(df)
df.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for name, obj in list(globals().items()):
    if isinstance(obj, pd.DataFrame):
        print(name, obj.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batch_df = df.sample(frac=0.7, random_state=42)
stream_df = df.drop(batch_df.index)

print(batch_df.shape)
print(stream_df.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations as gx

context = gx.get_context()

data_source = context.data_sources.add_pandas(
    name="pandas_source"
)

data_asset = data_source.add_dataframe_asset(
    name="hotel_asset"
)

batch_definition = data_asset.add_batch_definition_whole_dataframe(
    name="hotel_batch"
)

batch_ge = batch_definition.get_batch(
    batch_parameters={"dataframe": batch_df}
)

validator = context.get_validator(
    batch=batch_ge
)

print(type(validator))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_columns = [
    'booking_id',
    'hotel_id',
    'customer_id',
    'booking_date',
    'checkin_date',
    'checkout_date',
    'nights',
    'room_type',
    'booking_channel',
    'total_price',
    'is_cancelled',
    'cancellation_reason',
    'payment_status',
    'satisfaction_score',
    'minibar_charged',
    'late_checkout'
]

# 1. NOT NULL
validator.expect_column_values_to_not_be_null("booking_id")
validator.expect_column_values_to_not_be_null("hotel_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_not_be_null("booking_date")

# 2. UNIQUE
validator.expect_column_values_to_be_unique("booking_id")

# 3. SCHEMA VALIDATION
validator.expect_table_columns_to_match_ordered_list(
    expected_columns
)

# 4. DATE FORMAT VALIDATION
# 4. DATE VALIDATION
validator.expect_column_values_to_not_be_null("booking_date")
validator.expect_column_values_to_not_be_null("checkin_date")
validator.expect_column_values_to_not_be_null("checkout_date")

results = validator.validate()

print(results["success"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Validation success:", results["success"])
print("Expectations run:", results["statistics"]["evaluated_expectations"])
print("Passed:", results["statistics"]["successful_expectations"])
print("Failed:", results["statistics"]["unsuccessful_expectations"])
print("Success %:", results["statistics"]["success_percent"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- STREAM (%30) GREAT EXPECTATIONS VALIDATION ---

stream_batch = batch_definition.get_batch(
    batch_parameters={"dataframe": stream_df}
)

stream_validator = context.get_validator(
    batch=stream_batch
)

expected_columns = [
    'booking_id',
    'hotel_id',
    'customer_id',
    'booking_date',
    'checkin_date',
    'checkout_date',
    'nights',
    'room_type',
    'booking_channel',
    'total_price',
    'is_cancelled',
    'cancellation_reason',
    'payment_status',
    'satisfaction_score',
    'minibar_charged',
    'late_checkout'
]

# 1. NOT NULL VALIDATION
stream_validator.expect_column_values_to_not_be_null("booking_id")
stream_validator.expect_column_values_to_not_be_null("hotel_id")
stream_validator.expect_column_values_to_not_be_null("customer_id")
stream_validator.expect_column_values_to_not_be_null("booking_date")

# 2. UNIQUE VALIDATION
stream_validator.expect_column_values_to_be_unique("booking_id")

# 3. SCHEMA VALIDATION
stream_validator.expect_table_columns_to_match_ordered_list(
    expected_columns
)

# 4. DATE VALIDATION
# Tarihler parse edilemediyse null olur → bu yüzden null check kullanıyoruz
stream_validator.expect_column_values_to_not_be_null("booking_date")
stream_validator.expect_column_values_to_not_be_null("checkin_date")
stream_validator.expect_column_values_to_not_be_null("checkout_date")

# VALIDATION ÇALIŞTIR
stream_results = stream_validator.validate()

# SONUÇ ÖZETİ
print("Validation success:", stream_results["success"])
print("Expectations run:", stream_results["statistics"]["evaluated_expectations"])
print("Passed:", stream_results["statistics"]["successful_expectations"])
print("Failed:", stream_results["statistics"]["unsuccessful_expectations"])
print("Success %:", stream_results["statistics"]["success_percent"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SONUÇ ÖZETİ
print("Validation success:", stream_results["success"])
print("Expectations run:", stream_results["statistics"]["evaluated_expectations"])
print("Passed:", stream_results["statistics"]["successful_expectations"])
print("Failed:", stream_results["statistics"]["unsuccessful_expectations"])
print("Success %:", stream_results["statistics"]["success_percent"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build Great Expectations Data Docs report
context.build_data_docs()

# Show generated report path(s)
docs_urls = context.get_docs_sites_urls()

print(docs_urls)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Great Expectations Data Docs report was successfully generated and saved as an artifact using `context.build_data_docs()`. The report contains validation results for batch (70%) and stream (30%) datasets, including null validation, uniqueness checks, schema consistency, and date integrity validation.

