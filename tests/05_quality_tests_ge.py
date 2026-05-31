"""
Great Expectations quality tests for Happy Bookings
Fabric notebook'undan adapt edildi — pandas versiyonu (GitHub Actions icin)
"""

import great_expectations as gx
import pandas as pd
import sys

print("Great Expectations version:", gx.__version__)

# --- MOCK DATA (CI ortaminda Fabric baglantisi yok) ---
# Gercek ortamda bu satiri degistir:
# df = pd.read_sql("SELECT * FROM bronze_booking.dbo.fact_booking", connection)

df = pd.DataFrame({
    "booking_id":          ["B001", "B002", "B003", "B004", "B005"],
    "hotel_id":            ["H1",   "H2",   "H1",   "H3",   "H2"],
    "customer_id":         ["C1",   "C2",   "C3",   "C4",   "C5"],
    "booking_date":        ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
    "checkin_date":        ["2024-02-01", "2024-02-05", "2024-02-10", "2024-02-15", "2024-02-20"],
    "checkout_date":       ["2024-02-03", "2024-02-07", "2024-02-12", "2024-02-17", "2024-02-22"],
    "nights":              [2.0, 2.0, 2.0, 2.0, 2.0],
    "room_type":           ["Single", "Double", "Suite", "Single", "Double"],
    "booking_channel":     ["Web", "App", "Phone", "Web", "App"],
    "total_price":         [150.0, 200.0, 500.0, 150.0, 200.0],
    "is_cancelled":        ["No", "No", "Yes", "No", "No"],
    "cancellation_reason": [None, None, "personal", None, None],
    "payment_status":      ["Paid", "Paid", "Refunded", "Paid", "Paid"],
    "satisfaction_score":  [4.5, 3.0, None, 5.0, 4.0],
    "minibar_charged":     [0.0, 25.0, 0.0, 10.0, 0.0],
    "late_checkout":       ["No", "Yes", "No", "No", "Yes"],
})

print(f"Tablo yuklendi: {len(df):,} satir, {len(df.columns)} kolon")

# --- BATCH / STREAM SPLIT ---
batch_df  = df.sample(frac=0.7, random_state=42)
stream_df = df.drop(batch_df.index)

print(f"Batch:  {batch_df.shape}")
print(f"Stream: {stream_df.shape}")

# --- GREAT EXPECTATIONS CONTEXT ---
context = gx.get_context()

data_source = context.data_sources.add_pandas(name="pandas_source")
data_asset  = data_source.add_dataframe_asset(name="hotel_asset")
batch_def   = data_asset.add_batch_definition_whole_dataframe(name="hotel_batch")

expected_columns = [
    "booking_id", "hotel_id", "customer_id",
    "booking_date", "checkin_date", "checkout_date",
    "nights", "room_type", "booking_channel", "total_price",
    "is_cancelled", "cancellation_reason", "payment_status",
    "satisfaction_score", "minibar_charged", "late_checkout",
]

# --- BATCH (%70) VALIDATION ---
print("\n--- BATCH (%70) VALIDATION ---")
batch_ge  = batch_def.get_batch(batch_parameters={"dataframe": batch_df})
validator = context.get_validator(batch=batch_ge)

validator.expect_column_values_to_not_be_null("booking_id")
validator.expect_column_values_to_not_be_null("hotel_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_not_be_null("booking_date")
validator.expect_column_values_to_be_unique("booking_id")
validator.expect_table_columns_to_match_ordered_list(expected_columns)

batch_results = validator.validate()
print("Validation success:", batch_results["success"])
print("Expectations run: ", batch_results["statistics"]["evaluated_expectations"])
print("Passed:           ", batch_results["statistics"]["successful_expectations"])
print("Failed:           ", batch_results["statistics"]["unsuccessful_expectations"])
print("Success %:        ", batch_results["statistics"]["success_percent"])

# --- STREAM (%30) VALIDATION ---
print("\n--- STREAM (%30) VALIDATION ---")
stream_batch     = batch_def.get_batch(batch_parameters={"dataframe": stream_df})
stream_validator = context.get_validator(batch=stream_batch)

stream_validator.expect_column_values_to_not_be_null("booking_id")
stream_validator.expect_column_values_to_not_be_null("hotel_id")
stream_validator.expect_column_values_to_not_be_null("customer_id")
stream_validator.expect_column_values_to_not_be_null("booking_date")
stream_validator.expect_column_values_to_not_be_null("checkin_date")
stream_validator.expect_column_values_to_not_be_null("checkout_date")
stream_validator.expect_column_values_to_be_unique("booking_id")
stream_validator.expect_table_columns_to_match_ordered_list(expected_columns)

stream_results = stream_validator.validate()
print("Validation success:", stream_results["success"])
print("Expectations run: ", stream_results["statistics"]["evaluated_expectations"])
print("Passed:           ", stream_results["statistics"]["successful_expectations"])
print("Failed:           ", stream_results["statistics"]["unsuccessful_expectations"])
print("Success %:        ", stream_results["statistics"]["success_percent"])

# --- SONUC ---
overall = batch_results["success"] and stream_results["success"]
print("\n=== OVERALL RESULT:", "PASSED" if overall else "FAILED", "===")

if not overall:
    sys.exit(1)
