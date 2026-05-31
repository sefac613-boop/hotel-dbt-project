# Happy Bookings — dbt + Microsoft Fabric Pipeline

A data engineering project that transforms hotel reservation data using dbt on Microsoft Fabric.

## Architecture

````\nKafka / API / DB\n      |
      v
bronze_ingest_batch.py   <- Ingests raw data into Fabric Lakehouse
      |
      v
dbt models (Bronze -> Silver -> Gold)
      |
      v
Microsoft Fabric (Views, Semantic Model, Power BI)
````\n
## dbt Models

| Model | Type | Source | Description |
|-------|------|--------|-------------|
| stg_fact_booking | view | fact_booking | Reservation transaction data |
| stg_dim_city | view | dim_city | Hotel location and star rating info |
| stg_kpi_revenue | view | kpi_revenue | Revenue and cancellation KPIs |

## Where to Monitor Results

- Power BI: Fabric workspace -> auto_report_booking
- Semantic Model: Happy_booking_semantic_model
- SQL Queries: bronze_booking -> dbo -> Views
