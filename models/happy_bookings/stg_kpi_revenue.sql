{{ config(materialized='view') }}

SELECT * FROM {{ source('fabric_onelake', 'kpi_revenue') }}
