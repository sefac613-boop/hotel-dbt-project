{{ config(materialized='view') }}

SELECT * FROM {{ source('fabric_onelake', 'fact_booking') }}
