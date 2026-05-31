{{ config(materialized='view') }}

SELECT * FROM {{ source('fabric_onelake', 'dim_city') }}
