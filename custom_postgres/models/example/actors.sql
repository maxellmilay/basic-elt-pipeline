-- models/example/actors.sql

-- Configuration for the model
{{
  config(
    unique_key='actor_id'
  )
}}

-- Define the CTE (Common Table Expression) to pull data from the source
WITH source_data AS (
    SELECT * FROM {{ source('destination_db', 'actors') }}
)

-- Select statement to define the columns and transformations
SELECT
    *
FROM source_data

-- Incremental load condition to only include new or updated records
{% if is_incremental() %}
WHERE actor_id > (SELECT max(actor_id) FROM {{ this }})
{% endif %}