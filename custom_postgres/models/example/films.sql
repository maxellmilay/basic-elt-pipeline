-- models/example/films.sql

-- Configuration for the model
{{
  config(
    unique_key='film_id'
  )
}}

-- Define the CTE (Common Table Expression) to pull data from the source
WITH source_data AS (
    SELECT * FROM {{ source('destination_db', 'films') }}
)

-- Select statement to define the columns and transformations
SELECT
    *
FROM source_data

-- Incremental load condition to only include new or updated records
{% if is_incremental() %}
WHERE film_id > (SELECT max(film_id) FROM {{ this }})
{% endif %}