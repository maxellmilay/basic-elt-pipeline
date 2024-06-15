{% macro generate_film_ratings() %}

{{
  config(
    unique_key='film_id'
  )
}}
WITH source_data AS(
    WITH films_with_ratings AS (
        SELECT
            film_id,
            title,
            release_date,
            price,
            rating,
            user_rating,
            CASE
                WHEN user_rating >= 4.5 THEN 'Excellent'
                WHEN user_rating >= 4.0 THEN 'Good'
                WHEN user_rating >= 3.0 THEN 'Average'
                ELSE 'Poor'
            END AS rating_cetegory
        FROM {{ ref('films') }}
    ),

    films_with_actors AS (
        SELECT
            f.film_id,
            f.title,
            STRING_AGG(a.actor_name,', ') AS actors
        FROM {{ ref('films') }} f
        LEFT JOIN {{ ref('film_actors') }} fa ON f.film_id = fa.film_id
        LEFT JOIN {{ ref('actors') }} a ON fa.actor_id = a.actor_id
        GROUP BY f.film_id, f.title
    )

    SELECT
        fwf.*,
        fwa.actors
    FROM films_with_ratings fwf
    LEFT JOIN films_with_actors fwa ON fwf.film_id = fwa.film_id
)

-- Select statement to define the columns and transformations
SELECT
    *
FROM source_data

-- Incremental load condition to only include new or updated records
{% if is_incremental() %}
WHERE film_id > (SELECT max(film_id) FROM {{ this }})
{% endif %}

{% endmacro %}