-- Seating type dimension for open restaurant seating applications
WITH seating_types AS (
   SELECT DISTINCT
       seating_interest_sidewalk AS seating_interest,
       CASE WHEN approved_for_roadway_seating = "yes" then True else False END AS approved_for_roadway,
       CASE WHEN approved_for_sidewalk_seating = "yes" then True else False END AS approved_for_sidewalk,
   FROM {{ ref('stg_nyc_open_restaurant_apps') }}--TODO: reference the appropriate staging table!
   WHERE seating_interest_sidewalk IS NOT NULL
),
seating_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([
           'seating_interest',
           'approved_for_sidewalk',
           'approved_for_roadway'
       ]) }} AS seating_type_key,

   approved_for_roadway,
   approved_for_sidewalk

   FROM seating_types
)

SELECT * FROM seating_dimension