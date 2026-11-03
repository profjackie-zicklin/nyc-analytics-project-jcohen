-- Clean and standardize 311 DOT service request data
-- One row per service request

WITH source AS (
   SELECT * FROM {{ source('raw', 'source_dob_jobs_data') }}
), -- Easier to refer to the dbt reference to a long name table this way

cleaned AS (
   SELECT
       -- Get all columns from source, except ones we're transforming below
       -- To do cleaning on them or explicitly cast them as types just in case
       * EXCEPT (
         filing_date,
         current_status_date,
         first_permit_date,
         approved_date,
         signoff_date,
         borough,
         zip,
         postcode
       ),

       -- Date/Time
       CAST(filing_date AS TIMESTAMP) as filing_date,
       CAST(current_status_date AS TIMESTAMP) as current_status_date,
       CAST(first_permit_date AS TIMESTAMP) as first_permit_date,
       CAST(approved_date AS TIMESTAMP) as approved_date,
       CAST(signoff_date AS TIMESTAMP) as signoff_date,

       -- Location - clean zip code, handling several common zip code data problems
       CASE
           WHEN UPPER(TRIM(CAST(postcode AS STRING))) IN ('N/A', 'NA') THEN NULL
           WHEN UPPER(TRIM(CAST(postcode AS STRING))) = 'ANONYMOUS' THEN 'Anonymous'
           WHEN LENGTH(CAST(postcode AS STRING)) = 5 THEN CAST(postcode AS STRING)
           WHEN LENGTH(CAST(postcode AS STRING)) = 9 THEN CAST(postcode AS STRING)
           WHEN LENGTH(CAST(postcode AS STRING)) = 10
               AND REGEXP_CONTAINS(CAST(postcode AS STRING), r'^\d{5}-\d{4}')
           THEN CAST(postcode AS STRING)
           ELSE NULL
       END AS zip,

       -- Location - standardized borough, just in case
       CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
           ELSE 'UNKNOWN or CITYWIDE'
       END AS borough,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   -- Filters
   WHERE borough IS NOT NULL

   -- Deduplicate
--    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY created_date DESC) = 1
)

SELECT * FROM cleaned
-- All should be part of this table after running: stg_nyc_311_dot
