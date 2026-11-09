-- Property dimension for DOB NOW job filings
-- Grain: one row per distinct combo of property identifiers (bin, bbl, block, lot)
--
-- Only the stable identifiers live here. Block and lot are embedded in the BBL, so they
-- can't conflict with it. As-filed attributes (house_no, street_name) and point-in-time
-- geography (community_board, council_district, census_tract, nta) vary between filings
-- for the same building, so they live on the fact table instead.

WITH stg_dob AS (
    SELECT * FROM {{ ref('stg_dob_jobs_data') }}
),

distinct_properties AS (

    SELECT DISTINCT
        bin,
        bbl,
        block,
        lot
    FROM stg_dob
    WHERE bin   IS NOT NULL
       OR bbl   IS NOT NULL
       OR block IS NOT NULL
       OR lot   IS NOT NULL

),

dim_property AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['bin', 'bbl', 'block', 'lot']) }} AS property_key,

        bin,
        bbl,
        block,
        lot

    FROM distinct_properties

),

-- Null-placeholder row: filings with no property identifiers
null_placeholder AS (

    SELECT
        '-1' AS property_key,
        CAST(NULL AS STRING) AS bin,
        CAST(NULL AS STRING) AS bbl,
        CAST(NULL AS STRING) AS block,
        CAST(NULL AS STRING) AS lot

)

SELECT * FROM dim_property

UNION ALL

SELECT * FROM null_placeholder