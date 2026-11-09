-- Job type dimension for DOB NOW job filings
-- Grain: one row per distinct combo of job type and filing review type

WITH stg_dob AS (
    SELECT * FROM {{ ref('stg_dob_jobs_data') }}
),

distinct_job_types AS (

    SELECT DISTINCT
        job_type,
        filing_review_type
    FROM stg_dob
    WHERE job_type           IS NOT NULL
       OR filing_review_type IS NOT NULL

),

dim_job_type AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['job_type', 'filing_review_type']) }} AS job_type_key,

        job_type,
        filing_review_type

    FROM distinct_job_types

),

-- Null-placeholder row
-- Job type is expected on every filing, so a fact row landing here is worth investigating
null_placeholder AS (

    SELECT
        '-1' AS job_type_key,
        CAST(NULL AS STRING) AS job_type,
        CAST(NULL AS STRING) AS filing_review_type

)

SELECT * FROM dim_job_type

UNION ALL

SELECT * FROM null_placeholder