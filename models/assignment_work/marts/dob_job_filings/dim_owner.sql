-- Owner dimension for DOB NOW job filings
-- Grain: one row per distinct combo of property owner attributes

WITH stg_dob AS (
    SELECT * FROM {{ ref('stg_dob_jobs_data') }}
),

distinct_owners AS (

    SELECT DISTINCT
        owner_first_name    AS first_name,
        owner_last_name     AS last_name,
        owner_business_name AS business_name,
        owner_type          AS owner_type
    FROM stg_dob
    WHERE owner_first_name    IS NOT NULL
       OR owner_last_name     IS NOT NULL
       OR owner_business_name IS NOT NULL
       OR owner_type          IS NOT NULL

),

dim_owner AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'first_name', 'last_name', 'business_name', 'owner_type'
        ]) }} AS owner_key,

        first_name,
        last_name,
        business_name,
        owner_type

    FROM distinct_owners

),

-- Null-placeholder row: filings with no owner information
null_placeholder AS (

    SELECT
        '-1' AS owner_key,
        CAST(NULL AS STRING) AS first_name,
        CAST(NULL AS STRING) AS last_name,
        CAST(NULL AS STRING) AS business_name,
        CAST(NULL AS STRING) AS owner_type

)

SELECT * FROM dim_owner

UNION ALL

SELECT * FROM null_placeholder