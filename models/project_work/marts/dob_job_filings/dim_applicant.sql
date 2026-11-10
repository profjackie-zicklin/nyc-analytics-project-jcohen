-- Applicant dimension for DOB NOW job filings
-- Grain: one row per distinct combo of applicant attributes

WITH stg_dob AS (
    SELECT * FROM {{ ref('stg_dob_jobs_data') }}
),

distinct_applicants AS (

    SELECT DISTINCT
        applicant_license           AS license_number,
        applicant_professional_title AS professional_title,
        applicant_first_name        AS first_name,
        applicants_middle_initial    AS middle_initial,
        applicant_last_name         AS last_name,
        applicant_business_name     AS business_name
    FROM stg_dob
    WHERE applicant_license            IS NOT NULL
       OR applicant_professional_title IS NOT NULL
       OR applicant_first_name         IS NOT NULL
       OR applicants_middle_initial     IS NOT NULL
       OR applicant_last_name          IS NOT NULL
       OR applicant_business_name      IS NOT NULL

),

dim_applicant AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'license_number', 'professional_title', 'first_name',
            'middle_initial', 'last_name', 'business_name'
        ]) }} AS applicant_key,

        license_number,
        professional_title,
        first_name,
        middle_initial,
        last_name,
        business_name

    FROM distinct_applicants

),

-- Null-placeholder row: filings with no applicant information
null_placeholder AS (

    SELECT
        '-1' AS applicant_key,
        CAST(NULL AS STRING) AS license_number,
        CAST(NULL AS STRING) AS professional_title,
        CAST(NULL AS STRING) AS first_name,
        CAST(NULL AS STRING) AS middle_initial,
        CAST(NULL AS STRING) AS last_name,
        CAST(NULL AS STRING) AS business_name

)

SELECT * FROM dim_applicant

UNION ALL

SELECT * FROM null_placeholder