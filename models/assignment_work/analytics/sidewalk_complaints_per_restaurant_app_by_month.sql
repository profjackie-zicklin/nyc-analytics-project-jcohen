WITH restaurant_counts AS (                                                                                                                                                        
      SELECT
          l.borough,                                                                                                                                                                 
          l.zip_code,                    
          d.month,                                                                                                                                                                   
          COUNT(*) AS total_restaurants                                                                                                                                              
      FROM {{ ref('fact_restaurant_applications') }} f
      JOIN {{ ref('dim_location') }} l ON f.location_key = l.location_key
      JOIN {{ ref('dim_date') }} d ON f.submission_date_key = d.date_key
      GROUP BY l.borough, l.zip_code, d.month
  ),

  sidewalk_complaint_counts AS (
      SELECT
          l.borough,
          l.zip_code,
          d.month,
          COUNT(*) AS total_complaints
      FROM {{ ref('fact_dot_311_requests') }} f
      INNER JOIN {{ ref('dim_location') }} l ON f.location_key = l.location_key
      INNER JOIN {{ ref('dim_date') }} d ON f.created_date_key = d.date_key
      INNER JOIN {{ ref('dim_complaint_type') }} ct ON f.complaint_type_key = ct.complaint_type_key
      WHERE ct.complaint_category = 'Sidewalk Issues'
      GROUP BY l.borough, l.zip_code, d.month
  )

  SELECT
      r.borough,
      r.zip_code,
      r.month,
      CONCAT('In ', FORMAT_DATE('%B', DATE(2000, r.month, 1))) AS month_label,
      r.total_restaurants,
      c.total_complaints AS total_sidewalk_complaints,
  FROM restaurant_counts r
  JOIN sidewalk_complaint_counts c
      ON r.borough = c.borough
      AND r.zip_code = c.zip_code
      AND r.month = c.month
  ORDER BY r.borough, r.zip_code, r.month