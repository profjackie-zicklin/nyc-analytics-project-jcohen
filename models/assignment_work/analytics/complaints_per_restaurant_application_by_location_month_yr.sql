 WITH restaurant_counts AS (                                                                                                                                                        
      SELECT                                                                                                                                                                         
          l.borough,                                                                                                                                                                 
          l.zip_code,                                       
          d.year,
          d.month,
          COUNT(*) AS total_restaurants
      FROM {{ ref('fact_restaurant_applications') }} f
      JOIN {{ ref('dim_location') }} l ON f.location_key = l.location_key
      JOIN {{ ref('dim_date') }} d ON f.submission_date_key = d.date_key
      GROUP BY l.borough, l.zip_code, d.year, d.month
  ),

  sidewalk_street_complaint_counts AS (
      SELECT
          l.borough,
          l.zip_code,
          d.year,
          d.month,
          COUNT(*) AS total_complaints
      FROM {{ ref('fact_dot_311_requests') }} f
      JOIN {{ ref('dim_location') }} l ON f.location_key = l.location_key
      JOIN {{ ref('dim_date') }} d ON f.created_date_key = d.date_key
      JOIN {{ ref('dim_complaint_type') }} ct ON f.complaint_type_key = ct.complaint_type_key
      WHERE ct.complaint_category IN ('Street Issues', 'Sidewalk Issues')
      GROUP BY l.borough, l.zip_code, d.year, d.month
  )

  SELECT
      r.borough,
      r.zip_code,
      r.year,
      r.month,
      r.total_restaurants,
      c.total_complaints as total_sidewalk_or_street_complaints,
      ROUND(c.total_complaints * 1.0 / r.total_restaurants, 2) AS complaints_per_restaurant
  FROM restaurant_counts r
  JOIN sidewalk_street_complaint_counts c
      ON r.borough = c.borough
      AND r.zip_code = c.zip_code
      AND r.year = c.year
      AND r.month = c.month
  ORDER BY r.borough, r.zip_code, r.year, r.month