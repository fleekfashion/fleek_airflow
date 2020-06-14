SELECT DISTINCT * 
FROM `{{params.cj_table}}` cj 
WHERE cj.product_id NOT IN ( 
  SELECT product_id 
  FROM `{{ params.active_table}}`
)

