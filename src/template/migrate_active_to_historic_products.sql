SELECT * 
FROM `{{params.active_table}}` active_table
WHERE active_table.product_id NOT IN ( 
  SELECT product_id 
  FROM `{{ params.cj_table}}`
)

