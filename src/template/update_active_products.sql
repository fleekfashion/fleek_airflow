WITH updated_products AS (
  SELECT 
    cj.*,
    active_table.product_embedding,
    active_table.n_views,
    active_table.n_likes,
    active_table.n_add_to_cart,
    active_table.n_conversions
  FROM `{{params.cj_table}}` cj
  INNER JOIN `{{params.active_table}}` active_table
    ON cj.product_id = active_table.product_id
),
old_products AS (
  SELECT * 
  FROM `{{params.active_table}}` active_table
  WHERE active_table.product_id NOT IN ( 
    SELECT product_id 
    FROM `{{ params.cj_table}}`
  )
  AND active_table.execution_date < DATE_ADD(DATE("{{ ds }}"), INTERVAL -10 DAY) 
)

SELECT * FROM updated_products
  UNION ALL
SELECT * FROM old_products
