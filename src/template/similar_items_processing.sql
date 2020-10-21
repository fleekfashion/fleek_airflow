WITH all_products AS (
  SELECT * FROM {{params.active_table}} 
    UNION ALL
  (
    SELECT * FROM {{params.historic_table}} 
    WHERE product_id NOT IN (select product_id FROM {{params.active_table}}) 
  )
), 
pinfo AS (
  SELECT 
    sp.*,
    p.product_labels as root_label,
    ap.product_labels as similar_label
  FROM prefiltered_similar_products sp
  INNER JOIN all_products p 
  ON p.product_id=sp.product_id
  INNER JOIN {{params.active_table}} ap
  ON sp.similar_product_id=ap.product_id
)

SELECT product_id, similar_product_id, similarity_score
FROM pinfo
WHERE size(array_intersect(root_label, similar_label)) > 0
