DELETE
FROM {{ params.processed_similarity_table }}
WHERE 
  product_id in (
    SELECT product_id FROM {{params.product_similarity_table}}
  );

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
    array_intersect(sp.product_secondary_labels, ap.product_secondary_labels) as shared_secondary_labels
  FROM {{params.product_similarity_table}} sp
  INNER JOIN all_products p 
  ON p.product_id=sp.product_id
  INNER JOIN {{params.active_table}} ap
  ON sp.similar_product_id=ap.product_id
)

SELECT 
  product_id, 
  similar_product_id,
  CASE WHEN size(array_intersect(root_label, similar_label)) > 0
    THEN 1.0*similarity_score
    ELSE similarity_score
  END AS similarity_score
FROM pinfo
