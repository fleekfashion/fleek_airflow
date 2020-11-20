WITH top_p AS (
  SELECT *
  FROM {{ params.active_table }} 
  WHERE n_views > {{ params.min_views }} 
    AND n_likes + n_add_to_cart > 1
  ORDER BY (n_likes + n_add_to_cart) / n_views DESC
  LIMIT {{ params.limit }} 
)

MERGE INTO {{ params.active_table }} TARGET
USING (SELECT * FROM top_p) SRC
ON TARGET.product_id = SRC.product_id
WHEN MATCHED THEN UPDATE SET
  TARGET.product_tags = array_union(TARGET.product_tags, array('{{ params.tag }}'))
