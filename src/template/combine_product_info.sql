CREATE OR REPLACE TEMPORARY VIEW pi AS (
  WITH labels AS (
    SELECT 
      product_id,
      collect_set(product_label) as product_labels
    FROM {{ params.labels }}
    GROUP BY product_id
  )
  SELECT 
    l.product_id,
    l.product_labels,
    {{ params.columns }}
  FROM {{params.src}} pi
  INNER JOIN labels l
    ON pi.product_id=l.product_id
  WHERE size(l.product_labels) > 0
);

SELECT 
  * 
FROM pi 
WHERE NOT ({{params.drop_args_filter}})
