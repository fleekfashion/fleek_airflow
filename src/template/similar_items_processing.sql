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
    array_intersect(root_p.product_secondary_labels, similar_p.product_secondary_labels) as shared_secondary_labels
  FROM {{params.product_similarity_table}} sp
  INNER JOIN all_products root_p 
  ON root_p.product_id=sp.product_id
  INNER JOIN {{params.active_table}} similar_p
  ON sp.similar_product_id=similar_p.product_id
)

SELECT 
  product_id, 
  similar_product_id,
  similarity_score * ( 
    1.0 + 
    .05*size(shared_secondary_labels) +
    .15*size(
      array_intersect(
        shared_secondary_labels,
        ARRAY(
          'jean',
          'jeans',
          'denim',
          'leggings',
          'yoga',
          'biker',
          'camo',
          'button-down',
          'long-sleeve',
          'short-sleeve',
          'turtleneck',
          'bodysuit',
          'blazer',
          'sweatpants',
          'pajama',
          'fleece',
          'bomber',
          'puffer',
          'active',
          'cycling',
          'running'
        )
      )
    )
  ) as similarity_score
FROM pinfo
