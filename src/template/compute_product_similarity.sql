CREATE OR REPLACE TEMPORARY VIEW active_products AS (
  SELECT 
    product_id,
    product_image_embedding,
    explode(product_labels) as product_label
  FROM {{ params.active_table }} 
);

CREATE OR REPLACE TEMPORARY VIEW all_products AS (
  SELECT 
    *
  FROM 
    active_products 

    UNION ALL

  SELECT 
    product_id,
    product_image_embedding,
    explode(product_labels) as product_label
  FROM {{ params.historic_table }} 
  WHERE 
    product_id NOT IN (
      SELECT product_id from {{ params.active_table }}
    ) 
    AND execution_date > DATE_SUB( '{{ ds }}', {{ params.historic_days }})
);

CREATE OR REPLACE TEMPORARY VIEW product_pairs AS (
  SELECT 
    ap.product_id, 
    ap.product_image_embedding, 
    p.product_id as similar_product_id, 
    p.product_image_embedding as similar_product_image_embedding
  FROM all_products ap
  INNER JOIN active_products p
  ON ap.product_label=p.product_label
  AND ap.product_id != p.product_id
);

CREATE OR REPLACE TEMPORARY VIEW similar_product_scores AS (
  SELECT 
    product_id,
    similar_product_id,
    AGGREGATE(
      TRANSFORM(
        arrays_zip(product_image_embedding, similar_product_image_embedding),
        x -> x['product_image_embedding']*x['similar_product_image_embedding']
      ),
      CAST(0 as float),
      (acc, value) -> acc + value
    ) as similarity_score
    
  FROM product_pairs 
);

CREATE OR REPLACE TEMPORARY VIEW processed_scores AS (
  SELECT 
    * 
  FROM similar_product_scores 
  WHERE similarity_score > {{ params.min_score }}
  ORDER BY product_id, similarity_score DESC
);

SELECT 
  *
FROM
  processed_scores
