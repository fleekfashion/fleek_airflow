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

CREATE OR REPLACE TEMPORARY VIEW new_products AS (
  SELECT 
    product_id,
    product_image_embedding,
    explode(product_labels) as product_label
  FROM {{ params.active_table }} 
  WHERE product_id NOT IN (
    SELECT 
      DISTINCT product_id
    FROM {{ params.output_table }}
  )
);

CREATE OR REPLACE TEMPORARY VIEW old_products AS (
  SELECT *
  FROM all_products
  WHERE product_id NOT IN (
    SELECT product_id
    FROM new_products
  )
);

CREATE OR REPLACE TEMPORARY VIEW product_pairs AS (
  WITH old_to_new_scores AS (
    SELECT 
      op.product_id, 
      op.product_image_embedding, 
      p.product_id as similar_product_id, 
      p.product_image_embedding as similar_product_image_embedding
    FROM old_products op
    INNER JOIN new_products p
      ON op.product_label=p.product_label
      AND op.product_id != p.product_id
  ), new_to_all_scores AS (
    SELECT 
      np.product_id, 
      np.product_image_embedding, 
      ap.product_id as similar_product_id, 
      ap.product_image_embedding as similar_product_image_embedding
    FROM new_products np
    INNER JOIN active_products ap
      ON np.product_label=ap.product_label
      AND np.product_id != ap.product_id
  )
  SELECT 
    *
  FROM (
    SELECT *
    FROM old_to_new_scores
      
      UNION ALL

    SELECT * 
    FROM new_to_all_scores
  ) t
  WHERE similar_product_id IN (
    SELECT 
      product_id
    FROM active_products
  )
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
);


CREATE OR REPLACE TEMPORARY VIEW existing_active_scores AS (
  SELECT 
    * 
  FROM {{ params.output_table }}
  WHERE similar_product_id IN (
    SELECT 
      product_id
    FROM active_products
  )
);

CREATE OR REPLACE TEMPORARY VIEW all_scores AS (
  SELECT 
    *
  FROM (
    SELECT
      *
    FROM
      processed_scores

    UNION ALL

    SELECT
      *
    FROM
      existing_active_scores 
  )
  ORDER BY 
    product_id, similarity_score DESC
);

SELECT
  *
FROM all_scores
WHERE similarity_score > {{ params.min_score }}
