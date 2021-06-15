BEGIN;

DELETE FROM {{ params.prod_table }}
WHERE product_id IN (
  SELECT 
    product_id
  FROM {{ params.product_info }}
);

INSERT INTO 
  {{ params.prod_table }} 
  ({{ params.columns }})
SELECT product_id, UNNEST({{ params.label_column }})
FROM {{ params.product_info }};

END;

