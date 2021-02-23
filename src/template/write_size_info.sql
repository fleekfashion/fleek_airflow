DELETE FROM {{ params.prod_table }}
WHERE (product_id, size) NOT IN (
  SELECT 
    product_id,
    size
  FROM {{ params.staging_table }}
);
INSERT INTO {{ params.prod_table }} ({{ columns }})
SELECT {{ columns }}
FROM {{ params.staging_table }}
WHERE (product_id, size) NOT IN (
  SELECT 
    product_id,
    size
  FROM {{ params.prod_table}}
);
