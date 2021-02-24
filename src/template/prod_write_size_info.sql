TRUNCATE {{ params.prod_table }};
BEGIN;
  INSERT INTO {{ params.prod_table }} ({{ params.columns }})
  SELECT {{ params.columns }}
  FROM {{ params.staging_table }}
  WHERE (product_id, size) NOT IN (
    SELECT 
      product_id,
      size
    FROM {{ params.prod_table}}
  );
END;
