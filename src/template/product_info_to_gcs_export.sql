WITH table AS (
  SELECT 
    *,
    true as is_active
  FROM {{params.prod_table}}
)

SELECT {{ params.columns }}
FROM table
