WITH dedup AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY {{ params.unique_col }}) as rn 
    FROM {{ params.table }}
)

SELECT * EXCEPT(rn) 
FROM dedup
WHERE dedup.rn > 1
