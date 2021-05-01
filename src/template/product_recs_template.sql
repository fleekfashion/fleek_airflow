WITH user_action_counts AS (
  SELECT 
    user_id,
    count(*) as c
  FROM {{ params.events_table }} 
  WHERE 
    event in ('trashed_item', 'faved_item', 'bagged_item')
    AND execution_date > date_sub(current_date(), {{ params.n_days }} )
    AND product_id IS NOT NULL
  GROUP BY user_id
), random_product AS (
  SELECT product_id
  FROM {{ params.active_table }}
  LIMIT 1
)

SELECT
  user_id,
  product_id,
  1::int as index,
  1::double as score
FROM user_action_counts 
JOIN random_product
WHERE c > 100
