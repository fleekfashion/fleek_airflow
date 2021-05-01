WITH user_action_counts AS (
  SELECT 
    user_id, 
    count(*) as c
  FROM {{ params.events_table }} 
  WHERE event in ('trashed_item', 'faved_item', 'bagged_item')
    AND execution_date > date_sub(current_date(), {{ params.n_days }} )
  GROUP BY user_id
)

SELECT
  first(user_id) as user_id,
  first(product_id) as product_id,
  1::int as index,
  1::double as score
FROM {{ params.events_table }}
WHERE user_id IN (
    SELECT user_id
    FROM user_action_counts
    WHERE c > 100
  ) AND execution_date > date_sub(current_date(), {{ params.n_days }} )
