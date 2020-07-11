MERGE `{{params.active_products_table}}` as TARGET
USING (
  SELECT 
    product_id,
    SUM(CASE WHEN event="faved_item" OR event="trashed_item" THEN 1 ELSE 0 END) as n_views,
    SUM(CASE WHEN event="faved_item" THEN 1 ELSE 0 END) as n_likes,
    SUM(CASE WHEN event="bagged_item" THEN 1 ELSE 0 END) as n_add_to_cart
  FROM `{{ params.user_events_table }}` 
  WHERE product_id IS NOT NULL
    AND 
      {{ prev_execution_date_success.int_timestamp or 1}} < event_timestamp
    AND event_timestamp <= {{ execution_date.int_timestamp }}
    AND user_id NOT IN (
      1338143769388061356,
      1596069326878625953,
      182814591431031699
    )
  GROUP BY product_id
) AS SOURCE
ON TARGET.product_id = SOURCE.product_id
WHEN MATCHED THEN UPDATE SET
  TARGET.n_views = TARGET.n_views + SOURCE.n_views,
  TARGET.n_likes = TARGET.n_likes + SOURCE.n_likes,
  TARGET.n_add_to_cart = TARGET.n_add_to_cart + SOURCE.n_add_to_cart
