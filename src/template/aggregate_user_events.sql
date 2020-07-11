SELECT
  user_id,
  ARRAY_AGG(product_id) as product_ids,
  ARRAY_AGG(event) as events,
  ARRAY_AGG(event_timestamp) as event_timestamps,
  ARRAY_AGG(method) as methods
FROM `{{params.user_events_table}}`
WHERE event IN ("faved_item", "trashed_item", "bagged_item")
AND product_id is NOT NULL
GROUP BY user_id
