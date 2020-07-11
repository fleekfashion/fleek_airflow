SELECT
  user_id,
  ARRAY_AGG(
    STRUCT<product_id INT64, event STRING, method STRING, event_timestamp INT64> (product_id, event, method, event_timestamp)
  ) as events
FROM `{{params.user_events_table}}`
WHERE event IN ("faved_item", "trashed_item", "bagged_item")
AND product_id is NOT NULL
GROUP BY user_id
