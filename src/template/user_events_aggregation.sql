SELECT
  user_id,
  ARRAY_AGG(product_id) as product_ids,
  ARRAY_AGG(event) as events,
  ARRAY_AGG(
    CASE
      WHEN event="faved_item" THEN 1.0
      WHEN event="trashed_item" THEN -.05
      WHEN event="bagged_item" THEN 2.0
    END * 1 / ( 2 + 
      LOG(
        ( {{ execution_date.int_timestamp }} - event_timestamp )/60*60*24)
    )
  ) AS weights,
FROM `user_data.user_events`
WHERE event IN ("faved_item", "trashed_item", "bagged_item")
AND product_id is NOT NULL
GROUP BY user_id
