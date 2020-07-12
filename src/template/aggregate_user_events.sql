MERGE `{{ params.aggregated_events_table }}` AS TARGET
USING (
  SELECT
    user_id,
    ARRAY_AGG(
      STRUCT<product_id INT64, event STRING, method STRING, event_timestamp INT64> (product_id, event, method, event_timestamp)
    ) as events
  FROM `{{params.user_events_table}}`
  WHERE event IN ("faved_item", "trashed_item", "bagged_item")
  AND product_id is NOT NULL
  AND {{ prev_execution_date.int_timestamp or 1}} < event_timestamp
  AND event_timestamp <= {{ execution_date.int_timestamp }}
  AND DATE("{{ prev_ds }}") <= execution_date 
  AND execution_date <= DATE("{{ ds }}")
  GROUP BY user_id
) AS SOURCE
ON TARGET.user_id = SOURCE.user_id
WHEN MATCHED THEN UPDATE SET
  TARGET.events = ARRAY_CONCAT(SOURCE.events, TARGET.events)
WHEN NOT MATCHED BY TARGET THEN
  INSERT ( user_id, events )
  VALUES ( user_id, events )
