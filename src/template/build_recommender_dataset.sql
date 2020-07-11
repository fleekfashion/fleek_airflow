CREATE TEMPORARY FUNCTION _timedecay(event_timestamp INT64) AS (
  1.0 /
    LOG(2 +
      TIMESTAMP_DIFF(
        TIMESTAMP_SECONDS({{ execution_date.int_timestamp }}), 
        TIMESTAMP_SECONDS(event_timestamp),
        HOUR
      )
    ) 
);


CREATE TEMPORARY FUNCTION _get_count(arr ANY TYPE, val ANY TYPE) AS (
  (SELECT COUNT(*) FROM UNNEST(arr) a WHERE a=val)
);

CREATE TEMPORARY FUNCTION _get_negative_weight(events ANY TYPE) AS (
  -1.0* ( 1 - (_get_count( ARRAY(SELECT event.event FROM UNNEST(events) as event),
    "trashed_item")/(ARRAY_LENGTH(events))) )/2
);

CREATE TEMPORARY FUNCTION _event_weights(event String, negative_weight FLOAT64) AS (
    CASE
      WHEN event="faved_item" THEN {{params.fave_weight}} 
      WHEN event="trashed_item" THEN negative_weight
      WHEN event="bagged_item" THEN {{params.bag_weight}} 
    END 
);

with processed_data AS (
  SELECT
    *,
    _get_negative_weight(events) as negative_weight
  FROM `{{params.table}}` 
)

SELECT 
  user_id,
  ARRAY(
    SELECT
       STRUCT<product_id INT64, weight FLOAT64> (
       event.product_id,
      _event_weights(event.event, negative_weight
      )*_timedecay(event.event_timestamp)
    )
    FROM UNNEST(events) as event
  ) as events
FROM processed_data
