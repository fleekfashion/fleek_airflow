-- Delete all products in price drop boards
DELETE FROM {{ params.board_product_table }} 
WHERE board_id IN
(
	SELECT board_id
	FROM {{ params.board_table }}
	WHERE board_type = 'price_drop'
);

-- Compute and insert new price drop products into price drop boards
WITH PROD_FAVE_IDS AS (
    SELECT *
	FROM {{ params.user_product_faves_table  }}
    WHERE product_id IN (
        SELECT product_id
        FROM {{ params.product_info_table }}
        WHERE is_active
    )
), 
FAVE_DAY_PRICE AS (
    SELECT 
        pfi.*, 
        pph.product_price AS fave_product_price
	FROM PROD_FAVE_IDS pfi
	JOIN {{ params.price_history_table }} pph
    ON pfi.product_id = pph.product_id 
    AND to_timestamp(pfi.event_timestamp - 86400)::date = pph.execution_date -- solve one day error
    WHERE execution_date > '{{macros.ds_add(ds, -params.max_days)}}'
),
PRICE_DROP_PRODUCTS AS (
	SELECT 
        fdp.user_id, 
        fdp.product_id, 
        fdp.event_timestamp, 
        pi.product_sale_price AS current_product_price
	FROM FAVE_DAY_PRICE fdp 
	INNER JOIN {{ params.product_info_table }} pi
    ON pi.product_id = fdp.product_id
  WHERE
    (
      fdp.fave_product_price >= pi.product_sale_price + {{params.min_decrease_total}}
      OR
      fdp.fave_product_price * (1 - {{params.min_decrease_pct}}) >= pi.product_sale_price
    )
    AND fdp.fave_product_price >= pi.product_sale_price + {{params.min_decrease_required}}
),
USER_IDS_TO_BOARD_IDS AS (
	SELECT 
        b.board_id,
        ub.user_id
	FROM {{ params.board_table }} b 
	INNER JOIN {{ params.user_board_table }} ub 
    ON ub.board_id = b.board_id
	WHERE b.board_type = 'price_drop'
),
BOARD_IDS_TO_PRODUCTS AS (
	SELECT jpdp.product_id, jui.board_id, jpdp.current_product_price
	FROM PRICE_DROP_PRODUCTS jpdp
	JOIN USER_IDS_TO_BOARD_IDS jui
	ON jui.user_id = jpdp.user_id
),
ADD_PRICE_DROP_TIMESTAMP AS (
	SELECT 
    jbi.product_id, 
    jbi.board_id, 
    min((pph.execution_date - '1970-01-01') * 24 * 60 * 60) AS last_modified_timestamp -- convert execution date to unix time
	FROM BOARD_IDS_TO_PRODUCTS jbi
	JOIN {{ params.price_history_table }} pph
	ON (
		jbi.product_id = pph.product_id
		AND
		pph.product_price = jbi.current_product_price
	)
	GROUP BY jbi.product_id, jbi.board_id
)
INSERT INTO {{ params.board_product_table }} (
	board_id,
	product_id,
	last_modified_timestamp
)
SELECT 
  board_id, 
  product_id, 
  last_modified_timestamp 
FROM ADD_PRICE_DROP_TIMESTAMP;

-- Update price drop board last_modified_timestamp to most recently updated product
DROP TABLE IF EXISTS {{ params.temp_table }};
CREATE TABLE {{ params.temp_table }} AS (
	SELECT 
		b.board_id, 
		max(bp.last_modified_timestamp) as last_modified_timestamp
	FROM {{ params.board_table }} b
	INNER JOIN {{ params.board_product_table }} bp
		ON b.board_id = bp.board_id
	WHERE b.board_type = 'price_drop'
	GROUP BY b.board_id
);
UPDATE {{ params.board_table }} b
SET last_modified_timestamp = t.last_modified_timestamp
FROM {{ params.temp_table }} t
WHERE b.board_id = t.board_id;

