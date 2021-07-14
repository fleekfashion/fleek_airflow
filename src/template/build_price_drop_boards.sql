-- Delete all products in price drop boards
DELETE FROM prod.board_product 
WHERE board_id IN
(
	SELECT board_id
	FROM prod.board
	WHERE board_type = 'price_drop'
);

-- Compute and insert new price drop products into price drop boards
WITH PROD_FAVE_IDS AS (
    SELECT *
	FROM prod.user_product_faves
    WHERE product_id IN (
        SELECT product_id
        FROM prod.product_info
        WHERE is_active
    )
), 
FAVE_DAY_PRICE AS (
    SELECT 
        pfi.*, 
        pph.product_price AS fave_product_price
	FROM PROD_FAVE_IDS pfi
	JOIN prod.product_price_history pph
    ON pfi.product_id = pph.product_id 
    AND to_timestamp(pfi.event_timestamp - 86400)::date = pph.execution_date -- solve one day error
    WHERE execution_date > '2021-06-14'
),
PRICE_DROP_PRODUCTS AS (
	SELECT 
        fdp.user_id, 
        fdp.product_id, 
        fdp.event_timestamp, 
        pi.product_sale_price AS current_product_price
	FROM FAVE_DAY_PRICE fdp 
	INNER JOIN prod.product_info pi
    ON pi.product_id = fdp.product_id
  WHERE
    (
      fdp.fave_product_price >= pi.product_sale_price + 10 
      OR
      fdp.fave_product_price * 0.9 >= pi.product_sale_price
    )
    AND fdp.fave_product_price >= pi.product_sale_price + 2
),
USER_IDS_TO_BOARD_IDS AS (
	SELECT 
        b.board_id,
        ub.user_id
	FROM prod.board b 
	INNER JOIN prod.user_board ub 
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
	JOIN prod.product_price_history pph
	ON (
		jbi.product_id = pph.product_id
		AND
		pph.product_price = jbi.current_product_price
	)
	GROUP BY jbi.product_id, jbi.board_id
)
INSERT INTO prod.board_product (
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
DROP TABLE IF EXISTS temp_price_drop;
CREATE TABLE temp_price_drop AS (
	SELECT 
		b.board_id, 
		max(bp.last_modified_timestamp) as last_modified_timestamp
	FROM prod.board b
	INNER JOIN prod.board_product bp
		ON b.board_id = bp.board_id
	WHERE b.board_type = 'price_drop'
	GROUP BY b.board_id
);
UPDATE prod.board b
SET last_modified_timestamp = t.last_modified_timestamp
FROM temp_price_drop t
WHERE b.board_id = t.board_id;

