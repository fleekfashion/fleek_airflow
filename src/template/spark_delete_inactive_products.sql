DELETE
  FROM {{params.active_table}} active_table
  WHERE product_id NOT IN (
    SELECT product_id 
    FROM {{params.product_info_table}}
    WHERE execution_date="{{ds}}"
  )
