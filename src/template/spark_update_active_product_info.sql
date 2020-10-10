MERGE INTO {{params.active_table}} as TARGET
USING (
  SELECT * 
  FROM {{params.product_info_table}} active_table
  WHERE execution_date="{{ds}}"
) as SOURCE
  ON TARGET.product_id = SOURCE.product_id
WHEN MATCHED THEN 
  UPDATE SET
    {% for col in params.columns[:-1] %}
    TARGET.{{col}} = SOURCE.{{col}}, 
    {% endfor %}
    TARGET.{{params.columns[-1]}} = SOURCE.{{params.columns[-1]}}
