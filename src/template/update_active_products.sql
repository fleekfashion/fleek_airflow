MERGE `{{params.active_table}}` as TARGET
USING (
  SELECT DISTINCT * 
  FROM `{{params.cj_table}}`
) as SOURCE
  ON TARGET.product_id = SOURCE.product_id
  WHEN MATCHED THEN 
    UPDATE SET {% for col in params.cj_columns[:-1] %}
    TARGET.{{col}} = SOURCE.{{col}}, 
    {% endfor %}
    TARGET.{{params.cj_columns[-1]}} = SOURCE.{{params.cj_columns[-1]}}
  WHEN NOT MATCHED BY SOURCE AND TARGET.execution_date < DATE_ADD(DATE("{{ ds }}"), INTERVAL -2 DAY) THEN
    DELETE
