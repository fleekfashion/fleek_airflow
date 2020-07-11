DELETE FROM `{{ params.active_table }}`
WHERE execution_date < DATE_ADD(DATE("{{ ds }}"), INTERVAL -2 DAY)
