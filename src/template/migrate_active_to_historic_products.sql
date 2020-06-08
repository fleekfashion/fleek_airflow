SELECT * 
FROM `{{params.active_table}}` active_table
WHERE active_table.execution_date < DATE_ADD(DATE("{{ ds }}"), INTERVAL -10 DAY) 

