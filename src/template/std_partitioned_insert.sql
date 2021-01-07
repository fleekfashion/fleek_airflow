INSERT {{params.mode}}  {{params.target }}
SELECT {{ params.columns }}
FROM {{ params.src }}
WHERE {{params.partition_field}} = '{{ds}}'
