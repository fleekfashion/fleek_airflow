INSERT {{ params.mode }}  {{ params.target }}
SELECT {{ params.columns }}
FROM {{ params.src }}
{{ params.filter | default('') }}
