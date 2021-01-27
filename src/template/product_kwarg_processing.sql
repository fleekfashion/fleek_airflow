CREATE OR REPLACE TABLE {{ params.src }} 
DEEP CLONE {{ params.output }};


{% for u in params.updates  %}
UPDATE {{ params.output}} AS t 
  {{ u }};
{% endfor %}
