{% test zip_is_5_digits(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null and not regexp_matches({{ column_name }}, '^[0-9]{5}$')
{% endtest %}
