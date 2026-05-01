{% macro celsius_to_fahrenheit(celsius_col) %}
    ({{ celsius_col }} * 9.0 / 5.0 + 32)
{% endmacro %}


{% macro fuel_consumed_litres(fuel_flow_col, dt_seconds) %}
    ({{ fuel_flow_col }} / 3600.0 * {{ dt_seconds }})
{% endmacro %}
