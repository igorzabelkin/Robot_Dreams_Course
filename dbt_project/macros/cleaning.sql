{% macro null_if_blank(col) -%}
NULLIF(TRIM({{ col }}), '')
{%- endmacro %}

{% macro null_if_na(col) -%}
CASE WHEN LOWER(TRIM({{ col }})) IN ('n/a','na','none','null','-','—') THEN NULL ELSE {{ col }} END
{%- endmacro %}

{% macro clean_string(col) -%}
{{ null_if_blank(null_if_na(col)) }}
{%- endmacro %}

{% macro to_number(col) -%}
TRY_TO_NUMBER({{ col }})
{%- endmacro %}

{% macro to_int(col) -%}
TRY_TO_NUMBER({{ col }}, 38, 0)
{%- endmacro %}

{% macro to_date(col) -%}
TRY_TO_DATE({{ col }})
{%- endmacro %}

{% macro to_timestamp(col) -%}
TRY_TO_TIMESTAMP_NTZ({{ col }})
{%- endmacro %}

{% macro to_decimal(col, p=38, s=2) -%}
TRY_TO_DECIMAL({{ col }}, {{ p }}, {{ s }})
{%- endmacro %}
