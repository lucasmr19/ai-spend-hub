{# Pseudonymise an identifier: sha256(salt || value) as 64 hex chars. Same output on every engine. #}
{% macro mask_id(column) -%}
  {{ return(adapter.dispatch('mask_id', 'ai_spend')(column)) }}
{%- endmacro %}

{% macro default__mask_id(column) -%}
  sha256({{ "'" ~ var('pii_salt') ~ "'" }} || {{ column }})
{%- endmacro %}

{% macro postgres__mask_id(column) -%}
  encode(sha256(convert_to({{ "'" ~ var('pii_salt') ~ "'" }} || {{ column }}, 'UTF8')), 'hex')
{%- endmacro %}

{# Use the project schema name as-is (staging / marts) instead of <target schema>_<custom schema>. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{- custom_schema_name if custom_schema_name is not none else target.schema -}}
{%- endmacro %}
