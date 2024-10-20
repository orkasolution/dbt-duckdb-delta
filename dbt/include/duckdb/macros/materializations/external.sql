{% materialization external, adapter="duckdb", supported_languages=['sql', 'python'] %}

  {%- set location = render(config.get('location', default=external_location(this, config))) -%})
  {%- set rendered_options = render_write_options(config) -%}

  {%- set format = config.get('format') -%}
  {%- set allowed_formats = ['csv', 'parquet', 'json'] -%}
  {%- if format -%}
      {%- if format not in allowed_formats -%}
          {{ exceptions.raise_compiler_error("Invalid format: " ~ format ~ ". Allowed formats are: " ~ allowed_formats | join(', ')) }}
      {%- endif -%}
  {%- else -%}
    {%- set format = location.split('.')[-1].lower() if '.' in location else 'parquet' -%}
    {%- set format = format if format in allowed_formats else 'parquet' -%}
  {%- endif -%}

  {%- set write_options = adapter.external_write_options(location, rendered_options) -%}
  {%- set read_location = adapter.external_read_location(location, rendered_options) -%}
  {%- set parquet_read_options = config.get('parquet_read_options', {'union_by_name': False}) -%}
  {%- set json_read_options = config.get('json_read_options', {'auto_detect': True}) -%}
  {%- set csv_read_options = config.get('csv_read_options', {'auto_detect': True}) -%}

  -- set language - python or sql
  -- i have to learn general about python models
  {%- set language = model['language'] -%}

  {%- set target_relation = this.incorporate(type='view') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('create_table', language=language) -%}
    {{- create_table_as(False, temp_relation, compiled_code, language) }}
  {%- endcall %}

  -- write a temp relation into file
  {{ write_to_file(temp_relation, location, write_options) }}

-- create a view on top of the location
  {% call statement('main', language='sql') -%}
    {% if format == 'json' %}
      create or replace view {{ intermediate_relation }} as (
        select * from read_json('{{ read_location }}'
        {%- for key, value in json_read_options.items() -%}
          , {{ key }}=
          {%- if value is string -%}
            '{{ value }}'
          {%- else -%}
            {{ value }}
          {%- endif -%}
        {%- endfor -%}
        )
      );
    {% elif format == 'parquet' %}
      create or replace view {{ intermediate_relation }} as (
        select * from read_parquet('{{ read_location }}'
        {%- for key, value in parquet_read_options.items() -%}
          , {{ key }}=
          {%- if value is string -%}
            '{{ value }}'
          {%- else -%}
            {{ value }}
          {%- endif -%}
        {%- endfor -%}
        )
      );
    {% elif format == 'csv' %}
    create or replace view {{ intermediate_relation }} as (
      select * from read_csv('{{ read_location }}'
      {%- for key, value in csv_read_options.items() -%}
        , {{ key }}=
        {%- if value is string -%}
          '{{ value }}'
        {%- else -%}
          {{ value }}
        {%- endif -%}
      {%- endfor -%}
      )
    );
    {% endif %}
  {%- endcall %}

  {%- set location = render(config.get('location', default=external_location(this, config))) -%})
  -- just a check if the options is a dictionary to stay compielnt but it will be used over config in the plugins
  {%- set rendered_options = render_write_options(config) -%}
  {%- set format = config.get('format', 'default') -%}
  {%- set plugin_name = config.get('plugin', 'native') -%}

  {% do store_relation(plugin_name, target_relation, location, format, config, False) %}
  -- in this moment target should exists as a view so we can setup grants or docu

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
