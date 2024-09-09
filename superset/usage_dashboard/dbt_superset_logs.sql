{{
    config(
        materialized = 'incremental',
        unique_key='id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=["monitoring", "superset"]
    )
}}

{% set start_date =  " '2023-04-21' :: timestamp_ntz " %}

with
cte_latest_date as (
    {% if is_incremental() %}
        select max(dttm) as latest_dttm
        from {{ this }}
    {% else %}
    select {{ start_date }} as latest_dttm
  {% endif %}
),

cte_timestamp_limits as (
    select
        latest_dttm       as min_ts,
        current_timestamp as max_ts
    from cte_latest_date
),

/* logs table contains both user generated events and other generic actions (like API requests) */
cte_superset_logs_all as (
    select
        dttm::timestamp_ntz as dttm,
        id,
        action,
        user_id,
        json
    from {{ source('stage', 'superset_logs') }}
    where 1 = 1
        and dttm > (select min_ts from cte_timestamp_limits)
        and dttm <= (select max_ts from cte_timestamp_limits)
),

cte_event_logs as (
    select
        dttm,
        id,
        action,
        user_id,
        json
    from cte_superset_logs_all
    where 1 = 1
        and action = 'log'
),

cte_final as (
    select
        dttm,
        id,
        user_id,
        parse_json(json):source::varchar as source,
        parse_json(json):source_id::varchar as source_id,
        parse_json(json):slice_id::varchar as slice_id,
        parse_json(json):event_name::varchar as event_name,
        parse_json(json):event_type::varchar as event_type,
        parse_json(json):impression_id::varchar as impression_id,
        parse_json(json):duration::varchar as duration,
        json
    from cte_event_logs
)

select
    dttm,
    id,
    user_id,
    case 
        when source = 'explore'
            then 'CHART'
        when source = 'sqlLab'
            then 'SQL_LAB'
        else upper(source) 
    end as event_source,  
    source_id,
    slice_id,
    event_name,
    event_type,
    impression_id,
    duration,
    json
from cte_final