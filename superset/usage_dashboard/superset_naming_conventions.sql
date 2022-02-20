with cte_names as (
  select
  table_name as name,
  'virtualized dataset' as source,
  'published' as status,
  created_by_fk as created_by_user_id
  from tables
  where 1=1
  and is_sqllab_view = true

  union

  select
  dashboard_title as name,
  'dashboard' as source,
  case
    when d.published = true then 'published'
    else 'draft'
  end as status,
  du.user_id as created_by_user_id
  from dashboards as d
  left join dashboard_user as du on d.id=du.dashboard_id

  union

  select
  slice_name as name,
  'charts' as source,
  'published' as status,
  su.user_id as created_by_user_id
  from slices as s
  left join slice_user as su on s.id=su.slice_id
),

cte_names_with_properties as (
select
name,
source,
u.username as owner,
status,
case
  when lower(split_part(name,'.', 1)) = split_part(name,'.', 1) then true
  else false
end as domain_is_lowercase,
case
  when lower(name) like '%.%' then true
  else false
end as has_dot,
case
  when lower(name) like 'archived%' then true
  else false
end as is_archived,
split_part(name,'.', 1) as domains,
case
  when lower(name) like '%untitled%' then true
  when lower(name) ~ '\d{2}\/\d{2}\/\d{4}' then true
  else false
end as has_bad_phrases
from cte_names as tmp
left join ab_user as u on tmp.created_by_user_id = u.id
)

select
*,
case
  when domain_is_lowercase is true
   and has_dot is true
   and has_bad_phrases is false
   and is_archived is false
  then true
  else false
end as is_valid
from cte_names_with_properties
