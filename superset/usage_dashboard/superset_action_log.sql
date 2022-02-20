select
date_trunc('day', l.dttm) as action_date,
case
  when lower(l.action) = 'queries' then 'query from sqllab'
  when lower(l.action) = 'chartrestapi.data' then 'query from charts'
  when lower(l.action) = 'count' then 'dashboard view'
  when lower(l.action) like 'annotation%' then 'annotations'
  when lower(l.action) like 'css%' then 'css'
  else lower(l.action)
end as action,
l.user_id,
u.user_name,
u.created_on as user_registration_date,
l.dashboard_id,
d.dashboard_title,
case
  when d.published = true then 'published'
  else 'draft'
end as dashboard_status,
l.slice_id,
s.slice_name,
s.datasource_type,
s.datasource_name,
s.datasource_id,
count(1) as action_count
from logs as l
left join ab_user as u on u.id = l.user_id
left join dashboards as d on d.id = l.dashboard_id
left join slices as s on s.id = l.slice_id
where 1=1
and l.action != 'log'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
