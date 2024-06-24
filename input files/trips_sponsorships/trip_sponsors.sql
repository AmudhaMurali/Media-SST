
-----------------------------------------------------------------------
-- all trip product sponsors (i.e. where we are tracking dwell time)
-- this is a supporting table
-----------------------------------------------------------------------

begin;
delete from &{pipeline_schema}.trip_sponsors;

insert into &{pipeline_schema}.trip_sponsors
select distinct lower(dt.username) as username,
       mm.safeusername as safe_username,
       mm.display_name as display_name,
       mm.memberid as user_id,
       to_date(mm.CREATIONDATE) as creation_date
from display_ads.public.sponsored_trips_dwell_time_v2 dt
left join rio_sf.cx_analytics.member_metadata mm on lower(dt.username) = mm.safeusername
;

commit;