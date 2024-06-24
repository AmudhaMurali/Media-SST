
-----------------------------------------------------------------------
-- all sponsored trips (i.e. where we are tracking dwell time)
-- this is a supporting table but needed because "TripAdvisor"
-- is a username that is publishing sponsored trips, but we don't
-- want to pull in all TA trips
-----------------------------------------------------------------------

begin;
delete from &{pipeline_schema}.sponsored_trips;

insert into &{pipeline_schema}.sponsored_trips (
select distinct trip_id as trip_id,
       username         as username
from display_ads.public.sponsored_trips_dwell_time_v2)
;

commit;