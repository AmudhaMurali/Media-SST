
-----------------------------------------------------------
-- remake of the Woodsy Job partner_trip_impressions_v2
-- gives a count of uniques and user reach (impressions)
-- for each sponsored trip
-- this is a final table (i.e. to be used in tableau)
-----------------------------------------------------------

begin;
delete from &{pipeline_schema}.sponsored_trips_impressions
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.sponsored_trips_impressions
SELECT imps.ds                                  as ds,
       imps.trip_id                             as trip_id,
       trips.trip_title                         as trip_title,
       trips.username                           as username,
       trips.display_name                       as display_name,
       uu.os_type                               as os_type,
       uu.locale                                as locale,
       count(distinct imps.unique_id)           as uniques,
       count(imps.event_id)                     as impressions
FROM display_ads.public.sponsored_trip_imps imps
JOIN rio_sf.rust.a_unique_users uu on lower(uu.unique_id) = lower(imps.unique_id)
                                   and uu.ds = imps.ds
                                   and uu.is_blessed = 1
JOIN (
        select sl.user_id               as user_id,
            mm.USERNAME                 as username,
            mm.DISPLAY_NAME             as display_name,
            sl.id                       as trip_id,
            sl.title                    as trip_title,
            sl.description              as trip_desc,
            to_date(sl.created)         as created,
            to_date(sl.first_published) as first_published
        from rio_sf.cx_analytics.t_saves_lists sl
        left join rio_sf.cx_analytics.member_metadata mm on sl.USER_ID = mm.MEMBERID
        WHERE to_date(created)>= '2019-01-01'
        and FIRST_PUBLISHED is not null) trips on trips.trip_id = imps.trip_id
join &{pipeline_schema}.sponsored_trips st on imps.trip_id = st.trip_id
WHERE imps.ds between '&{start_dt}' and '&{end_dt}'
--AND trips.user_id in (select distinct user_id from &{pipeline_schema}.trip_sponsors)
AND lower(trips.username) in (select distinct lower(username) from &{pipeline_schema}.trip_sponsors)
GROUP BY imps.ds,
         imps.trip_id,
         trips.trip_title,
         trips.username,
         trips.display_name,
         uu.os_type,
         uu.locale
;

commit;