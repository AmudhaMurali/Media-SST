
insert overwrite into &{pipeline_schema}.active_sponsored_trips_detail

select sl.MEMBER_ID                 as user_id,
        mm.username                 as username,
        mm.display_name             as display_name,
        sl.id                       as trip_id,
        sl.title                    as trip_title,
        sl.description              as trip_desc,
        to_date(sl.created)         as created,
        to_date(sl.first_published) as first_published
from ENTERPRISE_DATA.TRIPS.VW_TRIPS sl
left join rio_sf.cx_analytics.member_metadata mm on sl.MEMBER_ID = mm.memberid
join (select distinct trip_id from (select distinct trip_id from DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.SPONSORED_TRIPS_ARCHIVE
                                    union all
                                    select distinct trip_id from rio_sf.cx_analytics.trips_orderid_mapping)
     ) sp on sl.id = sp.trip_id
where to_date(sl.created)  >= '2019-01-01'
and first_published is not null