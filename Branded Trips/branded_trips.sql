DELETE FROM &{pipeline_schema}.base_media_blessed_joined
WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';

INSERT INTO &{pipeline_schema}.branded_trip_impressions
SELECT
			ds,
            os_type,
            unique_id,


from &{pipeline_schema}.base_media_impressions
where ds between '&{start_dt}' and '&{end_dt}';


------------------------------------ Blessed Join ------------------------------------


INSERT INTO &{pipeline_schema}.branded_trip_impressions
SELECT
			ds,
            os_type,
            unique_id,


from &{pipeline_schema}.base_media_impressions
where ds between '&{start_dt}' and '&{end_dt}';