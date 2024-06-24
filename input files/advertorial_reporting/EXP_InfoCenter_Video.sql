-- This is currently unused in reporting

INSERT OVERWRITE TABLE ${pipeline_schema}.Infocenter_video PARTITION(ds)

SELECT
    md.id,
    display_name,
    COALESCE(md.description, md.title) AS title,
    'video' AS type,
    ma.attrval as video_duration_seconds,
    MAX(Case when l.page_action = 'play_time' then CAST(l.product_attr AS INT) else null end) AS time_played_seconds,
    GREATEST(
        NVL(MAX(Case when l.page_action = 'play_time' then CAST(l.product_attr AS INT) else NULL END), 0),
        NVL(MAX(Case when l.page_action = 'pause' then CAST(l.product_attr AS DECIMAL(19,9)) else NULL END), 0)) AS max_played_time_seconds,
    MAX(case when l.page_action = 'complete' then 1 else 0 end)=1 AS played_to_end,
    MAX(case when l.page_action = 'complete' then 100 when l.page_action like 'progress_%' then CAST(REGEXP_EXTRACT(l.page_action, '[0-9]+', 0)
        AS INT) ELSE NULL END) AS max_progress_percentage,
    ad.locale,
    l.ds
FROM default.a_lookback_blessed_joined l
JOIN (
  SELECT
    ga.ds,
    ga.client_type,
    ga.device_class,
    COALESCE(device_id, ta_persistentcookie) AS uuid,
    locale,
    ga.page_properties AS page_properties,
    regexp_extract(page_properties, 'InfoCenterV6_(.*)', 1) as display_name,
    ga.page_action AS page_action,
    ga.url,
    ga.gass
  FROM sales.a_adsol_garecord AS ga
  LEFT JOIN t_location loc ON loc.id = ga.gasl
  WHERE ds >= DATE_SUB('${hiveconf:start_dt}', 2) AND ds <= '${hiveconf:end_dt}'
    AND ga.page_properties LIKE 'InfoCenterV6%'
    AND ga.gass = 'InfoCenterV6') ad ON l.unique_id = ad.uuid
JOIN default.t_media md ON l.product_id = md.id
JOIN default.t_media_attrs ma ON md.id = ma.mediaid AND ma.attrname = 'dur'
WHERE l.ds >= DATE_SUB('${hiveconf:start_dt}', 2) AND l.ds <= '${hiveconf:end_dt}'
GROUP BY
    md.id,
    display_name,
    COALESCE(md.description, md.title),
    'video',
    ma.attrval,
    ad.locale,
    l.ds;