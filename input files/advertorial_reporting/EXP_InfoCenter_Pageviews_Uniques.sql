INSERT OVERWRITE TABLE ${pipeline_schema}.Infocenter_pageviews_uniques PARTITION(ds)

SELECT
  pvuu.locale
  ,pvuu.device_class
  ,pvuu.page_action as display_name
  ,pvuu.pageviews
  ,pvuu.unique_users
  ,pvuu.ds
FROM (
    SELECT ad.ds
        ,ad.locale
        ,ad.page_action
        ,ad.device_class
        ,sum(ad.pageviews) as pageviews
        ,count(ad.uuid) AS unique_users
    FROM (
        SELECT ds
            ,locale
            ,page_action
            ,device_class
            ,coalesce(device_id, ta_persistentcookie) as uuid
            ,count(1) AS pageviews
        FROM sales.a_adsol_lookback
        WHERE ds >= DATE_SUB('${hiveconf:start_dt}', 2) AND ds <= '${hiveconf:end_dt}'
            AND is_real_pv = 1
            AND servlet_name = 'InfoCenterV6'
        GROUP BY ds
            , locale, page_action, device_class, coalesce(device_id, ta_persistentcookie)
        ) ad
    GROUP BY ad.ds
        ,ad.locale
        ,ad.page_action
        ,ad.device_class
    ) pvuu