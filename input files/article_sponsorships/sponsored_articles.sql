DELETE FROM &{pipeline_schema_sf}.sponsored_articles;
INSERT into &{pipeline_schema_sf}.sponsored_articles

select distinct * from (
select distinct
  s.campaign_id  as  id,
  s.article_id as space,
  concat('l',s.article_id) as url_article_id,
  'Article' as servlet_name,
  s.locale,
  cast(regexp_replace(PARSE_JSON(r.config):slugText::string, '_', ' ') AS STRING)  as article_title,
  s.sponsor_name ,
  null as use_sponsor_info,
  s.campaign_start_date,
  s.campaign_end_date ,
  s.url as sponsor_url ,
  null as created_at,
  null as last_updated_at,
  null as is_original

from  &{pipeline_schema_sf}.article_sponsored_profile_key s
left join  DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_rich_content_document r  on r.space = s.article_id and r.lang = s.locale
where r.name = 'article'
and r.status = 4
and s.campaign_start_date >= '2022-01-01'

union all

select * from analytics.public.sponsored_articles_historical
where campaign_start_date < '2022-01-01'

)



