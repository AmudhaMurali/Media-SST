DELETE FROM &{pipeline_schema_sf}.article_title;
INSERT INTO &{pipeline_schema_sf}.article_title
select distinct
  r.space as article_id,
  'all' as locale,
  cast(regexp_replace(PARSE_JSON(r.config):slugText::string, '_', ' ') AS STRING)  as article_title

from  DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_rich_content_document r;

