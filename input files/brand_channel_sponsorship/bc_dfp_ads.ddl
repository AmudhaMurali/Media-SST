
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_dfp_ads (

ds                      date,
unique_id               string,
element_type            string,
device                  string,
locale                  string,
bc_geo_id               int,
bc_geo_name             string,
user_country_id         int,
user_country_name       string,
marketing_campaign_id   int,
impressions             int,
had_interaction         boolean,
interactions            int

);

GRANT SELECT ON &{pipeline_schema_sf}.bc_dfp_ads TO PUBLIC;
