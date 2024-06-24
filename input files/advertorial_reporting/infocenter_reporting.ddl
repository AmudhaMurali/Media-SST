CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.infocenter_reporting (
        ds                      date,
        unique_id               string,
        servlet_name            string,
        new_page_action         string,
        page_action             string,
        page_action_coalesce    string,
        url                     string,
        utm_source              string,
        utm_medium              string,
        last_referrer           string,
        last_referrer_domain    string,
        landing_page_referrer   string,
        landing_page            string,
        external_referral       boolean,
        client_type             string,
        os_type_id              int,
        os_type_name            string,
        user_country_id         int,
        user_country_name       string,
        pos                     string,
        is_blessed              boolean,
        utm_source_tran         string,
        mcid_tran               string
);

GRANT SELECT ON &{pipeline_schema_sf}.infocenter_reporting TO PUBLIC;