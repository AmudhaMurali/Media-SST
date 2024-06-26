
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ip_hotel_comp_set (

    brand_code              string,
    brand                   string,
    parent_brand            string,
    comp_brand_code         string,
    comp_brand              string,
    comp_parent_brand       string,
    property_country_name   string,
    total_score             double,
    rank                    int

);

GRANT SELECT ON &{pipeline_schema}.ip_hotel_comp_set TO PUBLIC;




