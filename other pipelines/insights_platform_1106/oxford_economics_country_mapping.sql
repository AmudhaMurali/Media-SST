--- oxford economics country name mapped to TA geo id

begin;
delete from &{pipeline_schema}.oxford_economics_country_mapping;

insert into   &{pipeline_schema}.oxford_economics_country_mapping
  			  (primaryname, id) values
('Australia','255055'),
('Austria','190410'),
('Canada','153339'),
('China','294211'),
('France','187070'),
('Germany','187275'),
('Greece','189398'),
('Hong Kong','294217'),
('India','293860'),
('Italy','187768'),
('Japan','294232'),
('Macau','664891'),
('Malaysia','293951'),
('Mexico','150768'),
('Portugal','189100'),
('Singapore','294262'),
('Spain','187427'),
('Sweden','189806'),
('Switzerland','188045'),
('Thailand','293915'),
('The Netherlands','188553'),
('Turkey','293969'),
('United Arab Emirates','294012'),
('United Kingdom','186216'),
('United States','191'),
('Abu Dhabi','294013'),
('Croatia','294453'),
('Finland','189896'),
('Ireland','186591'),
('Jordan','293985'),
('Kenya','294206'),
('Lithuania','274947'),
('Morocco','293730'),
('Saudi Arabia','293991'),
('South Africa','293740')

;

commit;