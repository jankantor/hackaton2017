-- create source table

CREATE TABLE parties_navstevy AS SELECT
    a.*,
    0::integer prodej,
    0::integer hotovost,
    0::integer servis
  FROM z16028_cs_atm.parties a;

with hotovost as (select
   pt_unified_key,
   count(1) pocet
  FROM z16028_cs_atm.navstevy
  WHERE typ_trn = 'HOTOVOSTNI'
  GROUP BY pt_unified_key)
UPDATE parties_navstevy p SET
   hotovost = b.pocet
  FROM hotovost b
  WHERE p.pt_unified_key = b.pt_unified_key;


with prodej as (select
   pt_unified_key,
   count(1) pocet
  FROM z16028_cs_atm.navstevy
  WHERE typ_trn = 'PRODEJ'
  GROUP BY pt_unified_key)
UPDATE parties_navstevy p SET
   prodej = b.pocet
  FROM prodej b
  WHERE p.pt_unified_key = b.pt_unified_key;


with servis as (select
   pt_unified_key,
   count(1) pocet
  FROM z16028_cs_atm.navstevy
  WHERE typ_trn = 'SERVIS'
  GROUP BY pt_unified_key)
UPDATE parties_navstevy p SET
   servis = b.pocet
  FROM servis b
  WHERE p.pt_unified_key = b.pt_unified_key;

-- input for ML

CREATE TABLE aws_input_tmp as select 
  pt_unified_key ,
  right(orgh_code,3)::integer id_pobocky,
    prodej,
    hotovost,
    servis,
  pttp_unified_id ,
  psgen_unified_id ,
  og_unified_id ,
  sb_unified_id ,
  pv_unified_id  ,
  ptstat_unified_id ,
  big8_unified_id ,
  bigmse_unified_id ,
  pth_birth_year ,
  active_b24_internetbank_flag ,
  active_s24_internetbank_flag ,
  active_telebank_flag ,
  active_gsmbank_flag ,
  active_mobilebank_flag ,
  st_transform(st_setsrid(st_makepoint(lon,lat),4326),5514) as geom
from parties_navstevy
where lon is not null 
    AND pttp_unified_id = 'F'
    AND right(orgh_code,3) != 'XNA';


alter table aws_input_tmp add column big1 character varying(1);
alter table aws_input_tmp add column big2 character varying(1);
alter table aws_input_tmp add column big3 character varying(1);
alter table aws_input_tmp add column big4 character varying(1);
alter table aws_input_tmp add column big5 character varying(1);
alter table aws_input_tmp add column big6 character varying(1);
alter table aws_input_tmp add column big7 character varying(1);
alter table aws_input_tmp add column big8 character varying(1);
alter table aws_input_tmp add column visit integer;

update aws_input_tmp set visit = 1 where servis > 0 or hotovost>0 or prodej>0 ;

update aws_input_tmp set big1 = substring(big8_unified_id,1,1);
update aws_input_tmp set big2 = substring(big8_unified_id,2,1);
update aws_input_tmp set big3 = substring(big8_unified_id,3,1);
update aws_input_tmp set big4 = substring(big8_unified_id,4,1);
update aws_input_tmp set big5 = substring(big8_unified_id,5,1);
update aws_input_tmp set big6 = substring(big8_unified_id,6,1);
update aws_input_tmp set big7 = substring(big8_unified_id,7,1);
update aws_input_tmp set big8 = substring(big8_unified_id,8,1);


alter table aws_input_tmp drop column big8_unified_id ;
alter table aws_input_tmp drop column bigmse_unified_id ;
alter table aws_input_tmp drop column prodej ;
alter table aws_input_tmp drop column servis ;
alter table aws_input_tmp drop column hotovost ;

create index on aws_input_tmp (id_pobocky);


-- prihozeni geomu materske pobocky a vypocet vzdalenosti
CREATE TABLE aws_input AS SELECT
   a.*,
   b.geom geom_pobocky
  FROM aws_input_tmp a LEFT JOIN z16028_cs_atm.cs_pobocky b ON a.id_pobocky = b.id_pobocky;



ALTER TABLE aws_input ADD COLUMN distance INTEGER;
UPDATE aws_input SET distance = St_Distance(geom,geom_pobocky);


--
ALTER TABLE aws_input
  DROP COLUMN geom,
  DROP COLUMN id_pobocky,
  DROP COLUMN geom_pobocky;

DROP TABLE aws_input_tmp;

-- add ID
ALTER TABLE aws_input ADD COLUMN id serial;


-- add age
ALTER TABLE aws_input ADD COLUMN age INTEGER;
UPDATE aws_input SET age =
   CASE WHEN 2017 - pth_birth_year::INTEGER < 15 THEN 15
        WHEN 2017 - pth_birth_year::INTEGER >= 15 AND 2017 - pth_birth_year::INTEGER < 20 THEN 20

        WHEN 2017 - pth_birth_year::INTEGER >= 20 AND 2017 - pth_birth_year::INTEGER < 30 THEN 30
        WHEN 2017 - pth_birth_year::INTEGER >= 30 AND 2017 - pth_birth_year::INTEGER < 40 THEN 40
        WHEN 2017 - pth_birth_year::INTEGER >= 40 AND 2017 - pth_birth_year::INTEGER < 50 THEN 50
        WHEN 2017 - pth_birth_year::INTEGER >= 50 AND 2017 - pth_birth_year::INTEGER < 60 THEN 60
        WHEN 2017 - pth_birth_year::INTEGER >= 60 THEN 100
    END;



-- CSVs
CREATE TABLE aws_test_sample AS SELECT 
 pt_unified_key,
 pttp_unified_id,
 psgen_unified_id,
 og_unified_id,
 sb_unified_id,
 pv_unified_id,
 ptstat_unified_id,
 active_b24_internetbank_flag,
 active_s24_internetbank_flag,
 active_telebank_flag,
 active_gsmbank_flag,
 active_mobilebank_flag,
 big1,
 big2,
 big3,
 big4,
 big5,
 big6,
 big7,
 big8,
 visit,
 distance,
 id,
 age
 from aws_input order by random() limit 2000000;

CREATE TABLE aws_validate_sample as SELECT
 pt_unified_key,
 pttp_unified_id,
 psgen_unified_id,
 og_unified_id, 
 sb_unified_id,
 pv_unified_id,
 ptstat_unified_id,
 active_b24_internetbank_flag,
 active_s24_internetbank_flag,
 active_telebank_flag,
 active_gsmbank_flag,
 active_mobilebank_flag,
 big1,
 big2,
 big3,
 big4,
 big5,
 big6,
 big7,
 big8,
 visit,
 distance,
 id,
 age
 from aws_input except select 
 pt_unified_key,
 pttp_unified_id,
 psgen_unified_id,
 og_unified_id, 
 sb_unified_id,
 pv_unified_id,
 ptstat_unified_id,
 active_b24_internetbank_flag,
 active_s24_internetbank_flag,
 active_telebank_flag,
 active_gsmbank_flag,
 active_mobilebank_flag,
 big1,
 big2,
 big3,
 big4,
 big5,
 big6,
 big7,
 big8,
 visit,
 distance,
 id,
 age
 from aws_test_sample;

\copy aws_test_sample to ~/git_sukic/hackaton2017/aws_test_sample.csv with (format csv, header true) 
\copy aws_validate_sample to ~/git_sukic/hackaton2017/aws_validate_sample.csv with (format csv, header true) 





