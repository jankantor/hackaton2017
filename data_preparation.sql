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


-- CSVs
CREATE TABLE aws_test_sample AS SELECT * from aws_input order by random() limit 2000000;
CREATE TABLE aws_validate_sample as SELECT * from aws_input except select * from aws_test_sample;

\copy aws_test_sample to ~/git_sukic/hackaton2017/aws_test_sample.csv with (format csv, header true) 
\copy aws_validate_sample to ~/git_sukic/hackaton2017/aws_validate_sample.csv with (format csv, header true) 





