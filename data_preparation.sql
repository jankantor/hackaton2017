-- create source table

CREATE TABLE parties_navstevy AS SELECT
    a.*,
    null::integer prodej,
    null::integer hotovost,
    null::integer servis
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

