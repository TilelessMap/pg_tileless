-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_tileless" to load this file. \quit


/*CREATE OR REPLACE FUNCTION TWKB_Write2SQLite(sql_string text,sqlitedb text,table_name text , twkb_name text, geom_name text default '', id_name text default '')
RETURNS void
AS 'MODULE_PATHNAME', 'TWKB_Write2SQLite'
LANGUAGE c IMMUTABLE;*/

create schema if not exists tileless;

create schema if not exists tmp;

CREATE OR REPLACE FUNCTION tileless.TWKB_Write2SQLite(sqlitedb text,dataset_name text ,sql_string text, id_name text,twkb_name text default NULL,idx_tbl text default NULL, idx_geom text default NULL, idx_id text default NULL, create_table int default 1)
RETURNS int
AS 'MODULE_PATHNAME', 'TWKB_Write2SQLite'
LANGUAGE c ;


CREATE OR REPLACE FUNCTION tileless.pack_twkb_polygon(
    tbl text,
    geom_fld text,
    id_fld text,
    other_flds text,
    db text,
    layer_name text,
    prefix text default 'd'||now(),
    n_decimals integer default 0,
    subdivide boolean default true,
    cluster_index integer default 0)
  RETURNS int AS
$BODY$
declare
res int;
BEGIN

if length(other_flds) >0 then
	other_flds = ','||other_flds; 
end if;

execute 'drop sequence if exists tmp.'||prefix||'_sequence;
CREATE SEQUENCE tmp.'||prefix||'_sequence
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;';

/*

RAISE NOTICE 'Starta spatial ordering %s',now()::text;

execute 
'drop table if exists tmp.'||prefix||'_ordered;
create table tmp.'||prefix||'_ordered as
select *  from 
(select '||id_fld||', '||geom_fld||',  st_centroid('||geom_fld||') centr '||other_flds||'
from '||tbl||') a
order by round(st_x(centr)/10000),round(st_y(centr)/10000);';


RAISE NOTICE 'Klar med spatial ordering';*/
RAISE NOTICE 'Starta subgeoms %s',now()::text;

--select * from __trianglar
if subdivide then
	execute 
	'drop table if exists tmp.'||prefix||'_subgeoms;
	create table tmp.'||prefix||'_subgeoms as
	with a as (select '||id_fld||' orig_id, 
	(st_dump(st_collectionextract(st_makevalid(st_removerepeatedpoints(st_snaptogrid(st_simplifyvw(st_subdivide('||geom_fld||',2048), 0.5/10^('||n_decimals||')),1/10^('||n_decimals||')),0)),3))).geom geom '||other_flds||'
	/*from tmp.'||prefix||'_ordered*/
	from '||tbl||'
	)
	,b as (select orig_id, ST_ForceRHR((st_dump(st_collectionextract(st_makevalid(geom),3))).geom) geom '||other_flds||' from a)
	select * from b;';
else
	execute 
	'drop table if exists tmp.'||prefix||'_subgeoms;
	create table tmp.'||prefix||'_subgeoms as
	with a as (select '||id_fld||' orig_id, 
	(st_dump(st_collectionextract(st_makevalid(st_removerepeatedpoints(st_snaptogrid(st_simplifyvw('||geom_fld||', 0.5/10^('||n_decimals||')),1/10^('||n_decimals||')),0)),3))).geom geom '||other_flds||'
	/*from tmp.'||prefix||'_ordered*/
	from '||tbl||'
	)
	,b as (select orig_id, ST_ForceRHR((st_dump(st_collectionextract(st_makevalid(geom),3))).geom) geom '||other_flds||' from a)
	select * from b;';
end if;
execute 'alter table tmp.'||prefix||'_subgeoms add column twkb_id serial primary key;';


RAISE NOTICE 'Klar med subgeoms';
RAISE NOTICE 'Starta trianglar %s',now()::text;


execute 'drop table if exists tmp.'||prefix||'_trianglar;
create table tmp.'||prefix||'_trianglar as
with a as (select twkb_id, (st_dump(st_tesselate(geom))).geom tri,nextval(''tmp.'||prefix||'_sequence''::regclass) trid from tmp.'||prefix||'_subgeoms) 
,b as (select twkb_id, trid, st_dumppoints(tri) d from a)
,c as (select twkb_id, trid, (d).geom tri, (d).path from b where (d).path[2] < 4)
select * from c;
create index idx_'||prefix||'_tri_geom on tmp.'||prefix||'_trianglar using gist(tri);
create index idx_'||prefix||'_tri_id on tmp.'||prefix||'_trianglar using btree(twkb_id);
analyze tmp.'||prefix||'_trianglar;';

RAISE NOTICE 'Klar med trianglar';
RAISE NOTICE 'Starta boundary %s',now()::text;


execute 'drop view if exists onepoly; drop table if exists tmp.'||prefix||'_boundary;';


if cluster_index::boolean then
	execute
	'create table tmp.'||prefix||'_boundary as
	with a as (select twkb_id,orig_id,  st_dumprings(geom) d '||other_flds||' from tmp.'||prefix||'_subgeoms )
	,b as (select twkb_id, orig_id, ST_ExteriorRing((d).geom) geom, (d).path path '||other_flds||' from a order by (d).path)
	,c as (select st_npoints(geom) npoints, twkb_id, orig_id,geom, path '||other_flds||' from b)
	,d as (select twkb_id, orig_id,st_collect(st_removepoint(geom,npoints-1) order by path) geom '||other_flds||' from c group by twkb_id, orig_id  '||other_flds||')
	select twkb_id, orig_id,geom, 
	dense_rank() over(order by 
	round((st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/'||cluster_index||'), round((st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/'||cluster_index||')) idx_id ,
	(st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/10000 boxx, (st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/10000 boxy '||other_flds||' from d;
	drop table if exists  tmp.'||prefix||'_for_index;
	create table tmp.'||prefix||'_for_index as with a as (SELECT ST_COLLECT(geom) geom, idx_id from tmp.'||prefix||'_boundary group by idx_id)
	select idx_id, geom from a order by (st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/10000, (st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/10000';
else
	execute
	'create table tmp.'||prefix||'_boundary as
	with a as (select twkb_id,orig_id,  st_dumprings(geom) d '||other_flds||' from tmp.'||prefix||'_subgeoms )
	,b as (select twkb_id, orig_id, ST_ExteriorRing((d).geom) geom, (d).path path '||other_flds||' from a order by (d).path)
	,c as (select st_npoints(geom) npoints, twkb_id, orig_id,geom, path '||other_flds||' from b)
	,d as (select twkb_id, orig_id,st_collect(st_removepoint(geom,npoints-1) order by path) geom '||other_flds||' from c group by twkb_id, orig_id  '||other_flds||')
	select twkb_id, orig_id,geom, 
	(st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/10000 boxx, (st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/10000 boxy '||other_flds||' from d;
/*	drop table if exists  tmp.'||prefix||'_for_index;
	create table tmp.'||prefix||'_for_index as SELECT ST_COLLECT(geom) geom, idx_id from tmp.'||prefix||'_boundary group by idx_id*/';
end if;


execute 'drop table if exists tmp.'||prefix||'_boundarypoints;
create table tmp.'||prefix||'_boundarypoints as
with a as (select twkb_id, st_dumppoints(geom) d from tmp.'||prefix||'_subgeoms)
, b as (select twkb_id, (d).geom geom, (d).path path from a)
, c as(select twkb_id, geom,path, max(path[2]) over (partition by twkb_id, path[1]) max_path from b)
, d as (select twkb_id, st_collect(geom order by path) geom from c where path[2] < max_path group by twkb_id) 
/*, d as (select twkb_id, st_collect(geom order by path) geom from c group by twkb_id) */
--, p as(insert into point_arrays(p_array, twkb_id) select st_astwkb(geom), twkb_id from d)
, e as(select twkb_id, st_dumppoints(geom) d from d)
select twkb_id, (d).geom geom, (d).path[1] point_index from e order by twkb_id, (d).path;';


RAISE NOTICE 'Klar med boundary';
RAISE NOTICE 'Starta triindex %s',now()::text;


execute 'drop table if exists tmp.'||prefix||'_triindex;
create table tmp.'||prefix||'_triindex as
select p.twkb_id,t.twkb_id tid, t.trid,  p.point_index - 1 point_index from tmp.'||prefix||'_boundarypoints p inner join  tmp.'||prefix||'_trianglar t on p.geom && t.tri and t.tri && p.geom limit 0;
 
insert into tmp.'||prefix||'_triindex(twkb_id, tid, trid, point_index)
select p.twkb_id,t.twkb_id tid, t.trid,  p.point_index - 1 point_index from tmp.'||prefix||'_boundarypoints p inner join  tmp.'||prefix||'_trianglar t on p.geom && t.tri and t.tri && p.geom;

delete from tmp.'||prefix||'_triindex where not twkb_id=tid;';



RAISE NOTICE 'Klar med triindex';
RAISE NOTICE 'Starta index_array %s',now()::text;

execute 'drop table if exists tmp.'||prefix||'_index_array_step0;

create table tmp.'||prefix||'_index_array_step0 as
select twkb_id, array_agg(point_index order by point_index) t from tmp.'||prefix||'_triindex group by twkb_id, trid;

create index idx_'||prefix||'_ta0_id
on tmp.'||prefix||'_index_array_step0
using btree(twkb_id);
analyze tmp.'||prefix||'_index_array_step0;

drop table if exists tmp.'||prefix||'_index_array;

create table tmp.'||prefix||'_index_array as
select twkb_id, st_astwkb(ST_MakeLine(st_makepoint(t[1], t[2], t[3]) order by round(t[1]/127),round(t[2]/127),round(t[2]/127))) pi from (select * from tmp.'||prefix||'_index_array_step0 order by twkb_id ) a group by twkb_id order by twkb_id limit 0;

insert into tmp.'||prefix||'_index_array(twkb_id, pi)
select twkb_id, st_astwkb(ST_MakeLine(st_makepoint(t[1], t[2], t[3]))) pi from (select * from tmp.'||prefix||'_index_array_step0 order by twkb_id ) a group by twkb_id order by twkb_id ;';



RAISE NOTICE 'Klar med index_array';
RAISE NOTICE 'Starta skrivning till sqlite %s',now()::text;


execute 'create or replace temp view onepoly as
select * from tmp.'||prefix||'_boundary;';

execute 'drop table if exists tileless.'||layer_name||'; create table tileless.'||layer_name||' as 
SELECT bd.twkb_id, bd.orig_id, st_astwkb(geom, '||n_decimals||') twkb,ia.pi tri_index '||other_flds||', geom from 
tmp.'||prefix||'_boundary bd inner join
tmp.'||prefix||'_index_array ia on bd.twkb_id=ia.twkb_id';

if cluster_index::boolean then
	execute 'select tileless.TWKB_Write2SQLite('''||db||''',
	'''||layer_name||''',
	''SELECT bd.idx_id, bd.twkb_id, bd.orig_id, st_astwkb(geom, '||n_decimals||') twkb,ia.pi tri_index '||other_flds||' from 
	tmp.'||prefix||'_boundary bd inner join
	tmp.'||prefix||'_index_array ia on bd.twkb_id=ia.twkb_id order by boxx, boxy'',
	''twkb_id'',''twkb'', ''tmp.'||prefix||'_for_index'',''geom'', ''idx_id'',1);' into res;
else
	execute 'select tileless.TWKB_Write2SQLite('''||db||''',
	'''||layer_name||''',
	''SELECT bd.twkb_id, bd.orig_id, st_astwkb(geom, '||n_decimals||') twkb,ia.pi tri_index '||other_flds||' from 
	tmp.'||prefix||'_boundary bd inner join
	tmp.'||prefix||'_index_array ia on bd.twkb_id=ia.twkb_id order by boxx, boxy'',
	''twkb_id'',''twkb'', ''tmp.'||prefix||'_boundary'',''geom'', ''twkb_id'',1);' into res;
end if;


return res;
END
$BODY$
  LANGUAGE plpgsql VOLATILE;
  
  
  
  
  
CREATE OR REPLACE FUNCTION tileless.pack_twkb_linestring(
    tbl text,
    geom_fld text,
    id_fld text,
    other_flds text,
    db text,
    layer_name text,
    prefix text default 'd'||now(),
    n_decimals integer default 0,
    subdivide boolean default true,
    cluster_index integer default 0)
  RETURNS bigint AS
$BODY$
declare 
res int;
BEGIN

if length(other_flds) >0 then
	other_flds = ','||other_flds; 
end if;

execute 'drop sequence if exists tileless.'||prefix||'_sequence;
CREATE SEQUENCE tileless.'||prefix||'_sequence
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;';


/*
RAISE NOTICE 'Starta spatial ordering %s',now()::text;

execute 
'drop table if exists tmp.'||prefix||'_ordered;
create table tmp.'||prefix||'_ordered as
select *  from 
(select '||id_fld||', '||geom_fld||',  st_centroid('||geom_fld||') centr '||other_flds||'
from '||tbl||') a
order by round(st_x(centr)/10000),round(st_y(centr)/10000);';
*/

RAISE NOTICE 'Klar med spatial ordering';
RAISE NOTICE 'Starta subgeoms %s',now()::text;

--select * from __trianglar
if subdivide then
	execute 
	'drop table if exists tmp.'||prefix||'_subgeoms;
	create table tmp.'||prefix||'_subgeoms as
	with a as (select '||id_fld||' orig_id, 
	(st_dump(st_collectionextract(st_makevalid(st_removerepeatedpoints(st_snaptogrid(st_simplifyvw(st_subdivide('||geom_fld||',2048), 0.5/10^'||n_decimals||'),1/10^'||n_decimals||'),0)),2))).geom geom '||other_flds||'
	/*from tmp.'||prefix||'_ordered*/
	from '||tbl||'
	)
	,b as (select orig_id, ST_ForceRHR((st_dump(st_collectionextract(st_makevalid(geom),2))).geom) geom '||other_flds||' from a)
	select *,(st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/10000 boxx, (st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/10000 boxy from b;';
else
	execute 
	'drop table if exists tmp.'||prefix||'_subgeoms;
	create table tmp.'||prefix||'_subgeoms as
	with a as (select '||id_fld||' orig_id, 
	(st_dump(st_collectionextract(st_makevalid(st_removerepeatedpoints(st_snaptogrid(st_simplifyvw('||geom_fld||', 0.5/10^'||n_decimals||'),1/10^'||n_decimals||'),0)),2))).geom geom '||other_flds||'
	/*from tmp.'||prefix||'_ordered*/
	from '||tbl||'
	)
	,b as (select orig_id, ST_ForceRHR((st_dump(st_collectionextract(st_makevalid(geom),2))).geom) geom '||other_flds||' from a)
	select *,(st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/10000 boxx, (st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/10000 boxy from b;';
end if;
execute 'alter table tmp.'||prefix||'_subgeoms add column twkb_id serial primary key;';


RAISE NOTICE 'Klar med subgeoms';

if cluster_index::boolean then
execute '
	drop table if exists  tmp.'||prefix||'_w_rank;
	create table tmp.'||prefix||'_w_rank as select * , 
	dense_rank() over(order by 
	round((st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/'||cluster_index||'), round((st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/'||cluster_index||')) idx_id from tmp.'||prefix||'_subgeoms;
	drop table if exists  tmp.'||prefix||'_for_index;
	create table tmp.'||prefix||'_for_index as with a as (SELECT ST_COLLECT(geom) geom, idx_id from tmp.'||prefix||'_w_rank group by idx_id)
	select idx_id, geom from a order by (st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/10000, (st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/10000;


	select tileless.TWKB_Write2SQLite('''||db||''',
	'''||layer_name||''',
	''SELECT bd.idx_id, bd.twkb_id, bd.orig_id, st_astwkb(geom, '||n_decimals||') twkb '||other_flds||' from 
	tmp.'||prefix||'_w_rank bd order by boxx, boxy'',
	''twkb_id'',''twkb'', ''tmp.'||prefix||'_for_index'',''geom'', ''idx_id'',1);' into res;
else
	execute 'select tileless.TWKB_Write2SQLite('''||db||''',
	'''||layer_name||''',
	''SELECT bd.twkb_id, bd.orig_id, st_astwkb(geom, '||n_decimals||') twkb '||other_flds||' from 
	tmp.'||prefix||'_subgeoms bd order by boxx, boxy'',
	''twkb_id'', ''twkb'',''tmp.'||prefix||'_subgeoms'',''geom'', ''twkb_id'',1);' into res;
end if;


return res;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

  

CREATE or replace FUNCTION tileless.pack_twkb_point(
    tbl text,
    geom_fld text,
    id_fld text,
    other_flds text,
    db text,
    layer_name text,
    prefix text DEFAULT ('d'::text || now()),
    n_decimals integer DEFAULT 0,
    cluster_index integer DEFAULT 0)
  RETURNS bigint AS
$BODY$
declare 
res int;
BEGIN

if length(other_flds) >0 then
	other_flds = ','||other_flds; 
end if;

execute 'drop sequence if exists tileless.'||prefix||'_sequence;
CREATE SEQUENCE tileless.'||prefix||'_sequence
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;';


/*
RAISE NOTICE 'Starta spatial ordering %s',now()::text;

execute 
'drop table if exists tmp.'||prefix||'_ordered;
create table tmp.'||prefix||'_ordered as
select *  from 
(select '||id_fld||', '||geom_fld||',  st_centroid('||geom_fld||') centr '||other_flds||'
from '||tbl||') a
order by round(st_x(centr)/10000),round(st_y(centr)/10000);';
*/

RAISE NOTICE 'Klar med spatial ordering';
RAISE NOTICE 'Starta subgeoms %s',now()::text;

	execute 
	'drop table if exists tmp.'||prefix||'_subgeoms;
	create table tmp.'||prefix||'_subgeoms as
	with a as (select '||id_fld||' orig_id, 
	(st_dump(st_collectionextract(st_makevalid(st_removerepeatedpoints(st_snaptogrid('||geom_fld||',1/10^('||n_decimals||')),0)),1))).geom geom '||other_flds||'
	/*from tmp.'||prefix||'_ordered*/
	from '||tbl||'
	)
	,b as (select orig_id, ST_ForceRHR((st_dump(st_collectionextract(st_makevalid(geom),1))).geom) geom '||other_flds||' from a)
	select *,(st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/10000 boxx, (st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/10000 boxy from b;';

execute 'alter table tmp.'||prefix||'_subgeoms add column twkb_id serial primary key;';


RAISE NOTICE 'Klar med subgeoms';

if cluster_index::boolean then
execute '
	drop table if exists  tmp.'||prefix||'_w_rank;
	create table tmp.'||prefix||'_w_rank as select * , 
	dense_rank() over(order by 
	round((st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/'||cluster_index||'), round((st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/'||cluster_index||')) idx_id from tmp.'||prefix||'_subgeoms;
	drop table if exists  tmp.'||prefix||'_for_index;
	create table tmp.'||prefix||'_for_index as with a as (SELECT ST_COLLECT(geom) geom, idx_id from tmp.'||prefix||'_w_rank group by idx_id)
	select idx_id, geom from a order by (st_xmin(geom) + (st_xmax(geom)-st_xmin(geom))/2)/10000, (st_ymin(geom) + (st_ymax(geom)-st_ymin(geom))/2)/10000;


	select tileless.TWKB_Write2SQLite('''||db||''',
	'''||layer_name||''',
	''SELECT bd.idx_id, bd.twkb_id, bd.orig_id, st_astwkb(geom, '||n_decimals||') twkb '||other_flds||' from 
	tmp.'||prefix||'_w_rank bd order by boxx, boxy'',
	''twkb_id'',''twkb'', ''tmp.'||prefix||'_for_index'',''geom'', ''idx_id'',1);' into res;
else
	execute 'select tileless.TWKB_Write2SQLite('''||db||''',
	'''||layer_name||''',
	''SELECT bd.twkb_id, bd.orig_id, st_astwkb(geom, '||n_decimals||') twkb '||other_flds||' from 
	tmp.'||prefix||'_subgeoms bd order by boxx, boxy'',
	''twkb_id'', ''twkb'',''tmp.'||prefix||'_subgeoms'',''geom'', ''twkb_id'',1);' into res;
end if;


return res;
END
$BODY$
  LANGUAGE plpgsql VOLATILE;
