/**********************************************************************
 *
 * pg_twkb - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2016 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the LICENCE file.
 *
 **********************************************************************/



#include <math.h>
#include "pg_tileless.h"
#include "utils/builtins.h"
#include "executor/spi.h"

#include <sqlite3.h>

/*
 * This is required for builds against pgsql
 */
PG_MODULE_MAGIC;


PG_FUNCTION_INFO_V1(TWKB_Write2SQLite);
Datum TWKB_Write2SQLite(PG_FUNCTION_ARGS)
{
    
    
    char *sqlitedb_name,*dataset_name, *sql_string, *twkb_name, *id_name,*idx_tbl, *idx_geom, *idx_id;
    int create, res;
    /*Name of sqlite-database to write to*/
    if ( PG_NARGS() < 1 || PG_ARGISNULL(0) )
    {
        lwerror("No sqlitedb to write to");
        PG_RETURN_NULL();
    }
    else   
        sqlitedb_name =  text_to_cstring(PG_GETARG_TEXT_P(0));

    
    /*Name of dataset in sqlite*/
    if ( PG_NARGS() < 2 || PG_ARGISNULL(1) )
    {
        lwerror("No name of dataset");
        PG_RETURN_NULL();
    }
    else 
        dataset_name =  text_to_cstring(PG_GETARG_TEXT_P(1));

    /*SQL-query to fetch data*/
    if( PG_NARGS() < 3 || PG_ARGISNULL(2))
    {
        lwnotice("No sql query to use");
        PG_RETURN_NULL();
    }
    else
        sql_string = text_to_cstring(PG_GETARG_TEXT_P(2));


    /*Name of id-column in sql-query above*/
    if( PG_NARGS() < 4 || PG_ARGISNULL(3))
    {
        id_name = NULL;
    }
    else 
        id_name = text_to_cstring(PG_GETARG_TEXT_P(3));
    
    /*Name of twkb-column in sql-query above*/
    if( PG_NARGS() < 5|| PG_ARGISNULL(4))
    {
        twkb_name = NULL;
    }
    else
        twkb_name = text_to_cstring(PG_GETARG_TEXT_P(4));

    /*Name of table to create spatial index from with corresponding id*/
    if( PG_NARGS() < 6|| PG_ARGISNULL(5))
    {
        idx_tbl = NULL;
    }
    else
        idx_tbl = text_to_cstring(PG_GETARG_TEXT_P(5));

    /*Geometry column to create spatial index from*/
    if( PG_NARGS() < 7|| PG_ARGISNULL(6))
    {
        idx_geom = NULL;
    }
    else    
        idx_geom = text_to_cstring(PG_GETARG_TEXT_P(6));
    /*id in spatial index that points to right twkb*/
    if( PG_NARGS() < 8|| PG_ARGISNULL(7))
    {
        idx_id = NULL;
    }
    else 
        idx_id = text_to_cstring(PG_GETARG_TEXT_P(7));
    /*if the table shall be created*/
    if( PG_NARGS() < 9|| PG_ARGISNULL(8))
    {
        create = 1;
    }
    else
        create = PG_GETARG_INT32(8);



//	PG_FREE_IF_COPY(bytea_twkb, 0);
    res = write2sqlite(sqlitedb_name,dataset_name, sql_string,id_name, twkb_name,idx_geom,idx_tbl, idx_id, create);

elog(INFO, "back from writing");
elog(INFO, "have res = %d\n",res); 
    if(sqlitedb_name)
        pfree(sqlitedb_name);
    
    
    if(sql_string)
        pfree(sql_string);
    if(dataset_name)
        pfree(dataset_name);
    if(twkb_name)
        pfree(twkb_name);
    if(idx_tbl)
        pfree(idx_tbl);
    if(idx_geom)
        pfree(idx_geom);
    if(idx_id)
        pfree(idx_id);
    if(id_name)
        pfree(id_name);
    elog(INFO, "return res = %d\n",res);
    PG_RETURN_INT32(res);
}
