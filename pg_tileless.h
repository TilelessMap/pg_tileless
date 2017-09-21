/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 *
 * Copyright (C) 2013 Nicklas Avén
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the LICENCE file.
 *
 **********************************************************************/

#include "postgres.h"



#if POSTGIS_PGSQL_VERSION > 92
#include "access/htup_details.h"
#endif

#define POSTGIS_PGSQL_VERSION 96

/**
 * Write a notice out to the notice handler.
 *
 * Uses standard printf() substitutions.
 * Use for messages you always want output.
 * For debugging, use LWDEBUG() or LWDEBUGF().
 * @ingroup logging
 */
void lwnotice(const char *fmt, ...);

/**
 * Write a notice out to the error handler.
 *
 * Uses standard printf() substitutions.
 * Use for errors you always want output.
 * For debugging, use LWDEBUG() or LWDEBUGF().
 * @ingroup logging
 */
void lwerror(const char *fmt, ...);

int getsqlitetype(char *pgtype, char *sqlitetype);
int write2sqlite(char *sqlitedb_name,char *dataset_name, char *sql_string,char *id_name, char *twkb_name,char *idx_geom,char *idx_tbl, char *idx_id, int create);
