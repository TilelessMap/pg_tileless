

LDFLAGS =  -L /usr/local/lib -l lwgeom -lsqlite3

MODULE_big = pg_tileless
OBJS = 	pg_tileless.o \
	sqlite_writer.o \

EXTENSION = pg_tileless
DATA = pg_tileless--0.1.sql \
pg_tileless--0.1--0.2.sql \
pg_tileless--0.2.sql 

EXTRA-CLEAN =

#PG_CONFIG = pg_config
PG_CONFIG =/usr/lib/postgresql/11/bin/pg_config
#PG_CONFIG =/usr/lib/postgresql/10/bin/pg_config

CFLAGS += $(shell $(CURL_CONFIG) --cflags) -g

LIBS += $(LDFLAGS)
SHLIB_LINK := $(LIBS)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
