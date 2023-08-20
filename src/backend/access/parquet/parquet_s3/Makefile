#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/appendonly
#
#-------------------------------------------------------------------------

subdir = src/backend/access/parquet/parquet_s3
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = parquet_s3_connection.o parquet_s3.o parquet_s3_local.o

include $(top_srcdir)/src/backend/common.mk

