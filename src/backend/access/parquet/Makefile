#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/appendonly
#
#-------------------------------------------------------------------------

subdir = src/backend/access/parquet
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = common.o  exec_state.o  \
	   parquet_writer.o  modify_state.o \
	   parquet_impl.o  parquet_reader.o  slvars.o \
	   parquet_am.o

SUBDIRS = parquet_s3

include $(top_srcdir)/src/backend/common.mk

