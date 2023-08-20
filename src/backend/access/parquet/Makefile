#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/appendonly
#
#-------------------------------------------------------------------------

subdir = src/backend/access/parquet
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS) -I$(top_builddir)/proto -I$(top_builddir)/src

OBJS = common.o  reader_state.o  \
	   parquet_writer.o  writer_state.o \
	   parquet_impl.o  parquet_reader.o \
	   parquet_am.o

SUBDIRS = parquet_s3

include $(top_srcdir)/src/backend/common.mk

