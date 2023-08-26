#-------------------------------------------------------------------------
#
# Makefile for wukongdb
#
# src/backend/wukongdb/Makefile
#
#-------------------------------------------------------------------------

include ./Makefile.conf

subdir = $(SDB_SUB_DIR)
top_builddir = $(SDB_TOP_BUILD_PREFIX)

include $(SDB_GLOBAL_MAKEFILE)

SUBDIRS = optimizer worker common cache arrow

include $(SDB_COMMON_MK)
