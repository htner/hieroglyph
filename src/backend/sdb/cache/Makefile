#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for wukongdb/optimizer/
#
# IDENTIFICATION
#    src/backend/wukongdb/optimizer/Makefile
#
#-------------------------------------------------------------------------
include ../../../../src/backend/sdb/Makefile.conf

subdir = $(SDB_SUB_DIR)
top_builddir = $(SDB_TOP_BUILD_PREFIX)

CXXFLAGS += -std=c++17 

include $(SDB_GLOBAL_MAKEFILE)

OBJS = $(patsubst %.cpp, %.o, $(wildcard *.cpp))

include $(SDB_COMMON_MK)
