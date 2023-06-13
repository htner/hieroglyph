#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for wukongdb protobuf
#
#-------------------------------------------------------------------------
subdir = proto

top_builddir = ..
include ${top_builddir}/src/Makefile.global

SUBDIRS = optimizer worker schedule

include ${top_builddir}/src/backend/common.mk

go:
	for DIR in $(SUBDIRS); do \
		make -C $$DIR go  ; \
	done
