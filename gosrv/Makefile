# Example:
#   make build
#   make clean

top_builddir=..
ifneq "$(wildcard $(top_builddir)/src/Makefile.global)" ""
include $(top_builddir)/src/Makefile.global
endif

UNAME := $(shell uname)
XARGS = xargs
ARCH ?= $(shell go env GOARCH)

# -r is only necessary on GNU xargs.
ifeq ($(UNAME), Linux)
XARGS += -r
endif
XARGS += rm -r
PROGRAMS = proxy schedule lake account stool lockmgr

bins=$(patsubst %,bin/%,$(PROGRAMS) )

all: clean build

.PHONY: build

proto: protogen

protogen:
	make -C ../proto go

# always build
build: protogen $(bins)

# git repository must owned by install user
ifneq "$(wildcard $(top_builddir)/src/Makefile.global)" ""
install: $(bins)
	for file in $(PROGRAMS); do \
		$(INSTALL_SCRIPT) './bin/'$$file '$(DESTDIR)$(bindir)/sdb_'$$file ; \
	done
else
install:
	@echo "You need to run the 'configure' program first."
endif

bin/%:
	GO_BUILD_FLAGS="-v" ./scripts/build.sh $(subst bin/,,$@)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	for file in $(PROGRAMS); do \
		rm -f '$(DESTDIR)$(bindir)/'$$file ; \
	done

clean distclean maintainer-clean:
	for file in $(PROGRAMS); do \
		rm -f './bin/'$$file ; \
	done
