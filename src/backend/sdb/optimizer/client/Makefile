
NEED_GPERFTOOLS=0
PROTOC = protoc
CXX = g++
GFLAGS_NS = gflags
ROOT_PATH = ../../../../..
#include $(BRPC_PATH)/config.mk
# Notes on the flags:
# 1. Added -fno-omit-frame-pointer: perf/tcmalloc-profiler use frame pointers by default
CXXFLAGS+=$(CPPFLAGS) -std=c++0x -DNDEBUG -O2 -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer -DGFLAGS_NS=${GFLAGS_NS}
ifeq ($(NEED_GPERFTOOLS), 1)
	CXXFLAGS+=-DBRPC_ENABLE_CPU_PROFILER
endif

#HDRS+=$(BRPC_PATH)/output/include

#LIBS+=$(BRPC_PATH)/output/lib
HDRPATHS=$(addprefix -I, $(HDRS)) -I$(ROOT_PATH)/proto
LIBPATHS=$(addprefix -L, $(LIBS))
COMMA=,
SOPATHS=$(addprefix -Wl$(COMMA)-rpath$(COMMA), $(LIBS))

SERVER_SOURCES = server.cpp $(ROOT_PATH)/proto/optimizer/optimizer.pb.cc
CLIENT_SOURCES = client.cpp $(ROOT_PATH)/proto/optimizer/optimizer.pb.cc

CLIENT_OBJS = $(addsuffix .o, $(basename $(CLIENT_SOURCES))) 
SERVER_OBJS = $(addsuffix .o, $(basename $(SERVER_SOURCES))) 

DYNAMIC_LINKINGS = -lpthread -lgflags -lprotobuf -lleveldb -lssl -lcrypto -ldl -lz
STATIC_LINKINGS += -lbrpc
LINK_OPTIONS_SO = -Xlinker "-(" $^ -Xlinker "-)" $(STATIC_LINKINGS) $(DYNAMIC_LINKINGS)
LINK_OPTIONS = -Xlinker "-(" $^ -Wl,-Bstatic $(STATIC_LINKINGS) -Wl,-Bdynamic -Xlinker "-)" $(DYNAMIC_LINKINGS)

.PHONY:all
all: echo_client echo_server

.PHONY:clean
clean:
	@echo "> Cleaning"
	rm -rf echo_client echo_server $(PROTO_GENS) $(PROTO_OBJS) $(CLIENT_OBJS) $(SERVER_OBJS)

echo_client:$(PROTO_OBJS) $(CLIENT_OBJS)
	@echo "> Linking $@"
ifneq ("$(LINK_SO)", "")
	$(CXX) $(LIBPATHS) $(SOPATHS) $(LINK_OPTIONS_SO) -o $@
else
	$(CXX) $(LIBPATHS) $(LINK_OPTIONS) -o $@
endif

echo_server:$(PROTO_OBJS) $(SERVER_OBJS)
	@echo "> Linking $@"
ifneq ("$(LINK_SO)", "")
	$(CXX) $(LIBPATHS) $(SOPATHS) $(LINK_OPTIONS_SO) -o $@
else
	$(CXX) $(LIBPATHS) $(LINK_OPTIONS) -o $@
endif

%.o:%.cpp
	@echo "> Compiling $@"
	$(CXX) -c $(HDRPATHS) $(CXXFLAGS) $< -o $@

%.o:%.cc
	@echo "> Compiling $@"
	$(CXX) -c $(HDRPATHS) $(CXXFLAGS) $< -o $@

postgres: $(OBJS)
	$(CXX) $(CXXFLAGS) $(call expand_subsys,$^) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(LIBS) -o $@
