#-------------------------------------------------------------------------
# Makefile--
#    Makefile for protobuf
#
#-------------------------------------------------------------------------

include Makefile.conf

#subdir = $(SDB_SUB_DIR)
#top_builddir = $(SDB_TOP_BUILD_PREFIX)

# include $(SDB_GLOBAL_MAKEFILE)

CFLAGS += -I${PROTOC_DIR}include `pkg-config --cflags protobuf`
CXXFLAGS += -I${PROTOC_DIR}include `pkg-config --cflags protobuf`


PROTO_FILE = $(wildcard *.proto)
PROTO_SRC = $(patsubst %.proto, %.pb.cc, $(PROTO_FILE))
PROTO_GO_SRC = $(patsubst %.proto, %.pb.go, $(PROTO_FILE))

OBJS = $(patsubst %.cc, %.o, $(PROTO_SRC))
OBJS += $(patsubst %.cpp, %.o, $(wildcard *.cpp))

# include $(SDB_COMMON_MK)


$(PROTO_SRC): $(PROTO_FILE)
	protoc $(wildcard *.proto) --cpp_out=./

.PHONY: go

go: $(PROTO_GO_SRC)


# for go-zero
# goctl rpc protoc $< --go_out=../../gosrv/$(SDB_DIR)/proto --go-grpc_out=../../gosrv/$(SDB_DIR)/proto --zrpc_out=../../gosrv/$(SDB_DIR)
# for go-micro
# @protoc --micro_out=../gosrv/$(SDB_DIR) --go_out=:../gosrv/$(SDB_DIR) $< 
# using google grpc here

%_service.pb.go: %_service.proto
	@mkdir -p ../gosrv/proto
	protoc $< --go_out=../gosrv/proto/ --go-grpc_out=../gosrv/proto/

%.pb.go: %.proto
	@mkdir -p ../gosrv/proto
	@protoc $< --go_out=../gosrv/proto/

clean:
	rm -f *.go
	rm -f *.cc
	rm -f *.h
