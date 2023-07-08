#!/usr/bin/env bash

source ./scripts/lib.sh

GIT_SHA=$(git rev-parse --short HEAD || echo "GitNotFound")
if [[ -n "$FAILPOINTS" ]]; then
  GIT_SHA="$GIT_SHA"-FAILPOINTS
fi

GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
BUILD_TIME=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

GO_LDFLAGS=(${GO_LDFLAGS} " -X github.com/htner/sdb/gosrv/pkg/build.GitVersion=${GIT_SHA} \
                                -X github.com/htner/sdb/gosrv/pkg/build.GitBranch=${GIT_BRANCH} \
                                -X github.com/htner/sdb/gosrv/pkg/build.BuildTime=${BUILD_TIME}")
echo ${GO_LDFLAGS}

commands=($@)
out="bin"

build_cmd() {
  run rm -f "${out}/${command}"
  (
    cd ${command}
    go build  \
    "-ldflags=${GO_LDFLAGS}" \
    -o="../${out}/${command}" . || return 2
  ) || return 2
}

tools_build() {
  if [[ -n "${BINDIR}" ]]; then out="${BINDIR}"; fi

  for command in "${commands[@]}"  
  do 
    build_cmd $out $command
    res=`echo $?`
    if [ "$res" != "0" ]; then
      return $res
    fi 

    # Verify whether symbol we overwrote exists
    # For cross-compiling we cannot run: ${out}/gpstart --version | grep -q "build.GitVersion"

    # We need symbols to do this check:
    if [[ "${GO_LDFLAGS[*]}" != *"-s"* ]]; then
	  go tool nm "${out}/${command}" | grep "build.GitVersion" > /dev/null
	  if [[ "${PIPESTATUS[*]}" != "0 0" ]]; then
		  #log_error "FAIL: Symbol build.GitVersion not found in binary: ${out}/${command}"
      log_success "build ${command} success"
		  #return 2
	  fi
  	fi
  done

  }

check_build_requirements() {
  GO_PATH=$(which go)
  if [[ -z "${GO_PATH}" ]]; then 
      log_error "FAIL: go is not installed"
      exit 3
  fi

  GO_VERSION=$(go version | awk '{print $3}')
  GO_VERSION=${GO_VERSION:2}
  MIN_GO_VERSION="1.16"
  # io.ReadAll require 1.16+
  if [[ $GO_VERSION < $MIN_GO_VERSION ]]; then
     log_error "FAIL: go must be 1.16+"
      exit 3
  fi
  return 0
}
# only build when called directly, not sourced
if echo "$0" | grep -E "build(.sh)?$" >/dev/null; then
  check_build_requirements;

  if tools_build; then
    log_success "SUCCESS: tools_build "
  else
    log_error "FAIL: tools_build"
    exit 2
  fi
fi
