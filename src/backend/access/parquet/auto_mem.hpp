#pragma once

#include "backend/sdb/common/pg_export.hpp"
#include <memory>

class AutoMemContext {
public:
  AutoMemContext(MemoryContext to) {
    old_ = MemoryContextSwitchTo(to); 
  }
  ~AutoMemContext() {
    MemoryContextSwitchTo(old_); 
  }
private: 
  MemoryContext old_;
};

std::unique_ptr<AutoMemContext> AutoSwitch(MemoryContext to) {
  return std::make_unique<AutoMemContext>(to);
}
