#pragma once
extern "C" {
//#include "include/nodes/memnodes.h"
//#include "include/utils/palloc.h"
}
#include "backend/sdb/common/pg_export.hpp"

#define NewObject(pmc) new (pmc, __FILE__, __LINE__)

// BaseObject is a basic class
// All other class should inherit from BaseObject class which
// override operator new/delete.
//
class BaseObject {
public:
    ~BaseObject()
    {}

    void* operator new(size_t size, MemoryContextData* pmc, const char* file, int line)
    {
        //return MemoryContextAllocDebug(pmc, size, file, line);
        return MemoryContextAlloc(pmc, size);
    }

    void* operator new[](size_t size, MemoryContextData* pmc, const char* file, int line)
    {
        //return MemoryContextAllocDebug(pmc, size, file, line);
        return MemoryContextAlloc(pmc, size);
    }

    void operator delete(void* p)
    {
        pfree(p);
    }

    void operator delete[](void* p)
    {
        pfree(p);
    }
};

/*
 *It is used for delete object whose destructor is null and free memory in Destroy()
 *_objptr can't include type change, for example (A*)b, that will lead to compile error in (_objptr) = NULL
 *If _objptr include type change, please use DELETE_EX_TYPE to complete it.
 *It is supplied easily for developers to refactor destructor in the future
 */
#define DELETE_EX(_objptr)    \
    do {                      \
        (_objptr)->Destroy(); \
        delete (_objptr);     \
        (_objptr) = NULL;     \
    } while (0)

// used for _objptr need to change to another type
#define DELETE_EX_TYPE(_objptr, _type)  \
    do {                                \
        ((_type*)(_objptr))->Destroy(); \
        delete (_type*)(_objptr);       \
        (_objptr) = NULL;               \
    } while (0)

#define DELETE_EX2(_objptr)   \
    do {                      \
        if ((_objptr) != nullptr) {    \
            delete (_objptr);          \
            (_objptr) = NULL;          \
        }                              \
    } while (0)

