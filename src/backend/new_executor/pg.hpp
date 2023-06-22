#pragma once

extern "C" {
#include "c.h"
#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "commands/variable.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/numeric.h"
#include "utils/fmgrprotos.h"
}

#undef IsPowerOf2
#undef Abs 
