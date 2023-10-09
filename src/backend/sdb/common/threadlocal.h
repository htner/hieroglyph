#ifndef GS_THREADLOCAL_H_
#define GS_THREADLOCAL_H_

#ifdef PC_LINT
#define THR_LOCAL
#endif

#ifndef THR_LOCAL
#ifndef WIN32
#define THR_LOCAL __thread
#else
#define THR_LOCAL __declspec(thread)
#endif
#endif

#endif

