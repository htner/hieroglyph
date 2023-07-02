
#ifndef WRITE_RESULT_OBJECT_H
#define WRITE_RESULT_OBJECT_H

extern void CreateObjectStream(const char* dirname, const char *filename);

extern int WriteResultToObject(char msgtype, const char *buf, int size);

extern void WriteResultEnd();

extern void WriteResultFlush();

#endif // WRITE_RESULT_OBJECT_H

