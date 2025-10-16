#ifndef VEC_H_
#define VEC_H_

#include <mm.h>
#include <str.h>

#define VEC_INIT_CAPACITY 4

typedef struct {
    long   size;
    long   capacity; 
    void** h;
} vec_t;

int vec_init(vec_t* v);
int vec_push(vec_t* v, void* d);
int vec_pop(vec_t* v);
int vec_get(vec_t* v, int idx, void** d);
int vec_free(vec_t* v);

#endif