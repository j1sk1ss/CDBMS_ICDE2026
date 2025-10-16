#include <vec.h>

int vec_init(vec_t* v) {
    if (!v) return -1;
    v->size = 0;
    v->capacity = VEC_INIT_CAPACITY;
    v->h = ll_malloc(v->capacity * sizeof(void*));
    if (!v->h) return -1;
    return 0;
}

int vec_push(vec_t* v, void* d) {
    if (!v) return -1;

    if (v->size >= v->capacity) {
        long new_capacity = v->capacity * 2;
        void** tmp = ll_realloc(v->h, new_capacity * sizeof(void*));
        if (!tmp) return -1;
        v->h = tmp;
        v->capacity = new_capacity;
    }

    v->h[v->size++] = d;
    return 0;
}

int vec_pop(vec_t* v) {
    if (!v || v->size == 0) return -1;
    v->size--;
    return 0;
}

int vec_get(vec_t* v, int idx, void** d) {
    if (!v || idx < 0 || idx >= v->size || !d) return -1;
    *d = v->h[idx];
    return 0;
}

int vec_free(vec_t* v) {
    if (!v) return -1;
    if (v->h) ll_free(v->h);
    v->h = NULL;
    v->size = 0;
    v->capacity = 0;
    return 0;
}
