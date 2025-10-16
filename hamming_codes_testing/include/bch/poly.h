#ifndef POLY_H_
#define POLY_H_

#include <mm.h>
#include <vec.h>

typedef struct {
    vec_t coeffs;
} poly_t;

int init_poly(poly_t* p);
int free_poly(poly_t* p);
int push_poly(poly_t* p, long value);
long get_poly(const poly_t* p, int idx);
int set_poly(poly_t* p, int idx, long value);
int add_poly(poly_t* res, const poly_t* a, const poly_t* b);
int mul_poly(poly_t* res, const poly_t* a, const poly_t* b);
int mod_poly(poly_t* quotient, poly_t* remainder, const poly_t* dividend, const poly_t* divisor);

#endif