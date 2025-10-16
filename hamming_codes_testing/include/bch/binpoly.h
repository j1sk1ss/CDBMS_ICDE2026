#ifndef BIN_POLY_H_
#define BIN_POLY_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <mm.h>
#include <vec.h>

typedef struct {
    vec_t coeffs;
} bin_polynom_t;

int init_binpoly(bin_polynom_t* p);
int free_binpoly(bin_polynom_t* p);
int add_binpoly(bin_polynom_t* res, bin_polynom_t* a, bin_polynom_t* b);
int sub_binpoly(bin_polynom_t* res, bin_polynom_t* a, bin_polynom_t* b);
int mul_binpoly(bin_polynom_t* res, bin_polynom_t* a, bin_polynom_t* b);
int shl_binpoly(bin_polynom_t* res, bin_polynom_t* a, bin_polynom_t* b);
int mod_binpoly(bin_polynom_t* res, bin_polynom_t* a, bin_polynom_t* b);
int eq_binpoly(bin_polynom_t* a, bin_polynom_t* b);
int lw_binpoly(bin_polynom_t* a, bin_polynom_t* b);

#ifdef __cplusplus
}
#endif
#endif