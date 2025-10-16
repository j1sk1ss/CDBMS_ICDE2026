#include <binpoly.h>

int init_poly(bin_polynom_t* p) {
    if (!p) return -1;
    return vec_init(&p->coeffs);
}

void trim_poly(bin_polynom_t* p) {
    while (p->coeffs.size > 0) {
        unsigned char* coeff;
        vec_get(&p->coeffs, p->coeffs.size - 1, (void**)&coeff);
        if (*coeff != 0) break;
        p->coeffs.size--;
    }
}

int add_poly(bin_polynom_t* res, bin_polynom_t* a, bin_polynom_t* b) {
    if (!res || !a || !b) return -1;
    int max_size = a->coeffs.size > b->coeffs.size ? a->coeffs.size : b->coeffs.size;

    for (int i = 0; i < max_size; i++) {
        unsigned char ai = 0, bi = 0;
        if (i < a->coeffs.size) vec_get(&a->coeffs, i, (void**)&ai);
        if (i < b->coeffs.size) vec_get(&b->coeffs, i, (void**)&bi);
        unsigned char* c = ll_malloc(sizeof(unsigned char));
        *c = ai ^ bi;
        vec_push(&res->coeffs, c);
    }

    trim_poly(res);
    return 0;
}

int sub_poly(bin_polynom_t* res, bin_polynom_t* a, bin_polynom_t* b) {
    return add_poly(res, a, b);
}

int mul_poly(bin_polynom_t* res, bin_polynom_t* a, bin_polynom_t* b) {
    int deg_a = a->coeffs.size;
    int deg_b = b->coeffs.size;
    for (int i = 0; i < deg_a + deg_b - 1; i++) {
        unsigned char* zero = ll_malloc(sizeof(unsigned char));
        *zero = 0;
        vec_push(&res->coeffs, zero);
    }

    for (int i = 0; i < deg_a; i++) {
        unsigned char ai = 0; vec_get(&a->coeffs, i, (void**)&ai);
        for (int j = 0; j < deg_b; j++) {
            unsigned char bi = 0; vec_get(&b->coeffs, j, (void**)&bi);
            unsigned char* cij = NULL; vec_get(&res->coeffs, i + j, (void**)&cij);
            *cij ^= ai & bi;
        }
    }

    trim_poly(res);
    return 0;
}

int shl_poly(bin_polynom_t* res, bin_polynom_t* a, int shift) {
    for (int i = 0; i < shift; i++) {
        unsigned char* zero = ll_malloc(sizeof(unsigned char));
        *zero = 0;
        vec_push(&res->coeffs, zero);
    }
    
    for (int i = 0; i < a->coeffs.size; i++) {
        unsigned char* ai; 
        vec_get(&a->coeffs, i, (void**)&ai);
        unsigned char* ci = ll_malloc(sizeof(unsigned char));
        *ci = *ai;
        vec_push(&res->coeffs, ci);
    }

    return 0;
}

int eq_poly(bin_polynom_t* a, bin_polynom_t* b) {
    if (a->coeffs.size != b->coeffs.size) return 0;
    for (int i = 0; i < a->coeffs.size; i++) {
        unsigned char ai = 0, bi = 0;
        vec_get(&a->coeffs, i, (void**)&ai);
        vec_get(&b->coeffs, i, (void**)&bi);
        if (ai != bi) return 0;
    }
    return 1;
}

int lw_poly(bin_polynom_t* a, bin_polynom_t* b) {
    if (a->coeffs.size != b->coeffs.size) return a->coeffs.size < b->coeffs.size;
    for (int i = a->coeffs.size - 1; i >= 0; i--) {
        unsigned char ai = 0, bi = 0;
        vec_get(&a->coeffs, i, (void**)&ai);
        vec_get(&b->coeffs, i, (void**)&bi);
        if (ai != bi) return ai < bi;
    }
    return 0;
}

int mod_poly(bin_polynom_t* remainder, bin_polynom_t* dividend, bin_polynom_t* divisor) {
    if (!remainder || !dividend || !divisor) return -1;

    // Проверка на деление на ноль
    if (divisor->coeffs.size == 0) return -1;

    // Создаем копию делимого
    bin_polynom_t rem;
    init_poly(&rem);
    for (int i = 0; i < dividend->coeffs.size; i++) {
        unsigned char* coeff = NULL;
        vec_get(&dividend->coeffs, i, (void**)&coeff);
        unsigned char* c = ll_malloc(sizeof(unsigned char));
        *c = *coeff;
        vec_push(&rem.coeffs, c);
    }

    int deg_divisor = divisor->coeffs.size - 1;

    while (rem.coeffs.size >= divisor->coeffs.size) {
        int shift = rem.coeffs.size - divisor->coeffs.size;
        unsigned char* leading = NULL;
        vec_get(&rem.coeffs, rem.coeffs.size - 1, (void**)&leading);

        if (*leading) {
            for (int i = 0; i < divisor->coeffs.size; i++) {
                unsigned char* d_coeff = NULL;
                vec_get(&divisor->coeffs, i, (void**)&d_coeff);
                unsigned char* r_coeff = NULL;
                vec_get(&rem.coeffs, i + shift, (void**)&r_coeff);
                *r_coeff ^= *d_coeff;
            }
        }

        rem.coeffs.size--;
    }

    for (int i = 0; i < rem.coeffs.size; i++) {
        unsigned char* r_coeff = NULL;
        vec_get(&rem.coeffs, i, (void**)&r_coeff);
        unsigned char* c = ll_malloc(sizeof(unsigned char));
        *c = *r_coeff;
        vec_push(&remainder->coeffs, c);
    }

    return 0;
}
