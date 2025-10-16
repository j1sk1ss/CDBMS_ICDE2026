#include <poly.h>

int poly_init(poly_t* p) {
    return vec_init(&p->coeffs);
}

int poly_free(poly_t* p) {
    for (int i = 0; i < p->coeffs.size; i++) {
        long* c = NULL;
        vec_get(&p->coeffs, i, (void**)&c);
        ll_free(c);
    }

    vec_free(&p->coeffs);
    return 1;
}

int poly_push(poly_t* p, long value) {
    long* c = ll_malloc(sizeof(long));
    if (!c) return -1;
    *c = value;
    return vec_push(&p->coeffs, c);
}

long poly_get(const poly_t* p, int idx) {
    long* c = NULL;
    if (vec_get((vec_t*)&p->coeffs, idx, (void**)&c) < 0) return 0;
    return *c;
}

int poly_set(poly_t* p, int idx, long value) {
    long* c = NULL;
    if (vec_get(&p->coeffs, idx, (void**)&c) < 0) {
        while (p->coeffs.size <= idx) poly_push(p, 0);
        vec_get(&p->coeffs, idx, (void**)&c);
    }

    *c = value;
    return 0;
}

int poly_add(poly_t* res, const poly_t* a, const poly_t* b) {
    int max_size = a->coeffs.size > b->coeffs.size ? a->coeffs.size : b->coeffs.size;
    for (int i = 0; i < max_size; i++) {
        long sum = poly_get(a, i) + poly_get(b, i);
        poly_set(res, i, sum);
    }
    return 0;
}

int poly_mul(poly_t* res, const poly_t* a, const poly_t* b) {
    int deg = a->coeffs.size + b->coeffs.size - 1;
    for (int i = 0; i < deg; i++) poly_set(res, i, 0);

    for (int i = 0; i < a->coeffs.size; i++) {
        for (int j = 0; j < b->coeffs.size; j++) {
            long val = poly_get(res, i + j) + poly_get(a, i) * poly_get(b, j);
            poly_set(res, i + j, val);
        }
    }
    return 0;
}

int poly_divmod(poly_t* quotient, poly_t* remainder, const poly_t* dividend, const poly_t* divisor) {
    for (int i = 0; i < dividend->coeffs.size; i++) {
        poly_set(remainder, i, poly_get(dividend, i));
    }

    int deg_divisor = divisor->coeffs.size - 1;

    while (remainder->coeffs.size >= divisor->coeffs.size) {
        int shift = remainder->coeffs.size - divisor->coeffs.size;
        long factor = poly_get(remainder, remainder->coeffs.size - 1);
        poly_set(quotient, shift, factor);

        for (int i = 0; i < divisor->coeffs.size; i++) {
            long val = poly_get(remainder, i + shift) - factor * poly_get(divisor, i);
            poly_set(remainder, i + shift, val);
        }

        while (remainder->coeffs.size > 0 && poly_get(remainder, remainder->coeffs.size - 1) == 0) {
            vec_pop(&remainder->coeffs);
        }
    }

    return 0;
}
