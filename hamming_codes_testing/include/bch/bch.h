#ifndef BCH_H
#define BCH_H
#ifdef __cplusplus
extern "C" {
#endif

#include <str.h>

#define BCH_M 4
#define BCH_T 1
#define BCH_N ((1 << BCH_M) - 1)
#define BCH_K 11

static inline unsigned long bch_encoded_size(unsigned long input_len) {
    return ((input_len * 8 + BCH_K - 1) / BCH_K * BCH_N + 7) / 8;
}

static inline unsigned long bch_decoded_size(unsigned long input_len) {
    return ((input_len * 8 + BCH_N - 1) / BCH_N * BCH_K + 7) / 8;
}

int bch_init();
unsigned long encode_bch(const unsigned char* input, unsigned long input_len, unsigned char* output);
unsigned long decode_bch(const unsigned char* input, unsigned long input_len, unsigned char* output);

#ifdef __cplusplus
}
#endif
#endif
