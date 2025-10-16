#ifndef HAMM_H_
#define HAMM_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <mm.h>
#include <str.h>

#define MIN(a,b) (((a) < (b)) ? (a) : (b))
#define MAX(a,b) (((a) > (b)) ? (a) : (b))

typedef unsigned char byte_t;

static inline byte_t get_bit_buff(const void* buf, long bit) {
    const byte_t* b = (const byte_t*)buf;
    return (b[bit / 8] >> (bit % 8)) & 1;
}

static inline void set_bit_buff(void* buf, long bit, byte_t val) {
    byte_t* b = (byte_t*)buf;
    if (val) b[bit / 8] |= (1 << (bit % 8));
    else b[bit / 8] &= ~(1 << (bit % 8));
}

static inline void toggle_bit_buff(void* buf, long bit) {
    byte_t* b = (byte_t*)buf;
    b[bit / 8] ^= (1 << (bit % 8));
}

/*
Calculate size of encoded buffer with input decoded size and parity bits count.

Params:
- dsize - Decoded size.
- m - Parity bits count.

Return encoded buffer size.
*/
static inline long calculate_encoded_size(long dsize, int m) {
    long n = (1 << m) - 1;
    long k = n - m;
    long blocks = (dsize * 8 + k - 1) / k;
    return (blocks * n + 7) / 8;
}

/*
Calculate size of decoded buffer with input encoded size and parity bits count.

Params:
- esize - Encoded size.
- m - Parity bits count.

Return decoded buffer size.
*/
static inline long calculate_decoded_size(long esize, int m) {
    long n = (1 << m) - 1;
    long k = n - m;
    long blocks = (esize * 8 + n - 1) / n;
    return (blocks * k + 7) / 8;
}

/*
Encode input decoded data with m-pariry bits.

Params:
- src - Input decoded data.
- out - Output location.
- m - Parity bits count.

Return encoded data.
*/
int encode_hamming(void* src, void* out, long m);

/*
Decode input encoded (source) data with m-pariry bits.

Params:
- src - Input encoded (source) data.
- out - Output location.
- m - Parity bits count.

Return decoded data.
*/
int decode_hamming(void* src, void* out, long m);

/*
Encode entire array (decoded with the same count of parity bits).
Note: out should have size lower than in.

Params:
- in - Input decoded data.
- in_size - Input decoded data size.
- out - Output location. (Size: calculate_encoded_size(in_size, m))
- m - Parity bits count.

Return actual output size.
*/
long encode_hamming_array(const byte_t* in, long in_size, byte_t* out, int m);

/*
Decode entire array with parity bits.
Note: out should have size larger than in.

Params:
- in - Input source data.
- in_size - Input source data size.
- out - Output location. (Size: calculate_decoded_size(in_size, m))
- m - Parity bits count.

Return actual output size.
*/
long decode_hamming_array(const byte_t* in, long in_size, byte_t* out, int m);

#ifdef __cplusplus
}
#endif
#endif