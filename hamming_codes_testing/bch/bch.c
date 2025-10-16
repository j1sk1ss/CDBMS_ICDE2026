#include <bch.h>

#define POLY 0x13

static int _alpha_to[1 << BCH_M];
static int _index_of[1 << BCH_M];
static int _g[BCH_N - BCH_K + 1];
static int _initialized = 0;

static int _generate_gf() {
    int mask = 1;
    _alpha_to[BCH_M] = 0;
    for (int i = 0; i < BCH_M; i++) {
        _alpha_to[i] = mask;
        _index_of[_alpha_to[i]] = i;
        if ((POLY >> (BCH_M - 1 - i)) & 1)
            _alpha_to[BCH_M] ^= mask;
        mask <<= 1;
    }

    _index_of[_alpha_to[BCH_M]] = BCH_M;
    mask >>= 1;
    for (int i = BCH_M + 1; i < (1 << BCH_M) - 1; i++) {
        if (_alpha_to[i - 1] >= mask) 
            _alpha_to[i] = _alpha_to[BCH_M] ^ ((_alpha_to[i - 1] ^ mask) << 1);
        else 
            _alpha_to[i] = _alpha_to[i - 1] << 1;
        _index_of[_alpha_to[i]] = i;
    }

    _index_of[0] = -1;
    return 1;
}

static int _gen_poly() {
    int b[2 * BCH_T];
    _g[0] = 1;
    
    for (int i = 0; i < 2 * BCH_T; i++) {
        b[i] = _alpha_to[i + 1];
        _g[i + 1] = 1;
        
        for (int j = i; j > 0; j--) {
            if (_g[j]) 
                _g[j] = _g[j - 1] ^ _alpha_to[(_index_of[_g[j]] + _index_of[b[i]]) % ((1 << BCH_M) - 1)];
            else 
                _g[j] = _g[j - 1];
        }
        _g[0] = _alpha_to[(_index_of[b[i]] + _index_of[_g[0]]) % ((1 << BCH_M) - 1)];
    }

    return 1;
}

int bch_init() {
    if (!_initialized) {
        _generate_gf();
        _gen_poly();
        _initialized = 1;
        return 1;
    }
    return 0;
}

static int _encode_bits(const unsigned char* data_bits, unsigned char* out_bits) {
    int reg[BCH_N - BCH_K] = {0};
    
    for (int i = 0; i < BCH_K; i++) {
        int feedback = data_bits[i] ^ reg[BCH_N - BCH_K - 1];
        for (int j = BCH_N - BCH_K - 1; j > 0; j--) {
            reg[j] = reg[j - 1] ^ (feedback ? _g[j] : 0);
        }
        reg[0] = feedback ? _g[0] : 0;
    }

    for (int i = 0; i < BCH_N - BCH_K; i++) 
        out_bits[i] = reg[i];
    for (int i = 0; i < BCH_K; i++) 
        out_bits[i + BCH_N - BCH_K] = data_bits[i];
    
    return 1;
}

static inline int _get_bit(const unsigned char* data, unsigned long bit_index) {
    return (data[bit_index / 8] >> (7 - (bit_index % 8))) & 1;
}

static inline void _set_bit(unsigned char* data, unsigned long bit_index, int value) {
    unsigned long byte_index = bit_index / 8;
    int bit_in_byte = 7 - (bit_index % 8);
    if (value) 
        data[byte_index] |= (1 << bit_in_byte);
    else 
        data[byte_index] &= ~(1 << bit_in_byte);
}

unsigned long encode_bch(const unsigned char* input, unsigned long input_len, unsigned char* output) {
    str_memset(output, 0, (input_len * 8 / BCH_K * BCH_N + 7) / 8);
    unsigned long in_bits = input_len * 8;
    unsigned long out_bit = 0;
    
    for (unsigned long pos = 0; pos < in_bits; pos += BCH_K) {
        unsigned char data_bits[BCH_K] = { 0 };
        unsigned char codeword[BCH_N] = { 0 };
        
        for (int i = 0; i < BCH_K && pos + i < in_bits; i++) 
            data_bits[i] = _get_bit(input, pos + i);
        
        _encode_bits(data_bits, codeword);
        
        for (int i = 0; i < BCH_N; i++) 
            _set_bit(output, out_bit + i, codeword[i]);
        
        out_bit += BCH_N;
    }

    return (out_bit + 7) / 8;
}

static int _decode_bits(unsigned char* codeword_bits) {
    int synd[2 * BCH_T];
    int error_loc[BCH_T];
    int num_errors = 0;

    // Вычисление синдромов
    for (int i = 0; i < 2 * BCH_T; i++) {
        synd[i] = 0;
        for (int j = 0; j < BCH_N; j++) {
            if (codeword_bits[j]) {
                synd[i] ^= _alpha_to[(i + 1) * j % ((1 << BCH_M) - 1)];
            }
        }
    }

    // Проверка на отсутствие ошибок
    int has_errors = 0;
    for (int i = 0; i < 2 * BCH_T; i++) {
        if (synd[i] != 0) {
            has_errors = 1;
            break;
        }
    }
    if (!has_errors) return 0;

    // Упрощенный алгоритм поиска ошибок (только для 1-2 ошибок)
    for (int i = 0; i < BCH_N && num_errors < BCH_T; i++) {
        int sum = 0;
        for (int j = 0; j < 2 * BCH_T; j++) {
            sum ^= _alpha_to[(synd[j] + j * i) % ((1 << BCH_M) - 1)];
        }
        if (sum == 0) {
            error_loc[num_errors++] = i;
        }
    }

    // Исправление ошибок
    for (int i = 0; i < num_errors; i++) {
        codeword_bits[error_loc[i]] ^= 1;
    }

    return num_errors;
}

unsigned long decode_bch(const unsigned char* input, unsigned long input_len, unsigned char* output) {
    str_memset(output, 0, (input_len * 8 / BCH_N * BCH_K + 7) / 8);
    unsigned long in_bits = input_len * 8;
    unsigned long out_bit = 0;

    for (unsigned long pos = 0; pos + BCH_N <= in_bits; pos += BCH_N) {
        unsigned char codeword[BCH_N] = { 0 };
        
        for (int i = 0; i < BCH_N; i++) 
            codeword[i] = _get_bit(input, pos + i);
        
        _decode_bits(codeword);
        
        for (int i = 0; i < BCH_K; i++) 
            _set_bit(output, out_bit + i, codeword[BCH_N - BCH_K + i]);
        
        out_bit += BCH_K;
    }

    return (out_bit + 7) / 8;
}