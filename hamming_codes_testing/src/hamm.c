#include <hamm.h>

int encode_hamming(void* src, void* out, long m) {
    long n = (1 << m) - 1;
    str_memset(out, 0, (n + 7) / 8);

    long data_bit = 0;
    for (long i = 1; i <= n; i++) {
        if ((i & (i - 1)) != 0) {
            byte_t bit = get_bit_buff(src, data_bit);
            set_bit_buff(out, i - 1, bit);
            data_bit++;
        }
    }

    for (long p = 0; p < m; p++) {
        long parity_pos = (1 << p) - 1;
        byte_t parity = 0;
        
        for (long i = 1; i <= n; i++) {
            if (i != (parity_pos + 1) && (i & (1 << p))) {
                parity ^= get_bit_buff(out, i - 1);
            }
        }
        
        set_bit_buff(out, parity_pos, parity);
    }

    return 1;
}

int decode_hamming(void* src, void* out, long m) {
    long n = (1 << m) - 1;
    byte_t* encoded = (byte_t*)ll_malloc((n + 7) / 8);
    if (!encoded) return 0;
    
    str_memset(encoded, 0, (n + 7) / 8);
    str_memcpy(encoded, src, (n + 7) / 8);

    long syndrome = 0;
    for (long p = 0; p < m; p++) {
        byte_t parity = 0;
        for (long i = 1; i <= n; i++) {
            if (i & (1 << p)) {
                parity ^= get_bit_buff(encoded, i - 1);
            }
        }

        if (parity) {
            syndrome |= (1 << p);
        }
    }

    if (syndrome != 0 && syndrome <= n) {
        toggle_bit_buff(encoded, syndrome - 1);
    }

    long data_bit = 0;
    for (long i = 1; i <= n; i++) {
        if ((i & (i - 1)) != 0) {
            byte_t bit = get_bit_buff(encoded, i - 1);
            set_bit_buff(out, data_bit, bit);
            data_bit++;
        }
    }

    ll_free(encoded);
    return 1;
}

long encode_hamming_array(const byte_t* in, long in_size, byte_t* out, int m) {
    long n = (1 << m) - 1;
    long k = n - m;

    long in_bits = in_size * 8;
    long blocks = (in_bits + k - 1) / k;
    long out_size = ((blocks * n) + 7) / 8;

    str_memset(out, 0, out_size);
    
    for (long b = 0; b < blocks; b++) {
        long in_offset_bits = b * k;
        long out_offset_bits = b * n;
        
        byte_t* block_in = (byte_t*)ll_malloc((k + 7) / 8);
        byte_t* block_out = (byte_t*)ll_malloc((n + 7) / 8);
        
        if (!block_in || !block_out) {
            ll_free(block_in);
            ll_free(block_out);
            return -1;
        }
        
        str_memset(block_in, 0, (k + 7) / 8);
        str_memset(block_out, 0, (n + 7) / 8);
        
        for (long i = 0; i < k && (in_offset_bits + i) < in_bits; i++) {
            byte_t bit = get_bit_buff(in, in_offset_bits + i);
            set_bit_buff(block_in, i, bit);
        }
        
        encode_hamming(block_in, block_out, m);
        
        for (long i = 0; i < n; i++) {
            byte_t bit = get_bit_buff(block_out, i);
            set_bit_buff(out, out_offset_bits + i, bit);
        }
        
        ll_free(block_in);
        ll_free(block_out);
    }

    return out_size;
}

long decode_hamming_array(const byte_t* in, long in_size, byte_t* out, int m) {
    long n = (1 << m) - 1;
    long k = n - m;

    long in_bits = in_size * 8;
    long blocks = (in_bits + n - 1) / n;
    long out_size = ((blocks * k) + 7) / 8;

    str_memset(out, 0, out_size);
    
    for (long b = 0; b < blocks; b++) {
        long in_offset_bits = b * n;
        long out_offset_bits = b * k;
        
        byte_t* block_in = (byte_t*)ll_malloc((n + 7) / 8);
        byte_t* block_out = (byte_t*)ll_malloc((k + 7) / 8);
        
        if (!block_in || !block_out) {
            ll_free(block_in);
            ll_free(block_out);
            return -1;
        }
        
        str_memset(block_in, 0, (n + 7) / 8);
        str_memset(block_out, 0, (k + 7) / 8);
        
        for (long i = 0; i < n && (in_offset_bits + i) < in_bits; i++) {
            byte_t bit = get_bit_buff(in, in_offset_bits + i);
            set_bit_buff(block_in, i, bit);
        }
        
        decode_hamming(block_in, block_out, m);
        
        for (long i = 0; i < k; i++) {
            byte_t bit = get_bit_buff(block_out, i);
            set_bit_buff(out, out_offset_bits + i, bit);
        }
        
        ll_free(block_in);
        ll_free(block_out);
    }

    return out_size;
}
