#include <mm.h>
#include <bch.h>
#include <stdio.h>
#include <string.h>

int main() {
    ll_init();
    char a[] = "Hello world!";
    unsigned long input_len = strlen(a);
    unsigned long encoded_len = bch_encoded_size(input_len);
    unsigned long decoded_len = bch_decoded_size(encoded_len);

    unsigned char* encoded = ll_malloc(encoded_len);
    unsigned char* decoded = ll_malloc(decoded_len);
    if (!encoded || !decoded) return 0;

    bch_init();
    encode_bch((unsigned char*)a, input_len, encoded);

    printf("Encoded (%zu bytes):\n", encoded_len);
    for (unsigned long i = 0; i < encoded_len; i++) printf("%02X ", encoded[i]);
    printf("\n");

    // encoded[3] ^= 0x08;
    decode_bch(encoded, encoded_len, decoded);

    printf("Decoded (%zu bytes): %.*s\n", decoded_len, (int)decoded_len, decoded);
    return 0;
}
