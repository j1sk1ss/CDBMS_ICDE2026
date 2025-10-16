#include <str.h>

void* str_memset(void* pointer, unsigned char value, long num) {
    unsigned int num_dwords = num / 4;
    unsigned int num_bytes  = num % 4;
    unsigned int* dest32 = (unsigned int*)pointer;
    unsigned char* dest8 = ((unsigned char*)pointer) + num_dwords * 4;
    unsigned char val8   = (unsigned char)value;
    unsigned int val32   = value | (value << 8) | (value << 16) | (value << 24);
    unsigned int i = 0;

    for (i = 0; i < num_dwords; i++) dest32[i] = val32;
    for (i = 0; i < num_bytes; i++) dest8[i] = val8;
    return pointer;
}

void* str_memcpy(void* __restrict destination, const void* __restrict source, unsigned int num) {
    unsigned int num_dwords = num / 4;
    unsigned int num_bytes  = num % 4;
    unsigned int* dest32 = (unsigned int*)destination;
    unsigned int* src32  = (unsigned int*)source;
    unsigned char* dest8 = ((unsigned char*)destination) + num_dwords * 4;
    unsigned char* src8  = ((unsigned char*)source) + num_dwords * 4;
    for (unsigned int i = 0; i < num_dwords; i++) dest32[i] = src32[i];
    for (unsigned int i = 0; i < num_bytes; i++) dest8[i] = src8[i];
    return destination;
}
