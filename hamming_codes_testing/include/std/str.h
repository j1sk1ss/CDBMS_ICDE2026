#ifndef STR_H_
#define STR_H_
#ifdef __cplusplus
extern "C" {
#endif

void* str_memset(void* pointer, unsigned char value, long num);
void* str_memcpy(void* __restrict destination, const void* __restrict source, unsigned int num);

#ifdef __cplusplus
}
#endif
#endif