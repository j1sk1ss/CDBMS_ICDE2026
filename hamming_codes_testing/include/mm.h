#ifndef LLALLOC_H_
#define LLALLOC_H_
#ifdef __cplusplus
extern "C" {
#endif

#ifndef NULL
    #define NULL ((void*)0)
#endif
#define ALLOC_BUFFER_SIZE 131072
#define ALIGNMENT         8  
#define MM_BLOCK_MAGIC    0xC07DEL
#define NO_OFFSET         0

typedef struct mm_block {
    unsigned int     magic;
    unsigned int     size;
    unsigned char    free;
    struct mm_block* next;
} mm_block_t;

int ll_init();
void* ll_malloc(unsigned int size);
void* ll_mallocoff(unsigned int size, unsigned int offset);
int ll_free(void* ptr);

#ifdef __cplusplus
}
#endif
#endif