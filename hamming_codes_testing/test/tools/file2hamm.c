#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <hamm.h>

#define PARITY_BITS_ARG "--pb"
#define TARGET_ARG      "--target"
#define OUTPUT_ARG      "--out"

static int _m = 4;
static const char* _target   = "image.img";
static const char* _out_path = "image.hamm";

/*
--pb - parity bits count (pb=0 => without encoding, just copy)
--target - Target file for encoding
--out - Path to save location (will create new file)
*/
int main(int argc, char* argv[]) {
    if (argc > 1) {
        for (int i = 1; i < argc; i++) {
            if (!strcmp(argv[i], PARITY_BITS_ARG)) _m = atoi(argv[i++ + 1]);
            else if (!strcmp(argv[i], TARGET_ARG)) _target = argv[i++ + 1];
            else if (!strcmp(argv[i], OUTPUT_ARG)) _out_path = argv[i++ + 1];
            else fprintf(stderr, "Unknown arg %s!\n", argv[i]);
        }
    }

    fprintf(stdout, "[file2hamm] _target=%s, _out_path=%s, _m=%i\n", _target, _out_path, _m);
    FILE* src_f = fopen(_target, "rb");
    if (!src_f) return EXIT_FAILURE;

    fseek(src_f, 0, SEEK_END);
    long in_size = ftell(src_f);
    fseek(src_f, 0, SEEK_SET);

    char* buffer = (char*)malloc(in_size);
    if (!buffer) {
        fclose(src_f);
        return EXIT_FAILURE;
    }

    fread(buffer, 1, in_size, src_f);
    fclose(src_f);
    
    FILE* fo = fopen(_out_path, "wb");
    if (!fo) return EXIT_FAILURE;

    if (!_m) fwrite(buffer, 1, in_size, fo);
    else {
        ll_init();
        long enc_size = calculate_encoded_size((int)in_size, _m);
        char* encoded = (char*)malloc(enc_size);
        if (!encoded) {
            free(buffer);
            return EXIT_FAILURE;
        }

        encode_hamming_array((const byte_t*)buffer, in_size, (byte_t*)encoded, _m);
        fwrite(encoded, 1, enc_size, fo);
        free(encoded);
    }

    fclose(fo);
    free(buffer);
    return EXIT_SUCCESS;
}
