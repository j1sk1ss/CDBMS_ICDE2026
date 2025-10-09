#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define FILE_SIZE_ARG "--fs"
#define OUTPUT_ARG    "--out"

static int _def_size = 512;
static const char* _out_path = "image.img";

int main(int argc, char* argv[]) {
    if (argc > 1) {
        for (int i = 1; i < argc; i++) {
            if (!strcmp(argv[i], FILE_SIZE_ARG))   _def_size = atoi(argv[i++ + 1]);
            else if (!strcmp(argv[i], OUTPUT_ARG)) _out_path = argv[i++ + 1];
            else fprintf(stderr, "Unknown arg %s!\n", argv[i]);
        }
    }

    char body[_def_size];
    memset(body, 0, _def_size);

    FILE* f = fopen(_out_path, "wb");
    if (!f) return EXIT_FAILURE;

    if (fwrite(body, 1, _def_size, f) != _def_size) {
        printf("Write error!\n");
    }

    fclose(f);
    return EXIT_SUCCESS;
}
