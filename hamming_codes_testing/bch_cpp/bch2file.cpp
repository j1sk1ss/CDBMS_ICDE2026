#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdlib>
#include <BCH.h>
#include <Utilities.h>

using namespace std;

#define PARITY_BITS_ARG "--pb"
#define TARGET_ARG      "--target"
#define OUTPUT_ARG      "--out"

static int _m = 7;
static const char* _target   = "encoded.bin";
static const char* _out_path = "decoded.bin";

int main(int argc, char* argv[]) {
    if (argc > 1) {
        for (int i = 1; i < argc; i++) {
            if (!strcmp(argv[i], PARITY_BITS_ARG)) _m = atoi(argv[++i]);
            else if (!strcmp(argv[i], TARGET_ARG)) _target = argv[++i];
            else if (!strcmp(argv[i], OUTPUT_ARG)) _out_path = argv[++i];
            else {
                cerr << "Unknown argument: " << argv[i] << endl;
                return EXIT_FAILURE;
            }
        }
    }

    cout << "[bch_decode] _target=" << _target << ", _out_path=" << _out_path << ", _m=" << _m << endl;

    ifstream fin(_target, ios::binary);
    if (!fin) {
        cerr << "Cannot open input file: " << _target << endl;
        return EXIT_FAILURE;
    }

    vector<unsigned char> encoded((istreambuf_iterator<char>(fin)), istreambuf_iterator<char>());
    fin.close();

    Coding::BCH bch(15, 7);
    Coding::bytes decoded = bch.decode(encoded);

    ofstream fout(_out_path, ios::binary);
    if (!fout) {
        cerr << "Cannot open output file: " << _out_path << endl;
        return EXIT_FAILURE;
    }

    fout.write(reinterpret_cast<const char*>(decoded.data()), decoded.size());
    fout.close();

    cout << "File decoded successfully: " << _out_path << endl;
    return EXIT_SUCCESS;
}
