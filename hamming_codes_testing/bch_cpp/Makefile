CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Iinclude

LIB_SRCS = src/*
LIB_OBJS = $(LIB_SRCS:.cpp=.o)

ENCODE_SRC = file2bch.cpp
DECODE_SRC = bch2file.cpp

ENCODE_BIN = file2bch
DECODE_BIN = bch2file

all: $(ENCODE_BIN) $(DECODE_BIN)

$(ENCODE_BIN): $(ENCODE_SRC) $(LIB_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(DECODE_BIN): $(DECODE_SRC) $(LIB_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(ENCODE_BIN) $(DECODE_BIN) *.o

.PHONY: all clean
