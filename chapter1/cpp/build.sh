#!/bin/sh
g++ examples.cc -o examples `pkg-config --cflags --libs arrow`
