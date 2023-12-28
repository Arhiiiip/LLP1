#ifndef LLP_LAB1_BENCHMARK_H
#define LLP_LAB1_BENCHMARK_H

#include <stdio.h>

void benchmarkNodeInsert(FILE *OutFile);
void benchmarkSelectByAttributes(FILE *OutFile);
void benchmarkDeleteElements(FILE *OutFile);
void benchmarkUpdateProgressingElements(FILE *OutFile);
void benchmarkFileSize(FILE *OutFile);
void benchmarkDop(FILE *OutFile);

#endif //LLP_LAB1_BENCHMARK_H
