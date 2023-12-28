#include <stdio.h>

#include "benchmark/benchmark.h"

int main() {
    const char *InsertBenchmarkResultName = "InsertTime.csv";
    const char *SelectBenchmarkResultName = "SelectByAttrsTime.csv";
    const char *DeleteBenchmarkResultName = "DeleteConstantElementsTime.csv";
    const char *UpdateProgressingBenchmarkResultName = "UpdateProgressingElementsTime.csv";
    const char *FileSizeBenchmarkResultName = "FileSizeByNodes.csv";
    const char *DopBenchmarkResultName = "DopBench.csv";

    FILE *Result;

    Result = fopen(InsertBenchmarkResultName, "w");
    benchmarkNodeInsert(Result);
    fclose(Result);

    Result = fopen(SelectBenchmarkResultName, "w");
    benchmarkSelectByAttributes(Result);
    fclose(Result);

    Result = fopen(UpdateProgressingBenchmarkResultName, "w");
    benchmarkUpdateProgressingElements(Result);
    fclose(Result);

    Result = fopen(FileSizeBenchmarkResultName, "w");
    benchmarkFileSize(Result);
    fclose(Result);

    Result = fopen(DeleteBenchmarkResultName, "w");
    benchmarkDeleteElements(Result);
    fclose(Result);

//Доп тест

    Result = fopen(DopBenchmarkResultName, "w");
    benchmarkDop(Result);
    fclose(Result);
}

