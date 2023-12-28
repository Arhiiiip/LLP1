cmake_minimum_required(VERSION 3.23)
project(LLP_lab_1 C)

set(CMAKE_C_STANDARD 11)

include_directories(configs)
include_directories(interaction-file)
include_directories(interaction-graph)
include_directories(structures-data)
include_directories(structures-request)

add_executable(LLP_lab_1
        benchmark/benchmark.h
        benchmark/benchmark.c
        configs/bech-config.h
        configs/config.h
        interaction-file/file-io.c
        interaction-file/file-io.h
        interaction-file/mfile-io.c
        interaction-graph/crud.c
        interaction-graph/graph-db.h
        interaction-graph/storage-manager.c
        interaction-graph/storage-manager.h
        structures-data/types.h
        structures-request/data-interfaces.h
        structures-request/request-structures.h
        structures-request/response-structures.h
        main.c)