#ifndef LLP_LAB1_STORAGE_MANAGER_H
#define LLP_LAB1_STORAGE_MANAGER_H

#include <malloc.h>

#include "../interaction-file/file-io.h"
#include "../structures-data/types.h"

struct StorageController {
    struct FileAllocator *Allocator;
    struct GraphStorage Storage;
    struct AddrInfo StorageAddr;
};

size_t increaseGraphNumber(struct StorageController *Controller);
size_t decreaseGraphNumber(struct StorageController *Controller);
size_t increaseNodeNumber(struct StorageController *Controller);
size_t increaseNodeLinkNumber(struct StorageController *Controller);
void updateLastGraph(struct StorageController *const Controller,
                     struct AddrInfo LastGraphAddr);
void updateFirstGraph(struct StorageController *const Controller,
                      struct AddrInfo FirstGraphAddr);


#endif //LLP_LAB1_STORAGE_MANAGER_H
