#include "storage-manager.h"

#include <malloc.h>

#include "../structures-data/types.h"
#include "../interaction-file/file-io.h"
#include "../structures-request/data-interfaces.h"
#include "../structures-data/types.h"

struct StorageController *beginWork(char *DataFile) {
    struct StorageController *Controller = malloc(sizeof(struct StorageController));
    Controller->Allocator = initFileAllocator(DataFile);
    if (Controller->Allocator == NULL) {
        return NULL;
    }
    struct AddrInfo MayBeStorageAddr = getFirstBlockData(Controller->Allocator);
    if (!MayBeStorageAddr.HasValue) {
        MayBeStorageAddr = allocate(Controller->Allocator, sizeof(struct GraphStorage));
        Controller->Storage.GraphCounter = 0;
        Controller->Storage.LastGraph = NULL_FULL_ADDR;
        Controller->Storage.Graphs = NULL_FULL_ADDR;
        Controller->Storage.NextGraphId = 1;
        Controller->Storage.NextNodeId = 1;
        Controller->Storage.NextNodeLinkId = 1;
        storeData(Controller->Allocator, MayBeStorageAddr, sizeof(struct GraphStorage),
                  &Controller->Storage);
    } else {
        fetchData(Controller->Allocator, MayBeStorageAddr, sizeof(struct GraphStorage),
                  &Controller->Storage);
    }
    return Controller;
}

void endWork(struct StorageController *Controller) {
    shutdownFileAllocator(Controller->Allocator);
    free(Controller);
}

size_t increaseGraphNumber(struct StorageController *const Controller) {
    Controller->Storage.NextGraphId++;
    Controller->Storage.GraphCounter++;
    struct AddrInfo StorageAddr = getFirstBlockData(Controller->Allocator);
    storeData(Controller->Allocator, StorageAddr, sizeof(struct GraphStorage),
              &Controller->Storage);
    return Controller->Storage.NextGraphId;
}

size_t decreaseGraphNumber(struct StorageController *const Controller) {
    Controller->Storage.GraphCounter--;
    struct AddrInfo StorageAddr = getFirstBlockData(Controller->Allocator);
    storeData(Controller->Allocator, StorageAddr, sizeof(struct GraphStorage),
              &Controller->Storage);
    return Controller->Storage.NextGraphId;
}

size_t increaseNodeNumber(struct StorageController *const Controller) {
    Controller->Storage.NextNodeId++;
    struct AddrInfo StorageAddr = getFirstBlockData(Controller->Allocator);
    storeData(Controller->Allocator, StorageAddr, sizeof(struct GraphStorage),
              &Controller->Storage);
    return Controller->Storage.NextNodeId;
}

size_t increaseNodeLinkNumber(struct StorageController *const Controller) {
    Controller->Storage.NextNodeLinkId++;
    struct AddrInfo StorageAddr = getFirstBlockData(Controller->Allocator);
    storeData(Controller->Allocator, StorageAddr, sizeof(struct GraphStorage),
              &Controller->Storage);
    return Controller->Storage.NextGraphId;
}

void updateLastGraph(struct StorageController *const Controller,
                     struct AddrInfo LastGraphAddr) {
    if (Controller->Storage.LastGraph.HasValue) {
        struct Graph OldLastGraph;
        fetchData(Controller->Allocator, Controller->Storage.LastGraph, sizeof(struct Graph),
                  &OldLastGraph);
        OldLastGraph.Next = LastGraphAddr;
        storeData(Controller->Allocator, Controller->Storage.LastGraph, sizeof(struct Graph),
                  &OldLastGraph);
    }
    Controller->Storage.LastGraph = LastGraphAddr;
    struct AddrInfo StorageAddr = getFirstBlockData(Controller->Allocator);
    storeData(Controller->Allocator, StorageAddr, sizeof(struct GraphStorage),
              &Controller->Storage);
}

void updateFirstGraph(struct StorageController *const Controller,
                      struct AddrInfo FirstGraphAddr) {
    Controller->Storage.Graphs = FirstGraphAddr;
    struct AddrInfo StorageAddr = getFirstBlockData(Controller->Allocator);
    storeData(Controller->Allocator, StorageAddr, sizeof(struct GraphStorage),
              &Controller->Storage);
}

