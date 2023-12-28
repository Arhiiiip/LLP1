#ifndef LLP_LAB1_FILE_IO_H
#define LLP_LAB1_FILE_IO_H
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../structures-data/types.h"
#include "../configs/config.h"

struct FileAllocator;

struct OptionalOffset {
    bool HasValue;
    size_t Offset;
};

#define NULL_OFFSET (struct OptionalOffset) { false, 0 }

static inline struct OptionalOffset getOptionalOffset(size_t Offset) {
    return (struct OptionalOffset){true, Offset};
}

struct BlockHeader {
    bool IsOccupied;
    size_t FullSize;
    size_t DataSize;
    struct OptionalOffset NextBlockOffset;
    struct OptionalOffset PrevBlockOffset;
};

struct Block {
    struct BlockHeader header;
    char data[];
};


struct FileAllocator *initFileAllocator(char *FileName);
void shutdownFileAllocator(struct FileAllocator *allocator);
void dropFileAllocator(struct FileAllocator *allocator);
struct AddrInfo allocate(struct FileAllocator *const allocator, size_t Size);
struct AddrInfo getFirstBlockData(const struct FileAllocator *const allocator);
void deallocate(const struct FileAllocator *const allocator, struct AddrInfo Addr);
int fetchData(const struct FileAllocator *const allocator, const struct AddrInfo Addr,
              const size_t Size, void *const Buffer);
int storeData(const struct FileAllocator *const allocator, const struct AddrInfo Addr,
              const size_t Size, const void *const Buffer);


#endif //LLP_LAB1_FILE_IO_H
