#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "file-io.h"

struct FileAllocator {
    int fd;       // Файловый дескриптор
    FILE *File;
    size_t FileSize;
    void* map;     // Указатель на отображение файла
};

struct SearchResult {
    size_t Offset;
    bool Found;
};

#define FIRST_BLOCK_OFFSET sizeof(uint64_t)

static void blockInit(const struct FileAllocator *const Allocator, size_t Offset,
                      size_t FullSize, struct OptionalOffset NextBlockOffset,
                      struct OptionalOffset PrevBlockOffset) {
    struct BlockHeader Header = {.IsOccupied = false,
            .FullSize = FullSize,
            .DataSize = FullSize - sizeof(struct BlockHeader),
            .NextBlockOffset = NextBlockOffset,
            .PrevBlockOffset = PrevBlockOffset};
    memcpy((char *)Allocator->map + Offset, &Header, sizeof(Header));
}

static void initEmptyFile(const struct FileAllocator *const Allocator) {
    blockInit(Allocator, 0, INITIAL_FILE_SIZE, NULL_OFFSET, NULL_OFFSET); // Initialize first block
    memset((char *)Allocator->map + FIRST_BLOCK_OFFSET, 0, INITIAL_FILE_SIZE - sizeof(struct BlockHeader));
}

static void extendFile(struct FileAllocator *const Allocator,
                       struct OptionalOffset LastBlockOffset) {
    struct BlockHeader OldLastBlock;
    memcpy(&OldLastBlock, (char *)Allocator->map + LastBlockOffset.Offset, sizeof(OldLastBlock));
    const size_t EndOfLastBlock = LastBlockOffset.Offset + OldLastBlock.FullSize;
    blockInit(Allocator, EndOfLastBlock, Allocator->FileSize, NULL_OFFSET, LastBlockOffset);
    OldLastBlock.NextBlockOffset = getOptionalOffset(EndOfLastBlock);
    memcpy((char *)Allocator->map + LastBlockOffset.Offset, &OldLastBlock, sizeof(OldLastBlock));
    Allocator->FileSize *= 2;
}

static void mergeWhilePossible(const struct FileAllocator *const Allocator,
                               const size_t Offset) {
    struct BlockHeader Header;
    memcpy(&Header, (char *)Allocator->map + Offset, sizeof(Header));
    if (!Header.IsOccupied) {
        if (Header.NextBlockOffset.HasValue) {
            struct BlockHeader NextHeader;
            memcpy(&NextHeader, (char *)Allocator->map + Header.NextBlockOffset.Offset, sizeof(NextHeader));
            if (!NextHeader.IsOccupied) {
                blockInit(Allocator, Offset, Header.FullSize + NextHeader.FullSize,
                          NextHeader.NextBlockOffset, Header.PrevBlockOffset);
                mergeWhilePossible(Allocator, Offset);
            }
        }
    }
}

static void mergeAllPossible(const struct FileAllocator *const Allocator) {
    mergeWhilePossible(Allocator, FIRST_BLOCK_OFFSET);
    struct BlockHeader Header;
    memcpy(&Header, (char *)Allocator->map + FIRST_BLOCK_OFFSET, sizeof(struct BlockHeader));
    while (Header.NextBlockOffset.HasValue) {
        mergeWhilePossible(Allocator, Header.NextBlockOffset.Offset);
        memcpy(&Header, (char *)Allocator->map + Header.NextBlockOffset.Offset, sizeof(struct BlockHeader));
    }
}

static size_t getHeadersNumber(const struct FileAllocator *const Allocator) {
    size_t HeadersNumber = 0;
    struct BlockHeader Header;
    memcpy(&Header, (char *)Allocator->map + FIRST_BLOCK_OFFSET, sizeof(Header));
    while (Header.NextBlockOffset.HasValue) {
        HeadersNumber++;
        memcpy(&Header, (char *)Allocator->map + Header.NextBlockOffset.Offset, sizeof(Header));
    }
    HeadersNumber++;
    return HeadersNumber;
}

static void readAllHeaders(const struct FileAllocator *const Allocator,
                           struct BlockHeader **Headers) {
    const size_t HeadersNumber = getHeadersNumber(Allocator);
    *Headers = malloc(HeadersNumber * sizeof(struct BlockHeader));
    memcpy(*Headers, (char *)Allocator->map + FIRST_BLOCK_OFFSET, HeadersNumber * sizeof(struct BlockHeader));
}



static struct SearchResult findBlock(const struct FileAllocator *const Allocator,
                                     const size_t DataSize) {
    struct SearchResult Result = {0, false};
    struct BlockHeader Header;
    size_t CurrentOffset = FIRST_BLOCK_OFFSET;
    fseek(Allocator->File, CurrentOffset, SEEK_SET);
    do {
        fread(&Header, sizeof(Header), 1, Allocator->File);
        if (!Header.IsOccupied) {
            mergeWhilePossible(Allocator, ftell(Allocator->File) - sizeof(Header));
            fread(&Header, sizeof(Header), 1, Allocator->File);
        }
        if (!Header.IsOccupied && Header.DataSize >= DataSize) {
            Result.Offset = ftell(Allocator->File) - sizeof(struct BlockHeader);
            Result.Found = true;
            break;
        }
        if (Header.NextBlockOffset.HasValue) {
            CurrentOffset = Header.NextBlockOffset.Offset;
        }
        fseek(Allocator->File, CurrentOffset, SEEK_SET);
    } while (Header.NextBlockOffset.HasValue);
    if (!Result.Found) {
        Result.Offset = CurrentOffset;
    }
    return Result;
}

static bool blockSplittable(struct BlockHeader *Header, const size_t DataSize) {
    return !Header->IsOccupied &&
           Header->FullSize >= DataSize + 2 * sizeof(struct BlockHeader) + BLOCK_MIN_CAPACITY;
}

static void splitIfTooBig(const struct FileAllocator *const Allocator, const size_t BlockOffset,
                          const size_t DataSize) {
    struct BlockHeader OldHeader;
    fseek(Allocator->File, BlockOffset, SEEK_SET);
    fread(&OldHeader, sizeof(OldHeader), 1, Allocator->File);
    if (blockSplittable(&OldHeader, DataSize)) {
        blockInit(Allocator, BlockOffset, DataSize + sizeof(struct BlockHeader),
                  getOptionalOffset(BlockOffset + DataSize + sizeof(struct BlockHeader)),
                  OldHeader.PrevBlockOffset);
        blockInit(Allocator, BlockOffset + DataSize + sizeof(struct BlockHeader),
                  OldHeader.FullSize - DataSize - sizeof(struct BlockHeader),
                  OldHeader.NextBlockOffset, getOptionalOffset(BlockOffset));
    }
}

//struct FileAllocator *initFileAllocator(char *fileName) {
//    struct FileAllocator *const Allocator = malloc(sizeof(struct FileAllocator));
//    Allocator->fd = open(fileName, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
//    if (Allocator->fd == -1) {
//        perror("open");
//        free(Allocator);
//        return NULL;
//    }
//
//    // Установка размера файла
//    if (lseek(Allocator->fd, INITIAL_FILE_SIZE - 1, SEEK_SET) == -1) {
//        perror("lseek");
//        close(Allocator->fd);
//        free(Allocator);
//        return NULL;
//    }
//
//    if (write(Allocator->fd, "", 1) != 1) {
//        perror("write");
//        close(Allocator->fd);
//        free(Allocator);
//        return NULL;
//    }
//
//    Allocator->FileSize = INITIAL_FILE_SIZE;
//
//    // Отображение файла в память
//    Allocator->map = mmap(NULL, INITIAL_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, Allocator->fd, 0);
//    if (Allocator->map == MAP_FAILED) {
//        perror("mmap");
//        close(Allocator->fd);
//        free(Allocator);
//        return NULL;
//    }
//
//    mergeAllPossible(Allocator);
//    return Allocator;
//}

//void shutdownFileAllocator(struct FileAllocator *Allocator) {
//    munmap(Allocator->map, Allocator->FileSize);
//    close(Allocator->fd);
//    free(Allocator);
//}

static size_t getRealDataSize(size_t DataSize) {
    size_t FullBlocks = DataSize / BLOCK_MIN_CAPACITY;
    FullBlocks += ((DataSize % BLOCK_MIN_CAPACITY)) == 0 ? 0 : 1;
    return FullBlocks * BLOCK_MIN_CAPACITY;
}

struct AddrInfo allocate(struct FileAllocator *const Allocator, const size_t DataSize) {
    if (DataSize == 0) {
        return NULL_FULL_ADDR;
    }
    const size_t RealDataSize = getRealDataSize(DataSize);
    struct SearchResult SearchResult = findBlock(Allocator, RealDataSize);
    while (!SearchResult.Found) {
        extendFile(Allocator, getOptionalOffset(SearchResult.Offset));
        SearchResult = findBlock(Allocator, RealDataSize);
    }
    splitIfTooBig(Allocator, SearchResult.Offset, RealDataSize);
    struct BlockHeader Header;
    memcpy(&Header, (char *)Allocator->map + SearchResult.Offset, sizeof(Header));
    Header.IsOccupied = true;
    memcpy((char *)Allocator->map + SearchResult.Offset, &Header, sizeof(Header));
    return getOptionalFullAddr(SearchResult.Offset, sizeof(Header));
}

void deallocate(const struct FileAllocator *const Allocator,
                const struct AddrInfo Addr) {
    if (!Addr.HasValue) {
        return;
    }
    struct BlockHeader Header;
    const size_t BlockOffset = Addr.BlockOffset;
    memcpy(&Header, (char *)Allocator->map + BlockOffset, sizeof(Header));
    Header.IsOccupied = false;
    memcpy((char *)Allocator->map + BlockOffset, &Header, sizeof(Header));
    mergeWhilePossible(Allocator, BlockOffset);
}

static void printHeaderInfo(const struct FileAllocator *const Allocator,
                            struct BlockHeader *Header) {
    fprintf(stderr,
            "Block at %lu, size %lu, data size %lu, occupied %d, next %lu, prev "
            "%lu\n",
            ftell(Allocator->File) - sizeof(struct BlockHeader), Header->FullSize,
            Header->DataSize, Header->IsOccupied, Header->NextBlockOffset.Offset,
            Header->PrevBlockOffset.Offset);
}

void debugPrintFile(const struct FileAllocator *const Allocator) {
    struct BlockHeader Header;
    memcpy(&Header, (char *)Allocator->map + FIRST_BLOCK_OFFSET, sizeof(struct BlockHeader));
    while (Header.NextBlockOffset.HasValue) {
        printHeaderInfo(Allocator, &Header);
        memcpy(&Header, (char *)Allocator->map + Header.NextBlockOffset.Offset, sizeof(struct BlockHeader));
    }
    printHeaderInfo(Allocator, &Header);
}

struct AddrInfo getFirstBlockData(const struct FileAllocator *const Allocator) {
    struct BlockHeader Header;
    memcpy(&Header, (char *)Allocator->map + FIRST_BLOCK_OFFSET, sizeof(struct BlockHeader));
    if (!Header.IsOccupied) {
        return NULL_FULL_ADDR;
    }
    return getOptionalFullAddr(FIRST_BLOCK_OFFSET, sizeof(struct BlockHeader));
}

size_t debugGetHeadersNumber(const struct FileAllocator *const Allocator) {
    return getHeadersNumber(Allocator);
}

void debugReadAllHeaders(const struct FileAllocator *const Allocator,
                         struct BlockHeader **Headers) {
    readAllHeaders(Allocator, Headers);
}

int fetchData(const struct FileAllocator *const Allocator, const struct AddrInfo Addr,
              const size_t Size, void *const Buffer) {
    memcpy(Buffer, (char *)Allocator->map + Addr.BlockOffset + Addr.DataOffset, Size);
    return Size;
}

int storeData(const struct FileAllocator *const Allocator, const struct AddrInfo Addr,
              const size_t Size, const void *const Buffer) {
    if (!Addr.HasValue) {
        return -1;
    }
    memcpy((char *)Allocator->map + Addr.BlockOffset + Addr.DataOffset, Buffer, Size);
    return Size;
}

size_t getFileSize(const struct FileAllocator *const Allocator) {
    return Allocator->FileSize;
}
