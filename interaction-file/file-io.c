#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "file-io.h"

struct FileAllocator {
    int FileDescriptor;
    size_t FileSize;
    void *MappedFile;
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
    memcpy((char *) Allocator->MappedFile + Offset, &Header, sizeof(Header));
    memset((char *) Allocator->MappedFile + Offset + FullSize - 1, 0, 1);
}

static void initEmptyFile(const struct FileAllocator *const Allocator) {
    blockInit(Allocator, 0, INITIAL_FILE_SIZE, NULL_OFFSET,
              NULL_OFFSET); // Initialize first block
    size_t *ZeroData = memset(malloc(INITIAL_FILE_SIZE), 0, INITIAL_FILE_SIZE);
    size_t zeroes_written = fwrite(
            ZeroData, sizeof(size_t),
            (INITIAL_FILE_SIZE - sizeof(struct BlockHeader)) / sizeof(size_t),
            Allocator->MappedFile);
    free(ZeroData);
}

static void extendFile(struct FileAllocator *const Allocator,
                       struct OptionalOffset LastBlockOffset) {
    struct BlockHeader OldLastBlock;
    memcpy(&OldLastBlock, (char *) Allocator->MappedFile + LastBlockOffset.Offset, sizeof(OldLastBlock));
    const size_t EndOfLastBlock = LastBlockOffset.Offset + OldLastBlock.FullSize;
    blockInit(Allocator, EndOfLastBlock, Allocator->FileSize, NULL_OFFSET, LastBlockOffset);
    OldLastBlock.NextBlockOffset = getOptionalOffset(EndOfLastBlock);
    memcpy((char *) Allocator->MappedFile + LastBlockOffset.Offset, &OldLastBlock, sizeof(OldLastBlock));
    Allocator->FileSize *= 2;
}

static void mergeWhilePossible(const struct FileAllocator *const Allocator, const size_t Offset) {
    struct BlockHeader Header;
    memcpy(&Header, (char *) Allocator->MappedFile + Offset, sizeof(Header));
    if (!Header.IsOccupied) {
        if (Header.NextBlockOffset.HasValue) {
            struct BlockHeader NextHeader;
            memcpy(&NextHeader, (char *) Allocator->MappedFile + Header.NextBlockOffset.Offset, sizeof(NextHeader));
            if (!NextHeader.IsOccupied) {
                blockInit(Allocator, Offset, Header.FullSize + NextHeader.FullSize,
                          NextHeader.NextBlockOffset, Header.PrevBlockOffset);
                mergeWhilePossible(Allocator, Offset);
            }
        }
        memcpy((char *) Allocator->MappedFile + Offset, &Header, sizeof(Header));
    }
}

static void mergeAllPossible(const struct FileAllocator *const Allocator) {
    struct BlockHeader Header;
    mergeWhilePossible(Allocator, FIRST_BLOCK_OFFSET);
    memcpy(&Header, (char *) Allocator->MappedFile + FIRST_BLOCK_OFFSET, sizeof(struct BlockHeader));
    while (Header.NextBlockOffset.HasValue) {
        mergeWhilePossible(Allocator, Header.NextBlockOffset.Offset);
        memcpy(&Header, (char *) Allocator->MappedFile + Header.NextBlockOffset.Offset, sizeof(struct BlockHeader));
    }
}

static size_t getHeadersNumber(const struct FileAllocator *const Allocator) {
    size_t HeadersNumber = 0;
    struct BlockHeader Header;
    memcpy(&Header, (char *) Allocator->MappedFile + FIRST_BLOCK_OFFSET, sizeof(Header));
    while (Header.NextBlockOffset.HasValue) {
        HeadersNumber++;
        memcpy(&Header, (char *) Allocator->MappedFile + Header.NextBlockOffset.Offset, sizeof(Header));
    }
    HeadersNumber++;
    return HeadersNumber;
}

static void readAllHeaders(const struct FileAllocator *const Allocator,
                           struct BlockHeader **Headers) {
    const size_t HeadersNumber = getHeadersNumber(Allocator);
    *Headers = malloc(HeadersNumber * sizeof(struct BlockHeader));
    memcpy(*Headers, (char *) Allocator->MappedFile + FIRST_BLOCK_OFFSET, sizeof(struct BlockHeader));
    for (size_t i = 0; i < HeadersNumber; i++) {
        memcpy(&(*Headers)[i], (char *) Allocator->MappedFile + (*Headers)[i].NextBlockOffset.Offset, sizeof(struct BlockHeader));
    }
}

struct FileAllocator *initFileAllocator(char *fileName) {
    struct FileAllocator *const Allocator = malloc(sizeof(struct FileAllocator));
    Allocator->FileDescriptor = open(fileName, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (Allocator->FileDescriptor == -1) {
        return NULL;
    }
    Allocator->FileSize = lseek(Allocator->FileDescriptor, 0, SEEK_END);
    Allocator->MappedFile = mmap(NULL, Allocator->FileSize, PROT_READ | PROT_WRITE, MAP_SHARED, Allocator->FileDescriptor, 0);
    if (Allocator->MappedFile == MAP_FAILED) {
        close(Allocator->FileDescriptor);
        free(Allocator);
        return NULL;
    }
    uint64_t check_ptr;
    int ReadResult = fread(&check_ptr, 1, 1, Allocator->MappedFile);
    if (ReadResult != 1) {
        Allocator->FileSize = lseek(Allocator->FileDescriptor, 0, SEEK_END);
        initEmptyFile(Allocator);
    }
    mergeAllPossible(Allocator);
    return Allocator;
}

void shutdownFileAllocator(struct FileAllocator *Allocator) {
    munmap(Allocator->MappedFile, Allocator->FileSize);
    close(Allocator->FileDescriptor);
    free(Allocator);
}

static bool blockSplittable(struct BlockHeader *Header, const size_t DataSize) {
    return !Header->IsOccupied &&
           Header->FullSize >= DataSize + 2 * sizeof(struct BlockHeader) + BLOCK_MIN_CAPACITY;
}

static void splitIfTooBig(const struct FileAllocator *const Allocator, const size_t BlockOffset,
                          const size_t DataSize) {
    struct BlockHeader OldHeader;
    memcpy(&OldHeader, (char *) Allocator->MappedFile + BlockOffset, sizeof(OldHeader));
    if (blockSplittable(&OldHeader, DataSize)) {
        blockInit(Allocator, BlockOffset, DataSize + sizeof(struct BlockHeader),
                  getOptionalOffset(BlockOffset + DataSize + sizeof(struct BlockHeader)),
                  OldHeader.PrevBlockOffset);
        blockInit(Allocator, BlockOffset + DataSize + sizeof(struct BlockHeader),
                  OldHeader.FullSize - DataSize - sizeof(struct BlockHeader),
                  OldHeader.NextBlockOffset, getOptionalOffset(BlockOffset));
    }
}

struct SearchResult {
    size_t Offset;
    bool Found;
};

// Returns the offset of the first block that is not occupied and has enough
// space for the data If no such block is found, returns the offset of the last
// block
static struct SearchResult findBlock(const struct FileAllocator *const Allocator,
                                     const size_t DataSize) {
    struct SearchResult Result = {0, false};
    struct BlockHeader Header;
    size_t CurrentOffset = FIRST_BLOCK_OFFSET;
    memcpy(&Header, (char *) Allocator->MappedFile + CurrentOffset, sizeof(Header));
    do {
        if (!Header.IsOccupied) {
            mergeWhilePossible(Allocator, CurrentOffset);
            memcpy(&Header, (char *) Allocator->MappedFile + CurrentOffset, sizeof(Header));
        }
        if (!Header.IsOccupied && Header.DataSize >= DataSize) {
            Result.Offset = CurrentOffset;
            Result.Found = true;
            break;
        }
        if (Header.NextBlockOffset.HasValue) {
            CurrentOffset = Header.NextBlockOffset.Offset;
        }
        memcpy(&Header, (char *) Allocator->MappedFile + CurrentOffset, sizeof(Header));
    } while (Header.NextBlockOffset.HasValue);
    if (!Result.Found) {
        Result.Offset = CurrentOffset;
    }
    return Result;
}

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
    memcpy(&Header, (char *) Allocator->MappedFile + SearchResult.Offset, sizeof(Header));
    Header.IsOccupied = true;
    memcpy((char *) Allocator->MappedFile + SearchResult.Offset, &Header, sizeof(Header));
    return getOptionalFullAddr(SearchResult.Offset, sizeof(Header));
}

void deallocate(const struct FileAllocator *const Allocator,
                const struct AddrInfo Addr) {
    if (!Addr.HasValue) {
        return;
    }
    struct BlockHeader Header;
    const size_t BlockOffset = Addr.BlockOffset;
    memcpy(&Header, (char *) Allocator->MappedFile + BlockOffset, sizeof(Header));
    Header.IsOccupied = false;
    memcpy((char *) Allocator->MappedFile + BlockOffset, &Header, sizeof(Header));
    mergeWhilePossible(Allocator, BlockOffset);
}

static void printHeaderInfo(const struct FileAllocator *const Allocator,
                            struct BlockHeader *Header) {
    fprintf(stderr,
            "Block at %lu, size %lu, data size %lu, occupied %d, next %lu, prev "
            "%lu\n",
            ftell(Allocator->MappedFile) - sizeof(struct BlockHeader), Header->FullSize,
            Header->DataSize, Header->IsOccupied, Header->NextBlockOffset.Offset,
            Header->PrevBlockOffset.Offset);
}

void debugPrintFile(const struct FileAllocator *const Allocator) {
    struct BlockHeader Header;
    memcpy(&Header, (char *) Allocator->MappedFile + FIRST_BLOCK_OFFSET, sizeof(Header));
    while (Header.NextBlockOffset.HasValue) {
        printHeaderInfo(Allocator, &Header);
        memcpy(&Header, (char *) Allocator->MappedFile + Header.NextBlockOffset.Offset, sizeof(Header));
    }
    printHeaderInfo(Allocator, &Header);
}

struct AddrInfo getFirstBlockData(const struct FileAllocator *const Allocator) {
    struct BlockHeader Header;
    memcpy(&Header, (char *) Allocator->MappedFile + FIRST_BLOCK_OFFSET, sizeof(Header));
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
    memcpy(Buffer, (char *) Allocator->MappedFile + Addr.BlockOffset + Addr.DataOffset, Size);
    return Size;
}

int storeData(const struct FileAllocator *const Allocator, const struct AddrInfo Addr,
              const size_t Size, const void *const Buffer) {
    if (!Addr.HasValue) {
        return -1;
    }
    memcpy((char *) Allocator->MappedFile + Addr.BlockOffset + Addr.DataOffset, Buffer, Size);
    return Size;
}

size_t getFileSize(const struct FileAllocator *const Allocator) {
    return Allocator->FileSize;
}
