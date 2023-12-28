#ifndef LLP_LAB1_TYPES_H
#define LLP_LAB1_TYPES_H


#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>


enum DATA_TYPE { INT, FLOAT, STRING, BOOL };

enum ConnectionType { DIRECTIONAL, UNIDIRECTIONAL };

struct AddrInfo {
    bool HasValue;
    size_t BlockOffset;
    size_t DataOffset;
};

#define SMALL_STRING_LIMIT 48

struct MyString {
    size_t Length;
    union StringData {
        struct AddrInfo DataPtr;
        char InlinedData[SMALL_STRING_LIMIT];
    } Data;
};

#define NULL_FULL_ADDR                                                                         \
    (struct AddrInfo) { false, 0, 0 }

static inline struct AddrInfo getOptionalFullAddr(size_t BlockOffset,
                                                  size_t DataOffset) {
    return (struct AddrInfo){true, BlockOffset, DataOffset};
}

static inline bool isOptionalFullAddrsEq(struct AddrInfo Addr1,
                                         struct AddrInfo Addr2) {
    return ((Addr1.HasValue == Addr2.HasValue) && (Addr1.BlockOffset == Addr2.BlockOffset) &&
            (Addr1.DataOffset == Addr2.DataOffset));
}

static inline bool inOneBlock(struct AddrInfo Addr1, struct AddrInfo Addr2) {
    return Addr1.HasValue && Addr2.HasValue && (Addr1.BlockOffset == Addr2.BlockOffset);
}

static inline bool inDifferentBlocks(struct AddrInfo Addr1,
                                     struct AddrInfo Addr2) {
    return Addr1.HasValue && Addr2.HasValue && (Addr1.BlockOffset != Addr2.BlockOffset);
}

struct AttributeDescription {
    enum DATA_TYPE Type;
    struct MyString Name;
    struct AddrInfo Next;
    size_t AttributeId;
};

struct Attribute {
    size_t Id;
    enum DATA_TYPE Type;
    union Value {
        int32_t IntValue;
        float FloatValue;
        struct MyString StringValue;
        bool BoolValue;
    } Value;
    struct AddrInfo Next;
};

struct String {
    size_t Length;
    char Value[];
};

struct Node {
    size_t Id;
    bool Deleted;
    struct AddrInfo Previous;
    struct AddrInfo Attributes;
    struct AddrInfo Next;
};

struct NodeLink {
    size_t Id;
    bool Deleted;
    size_t LeftNodeId;
    size_t RightNodeId;
    enum ConnectionType Type;
    float Weight;
    struct AddrInfo Next;
    struct AddrInfo Previous;
};

struct Graph {
    size_t Id;
    size_t NodeCounter;
    size_t LinkCounter;
    size_t AttributeCounter;
    size_t LazyDeletedNodeCounter;
    size_t LazyDeletedLinkCounter;
    size_t NodesPlaceable;
    size_t LinksPlaceable;
    size_t PlacedNodes;
    size_t PlacedLinks;
    struct MyString Name;
    struct AddrInfo Nodes;
    struct AddrInfo AttributesDecription;
    struct AddrInfo LastNode;
    struct AddrInfo Links;
    struct AddrInfo LastLink;
    struct AddrInfo Next;
    struct AddrInfo Previous;
};

struct GraphStorage {
    size_t GraphCounter;
    size_t NextGraphId;
    size_t NextNodeId;
    size_t NextNodeLinkId;
    struct AddrInfo Graphs;
    struct AddrInfo LastGraph;
};

#endif