#ifndef LLP_LAB1_DATA_INTERFACES_H
#define LLP_LAB1_DATA_INTERFACES_H



#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>

#include "../structures-data/types.h"

struct ExternalAttribute {
    enum DATA_TYPE Type;
    size_t Id;
    union ExternalAttributeValue {
        int32_t IntValue;
        float FloatValue;
        char *StringAddr;
        bool BoolValue;
    } Value;
};

struct ExternalAttributeDescription {
    enum DATA_TYPE Type;
    char *Name;
    size_t AttributeId;
    struct ExternalAttributeDescription *Next;
};

struct ExternalNode {
    size_t Id;
    struct ExternalAttribute *Attributes;
    size_t AttributesNumber;
};

struct ExternalNodeLink {
    size_t Id;
    size_t LeftNodeId;
    size_t RightNodeId;
    enum ConnectionType Type;
    float Weight;
};

struct ExternalGraph {
    size_t Id;
    char *Name;
    struct ExternalAttributeDescription *AttributesDescription;
    size_t AttributesDescriptionNumber;
    size_t NodesNumber;
    size_t LinksNumber;
};

void deleteExternalNode(struct ExternalNode **Node);
void deleteExternalNodeLink(struct ExternalNodeLink **Link);
void deleteExternalGraph(struct ExternalGraph **Graph);


#endif //LLP_LAB1_DATA_INTERFACES_H
