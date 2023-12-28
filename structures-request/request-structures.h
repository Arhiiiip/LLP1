#ifndef LLP_LAB1_REQUEST_STRUCTURES_H
#define LLP_LAB1_REQUEST_STRUCTURES_H



#include <inttypes.h>

#include "data-interfaces.h"

enum RequestType { CREATE, READ, UPDATE, DELETE };

enum CreateRequestType {
    CREATE_NODE,
    CREATE_NODE_LINK,
    CREATE_GRAPH,
};

enum GraphIdType { GRAPH_ID, GRAPH_NAME };

union GraphId {
    size_t GraphId;
    char *GraphName;
};

struct CreateNodeRequest {
    enum GraphIdType GraphIdType;
    union GraphId GraphId;
    struct ExternalAttribute *Attributes;
};

struct CreateNodeLinkRequest {
    enum GraphIdType GraphIdType;
    union GraphId GraphId;
    size_t LeftNodeId;
    size_t RightNodeId;
    enum ConnectionType Type;
    float Weight;
};

struct CreateGraphRequest {
    char *Name;
    struct ExternalAttributeDescription *AttributesDescription;
};

struct CreateRequest {
    enum CreateRequestType Type;
    union CreateRequestData {
        struct CreateNodeRequest Node;
        struct CreateNodeLinkRequest NodeLink;
        struct CreateGraphRequest Graph;
    } Data;
};

enum ReadRequestType {
    READ_NODE,
    READ_NODE_LINK,
    READ_GRAPH,
};

struct FloatFilter {
    bool HasMin;
    float Min;
    bool HasMax;
    float Max;
};

struct IntFilter {
    bool HasMin;
    int32_t Min;
    bool HasMax;
    int32_t Max;
};

enum StringFilterType { STRLEN_RANGE, STRING_EQUAL };

struct StringFilter {
    enum StringFilterType Type;
    union StringFilterData {
        struct IntFilter StrlenRange;
        char *StringEqual;
    } Data;
};

struct BoolFilter {
    bool Value;
};

enum LINKING_RELATION { HAS_LINK_TO, HAS_LINK_FROM };

struct LinkFilter {
    size_t NodeId;
    enum LINKING_RELATION Relation;
    struct FloatFilter WeightFilter;
};

enum FILTER_TYPE { INT_FILTER, FLOAT_FILTER, BOOL_FILTER, STRING_FILTER, LINK_FILTER };

struct AttributeFilter {
    size_t AttributeId;
    enum FILTER_TYPE Type;
    union AttributeFilterData {
        struct FloatFilter Float;
        struct IntFilter Int;
        struct StringFilter String;
        struct BoolFilter Bool;
        struct LinkFilter Link;
    } Data;
    struct AttributeFilter *Next;
};

struct ReadNodeRequest {
    enum GraphIdType GraphIdType;
    union GraphId GraphId;
    const struct AttributeFilter *AttributesFilterChain;
    bool ById;
    size_t Id;
};

enum NodeLinkRequestType { BY_ID, BY_LEFT_NODE_ID, BY_RIGHT_NODE_ID, ALL };

struct ReadNodeLinkRequest {
    enum GraphIdType GraphIdType;
    union GraphId GraphId;
    enum NodeLinkRequestType Type;
    size_t Id;
};

struct ReadGraphRequest {
    char *Name;
};

struct ReadRequest {
    enum ReadRequestType Type;
    union ReadRequestData {
        struct ReadNodeRequest Node;
        struct ReadNodeLinkRequest NodeLink;
        struct ReadGraphRequest Graph;
    } Data;
};

enum UpdateRequestType { UPDATE_NODE, UPDATE_NODE_LINK };

struct UpdateNodeRequest {
    enum GraphIdType GraphIdType;
    union GraphId GraphId;
    const struct AttributeFilter *AttributesFilterChain;
    bool ById;
    size_t Id;
    struct ExternalAttribute *Attributes;
    size_t UpdatedAttributesNumber;
};

struct UpdateNodeLinkRequest {
    enum GraphIdType GraphIdType;
    union GraphId GraphId;
    size_t Id;
    bool UpdateType;
    enum ConnectionType Type;
    bool UpdateWeight;
    float Weight;
};

struct UpdateRequest {
    enum UpdateRequestType Type;
    union UpdateRequestData {
        struct UpdateNodeRequest Node;
        struct UpdateNodeLinkRequest NodeLink;
    } Data;
};

enum DeleteRequestType {
    DELETE_NODE,
    DELETE_NODE_LINK,
    DELETE_GRAPH,
};

struct DeleteNodeRequest {
    enum GraphIdType GraphIdType;
    union GraphId GraphId;
    bool ById;
    size_t Id;
    struct AttributeFilter *AttributesFilterChain;
};

struct DeleteNodeLinkRequest {
    enum GraphIdType GraphIdType;
    union GraphId GraphId;
    enum NodeLinkRequestType Type;
    size_t Id;
};

struct DeleteGraphRequest {
    char *Name;
};

struct DeleteRequest {
    enum DeleteRequestType Type;
    union DeleteRequestData {
        struct DeleteNodeRequest Node;
        struct DeleteNodeLinkRequest NodeLink;
        struct DeleteGraphRequest Graph;
    } Data;
};

struct Request {
    enum RequestType Type;
    char *GraphName;
    union RequestData {
        struct CreateRequest Create;
        struct ReadRequest Read;
        struct UpdateRequest Update;
        struct DeleteRequest Delete;
    } Data;
};

#endif //LLP_LAB1_REQUEST_STRUCTURES_H
