#include <string.h>

#include "../structures-data/types.h"
#include "../structures-request/data-interfaces.h"
#include "../interaction-file/file-io.h"
#include "graph-db.h"
#include "storage-manager.h"

struct AddrInfo findGraphAddrById(const struct StorageController *Controller,
                                  size_t Id);
struct AddrInfo findGraphAddrByName(const struct StorageController *Controller,
                                    const char *Name);
struct AddrInfo findGraphAddrByGraphId(const struct StorageController *const Controller,
                                       const enum GraphIdType IdType,
                                       const union GraphId GraphId);
size_t findNodesByFilters(const struct StorageController *const Controller,
                          const struct AddrInfo GraphAddr,
                          const struct AttributeFilter *AttributeFilterChain,
                          struct AddrInfo **Result);
struct AddrInfo findNodeAddrById(const struct StorageController *const Controller,
                                 const struct AddrInfo GraphAddr, size_t Id);
struct AddrInfo findNodeLinkAddrById(const struct StorageController *const Controller,
                                     const struct AddrInfo GraphAddr,
                                     size_t Id);
size_t findNodeLinksByIdAndType(const struct StorageController *Controller,
                                const struct AddrInfo GraphAddr,
                                const enum NodeLinkRequestType Type, const size_t Id,
                                struct AddrInfo **Result);


struct AddrInfo findGraphAddrById(const struct StorageController *const Controller,
                                  const size_t Id) {
    struct AddrInfo GraphAddr = Controller->Storage.Graphs;
    while (GraphAddr.HasValue) {
        struct Graph Graph;
        fetchData(Controller->Allocator, GraphAddr, sizeof(struct Graph), &Graph);
        if (Graph.Id == Id) {
            return GraphAddr;
        }
        GraphAddr = Graph.Next;
    }
    return NULL_FULL_ADDR;
}

struct AddrInfo findGraphAddrByName(const struct StorageController *const Controller,
                                    const char *const Name) {
    struct AddrInfo GraphAddr = Controller->Storage.Graphs;
    struct AddrInfo Result = NULL_FULL_ADDR;
    while (GraphAddr.HasValue) {
        struct Graph Graph;
        fetchData(Controller->Allocator, GraphAddr, sizeof(struct Graph), &Graph);
        struct MyString GraphName = Graph.Name;
        char *GraphNameStr = malloc(GraphName.Length + 1);
        if (GraphName.Length > SMALL_STRING_LIMIT) {
            fetchData(Controller->Allocator, GraphName.Data.DataPtr, GraphName.Length,
                      GraphNameStr);
        } else {
            memcpy(GraphNameStr, GraphName.Data.InlinedData, GraphName.Length);
        }
        GraphNameStr[GraphName.Length] = 0;
        if (strcmp(GraphNameStr, Name) == 0) {
            Result = GraphAddr;
        }
        free(GraphNameStr);
        if (Result.HasValue)
            return Result;
        GraphAddr = Graph.Next;
    }
    return NULL_FULL_ADDR;
}

struct AddrInfo findGraphAddrByGraphId(const struct StorageController *const Controller,
                                       const enum GraphIdType IdType,
                                       const union GraphId GraphId) {
    if (IdType == GRAPH_ID) {
        return findGraphAddrById(Controller, GraphId.GraphId);
    }
    if (IdType == GRAPH_NAME) {
        return findGraphAddrByName(Controller, GraphId.GraphName);
    }
    return NULL_FULL_ADDR;
}

static bool matchIntFilter(const struct IntFilter *const Filter, int32_t Value) {
    if (Filter->HasMin && Filter->Min > Value) {
        return false;
    }
    if (Filter->HasMax && Filter->Max < Value) {
        return false;
    }
    return true;
}

static bool matchFloatFilter(const struct FloatFilter *const Filter, float Value) {
    if (Filter->HasMin && Filter->Min > Value) {
        return false;
    }
    if (Filter->HasMax && Filter->Max < Value) {
        return false;
    }
    return true;
}

static bool matchFilterAndAttributeType(enum FILTER_TYPE FT, enum DATA_TYPE AT) {
    if (AT == INT)
        return FT == INT_FILTER;
    if (AT == FLOAT)
        return FT == FLOAT_FILTER;
    if (AT == BOOL)
        return FT == BOOL_FILTER;
    if (AT == STRING)
        return FT == STRING_FILTER;
    return false;
}

static bool checkNodeMatchesFilter(const struct StorageController *const Controller,
                                   const struct AddrInfo NodeAddr,
                                   const struct Graph *const Graph,
                                   const struct AddrInfo GraphAddr,
                                   const struct AttributeFilter *const FilterChain) {
    if (FilterChain == NULL) {
        return true;
    }
    struct Node ToCheck;
    fetchData(Controller->Allocator, NodeAddr, sizeof(ToCheck), &ToCheck);
    const size_t AttributesSize = sizeof(struct Attribute) * Graph->AttributeCounter;
    struct Attribute *Attributes = malloc(AttributesSize);
    fetchData(Controller->Allocator, ToCheck.Attributes, AttributesSize, Attributes);
    const struct AttributeFilter *Filter = FilterChain;
    bool result = true;
    while (Filter != NULL) {
        if (Filter->Type == LINK_FILTER) {
            if (Filter->Data.Link.Relation == HAS_LINK_TO) {
                struct AddrInfo *LinkAddrs;
                bool HasLink = false;
                size_t NeighboursCnt = findNodeLinksByIdAndType(
                        Controller, GraphAddr, BY_LEFT_NODE_ID, ToCheck.Id, &LinkAddrs);
                for (size_t i = 0; i < NeighboursCnt; ++i) {
                    struct NodeLink Link;
                    fetchData(Controller->Allocator, LinkAddrs[i], sizeof(Link), &Link);
                    if (Link.RightNodeId == Filter->Data.Link.NodeId) {
                        if (matchFloatFilter(&(Filter->Data.Link.WeightFilter), Link.Weight)) {
                            HasLink = true;
                            break;
                        }
                    }
                }
                free(LinkAddrs);
                if (!HasLink) {
                    NeighboursCnt = findNodeLinksByIdAndType(
                            Controller, GraphAddr, BY_RIGHT_NODE_ID, ToCheck.Id, &LinkAddrs);
                    for (size_t i = 0; i < NeighboursCnt; ++i) {
                        struct NodeLink Link;
                        fetchData(Controller->Allocator, LinkAddrs[i], sizeof(Link), &Link);
                        if (Link.LeftNodeId == Filter->Data.Link.NodeId &&
                            Link.Type == UNIDIRECTIONAL) {
                            if (matchFloatFilter(&(Filter->Data.Link.WeightFilter),
                                                 Link.Weight)) {
                                HasLink = true;
                                break;
                            }
                        }
                    }
                    free(LinkAddrs);
                }
                if (!HasLink) {
                    result = false;
                    break;
                }
            }
            if (Filter->Data.Link.Relation == HAS_LINK_FROM) {
                struct AddrInfo *LinkAddrs;
                bool HasLink = false;
                size_t NeighboursCnt = findNodeLinksByIdAndType(
                        Controller, GraphAddr, BY_RIGHT_NODE_ID, ToCheck.Id, &LinkAddrs);
                for (size_t i = 0; i < NeighboursCnt; ++i) {
                    struct NodeLink Link;
                    fetchData(Controller->Allocator, LinkAddrs[i], sizeof(Link), &Link);
                    if (Link.LeftNodeId == Filter->Data.Link.NodeId) {
                        if (matchFloatFilter(&(Filter->Data.Link.WeightFilter), Link.Weight)) {
                            HasLink = true;
                            break;
                        }
                    }
                }
                free(LinkAddrs);
                if (!HasLink) {
                    NeighboursCnt = findNodeLinksByIdAndType(
                            Controller, GraphAddr, BY_LEFT_NODE_ID, ToCheck.Id, &LinkAddrs);
                    for (size_t i = 0; i < NeighboursCnt; ++i) {
                        struct NodeLink Link;
                        fetchData(Controller->Allocator, LinkAddrs[i], sizeof(Link), &Link);
                        if (Link.RightNodeId == Filter->Data.Link.NodeId &&
                            Link.Type == UNIDIRECTIONAL) {
                            if (matchFloatFilter(&(Filter->Data.Link.WeightFilter),
                                                 Link.Weight)) {
                                HasLink = true;
                                break;
                            }
                        }
                    }
                    free(LinkAddrs);
                }
                if (!HasLink) {
                    result = false;
                    break;
                }
            }
            Filter = Filter->Next;
            continue;
        }
        if (Graph->AttributeCounter == 0) {
            return false;
        }
        for (size_t i = 0; i < Graph->AttributeCounter; ++i) {
            if (Attributes[i].Id == Filter->AttributeId &&
                matchFilterAndAttributeType(Filter->Type, Attributes[i].Type)) {
                if (Filter->Type == BOOL_FILTER) {
                    if (Attributes[i].Value.BoolValue != Filter->Data.Bool.Value) {
                        result = false;
                        break;
                    }
                    continue;
                }
                if (Filter->Type == INT_FILTER) {
                    if (!matchIntFilter(&(Filter->Data.Int), Attributes[i].Value.IntValue)) {
                        result = false;
                        break;
                    }
                    continue;
                }
                if (Filter->Type == FLOAT_FILTER) {
                    if (!matchFloatFilter(&(Filter->Data.Float),
                                          Attributes[i].Value.FloatValue)) {
                        result = false;
                        break;
                    }
                    continue;
                }
                if (Filter->Type == STRING_FILTER) {
                    struct MyString AttributeString = Attributes[i].Value.StringValue;
                    if (Filter->Data.String.Type == STRLEN_RANGE) {
                        const size_t AttributeStringLength = AttributeString.Length;
                        if (!matchIntFilter(&(Filter->Data.String.Data.StrlenRange),
                                            AttributeStringLength)) {
                            result = false;
                            break;
                        }
                        continue;
                    }
                    if (Filter->Data.String.Type == STRING_EQUAL) {
                        char *AttributeStringStr = malloc(AttributeString.Length + 1);
                        if (AttributeString.Length > SMALL_STRING_LIMIT) {
                            fetchData(Controller->Allocator, AttributeString.Data.DataPtr,
                                      AttributeString.Length + 1, AttributeStringStr);
                        } else {
                            memcpy(AttributeStringStr, AttributeString.Data.InlinedData,
                                   AttributeString.Length + 1);
                        }
                        if (strcmp(AttributeStringStr, Filter->Data.String.Data.StringEqual) !=
                            0) {
                            result = false;
                        }
                        free(AttributeStringStr);
                        if (!result)
                            break;
                        continue;
                    }
                }
            }
        }
        if (!result) {
            break;
        }
        Filter = Filter->Next;
    }
    free(Attributes);
    return result;
}

size_t findNodesByFilters(const struct StorageController *const Controller,
                          const struct AddrInfo GraphAddr,
                          const struct AttributeFilter *AttributeFilterChain,
                          struct AddrInfo **Result) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    size_t GoodNodesCnt = 0;
    struct AddrInfo NodeAddr = Graph.Nodes;
    *Result = malloc(sizeof(struct AddrInfo) * GRAPH_NODES_PER_BLOCK);
    size_t ResultCapacity = GRAPH_NODES_PER_BLOCK;
    while (NodeAddr.HasValue) {
        struct Node ToCheck;
        fetchData(Controller->Allocator, NodeAddr, sizeof(ToCheck), &ToCheck);
        if (!ToCheck.Deleted && checkNodeMatchesFilter(Controller, NodeAddr, &Graph, GraphAddr,
                                                       AttributeFilterChain)) {
            (*Result)[GoodNodesCnt] = NodeAddr;
            GoodNodesCnt++;
        }
        if (GoodNodesCnt + 3 > ResultCapacity) {
            *Result = realloc(*Result, ResultCapacity * 2 * sizeof(struct AddrInfo));
            ResultCapacity = ResultCapacity * 2;
        }
        if (!isOptionalFullAddrsEq(NodeAddr, Graph.LastNode)) {
            NodeAddr = ToCheck.Next;
        } else
            break;
    }
    return GoodNodesCnt;
}

static bool checkNodeLinkMatchRequest(const struct NodeLink *const Link,
                                      const enum NodeLinkRequestType Type, const size_t Id) {
    return Type == ALL || (Type == BY_ID && Link->Id == Id) ||
           (Type == BY_LEFT_NODE_ID && Link->LeftNodeId == Id) ||
           (Type == BY_RIGHT_NODE_ID && Link->RightNodeId == Id);
}

size_t findNodeLinksByIdAndType(const struct StorageController *Controller,
                                const struct AddrInfo GraphAddr,
                                const enum NodeLinkRequestType Type, const size_t Id,
                                struct AddrInfo **Result) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    size_t Cnt = 0;
    struct AddrInfo NodeLinkAddr = Graph.Links;
    while (NodeLinkAddr.HasValue) {
        struct NodeLink Link;
        fetchData(Controller->Allocator, NodeLinkAddr, sizeof(Link), &Link);
        if (!Link.Deleted && checkNodeLinkMatchRequest(&Link, Type, Id)) {
            Cnt++;
        }
        if (isOptionalFullAddrsEq(NodeLinkAddr, Graph.LastLink))
            break;
        NodeLinkAddr = Link.Next;
    }
    *Result = malloc(sizeof(struct AddrInfo) * Cnt);
    NodeLinkAddr = Graph.Links;
    size_t Index = 0;
    while (NodeLinkAddr.HasValue) {
        struct NodeLink Link;
        fetchData(Controller->Allocator, NodeLinkAddr, sizeof(Link), &Link);
        if (!Link.Deleted && checkNodeLinkMatchRequest(&Link, Type, Id)) {
            (*Result)[Index] = NodeLinkAddr;
            Index++;
        }
        if (isOptionalFullAddrsEq(NodeLinkAddr, Graph.LastLink))
            break;
        NodeLinkAddr = Link.Next;
    }
    return Cnt;
}

struct AddrInfo findNodeLinkAddrById(const struct StorageController *const Controller,
                                     const struct AddrInfo GraphAddr,
                                     const size_t Id) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    struct AddrInfo NodeLinkAddr = Graph.Links;
    while (NodeLinkAddr.HasValue) {
        struct NodeLink Link;
        fetchData(Controller->Allocator, NodeLinkAddr, sizeof(Link), &Link);
        if (Link.Id == Id && !Link.Deleted) {
            return NodeLinkAddr;
        }
        NodeLinkAddr = Link.Next;
    }
    return NULL_FULL_ADDR;
}

struct AddrInfo findNodeAddrById(const struct StorageController *const Controller,
                                 const struct AddrInfo GraphAddr, size_t Id) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    struct AddrInfo NodeAddr = Graph.Nodes;
    while (NodeAddr.HasValue) {
        struct Node Node;
        fetchData(Controller->Allocator, NodeAddr, sizeof(Node), &Node);
        if (Node.Id == Id && !Node.Deleted) {
            return NodeAddr;
        }
        if (!isOptionalFullAddrsEq(NodeAddr, Graph.LastNode)) {
            NodeAddr = Node.Next;
        } else
            break;
    }
    return NULL_FULL_ADDR;
}


struct MyString createString(struct StorageController *const Controller,
                             const char *const String) {
    size_t StringLength = strlen(String) + 1;
    struct MyString MyString;
    MyString.Length = StringLength;
    if (StringLength < SMALL_STRING_LIMIT) {
        memcpy(MyString.Data.InlinedData, String, StringLength);
        MyString.Data.InlinedData[StringLength] = 0;
    } else {
        struct AddrInfo StringAddr = allocate(Controller->Allocator, StringLength);
        storeData(Controller->Allocator, StringAddr, sizeof(struct MyString), &MyString);
        MyString.Data.DataPtr = StringAddr;
        size_t CharsWritten =
                storeData(Controller->Allocator, MyString.Data.DataPtr, StringLength + 1, String);
        if (CharsWritten < StringLength) {
            perror("Error while writing string");
        }
    }
    return MyString;
}

static size_t getAttributeDescriptionNumber(const struct CreateGraphRequest *const Request) {
    size_t AttributeDescriptionNumber = 0;
    const struct ExternalAttributeDescription *CurrentAttributeDescription =
            (const struct ExternalAttributeDescription *)Request->AttributesDescription;
    while (CurrentAttributeDescription != NULL) {
        AttributeDescriptionNumber++;
        CurrentAttributeDescription =
                (const struct ExternalAttributeDescription *)CurrentAttributeDescription->Next;
    }
    return AttributeDescriptionNumber;
}

static struct AttributeDescription
attributeDescriptionFromExternal(struct StorageController *const Controller,
                                 const struct ExternalAttributeDescription *const External) {
    struct AttributeDescription Description;
    Description.Type = External->Type;
    Description.Name = createString(Controller, External->Name);
    Description.AttributeId = External->AttributeId;
    Description.Next = NULL_FULL_ADDR;
    return Description;
}

static void writeGraphAttributesDescription(struct StorageController *const Controller,
                                            const struct AddrInfo FirstAttributeAddr,
                                            const struct CreateGraphRequest *const Request) {
    const struct ExternalAttributeDescription *CurrentExternal =
            (const struct ExternalAttributeDescription *)Request->AttributesDescription;
    struct AddrInfo CurrentAttributeAddr = FirstAttributeAddr;
    do {
        struct AttributeDescription CurrentAttributeDescription =
                attributeDescriptionFromExternal(Controller, CurrentExternal);
        if (CurrentExternal->Next != NULL) {
            CurrentAttributeDescription.Next = getOptionalFullAddr(
                    CurrentAttributeAddr.BlockOffset,
                    CurrentAttributeAddr.DataOffset + sizeof(struct AttributeDescription));
        }
        storeData(Controller->Allocator, CurrentAttributeAddr,
                  sizeof(struct AttributeDescription), &CurrentAttributeDescription);
        CurrentAttributeAddr = CurrentAttributeDescription.Next;
        CurrentExternal = (const struct ExternalAttributeDescription *)CurrentExternal->Next;
    } while (CurrentExternal != NULL);
}

size_t createGraph(struct StorageController *const Controller,
                   const struct CreateGraphRequest *const Request) {
    size_t Id = Controller->Storage.NextGraphId;
    const size_t AttributeDescriptionNumber = getAttributeDescriptionNumber(Request);
    const size_t NodeSize =
            sizeof(struct Node) + sizeof(struct Attribute) * AttributeDescriptionNumber;
    const size_t GraphSize =
            sizeof(struct Graph) + sizeof(struct AttributeDescription) * AttributeDescriptionNumber;
    const size_t BlockNodesSize = GRAPH_NODES_PER_BLOCK * NodeSize;
    const size_t BlockLinksSize = GRAPH_LINKS_PER_BLOCK * sizeof(struct NodeLink);
    const size_t GraphAndDataSize = BlockNodesSize + BlockLinksSize + GraphSize;
    struct AddrInfo GraphAddr = allocate(Controller->Allocator, GraphAndDataSize);
    struct Graph *Graph = malloc(GraphSize);
    Graph->Id = Id;
    Graph->NodeCounter = 0;
    Graph->LinkCounter = 0;
    Graph->AttributeCounter = AttributeDescriptionNumber;
    Graph->AttributesDecription =
            getOptionalFullAddr(GraphAddr.BlockOffset, GraphAddr.DataOffset + sizeof(struct Graph));
    if (Request->AttributesDescription != NULL) {
        writeGraphAttributesDescription(Controller, Graph->AttributesDecription, Request);
    } else {
        Graph->AttributesDecription = NULL_FULL_ADDR;
    }
    Graph->Name = createString(Controller, Request->Name);
    Graph->Nodes = getOptionalFullAddr(GraphAddr.BlockOffset, GraphAddr.DataOffset + GraphSize);
    Graph->Nodes.HasValue = false;
    Graph->LastNode = Graph->Nodes;
    Graph->Links = getOptionalFullAddr(GraphAddr.BlockOffset,
                                       GraphAddr.DataOffset + GraphSize + BlockNodesSize);
    Graph->Links.HasValue = false;
    Graph->LastLink = Graph->Links;
    Graph->Next = NULL_FULL_ADDR;
    Graph->Previous = Controller->Storage.LastGraph;
    Graph->NodesPlaceable = GRAPH_NODES_PER_BLOCK;
    Graph->LinksPlaceable = GRAPH_LINKS_PER_BLOCK;
    Graph->PlacedNodes = 0;
    Graph->PlacedLinks = 0;
    Graph->LazyDeletedNodeCounter = 0;
    Graph->LazyDeletedLinkCounter = 0;
    storeData(Controller->Allocator, GraphAddr, sizeof(struct Graph), Graph);
    increaseGraphNumber(Controller);
    if (!Controller->Storage.Graphs.HasValue) {
        updateFirstGraph(Controller, GraphAddr);
    }
    updateLastGraph(Controller, GraphAddr);
    size_t GraphId = Graph->Id;
    free(Graph);
    return GraphId;
}

static struct AddrInfo getNewNodeAddr(struct StorageController *const Controller,
                                      struct Graph *const Graph) {
    if (Graph->NodesPlaceable > 0) {
        if (!isOptionalFullAddrsEq(Graph->LastNode, NULL_FULL_ADDR) &&
            !Graph->LastNode.HasValue) {
            Graph->LastNode.HasValue = true;
            return Graph->LastNode;
        }
        if (Graph->LastNode.HasValue) {
            struct Node LastNode;
            fetchData(Controller->Allocator, Graph->LastNode, sizeof(LastNode), &LastNode);
            if (LastNode.Next.HasValue) {
                return LastNode.Next;
            }
        }
        return NULL_FULL_ADDR;
    }
    const size_t NodesBlockSize =
            (sizeof(struct Node) + sizeof(struct Attribute) * Graph->AttributeCounter) *
            GRAPH_NODES_PER_BLOCK;
    struct Node LastNode;
    fetchData(Controller->Allocator, Graph->LastNode, sizeof(LastNode), &LastNode);
    const struct AddrInfo NewBlockAddr =
            allocate(Controller->Allocator, NodesBlockSize);
    Graph->NodesPlaceable = GRAPH_NODES_PER_BLOCK;
    LastNode.Next = NewBlockAddr;
    storeData(Controller->Allocator, Graph->LastNode, sizeof(LastNode), &LastNode);
    return NewBlockAddr;
}

static size_t createNodeByGraphAddr(struct StorageController *const Controller,
                                    const struct AddrInfo Addr,
                                    struct ExternalAttribute *Attributes) {
    struct Graph Graph;
    fetchData(Controller->Allocator, Addr, sizeof(Graph), &Graph);
    struct Node NewNode;
    const struct AddrInfo NewNodeAddr = getNewNodeAddr(Controller, &Graph);
    const size_t AttributesSize = sizeof(struct Attribute) * Graph.AttributeCounter;
    struct Attribute *AttributesToStore = malloc(AttributesSize);
    const struct AddrInfo AttributesAddr = getOptionalFullAddr(
            NewNodeAddr.BlockOffset, NewNodeAddr.DataOffset + sizeof(struct Node));
    struct AttributeDescription *GraphAttributesDescription =
            malloc(sizeof(struct AttributeDescription) * Graph.AttributeCounter);
    fetchData(Controller->Allocator, Graph.AttributesDecription,
              sizeof(struct AttributeDescription) * Graph.AttributeCounter,
              GraphAttributesDescription);
    for (size_t i = 0; i < Graph.AttributeCounter; ++i) {
        size_t AttributeId = Attributes[i].Id;
        if (Attributes[i].Type != GraphAttributesDescription[AttributeId].Type) {
            return 0;
        }
    }
    for (size_t i = 0; i < Graph.AttributeCounter; ++i) {
        struct Attribute Attribute;
        Attribute.Id = Attributes[i].Id;
        if (i != Graph.AttributeCounter - 1) {
            Attribute.Next =
                    getOptionalFullAddr(AttributesAddr.BlockOffset,
                                        AttributesAddr.DataOffset + (i + 1) * sizeof(Attribute));
        } else {
            Attribute.Next = NULL_FULL_ADDR;
        }
        Attribute.Type = Attributes[i].Type;
        if (Attribute.Type == INT) {
            Attribute.Value.IntValue = Attributes[i].Value.IntValue;
        }
        if (Attribute.Type == FLOAT) {
            Attribute.Value.FloatValue = Attributes[i].Value.FloatValue;
        }
        if (Attribute.Type == BOOL) {
            Attribute.Value.BoolValue = Attributes[i].Value.BoolValue;
        }
        if (Attribute.Type == STRING) {
            Attribute.Value.StringValue =
                    createString(Controller, Attributes[i].Value.StringAddr);
        }
        AttributesToStore[i] = Attribute;
    }
    storeData(Controller->Allocator, AttributesAddr, AttributesSize, AttributesToStore);
    Graph.NodesPlaceable -= 1;
    Graph.PlacedNodes += 1;
    NewNode.Id = Controller->Storage.NextNodeId;
    NewNode.Attributes = AttributesAddr;
    if (Graph.NodesPlaceable > 0) {
        const size_t FullNodeSize =
                sizeof(struct Node) + Graph.AttributeCounter * sizeof(struct Attribute);
        NewNode.Next =
                getOptionalFullAddr(NewNodeAddr.BlockOffset, NewNodeAddr.DataOffset + FullNodeSize);
    } else {
        NewNode.Next = NULL_FULL_ADDR;
    }
    NewNode.Deleted = false;
    increaseNodeNumber(Controller);
    Graph.NodeCounter += 1;
    if (!Graph.Nodes.HasValue) {
        Graph.Nodes = Graph.LastNode = NewNodeAddr;
        storeData(Controller->Allocator, Addr, sizeof(Graph), &Graph);
        NewNode.Previous = NULL_FULL_ADDR;
    } else {
        struct Node OldLastNode;
        fetchData(Controller->Allocator, Graph.LastNode, sizeof(OldLastNode), &OldLastNode);
        OldLastNode.Next = NewNodeAddr;
        storeData(Controller->Allocator, Graph.LastNode, sizeof(OldLastNode), &OldLastNode);
        NewNode.Previous = Graph.LastNode;
        Graph.LastNode = NewNodeAddr;
    }
    storeData(Controller->Allocator, Addr, sizeof(Graph), &Graph);
    storeData(Controller->Allocator, NewNodeAddr, sizeof(NewNode), &NewNode);

    free(AttributesToStore);
    free(GraphAttributesDescription);
    return NewNode.Id;
}

size_t createNode(struct StorageController *const Controller,
                  const struct CreateNodeRequest *const Request) {
    struct AddrInfo GraphAddr =
            findGraphAddrByGraphId(Controller, Request->GraphIdType, Request->GraphId);
    return createNodeByGraphAddr(Controller, GraphAddr, Request->Attributes);
}

static struct AddrInfo getNewLinkAddr(struct StorageController *const Controller,
                                      struct Graph *const Graph) {
    if (Graph->LinksPlaceable > 0) {
        if (!isOptionalFullAddrsEq(Graph->LastLink, NULL_FULL_ADDR) &&
            !Graph->LastLink.HasValue) {
            Graph->LastLink.HasValue = true;
            return Graph->LastLink;
        }
        if (Graph->LastLink.HasValue) {
            struct NodeLink LastLink;
            fetchData(Controller->Allocator, Graph->LastLink, sizeof(LastLink), &LastLink);
            if (LastLink.Next.HasValue) {
                return LastLink.Next;
            }
        }
        return NULL_FULL_ADDR;
    }
    const size_t LinkBlockSize = sizeof(struct NodeLink) * GRAPH_LINKS_PER_BLOCK;
    struct NodeLink LastLink;
    fetchData(Controller->Allocator, Graph->LastLink, sizeof(LastLink), &LastLink);
    const struct AddrInfo NewBlockAddr = allocate(Controller->Allocator, LinkBlockSize);
    Graph->LinksPlaceable = GRAPH_NODES_PER_BLOCK;
    LastLink.Next = NewBlockAddr;
    storeData(Controller->Allocator, Graph->LastLink, sizeof(LastLink), &LastLink);
    return NewBlockAddr;
}

static size_t createNodeLinkByGraphAddr(struct StorageController *const Controller,
                                        struct AddrInfo GraphAddr,
                                        const struct CreateNodeLinkRequest *const Request) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    struct AddrInfo NewLinkAddr = getNewLinkAddr(Controller, &Graph);
    struct NodeLink NewLink;
    Graph.LinksPlaceable -= 1;
    Graph.PlacedLinks += 1;
    NewLink.Id = Controller->Storage.NextNodeLinkId;
    NewLink.Type = Request->Type;
    if (Graph.LinksPlaceable > 0) {
        NewLink.Next = getOptionalFullAddr(NewLinkAddr.BlockOffset,
                                           NewLinkAddr.DataOffset + sizeof(NewLink));
    } else {
        NewLink.Next = NULL_FULL_ADDR;
    }
    if (!Graph.Links.HasValue) {
        Graph.Links = Graph.LastLink = NewLinkAddr;
        storeData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
        NewLink.Previous = NULL_FULL_ADDR;
    } else {
        struct NodeLink OldLastLink;
        fetchData(Controller->Allocator, Graph.LastLink, sizeof(OldLastLink), &OldLastLink);
        OldLastLink.Next = NewLinkAddr;
        storeData(Controller->Allocator, Graph.LastLink, sizeof(OldLastLink), &OldLastLink);
        NewLink.Previous = Graph.LastLink;
        Graph.LastLink = NewLinkAddr;
        storeData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    }
    NewLink.LeftNodeId = Request->LeftNodeId;
    NewLink.RightNodeId = Request->RightNodeId;
    NewLink.Weight = Request->Weight;
    NewLink.Deleted = false;
    if (Graph.LastLink.HasValue) {
        struct NodeLink OldLast;
        fetchData(Controller->Allocator, Graph.LastLink, sizeof(OldLast), &OldLast);
        OldLast.Next = NewLinkAddr;
        storeData(Controller->Allocator, Graph.LastLink, sizeof(OldLast), &OldLast);
    }
    storeData(Controller->Allocator, NewLinkAddr, sizeof(NewLink), &NewLink);
    Graph.LastLink = NewLinkAddr;
    Graph.LinkCounter += 1;
    if (!Graph.Links.HasValue) {
        Graph.Links = NewLinkAddr;
    }
    storeData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    increaseNodeLinkNumber(Controller);
    return NewLink.Id;
}

size_t createNodeLink(struct StorageController *const Controller,
                      const struct CreateNodeLinkRequest *const Request) {
    struct AddrInfo GraphAddr =
            findGraphAddrByGraphId(Controller, Request->GraphIdType, Request->GraphId);
    return createNodeLinkByGraphAddr(Controller, GraphAddr, Request);
}

void deleteString(const struct StorageController *const Controller,
                  const struct MyString String) {
    if (String.Length > SMALL_STRING_LIMIT) {
        deallocate(Controller->Allocator, String.Data.DataPtr);
    }
}

static void supressLinksEnd(const struct StorageController *const Controller,
                            struct AddrInfo Addr, struct Graph *Graph,
                            struct NodeLink *ToDelete) {
    struct NodeLink LinkC = *ToDelete;
    struct AddrInfo CurrAddr = Addr;
    while (LinkC.Deleted) {
        struct AddrInfo PAddr = LinkC.Previous;
        if (isOptionalFullAddrsEq(PAddr, NULL_FULL_ADDR)) {
            Graph->Links = NULL_FULL_ADDR;
            Graph->LastLink = CurrAddr;
            Graph->LastLink.HasValue = false;
            break;
        }
        Graph->PlacedLinks -= 1;
        Graph->LazyDeletedLinkCounter -= 1;
        Graph->LinksPlaceable += 1;
        struct NodeLink LinkP;
        fetchData(Controller->Allocator, PAddr, sizeof(LinkP), &LinkP);
        if (inDifferentBlocks(CurrAddr, PAddr)) {
            deallocate(Controller->Allocator, CurrAddr);
            Graph->LinksPlaceable = 0;
            LinkP.Next = NULL_FULL_ADDR;
            storeData(Controller->Allocator, PAddr, sizeof(LinkP), &LinkP);
        }
        Graph->LastLink = PAddr;
        LinkC = LinkP;
        CurrAddr = PAddr;
    }
}

static void vacuumateLinks(const struct StorageController *const Controller,
                           const struct AddrInfo GraphAddr) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    struct AddrInfo SpaceAddr = Graph.Links;
    if (!SpaceAddr.HasValue) {
        return;
    }
    while (Graph.LazyDeletedLinkCounter != 0) {
        struct NodeLink Space;
        fetchData(Controller->Allocator, SpaceAddr, sizeof(Space), &Space);
        while (!Space.Deleted && Space.Next.HasValue &&
               !isOptionalFullAddrsEq(SpaceAddr, Graph.LastLink)) {
            SpaceAddr = Space.Next;
            fetchData(Controller->Allocator, SpaceAddr, sizeof(Space), &Space);
        }
        if (!Space.Deleted) {
            break;
        }
        struct AddrInfo LoadAddr = SpaceAddr;
        struct NodeLink Load;
        fetchData(Controller->Allocator, LoadAddr, sizeof(struct Node), &Load);
        while (Load.Deleted && Load.Next.HasValue &&
               !isOptionalFullAddrsEq(LoadAddr, Graph.LastLink)) {
            LoadAddr = Load.Next;
            fetchData(Controller->Allocator, LoadAddr, sizeof(struct Node), &Load);
        }
        if (Load.Deleted && isOptionalFullAddrsEq(Graph.LastLink, LoadAddr)) {
            supressLinksEnd(Controller, LoadAddr, &Graph, &Load);
            break;
        }
        fetchData(Controller->Allocator, LoadAddr, sizeof(Load), &Load);
        Load.Deleted = true;
        storeData(Controller->Allocator, LoadAddr, sizeof(Load), &Load);
        Load.Next = Space.Next;
        Load.Previous = Space.Previous;
        Load.Deleted = false;
        storeData(Controller->Allocator, SpaceAddr, sizeof(Load), &Load);
        SpaceAddr = Space.Next;
    }
    storeData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
}

static void deleteSingleNodeLink(const struct StorageController *const Controller,
                                 const struct AddrInfo Addr,
                                 const struct AddrInfo GraphAddr) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    struct NodeLink ToDelete;
    fetchData(Controller->Allocator, Addr, sizeof(ToDelete), &ToDelete);
    Graph.LinkCounter -= 1;
    ToDelete.Deleted = true;
    Graph.LazyDeletedLinkCounter += 1;
    storeData(Controller->Allocator, Addr, sizeof(ToDelete), &ToDelete);
    if (isOptionalFullAddrsEq(Graph.LastLink, Addr)) {
        if (isOptionalFullAddrsEq(ToDelete.Previous, NULL_FULL_ADDR)) {
            Graph.Links = NULL_FULL_ADDR;
        } else {
            Graph.LastLink = ToDelete.Previous;
        }
        supressLinksEnd(Controller, Addr, &Graph, &ToDelete);
    }
    storeData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
}

static size_t deleteNodeLinksByNodeId(const struct StorageController *const Controller,
                                      const struct AddrInfo GraphAddr,
                                      const size_t NodeId, bool CheckLeft, bool CheckRight) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    struct AddrInfo NodeLinkAddr = Graph.Links;
    size_t deleted = 0;
    while (NodeLinkAddr.HasValue) {
        struct NodeLink Link;
        fetchData(Controller->Allocator, NodeLinkAddr, sizeof(Link), &Link);
        if (!Link.Deleted) {
            struct AddrInfo OldAddr = NodeLinkAddr;
            if ((CheckLeft && Link.LeftNodeId == NodeId) ||
                (CheckRight && Link.RightNodeId == NodeId)) {
                deleteSingleNodeLink(Controller, OldAddr, GraphAddr);
                deleted++;
                if (isOptionalFullAddrsEq(OldAddr, Graph.LastLink)) {
                    break;
                }
                fetchData(Controller->Allocator, NodeLinkAddr, sizeof(Link), &Link);
                fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
            } else if (isOptionalFullAddrsEq(OldAddr, Graph.LastLink)) {
                break;
            }
        }
        NodeLinkAddr = Link.Next;
    }
    if (Graph.LazyDeletedLinkCounter > Graph.PlacedLinks / 2) {
        vacuumateLinks(Controller, GraphAddr);
    }
    return deleted;
}

static void supressNodeEnd(const struct StorageController *const Controller,
                           struct AddrInfo Addr, struct Graph *Graph,
                           struct Node *ToDelete) {
    struct Node NodeC = *ToDelete;
    struct AddrInfo CAddr = Addr;
    while (NodeC.Deleted) {
        struct AddrInfo PAddr = NodeC.Previous;
        if (isOptionalFullAddrsEq(PAddr, NULL_FULL_ADDR)) {
            Graph->Nodes = NULL_FULL_ADDR;
            Graph->LastNode = CAddr;
            Graph->LastNode.HasValue = false;
            break;
        }
        Graph->PlacedNodes -= 1;
        Graph->LazyDeletedNodeCounter -= 1;
        Graph->NodesPlaceable += 1;
        struct Node NodeP;
        fetchData(Controller->Allocator, PAddr, sizeof(NodeP), &NodeP);
        if (inDifferentBlocks(CAddr, PAddr)) {
            deallocate(Controller->Allocator, CAddr);
            Graph->NodesPlaceable = 0;
            NodeP.Next = NULL_FULL_ADDR;
            storeData(Controller->Allocator, PAddr, sizeof(NodeP), &NodeP);
        }
        Graph->LastNode = PAddr;
        NodeC = NodeP;
        CAddr = PAddr;
    }
}

static void vacuumateNodes(const struct StorageController *const Controller,
                           const struct AddrInfo GraphAddr) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    struct AddrInfo SpaceAddr = Graph.Nodes;
    if (!SpaceAddr.HasValue) {
        return;
    }
    size_t NodeSize = sizeof(struct Node) + sizeof(struct Attribute) * Graph.AttributeCounter;
    while (Graph.LazyDeletedNodeCounter != 0) {
        struct Node Space;
        fetchData(Controller->Allocator, SpaceAddr, sizeof(Space), &Space);
        while (!Space.Deleted && Space.Next.HasValue &&
               !isOptionalFullAddrsEq(SpaceAddr, Graph.LastNode)) {
            SpaceAddr = Space.Next;
            fetchData(Controller->Allocator, SpaceAddr, sizeof(Space), &Space);
        }
        if (!Space.Deleted) {
            break;
        }
        struct AddrInfo LoadAddr = SpaceAddr;
        struct Node *Load = malloc(NodeSize);
        fetchData(Controller->Allocator, LoadAddr, sizeof(struct Node), Load);
        while (Load->Deleted && Load->Next.HasValue &&
               !isOptionalFullAddrsEq(LoadAddr, Graph.LastNode)) {
            LoadAddr = Load->Next;
            fetchData(Controller->Allocator, LoadAddr, sizeof(struct Node), Load);
        }
        if (Load->Deleted && isOptionalFullAddrsEq(Graph.LastNode, LoadAddr)) {
            supressNodeEnd(Controller, LoadAddr, &Graph, Load);
            free(Load);
            break;
        }
        fetchData(Controller->Allocator, LoadAddr, NodeSize, Load);
        Load->Deleted = true;
        storeData(Controller->Allocator, LoadAddr, NodeSize, Load);
        Load->Next = Space.Next;
        Load->Previous = Space.Previous;
        Load->Deleted = false;
        storeData(Controller->Allocator, SpaceAddr, NodeSize, Load);
        SpaceAddr = Space.Next;
        free(Load);
    }
    storeData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
}

static size_t deleteSingleNode(const struct StorageController *const Controller,
                               const struct AddrInfo Addr,
                               const struct AddrInfo GraphAddr) {
    struct Node ToDelete;
    struct Graph Graph;
    fetchData(Controller->Allocator, Addr, sizeof(ToDelete), &ToDelete);
    deleteNodeLinksByNodeId(Controller, GraphAddr, ToDelete.Id, true, true);
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    Graph.NodeCounter -= 1;
    size_t AttributesSize = sizeof(struct Attribute) * Graph.AttributeCounter;
    struct Attribute *Attributes = malloc(AttributesSize);
    fetchData(Controller->Allocator, ToDelete.Attributes, AttributesSize, Attributes);
    for (size_t i = 0; i < Graph.AttributeCounter; ++i) {
        if (Attributes[i].Type == STRING) {
            deleteString(Controller, Attributes[i].Value.StringValue);
        }
    }
    free(Attributes);
    ToDelete.Deleted = true;
    Graph.LazyDeletedNodeCounter += 1;
    storeData(Controller->Allocator, Addr, sizeof(ToDelete), &ToDelete);
    if (isOptionalFullAddrsEq(Graph.LastNode, Addr)) {
        if (isOptionalFullAddrsEq(ToDelete.Previous, NULL_FULL_ADDR)) {
            Graph.Nodes = NULL_FULL_ADDR;
        } else {
            Graph.LastNode = ToDelete.Previous;
        }
        supressNodeEnd(Controller, Addr, &Graph, &ToDelete);
    }
    storeData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    return 1;
}

static void deleteAllNodes(const struct StorageController *const Controller,
                           const struct AddrInfo GraphAddr) {
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    const struct AddrInfo StartAddr = Graph.Nodes;
    if (!StartAddr.HasValue) {
        return;
    }
    struct AddrInfo NodeAddr = StartAddr;
    while (NodeAddr.HasValue) {
        struct Node CurrentNode;
        fetchData(Controller->Allocator, NodeAddr, sizeof(CurrentNode), &CurrentNode);
        struct AddrInfo OldAddr = NodeAddr;
        deleteSingleNode(Controller, OldAddr, GraphAddr);
        if (isOptionalFullAddrsEq(Graph.LastNode, NodeAddr))
            break;
        NodeAddr = CurrentNode.Next;
    }
}

size_t deleteGraph(struct StorageController *const Controller,
                   const struct DeleteGraphRequest *const Request) {
    struct AddrInfo GraphAddr = findGraphAddrByName(Controller, Request->Name);
    struct Graph ToDelete;
    struct Graph BeforeDeleted;
    struct Graph AfterDeleted;
    fetchData(Controller->Allocator, GraphAddr, sizeof(ToDelete), &ToDelete);
    if (ToDelete.Previous.HasValue) {
        fetchData(Controller->Allocator, ToDelete.Previous, sizeof(BeforeDeleted),
                  &BeforeDeleted);
        BeforeDeleted.Next = ToDelete.Next;
        storeData(Controller->Allocator, ToDelete.Previous, sizeof(BeforeDeleted),
                  &BeforeDeleted);
    }
    if (ToDelete.Next.HasValue) {
        fetchData(Controller->Allocator, ToDelete.Next, sizeof(AfterDeleted), &AfterDeleted);
        AfterDeleted.Previous = ToDelete.Previous;
        storeData(Controller->Allocator, ToDelete.Next, sizeof(AfterDeleted), &AfterDeleted);
    }
    if (ToDelete.AttributesDecription.HasValue) {
        struct AttributeDescription *GraphAttributeDescriptions =
                malloc(ToDelete.AttributeCounter * sizeof(struct AttributeDescription));
        fetchData(Controller->Allocator, ToDelete.AttributesDecription,
                  ToDelete.AttributeCounter * sizeof(struct AttributeDescription),
                  GraphAttributeDescriptions);
        for (size_t i = 0; i < ToDelete.AttributeCounter; ++i) {
            deleteString(Controller, GraphAttributeDescriptions[i].Name);
        }
        free(GraphAttributeDescriptions);
    }
    deleteAllNodes(Controller, GraphAddr);
    deleteString(Controller, ToDelete.Name);
    deallocate(Controller->Allocator, GraphAddr);
    decreaseGraphNumber(Controller);
    if (isOptionalFullAddrsEq(GraphAddr, Controller->Storage.Graphs)) {
        updateFirstGraph(Controller, ToDelete.Next);
    }
    if (isOptionalFullAddrsEq(GraphAddr, Controller->Storage.LastGraph)) {
        updateLastGraph(Controller, ToDelete.Previous);
    }
    return 1;
}

size_t deleteNode(const struct StorageController *const Controller,
                  const struct DeleteNodeRequest *const Request) {
    struct AddrInfo GraphAddr =
            findGraphAddrByGraphId(Controller, Request->GraphIdType, Request->GraphId);
    if (Request->ById) {
        struct AddrInfo NodeAddr = findNodeAddrById(Controller, GraphAddr, Request->Id);
        deleteSingleNode(Controller, NodeAddr, GraphAddr);
        return 1;
    }
    struct AddrInfo *NodesToDelete;
    const size_t NodesCnt = findNodesByFilters(Controller, GraphAddr,
                                               Request->AttributesFilterChain, &NodesToDelete);
    for (size_t i = NodesCnt; i != 0; --i) {
        deleteSingleNode(Controller, NodesToDelete[i - 1], GraphAddr);
    }
    free(NodesToDelete);
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    if (Graph.LazyDeletedNodeCounter > Graph.PlacedNodes / 2 + 1) {
        vacuumateNodes(Controller, GraphAddr);
    }
    return NodesCnt;
}

size_t deleteNodeLink(const struct StorageController *const Controller,
                      const struct DeleteNodeLinkRequest *const Request) {
    struct AddrInfo GraphAddr =
            findGraphAddrByGraphId(Controller, Request->GraphIdType, Request->GraphId);
    size_t ret = 0;
    if (Request->Type == BY_LEFT_NODE_ID) {
        ret = deleteNodeLinksByNodeId(Controller, GraphAddr, Request->Id, true, false);
    }
    if (Request->Type == BY_RIGHT_NODE_ID) {
        ret = deleteNodeLinksByNodeId(Controller, GraphAddr, Request->Id, false, true);
    }
    if (Request->Type == BY_ID) {
        struct AddrInfo NodeLinkAddr =
                findNodeLinkAddrById(Controller, GraphAddr, Request->Id);
        deleteSingleNodeLink(Controller, NodeLinkAddr, GraphAddr);
        ret = 1;
        struct Graph Graph;
        fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
        if (Graph.LazyDeletedLinkCounter > Graph.PlacedLinks / 2) {
            vacuumateLinks(Controller, GraphAddr);
        }
    }
    return ret;
}

static void updateSingleNode(const struct StorageController *const Controller,
                             const struct AddrInfo NodeAddr,
                             size_t UpdatedAttributesNumber,
                             struct ExternalAttribute *NewAttributes, struct Graph *Graph) {
    const size_t AttributesSize = sizeof(struct Attribute) * Graph->AttributeCounter;
    struct Attribute *Attributes = malloc(AttributesSize);
    struct Node ToUpdate;
    fetchData(Controller->Allocator, NodeAddr, sizeof(ToUpdate), &ToUpdate);
    fetchData(Controller->Allocator, ToUpdate.Attributes, AttributesSize, Attributes);
    for (size_t i = 0; i < UpdatedAttributesNumber; ++i) {
        const size_t AttrId = NewAttributes[i].Id;
        if (Attributes[AttrId].Type == INT) {
            Attributes[AttrId].Value.IntValue = NewAttributes[i].Value.IntValue;
            continue;
        }
        if (Attributes[AttrId].Type == FLOAT) {
            Attributes[AttrId].Value.FloatValue = NewAttributes[i].Value.FloatValue;
            continue;
        }
        if (Attributes[AttrId].Type == BOOL) {
            Attributes[AttrId].Value.BoolValue = NewAttributes[i].Value.BoolValue;
            continue;
        }
        if (Attributes[AttrId].Type == STRING && NewAttributes[i].Type == STRING) {
            struct MyString NewString =
                    createString(Controller, NewAttributes[i].Value.StringAddr);
            deleteString(Controller, Attributes[AttrId].Value.StringValue);
            Attributes[AttrId].Value.StringValue = NewString;
        }
    }
    storeData(Controller->Allocator, ToUpdate.Attributes, AttributesSize, Attributes);
    free(Attributes);
}

size_t updateNode(const struct StorageController *const Controller,
                  const struct UpdateNodeRequest *const Request) {
    const struct AddrInfo GraphAddr =
            findGraphAddrByGraphId(Controller, Request->GraphIdType, Request->GraphId);
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    if (Request->ById) {
        const struct AddrInfo NodeAddr =
                findNodeAddrById(Controller, GraphAddr, Request->Id);
        if (NodeAddr.HasValue) {
            updateSingleNode(Controller, NodeAddr, Request->UpdatedAttributesNumber,
                             Request->Attributes, &Graph);
            return 1;
        }
        return 0;
    }
    struct AddrInfo *NodesToUpdate;
    size_t NodesToUpdateCnt = findNodesByFilters(
            Controller, GraphAddr, Request->AttributesFilterChain, &NodesToUpdate);
    for (size_t i = 0; i < NodesToUpdateCnt; ++i) {
        const struct AddrInfo NodeAddr = NodesToUpdate[i];
        updateSingleNode(Controller, NodeAddr, Request->UpdatedAttributesNumber,
                         Request->Attributes, &Graph);
    }
    free(NodesToUpdate);
    return NodesToUpdateCnt;
}

size_t updateNodeLink(const struct StorageController *const Controller,
                      const struct UpdateNodeLinkRequest *const Request) {
    if (!Request->UpdateType && !Request->UpdateWeight) {
        return 0;
    }
    const struct AddrInfo GraphAddr =
            findGraphAddrByGraphId(Controller, Request->GraphIdType, Request->GraphId);
    const struct AddrInfo NodeLinkAddr =
            findNodeLinkAddrById(Controller, GraphAddr, Request->Id);
    struct NodeLink ToUpdate;
    fetchData(Controller->Allocator, NodeLinkAddr, sizeof(ToUpdate), &ToUpdate);
    ToUpdate.Type = Request->UpdateType ? Request->Type : ToUpdate.Type;
    ToUpdate.Weight = Request->UpdateWeight ? Request->Weight : ToUpdate.Weight;
    storeData(Controller->Allocator, NodeLinkAddr, sizeof(ToUpdate), &ToUpdate);
    return 1;
}

struct NodeResultSet {
    const struct StorageController *Controller;
    struct AddrInfo GraphAddr;
    size_t Cnt;
    size_t Index;
    struct AddrInfo *NodeAddrs;
};

struct NodeLinkResultSet {
    const struct StorageController *Controller;
    size_t Cnt;
    size_t Index;
    struct AddrInfo *LinkAddrs;
};

struct GraphResultSet {
    const struct StorageController *Controller;
    size_t Cnt;
    size_t Index;
    struct AddrInfo *GraphAddrs;
};

static void getExternalNodeLink(const struct StorageController *const Controller,
                                const struct AddrInfo NodeLinkAddr,
                                struct ExternalNodeLink **Result) {
    struct NodeLink NodeLink;
    fetchData(Controller->Allocator, NodeLinkAddr, sizeof(NodeLink), &NodeLink);
    *Result = malloc(sizeof(struct ExternalNodeLink));
    (*Result)->Id = NodeLink.Id;
    (*Result)->Type = NodeLink.Type;
    (*Result)->Weight = NodeLink.Weight;
    (*Result)->LeftNodeId = NodeLink.LeftNodeId;
    (*Result)->RightNodeId = NodeLink.RightNodeId;
}

static size_t getNodeStringsSize(const struct Attribute *const Attributes,
                                 const size_t AttributeNumber) {
    size_t Result = 0;
    for (size_t i = 0; i < AttributeNumber; ++i) {
        if (Attributes[i].Type == STRING) {
            Result += Attributes[i].Value.StringValue.Length + 1;
        }
    }
    return Result;
}

static void getExternalNode(const struct StorageController *const Controller,
                            const struct AddrInfo NodeAddr,
                            const struct AddrInfo GraphAddr,
                            struct ExternalNode **Result) {
    struct Node Node;
    fetchData(Controller->Allocator, NodeAddr, sizeof(Node), &Node);
    struct Graph Graph;
    fetchData(Controller->Allocator, GraphAddr, sizeof(Graph), &Graph);
    const size_t AttributesSize = sizeof(struct Attribute) * Graph.AttributeCounter;
    struct Attribute *Attributes = malloc(AttributesSize);
    fetchData(Controller->Allocator, Node.Attributes, AttributesSize, Attributes);
    size_t ResultSize = sizeof(struct ExternalNode) +
                        sizeof(struct ExternalAttribute) * Graph.AttributeCounter +
                        getNodeStringsSize(Attributes, Graph.AttributeCounter);
    *Result = malloc(ResultSize);
    struct ExternalAttribute *ExternalAttributes =
            (struct ExternalAttribute *)((char *)*Result + sizeof(struct ExternalNode));
    char *Strings =
            (char *)ExternalAttributes + Graph.AttributeCounter * sizeof(struct ExternalAttribute);
    for (size_t i = 0; i < Graph.AttributeCounter; ++i) {
        ExternalAttributes[i].Id = Attributes[i].Id;
        ExternalAttributes[i].Type = Attributes[i].Type;
        if (Attributes[i].Type == BOOL) {
            ExternalAttributes[i].Value.BoolValue = Attributes[i].Value.BoolValue;
        } else if (Attributes[i].Type == INT) {
            ExternalAttributes[i].Value.IntValue = Attributes[i].Value.IntValue;
        }
        if (Attributes[i].Type == FLOAT) {
            ExternalAttributes[i].Value.FloatValue = Attributes[i].Value.FloatValue;
        } else if (Attributes[i].Type == STRING) {
            struct MyString String = Attributes[i].Value.StringValue;
            if (String.Length > SMALL_STRING_LIMIT) {
                fetchData(Controller->Allocator, String.Data.DataPtr, String.Length, Strings);
            } else {
                memcpy(Strings, String.Data.InlinedData, String.Length);
            }
            Strings[String.Length] = 0;
            ExternalAttributes[i].Value.StringAddr = Strings;
            Strings = Strings + String.Length + 1;
        }
    }
    (**Result).Attributes = ExternalAttributes;
    (**Result).AttributesNumber = Graph.AttributeCounter;
    (**Result).Id = Node.Id;
    free(Attributes);
}

static size_t getGraphStringsSize(const struct StorageController *const Controller,
                                  const struct AttributeDescription *const Descriptions,
                                  const struct Graph *const Graph) {
    struct MyString String = Graph->Name;
    size_t Result = 0;
    Result += String.Length + 1;
    for (size_t i = 0; i < Graph->AttributeCounter; ++i) {
        Result += Descriptions[i].Name.Length + 1;
    }
    return Result;
}

static void getExternalGraph(const struct StorageController *const Controller,
                             const struct AddrInfo Addr,
                             struct ExternalGraph **Result) {
    struct Graph Graph;
    fetchData(Controller->Allocator, Addr, sizeof(Graph), &Graph);
    const size_t AttributesDescriptionsSize =
            sizeof(struct AttributeDescription) * Graph.AttributeCounter;
    struct AttributeDescription *AttributesDescriptions = malloc(AttributesDescriptionsSize);
    fetchData(Controller->Allocator, Graph.AttributesDecription, AttributesDescriptionsSize,
              AttributesDescriptions);
    size_t ExternalGraphSize =
            sizeof(struct ExternalGraph) +
            Graph.AttributeCounter * sizeof(struct ExternalAttributeDescription) +
            getGraphStringsSize(Controller, AttributesDescriptions, &Graph);
    *Result = malloc(ExternalGraphSize);
    struct ExternalAttributeDescription *ExternalDescriptions =
            (struct ExternalAttributeDescription *)((char *)*Result + sizeof(struct ExternalGraph));
    char *Strings = (char *)ExternalDescriptions +
                    Graph.AttributeCounter * sizeof(struct ExternalAttributeDescription);
    (*Result)->Id = Graph.Id;
    (*Result)->LinksNumber = Graph.LinkCounter;
    (*Result)->NodesNumber = Graph.NodeCounter;
    (*Result)->AttributesDescriptionNumber = Graph.AttributeCounter;
    (*Result)->AttributesDescription = ExternalDescriptions;
    (*Result)->Name = Strings;
    struct MyString GraphName = Graph.Name;
    if (GraphName.Length > SMALL_STRING_LIMIT) {
        fetchData(Controller->Allocator, GraphName.Data.DataPtr, GraphName.Length, Strings);
    } else {
        memcpy(Strings, GraphName.Data.InlinedData, GraphName.Length);
    }
    Strings[GraphName.Length] = 0;
    Strings += GraphName.Length + 1;
    for (size_t i = 0; i < Graph.AttributeCounter; ++i) {
        ExternalDescriptions[i].AttributeId = AttributesDescriptions[i].AttributeId;
        ExternalDescriptions[i].Type = AttributesDescriptions[i].Type;
        ExternalDescriptions[i].Next =
                i == Graph.AttributeCounter - 1 ? NULL : ExternalDescriptions + i + 1;
        struct MyString AttrbuteName = AttributesDescriptions[i].Name;
        if (AttrbuteName.Length > SMALL_STRING_LIMIT) {
            fetchData(Controller->Allocator, AttrbuteName.Data.DataPtr, AttrbuteName.Length,
                      Strings);
        } else {
            memcpy(Strings, AttrbuteName.Data.InlinedData, AttrbuteName.Length);
        }
        Strings[AttrbuteName.Length] = 0;
        ExternalDescriptions[i].Name = Strings;
        Strings += AttrbuteName.Length + 1;
    }
    free(AttributesDescriptions);
}

struct NodeResultSet *readNode(const struct StorageController *const Controller,
                               const struct ReadNodeRequest *const Request) {
    struct AddrInfo *Nodes;
    struct AddrInfo GraphAddr =
            findGraphAddrByGraphId(Controller, Request->GraphIdType, Request->GraphId);
    struct NodeResultSet *Result = malloc(sizeof(struct NodeResultSet));
    Result->Cnt =
            findNodesByFilters(Controller, GraphAddr, Request->AttributesFilterChain, &Nodes);
    Result->Index = 0;
    Result->NodeAddrs = Nodes;
    Result->Controller = Controller;
    Result->GraphAddr = GraphAddr;
    return Result;
}

struct NodeLinkResultSet *readNodeLink(const struct StorageController *const Controller,
                                       const struct ReadNodeLinkRequest *const Request) {
    struct AddrInfo *NodeLinks;
    struct AddrInfo GraphAddr =
            findGraphAddrByGraphId(Controller, Request->GraphIdType, Request->GraphId);
    struct NodeLinkResultSet *Result = malloc(sizeof(struct NodeLinkResultSet));
    Result->Cnt =
            findNodeLinksByIdAndType(Controller, GraphAddr, Request->Type, Request->Id, &NodeLinks);
    Result->Controller = Controller;
    Result->Index = 0;
    Result->LinkAddrs = NodeLinks;
    return Result;
}

struct GraphResultSet *readGraph(const struct StorageController *const Controller,
                                 const struct ReadGraphRequest *const Request) {
    struct GraphResultSet *Result = malloc(sizeof(struct GraphResultSet));
    struct AddrInfo *GraphAddr = malloc(sizeof(struct AddrInfo));
    *GraphAddr = findGraphAddrByName(Controller, Request->Name);
    Result->GraphAddrs = GraphAddr;
    Result->Controller = Controller;
    Result->Cnt = 1;
    Result->Index = 0;
    return Result;
}

bool readResultNode(struct NodeResultSet *ResultSet, struct ExternalNode **Node) {
    if (nodeResultSetIsEmpty(ResultSet))
        return false;
    getExternalNode(ResultSet->Controller, ResultSet->NodeAddrs[ResultSet->Index],
                    ResultSet->GraphAddr, Node);
    return true;
}

bool hasNextNode(struct NodeResultSet *ResultSet) {
    return ResultSet->Cnt - ResultSet->Index > 1;
}

bool moveToNextNode(struct NodeResultSet *ResultSet) {
    if (hasNextNode(ResultSet)) {
        ResultSet->Index += 1;
        return true;
    }
    return false;
}

bool hasPreviousNode(struct NodeResultSet *ResultSet) { return ResultSet->Index > 0; }

bool moveToPreviousNode(struct NodeResultSet *ResultSet) {
    if (hasPreviousNode(ResultSet)) {
        ResultSet->Index -= 1;
        return true;
    }
    return false;
}

bool nodeResultSetIsEmpty(struct NodeResultSet *ResultSet) { return ResultSet->Cnt == 0; }

void deleteNodeResultSet(struct NodeResultSet **ReultSet) {
    free((**ReultSet).NodeAddrs);
    free(*ReultSet);
    *ReultSet = NULL;
}

bool readResultNodeLink(struct NodeLinkResultSet *ResultSet,
                        struct ExternalNodeLink **NodeLink) {
    if (nodeLinkResultSetIsEmpty(ResultSet))
        return false;
    getExternalNodeLink(ResultSet->Controller, ResultSet->LinkAddrs[ResultSet->Index],
                        NodeLink);
    return true;
}

bool hasNextNodeLink(struct NodeLinkResultSet *ResultSet) {
    return ResultSet->Cnt - ResultSet->Index > 1;
}

bool moveToNextNodeLink(struct NodeLinkResultSet *ResultSet) {
    if (hasNextNodeLink(ResultSet)) {
        ResultSet->Index += 1;
        return true;
    }
    return false;
}

bool hasPreviousNodeLink(struct NodeLinkResultSet *ResultSet) { return ResultSet->Index > 0; }

bool moveToPreviousNodeLink(struct NodeLinkResultSet *ResultSet) {
    if (hasPreviousNodeLink(ResultSet)) {
        ResultSet->Index -= 1;
        return true;
    }
    return false;
}

void deleteNodeLinkResultSet(struct NodeLinkResultSet **ResultSet) {
    free((**ResultSet).LinkAddrs);
    free(*ResultSet);
    *ResultSet = NULL;
}

bool nodeLinkResultSetIsEmpty(struct NodeLinkResultSet *ResultSet) {
    return ResultSet->Cnt == 0;
}

bool readResultGraph(struct GraphResultSet *ResultSet, struct ExternalGraph **Graph) {
    if (graphResultSetIsEmpty(ResultSet))
        return false;
    getExternalGraph(ResultSet->Controller, ResultSet->GraphAddrs[ResultSet->Index], Graph);
    return true;
}

bool hasNextGraph(struct GraphResultSet *ResultSet) {
    return ResultSet->Cnt - ResultSet->Index > 1;
}

bool moveToNextGraph(struct GraphResultSet *ResultSet) {
    if (hasNextGraph(ResultSet)) {
        ResultSet->Index += 1;
        return true;
    }
    return false;
}

bool hasPreviousGraph(struct GraphResultSet *ResultSet) { return ResultSet->Index > 0; }

bool moveToPreviousGraph(struct GraphResultSet *ResultSet) {
    if (hasPreviousGraph(ResultSet)) {
        ResultSet->Index -= 1;
        return true;
    }
    return false;
}

bool graphResultSetIsEmpty(struct GraphResultSet *ResultSet) { return ResultSet->Cnt == 0; }

void deleteGraphResultSet(struct GraphResultSet **ResultSet) {
    free((**ResultSet).GraphAddrs);
    free(*ResultSet);
    *ResultSet = NULL;
}

void deleteExternalNode(struct ExternalNode **Node) {
    if (Node == NULL || *Node == NULL)
        return;
    free(*Node);
    *Node = NULL;
}

void deleteExternalNodeLink(struct ExternalNodeLink **Link) {
    if (Link == NULL || *Link == NULL)
        return;
    free(*Link);
    *Link = NULL;
}

void deleteExternalGraph(struct ExternalGraph **Graph) {
    if (Graph == NULL || *Graph == NULL)
        return;
    free(*Graph);
    *Graph = NULL;
}

size_t nodeResultSetGetSize(struct NodeResultSet *ResultSet) { return ResultSet->Cnt; }

size_t nodeLinkResultSetGetSize(struct NodeLinkResultSet *ResultSet) { return ResultSet->Cnt; }

size_t graphSetGetSize(struct GraphResultSet *ResultSet) { return ResultSet->Cnt; }

