#ifndef LLP_LAB1_RESPONSE_STRUCTURES_H
#define LLP_LAB1_RESPONSE_STRUCTURES_H



#include "data-interfaces.h"

struct NodeResultSet;
struct NodeLinkResultSet;
struct GraphResultSet;

bool readResultNode(struct NodeResultSet *ResultSet, struct ExternalNode **Node);
bool nodeResultSetIsEmpty(struct NodeResultSet *ResultSet);
size_t nodeResultSetGetSize(struct NodeResultSet *ResultSet);
void deleteNodeResultSet(struct NodeResultSet **ResultSet);

bool readResultNodeLink(struct NodeLinkResultSet *ResultSet,
                        struct ExternalNodeLink **NodeLink);
bool nodeLinkResultSetIsEmpty(struct NodeLinkResultSet *ResultSet);
size_t nodeLinkResultSetGetSize(struct NodeLinkResultSet *ResultSet);
void deleteNodeLinkResultSet(struct NodeLinkResultSet **ResultSet);

bool readResultGraph(struct GraphResultSet *ResultSet, struct ExternalGraph **graph);
bool graphResultSetIsEmpty(struct GraphResultSet *ResultSet);
size_t graphResultSetGetSize(struct GraphResultSet *ResultSet);
void deleteGraphResultSet(struct GraphResultSet **ResultSet);


#endif //LLP_LAB1_RESPONSE_STRUCTURES_H
