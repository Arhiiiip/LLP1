#ifndef LLP_LAB1_GRAPH_DB_H
#define LLP_LAB1_GRAPH_DB_H



#include "../structures-request/data-interfaces.h"
#include "../structures-data/types.h"
#include "../structures-request/request-structures.h"
#include "../structures-request/response-structures.h"

struct StorageController;

struct StorageController *beginWork(char *DataFile);
void endWork(struct StorageController *Controller);
void dropStorage(struct StorageController *Controller);
void deleteString(const struct StorageController *const Controller, struct MyString String);
struct MyString createString(struct StorageController *const Controller,
                             const char *const String);

size_t createGraph(struct StorageController *const Controller,
                   const struct CreateGraphRequest *const Request);
size_t createNode(struct StorageController *const Controller,
                  const struct CreateNodeRequest *const Request);
size_t createNodeLink(struct StorageController *const Controller,
                      const struct CreateNodeLinkRequest *const Request);

struct NodeResultSet *readNode(const struct StorageController *const Controller,
                               const struct ReadNodeRequest *const Request);
struct NodeLinkResultSet *readNodeLink(const struct StorageController *const Controller,
                                       const struct ReadNodeLinkRequest *const Request);
struct GraphResultSet *readGraph(const struct StorageController *const Controller,
                                 const struct ReadGraphRequest *const Request);

size_t updateNode(const struct StorageController *const Controller,
                  const struct UpdateNodeRequest *const Request);
size_t updateNodeLink(const struct StorageController *const Controller,
                      const struct UpdateNodeLinkRequest *const Request);

size_t deleteNode(const struct StorageController *const Controller,
                  const struct DeleteNodeRequest *const Request);
size_t deleteNodeLink(const struct StorageController *const Controller,
                      const struct DeleteNodeLinkRequest *const Request);
size_t deleteGraph(struct StorageController *const Controller,
                   const struct DeleteGraphRequest *const Request);


#endif //LLP_LAB1_GRAPH_DB_H
