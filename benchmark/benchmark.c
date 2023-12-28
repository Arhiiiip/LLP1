#include "benchmark.h"

#include "../configs/bech-config.h"
#include "../structures-data/types.h"

#include <stdio.h>
#include <time.h>

void benchmarkNodeInsert(FILE *OutFile) {
    const char *CSVHeader = "Node Number, Insert time ns";
    FILE *CSVOut = OutFile;
    fprintf(CSVOut, "%s\n", CSVHeader);
    struct StorageController *Controller = beginWork("bench.bin");
    struct AttributeDescription GraphAttributes[4] = {
            {.AttributeId = 0, .Name = "Node Name", .Type = STRING, .Next = GraphAttributes + 1},
            {.AttributeId = 1, .Name = "Int value", .Type = INT, .Next = GraphAttributes + 2},
            {.AttributeId = 2, .Name = "Bool value", .Type = BOOL, .Next = GraphAttributes + 3},
            {.AttributeId = 3, .Name = "Float value", .Type = FLOAT, .Next = NULL}};
    struct CreateGraphRequest CGR = {.AttributesDescription = GraphAttributes, .Name = "G"};
    createGraph(Controller, &CGR);
    struct Attribute NodeAttributes[4] = {
            {.Id = 0, .Type = STRING, .Value.StringAddr = "Some String"},
            {
                    .Id = 1,
                    .Type = INT,
            },
            {.Id = 2, .Type = BOOL, .Value.BoolValue = false},
            {.Id = 3, .Type = FLOAT, .Value.FloatValue = 3.14f}};
    struct CreateNodeRequest CNR = {
            .Attributes = NodeAttributes, .GraphIdType = GRAPH_NAME, .GraphId.GraphName = "G"};
    for (int i = 0; i < 10; ++i) {
        clock_t Begin = clock();
        for (int j = 0; j < 10000; ++j) {
            NodeAttributes[1].Value.IntValue = i * 100 + j;
            createNode(Controller, &CNR);
        }
        clock_t End = clock();
        double TimeDiff = ((double) (End - Begin) * 10e9) / CLOCKS_PER_SEC;
        fprintf(CSVOut, "%d, %lf\n", (i + 1) * 100, TimeDiff);
    }
    struct DeleteGraphRequest DGR = {.Name = "G"};
    deleteGraph(Controller, &DGR);
    endWork(Controller);
}

void benchmarkSelectByAttributes(FILE *OutFile) {
    const char *CSVHeader = "Total Node Number,Selected Node Number,Select time ns";
    FILE *CSVOut = OutFile;
    fprintf(CSVOut, "%s\n", CSVHeader);
    struct StorageController *Controller = beginWork("bench.bin");
    struct AttributeDescription GraphAttributes[2] = {
            {.AttributeId = 0, .Name = "Int value", .Type = INT, .Next = GraphAttributes + 1},
            {.AttributeId = 1, .Name = "Divisible by 3", .Next = NULL, .Type = BOOL}};
    struct CreateGraphRequest CGR = {.AttributesDescription = GraphAttributes, .Name = "G"};
    createGraph(Controller, &CGR);
    struct AttributeInterface NodeAttributes[2] = {{
                                                           .Id = 0,
                                                           .Type = INT,
                                                   },
                                                   {.Id = 1, .Type = BOOL}};
    struct CreateNodeRequest CNR = {
            .Attributes = NodeAttributes, .GraphIdType = GRAPH_NAME, .GraphId.GraphName = "G"};
    struct AttributeFilter DivByThreeFilter = {
            .AttributeId = 1, .Type = BOOL_FILTER, .Data = true, .Next = NULL};
    struct ReadNodeRequest RNR = {.GraphIdType = GRAPH_NAME,
            .GraphId.GraphName = "G",
            .ById = false,
            .AttributesFilterChain = &DivByThreeFilter};
    for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 10000; ++j) {
            NodeAttributes[0].Value.IntValue = i * 100 + j;
            NodeAttributes[1].Value.BoolValue = ((j % 3) == 0);
            createNode(Controller, &CNR);
        }
        clock_t Begin = clock();
        struct NodeResultSet *NRS = readNode(Controller, &RNR);
        size_t ResultSetSize = nodeResultSetGetSize(NRS);
        while (hasNextNode(NRS)) {
            struct NodeInterface *Node;
            readResultNode(NRS, &Node);
            moveToNextNode(NRS);
            deleteExternalNode(&Node);
        }
        clock_t End = clock();
        deleteNodeResultSet(&NRS);
        double TimeDiff = ((double) (End - Begin) * 10e9) / CLOCKS_PER_SEC;
        fprintf(CSVOut, "%d,%zu,%lf\n", (i + 1) * 100, ResultSetSize, TimeDiff);
    }
    struct DeleteGraphRequest DGR = {.Name = "G"};
    deleteGraph(Controller, &DGR);
    endWork(Controller);
}

void benchmarkDeleteElements(FILE *OutFile) {
    FILE *CSVOut = OutFile;
    const char *CSVHeader = "Total Node Number,Deleted Node Number,Delete Time ns";
    fprintf(CSVOut, "%s\n", CSVHeader);
    struct StorageController *Controller = beginWork("bench.bin");
    struct AttributeDescription GraphAttributes[2] = {
            {.AttributeId = 0, .Name = "Int value", .Type = INT, .Next = GraphAttributes + 1},
            {.AttributeId = 1, .Name = "To be Deleted", .Next = NULL, .Type = BOOL}};
    struct CreateGraphRequest CGR = {.AttributesDescription = GraphAttributes, .Name = "G"};
    createGraph(Controller, &CGR);
    struct AttributeInterface NodeAttributes[2] = {{
                                                           .Id = 0,
                                                           .Type = INT,
                                                   },
                                                   {.Id = 1, .Type = BOOL}};
    struct CreateNodeRequest CNR = {
            .Attributes = NodeAttributes, .GraphIdType = GRAPH_NAME, .GraphId.GraphName = "G"};
    struct AttributeFilter DivByThreeFilter = {
            .AttributeId = 1, .Type = BOOL_FILTER, .Data = true, .Next = NULL};
    struct DeleteNodeRequest DNR = {.GraphIdType = GRAPH_NAME,
            .GraphId.GraphName = "G",
            .ById = false,
            .AttributesFilterChain = &DivByThreeFilter,};
    for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 10000; ++j) {
            NodeAttributes[0].Value.IntValue = i * 100 + j;
            NodeAttributes[1].Value.BoolValue = ((j % 5) == 0);
            createNode(Controller, &CNR);
        }
        clock_t Begin = clock();
        size_t DeletedNodeNumber = deleteNode(Controller, &DNR);
        clock_t End = clock();
        double TimeDiff = ((double) (End - Begin) * 10e9) / CLOCKS_PER_SEC;
        fprintf(CSVOut, "%d,%zu,%lf\n", (i + 1) * 100, DeletedNodeNumber, TimeDiff);
    }

    struct DeleteGraphRequest DGR = {.Name = "G"};
    deleteGraph(Controller, &DGR);
    endWork(Controller);
}

void benchmarkUpdateProgressingElements(FILE *OutFile) {
    FILE *CSVOut = OutFile;
    const char *CSVHeader = "Total Node Number,Updated Node Number,Delete Time ns";
    fprintf(CSVOut, "%s\n", CSVHeader);
    struct StorageController *Controller = beginWork("bench.bin");
    struct AttributeDescription GraphAttributes[3] = {
            {.AttributeId = 0, .Name = "Id", .Type = INT, .Next = GraphAttributes + 1},
            {.AttributeId = 1, .Name = "Reminder of id to 7", .Next = GraphAttributes + 2, .Type = INT},
            {.AttributeId = 2, .Name = "Updated", .Next = NULL, .Type = BOOL}
    };
    struct CreateGraphRequest CGR = {.AttributesDescription = GraphAttributes, .Name = "G"};
    createGraph(Controller, &CGR);
    struct AttributeInterface NodeAttributes[3] = {{
                                                           .Id = 0,
                                                           .Type = INT,
                                                   },
                                                   {
                                                           .Id = 1,
                                                           .Type = INT
                                                   },
                                                   {
                                                           .Id = 2,
                                                           .Type = BOOL,
                                                           .Value.BoolValue = false
                                                   }
    };
    struct CreateNodeRequest CNR = {
            .Attributes = NodeAttributes, .GraphIdType = GRAPH_NAME, .GraphId.GraphName = "G"};
    struct AttributeFilter UpdFilter = {
            .AttributeId = 1,
            .Type = INT_FILTER,
            .Next = NULL,
            .Data.Int = {
                    .HasMax = true,
                    .Max = 5,
                    .HasMin = true,
                    .Min = 1
            }
    };
    struct AttributeInterface UpdatedAttributes[2] = {
            {
                    .Id = 1,
                    .Type = INT,
                    .Value.IntValue = 2
            },
            {
                    .Id = 2,
                    .Type = BOOL,
                    .Value.BoolValue = true
            }
    };
    struct UpdateNodeRequest UNR = {
            .GraphIdType = GRAPH_NAME,
            .GraphId.GraphName = "G",
            .AttributesFilterChain = &UpdFilter,
            .UpdatedAttributesNumber = 2,
            .Attributes = UpdatedAttributes,
            .ById = false
    };
    for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 10000; ++j) {
            NodeAttributes[0].Value.IntValue = i * 100 + j;
            NodeAttributes[1].Value.IntValue = ((i * 100) + j) % 7;
            createNode(Controller, &CNR);
        }
        clock_t Begin = clock();
        size_t UpdatedNodeNumber = updateNode(Controller, &UNR);
        clock_t End = clock();
        double TimeDiff = ((double) (End - Begin) * 10e9) / CLOCKS_PER_SEC;
        fprintf(CSVOut, "%d,%zu,%lf\n", (i + 1) * 100, UpdatedNodeNumber, TimeDiff);
    }

    struct DeleteGraphRequest DGR = {.Name = "G"};
    deleteGraph(Controller, &DGR);
    endWork(Controller);
}

void benchmarkFileSize(FILE *OutFile) {
    FILE *CSVOut = OutFile;
    const char *CSVHeader = "Operation Number,Node Size,File Size";
    fprintf(OutFile, "%s\n", CSVHeader);
    struct StorageController *Controller = beginWork("bench.bin");
    struct AttributeDescription AttrDesc = {
            .AttributeId = 0,
            .Name = "Ordinal",
            .Next = NULL,
            .Type = INT
    };
    struct CreateGraphRequest CGR = {
            .AttributesDescription = &AttrDesc,
            .Name = "G"
    };
    createGraph(Controller, &CGR);
    struct AttributeInterface NodeAttribute = {
            .Id = 0,
            .Type = INT
    };
    struct CreateNodeRequest CNR = {
            .GraphIdType = GRAPH_NAME,
            .GraphId.GraphName = "G",
            .Attributes = &NodeAttribute
    };
    struct ReadNodeRequest ReadAll = {
            .GraphIdType = GRAPH_NAME,
            .GraphId.GraphName = "G",
            .ById = false,
            .AttributesFilterChain = NULL
    };
    int OpNum = 0;
    size_t NodeNum = 0;
    struct NodeResultSet *NRS;
    for (int i = 0; i < 100000; ++i) {
        CNR.Attributes->Value.IntValue = i + 1;
        createNode(Controller, &CNR);
        NodeNum++;
        OpNum++;
        fprintf(CSVOut, "%d,%zu,%zu\n", OpNum, NodeNum * (sizeof(struct Node) + sizeof(struct Attribute)),
                getFileSize(Controller->Allocator));
    }
    struct AttributeFilter After10k = {
            .AttributeId = 0,
            .Type = INT_FILTER,
            .Next = NULL,
            .Data.Int = {
                    .HasMax = false,
                    .HasMin = true,
                    .Min = 20000
            }
    };
    struct DeleteNodeRequest DNR = {
            .GraphIdType = GRAPH_NAME,
            .GraphId.GraphName = "G",
            .ById = false,
            .AttributesFilterChain = &After10k
    };
    NodeNum -= deleteNode(Controller, &DNR);
    fprintf(CSVOut, "%d,%zu,%zu\n", OpNum, NodeNum * (sizeof(struct Node) + sizeof(struct Attribute)),
            getFileSize(Controller->Allocator));
    for (int i = 0; i < 150000; ++i) {
        CNR.Attributes->Value.IntValue = 10000 + i + 1;
        createNode(Controller, &CNR);
        NodeNum++;
        OpNum++;
        fprintf(CSVOut, "%d,%zu,%zu\n", OpNum, NodeNum * (sizeof(struct Node) + sizeof(struct Attribute)),
                getFileSize(Controller->Allocator));
    }
    NRS = readNode(Controller, &ReadAll);
    if (nodeResultSetGetSize(NRS) == NodeNum) {
        fprintf(stderr, "File Size benchmark verified\n");
    }
    struct DeleteGraphRequest DGR = {.Name = "G"};
    deleteGraph(Controller, &DGR);
    endWork(Controller);
}


void benchmarkDop(FILE *OutFile) {
    const char *CSVHeader = "Total Node Number,Node Number,time ns";
    FILE *CSVOut = OutFile;
    fprintf(CSVOut, "%s\n", CSVHeader);
    struct StorageController *Controller = beginWork("bench.bin");
    struct AttributeDescription GraphAttributes[2] = {
            {.AttributeId = 0, .Name = "Int value", .Type = INT, .Next = GraphAttributes + 1},
            {.AttributeId = 1, .Name = "To be Deleted", .Next = NULL, .Type = BOOL}};
    struct CreateGraphRequest CGR = {.AttributesDescription = GraphAttributes, .Name = "G"};
    createGraph(Controller, &CGR);
    struct AttributeInterface NodeAttributes[2] = {{
                                                           .Id = 0,
                                                           .Type = INT,
                                                   },
                                                   {.Id = 1, .Type = BOOL}};
    struct CreateNodeRequest CNR = {
            .Attributes = NodeAttributes, .GraphIdType = GRAPH_NAME, .GraphId.GraphName = "G"};
    struct AttributeFilter DivByThreeFilter = {
            .AttributeId = 1, .Type = BOOL_FILTER, .Data = true, .Next = NULL};
    struct ReadNodeRequest RNR = {.GraphIdType = GRAPH_NAME,
            .GraphId.GraphName = "G",
            .ById = false,
            .AttributesFilterChain = &DivByThreeFilter};
    struct DeleteNodeRequest DNR = {.GraphIdType = GRAPH_NAME,
            .GraphId.GraphName = "G",
            .ById = false,
            .AttributesFilterChain = &DivByThreeFilter,};

    for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 1000; ++j) {
            NodeAttributes[0].Value.IntValue = i * 100 + j;
            if(j < 800){
                NodeAttributes[1].Value.BoolValue = 1;
            }else{
                NodeAttributes[1].Value.BoolValue = 0;
            }
            createNode(Controller, &CNR);
        }
        clock_t Begin = clock();
        struct NodeResultSet *NRS = readNode(Controller, &RNR);
        size_t ResultSetSize = nodeResultSetGetSize(NRS);
        while (hasNextNode(NRS)) {
            struct NodeInterface *Node;
            readResultNode(NRS, &Node);
            moveToNextNode(NRS);
            deleteExternalNode(&Node);
        }
        clock_t End = clock();
        deleteNodeResultSet(&NRS);
        double TimeDiff = ((double) (End - Begin) * 10e9) / CLOCKS_PER_SEC;
        fprintf(CSVOut, "%d,%zu,%lf\n", (i + 1) * 100, ResultSetSize, TimeDiff);

        clock_t BeginD = clock();
        size_t DeletedNodeNumber = deleteNode(Controller, &DNR);
        clock_t EndD = clock();
        double TimeDiffD = ((double) (End - Begin) * 10e9) / CLOCKS_PER_SEC;
        fprintf(CSVOut, "%d,%zu,%lf\n", (i + 1) * 100, DeletedNodeNumber, TimeDiffD);
    }

    struct DeleteGraphRequest DGR = {.Name = "G"};
    deleteGraph(Controller, &DGR);
    endWork(Controller);
}