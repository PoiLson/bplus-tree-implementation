#ifndef BP_DATANODE_H
#define BP_DATANODE_H
#include "record.h"
#include "bf.h"
#include "bp_file.h"

typedef struct bp_data
{
    int numOfRecords;   // Current number of records in this node
    int maxRecords;     // Maximum number of records the node can store
    Record* records;    // Array of records (fixed size for the block)

    int nextBlockID;    // Points to the next block in the linked list of leaf nodes (or -1 if none)

    int blockID;        // Block ID to represent where this data node resides

} BP_DataNode;


#endif 