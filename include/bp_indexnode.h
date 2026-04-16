#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include <record.h>
#include <bf.h>
#include <bp_file.h>
#include "bp_datanode.h"

typedef struct bp_index
{
    bool isLeaf;        // Indicates if it is a leaf node or an internal node
    int numKeys;        // Current number of keys stored in this node
    int maxKeys;        // Maximum number of keys that can be stored in this node
    int* keys;          // Array to store the keys (fixed size for the block)

    // Array to store indices (or block IDs) for children
    // For leaf nodes, these may be pointers to data blocks or linked data nodes
    //For index nodes, it points to blocks just like in leaf nodes
    int* children;      // Array of child block IDs or indices (size of maxKeys + 1)

    int blockID;        // Block ID to refer to the physical location of this node (for block-based handling)
    int parentID;       // Block ID of the parent node (may be used for upward propagation)

} BP_IndexNode;


#endif