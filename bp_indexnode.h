#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include <record.h>
#include <bf.h>
#include <bp_file.h>
#include "bp_datanode.h"


typedef struct bp_index
{
    bool isLeaf; //see if it a leaf in our bp tree
    int numKeys; //keys currently stored in
    int maxKeys; //how many keys it can have
    int* keys;

    //array that points to children of non-leaf indexes
    struct bp_index* child;

    //array that points to the data nodes
    //of the leaf index nodes
    BP_DataNode** leaf;

} BP_IndexNode;

#endif