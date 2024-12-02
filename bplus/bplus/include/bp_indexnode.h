#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include <record.h>
#include <bf.h>
#include <bp_file.h>


typedef struct bp_index
{
    bool isLeaf; //see if it a leaf in our bp tree
    int numOfKeys; //keys currently stored in
    int maxKids; //how many pointers it can have
    int* keys;
    struct bp_index* children;
} BPLUS_INDEX_NODE;

#endif