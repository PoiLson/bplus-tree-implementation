#ifndef BP_DATANODE_H
#define BP_DATANODE_H
#include <record.h>
#include <record.h>
#include <bf.h>
#include <bp_file.h>
#include <bp_indexnode.h>

typedef struct bp_data
{
    int numOfRecords; //currently how many records it has
    Record* records;
    struct bp_data* nextPtr;
    bool isLeaf;
} BPLUS_DATA_NODE;

#endif 