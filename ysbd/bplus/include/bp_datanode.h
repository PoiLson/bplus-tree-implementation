#ifndef BP_DATANODE_H
#define BP_DATANODE_H
#include "record.h"
#include "bf.h"
#include "bp_file.h"

typedef struct bp_data
{
    int numOfRecords; //currently how many records it has
    Record* records;
    int maxRecords;
    struct bp_data* nextPtr;

} BP_DataNode;

#endif 