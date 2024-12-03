#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include <bp_datanode.h>
#include "bp_indexnode.h"
#include <stdbool.h>

#define SIZEOFEURETHRIO 8;


#define CALL_BF(call)             \
    {                             \
        BF_ErrorCode code = call; \
        if (code != BF_OK)        \
        {                         \
            BF_PrintError(code);  \
            return BP_ERROR;      \
        }                         \
    }

BF_Block *curBlock = NULL;
void *curdata = NULL;
BPLUS_INFO *curBlockInfo = NULL;
int curBlockpos = 0;

BF_Block *infoblock = NULL;
void *infodata = NULL;

int BP_CreateFile(char *fileName)
{
    int fileDesc;
    BF_Block *block;

    // we create the file
    CALL_BF(BF_CreateFile(fileName));
    // we open it
    CALL_BF(BF_OpenFile(fileName, &fileDesc));

    // allocate the first block for metadata
    BF_Block_Init(&block);

    // store it inside the file
    CALL_BF(BF_AllocateBlock(fileDesc, block));

    // prepare the metadata (HP_info) for the first block of the heap file
    BPLUS_INFO bpInfo;
    strcpy(bpInfo.kindOfFile, "B+ Tree");
    bpInfo.totalBlocks = 1;
    bpInfo.maxRecordsPerBlock = (BF_BLOCK_SIZE - (sizeof(int) * 2 + sizeof(struct bp_data *))) / sizeof(Record);
    bpInfo.totalRecords = 0;
    bpInfo.lastBlockID = 1;
    bpInfo.rootID = 1;
    bpInfo.fd = fileDesc;
    bpInfo.sizeOfEurethrio = SIZEOFEURETHRIO;

    // memcpy the metadata into the first block
    char *data = BF_Block_GetData(block);
    memcpy(data, &bpInfo, sizeof(BPLUS_INFO));
    BF_Block_SetDirty(block);

    // Finally, we unpin the block and close the file
    CALL_BF(BF_UnpinBlock(block));

    // destroy the block
    BF_Block_Destroy(&block);

    // we want to create a root node in our file
    BF_Block *root;
    BF_Block_Init(&root);
    CALL_BF(BF_AllocateBlock(fileDesc, root));

    BP_IndexNode *rootNode = (BP_IndexNode *)BF_Block_GetData(root);
    rootNode->isLeaf = true;
    rootNode->numKeys = 0;
    rootNode->maxKeys = bpInfo.sizeOfEurethrio;

    rootNode->keys = malloc(rootNode->maxKeys * sizeof(int));
    memset(rootNode->keys, 0, rootNode->maxKeys * sizeof(int));

    rootNode->child = malloc((rootNode->maxKeys + 1) * sizeof(BP_IndexNode*));
    memset(rootNode->child, 0, (rootNode->maxKeys + 1) * sizeof(BP_IndexNode*));

    rootNode->leaf = malloc((rootNode->maxKeys + 1) * sizeof(BP_DataNode *));
    memset(rootNode->leaf, 0, (rootNode->maxKeys + 1) * sizeof(BP_DataNode *));

    rootNode->parentNode = NULL;

    BF_Block_SetDirty(root);
    CALL_BF(BF_UnpinBlock(root));

    // destroy the root block
    BF_Block_Destroy(&root);

    CALL_BF(BF_CloseFile(fileDesc));

    return 0;
}

BPLUS_INFO *BP_OpenFile(char *fileName, int *file_desc)
{
    BPLUS_INFO *bpInfo;
    BF_OpenFile(fileName, file_desc);

    BF_Block_Init(&infoblock);

    BF_GetBlock(*file_desc, 0, infoblock);  // it pins the block in the memory
    infodata = BF_Block_GetData(infoblock); // get the data fo the first block

    bpInfo = (BPLUS_INFO *)infodata;
    // checking that the data are ok!
    printf("Kind: %s\n", bpInfo->kindOfFile);
    printf("totalBlocks: %d\n", bpInfo->totalBlocks);
    printf("maxRecordsPerBlock: %d\n", bpInfo->maxRecordsPerBlock);
    printf("totalRecords: %d\n", bpInfo->totalRecords);
    printf("lastBlockID: %d\n", bpInfo->lastBlockID);

    // we return the info we got from the first block
    return bpInfo;
}

int BP_CloseFile(int file_desc, BPLUS_INFO *info)
{
    if (curBlock != NULL)
    {
        CALL_BF(BF_UnpinBlock(curBlock));
        BF_Block_Destroy(&curBlock);
    }

    CALL_BF(BF_UnpinBlock(infoblock)); // we need to unpin the block since we no longer need it
    BF_Block_Destroy(&infoblock);

    CALL_BF(BF_CloseFile(file_desc));
    infodata = NULL;
    info = NULL;
    curdata = NULL;

    return 0;
}

int BP_InsertEntry(int file_desc, BPLUS_INFO *bplus_info, Record record);

BP_IndexNode *findLeafNode(BP_IndexNode *root, int key);
void insertIntoLeaf(BP_DataNode *leaf, Record record);
void splitLeafNode(BP_IndexNode* parentNode, BP_DataNode *leaf, Record record, BPLUS_INFO *bplus_info);
void insertIntoIndexNode(BP_IndexNode *oldNode, int key, BP_IndexNode *newNode, BPLUS_INFO *bplus_info);
void splitIndexNode(BP_IndexNode *oldNode, int key, BP_DataNode* leafNode, BP_IndexNode* childNode, BPLUS_INFO *bplus_info);

int BP_InsertEntry(int file_desc, BPLUS_INFO *bplus_info, Record record)
{
    BF_Block *root;
    BF_Block_Init(&root);

    CALL_BF(BF_GetBlock(file_desc, bplus_info->rootID, root));
    BP_IndexNode *rootNode = (BP_IndexNode *)BF_Block_GetData(root);

    // printf("hi\n");
    // we are finding the leaf node to insert the key
    BP_IndexNode *leafNode = findLeafNode(rootNode, record.id);

    if (leafNode == NULL)
    {
        // we have already inserted that key in our tree
        return -1;
    }

    printf("RECORD ID: %d \n", record.id);


    // so now we are inserting the key into the leaf node
    // but first we check if it is full
    int flag = 0;
    if (leafNode->numKeys < leafNode->maxKeys)
    {
        if(leafNode->numKeys == 0)
        {
            leafNode->keys[0] = record.id;
            leafNode->numKeys++;
        }

        for (int idx = 0; idx < leafNode->numKeys; idx++)
        {
            // we can insert the record in the leaf node
            // if the key is smaller than the current key
            // and the leaf node has space for the record
            if (record.id < leafNode->keys[idx])
            {
                // if the leaf node does not exist we have to initialize it
                // we insert the record immediately
                if (leafNode->leaf[idx] == NULL)
                {
                    //i have to initialize a new block in our file
                    // BF_Block* block;
                    // BF_Block_Init(&block);
                    // CALL_BF(BF_AllocateBlock(file_desc, block));
                    // leafNode->leaf[idx] = (BP_DataNode*)BF_Block_GetData(block);


                    leafNode->leaf[idx] = malloc(sizeof(BP_DataNode));

                    leafNode->leaf[idx]->numOfRecords = 1;
                    leafNode->leaf[idx]->maxRecords = bplus_info->maxRecordsPerBlock;
                    leafNode->leaf[idx]->records = malloc((leafNode->leaf[idx]->maxRecords) * sizeof(Record));
                    memcpy(&leafNode->leaf[idx]->records[0], &record, sizeof(Record));
                    flag = 1;

                    // BF_Block_SetDirty(block);
                    // CALL_BF(BF_UnpinBlock(&block));
                    // BF_Block_Destroy(&block);
                    break;
                }

                // else it exists and we have to check if it has space in it
                if (leafNode->leaf[idx]->numOfRecords < bplus_info->maxRecordsPerBlock)
                {
                    insertIntoLeaf(leafNode->leaf[idx], record);
                    flag = 1;
                    break;
                }
                else
                {
                    // we have to split the leaf node
                    printf("(inside for)we have to split this data node, key: %d\n", record.id);
                    splitLeafNode(leafNode, leafNode->leaf[idx], record, bplus_info);
                    flag = 1;
                    break;
                }
            }
        }
        
        if(flag == 0)
        {     
            // if it does not exist, initialize it
            if (leafNode->leaf[(leafNode->numKeys)] == NULL)
            {
                //i have to initialize a new block in our file
                // BF_Block* block;
                // BF_Block_Init(&block);
                // CALL_BF(BF_AllocateBlock(file_desc, block));
                // leafNode->leaf[(leafNode->numKeys) + 1] = (BP_DataNode*)BF_Block_GetData(block);


                leafNode->leaf[(leafNode->numKeys) ] = malloc(sizeof(BP_DataNode));

                leafNode->leaf[(leafNode->numKeys) ]->numOfRecords = 1;
                leafNode->leaf[(leafNode->numKeys)]->maxRecords = bplus_info->maxRecordsPerBlock;
                leafNode->leaf[(leafNode->numKeys) ]->records = malloc((leafNode->leaf[(leafNode->numKeys)]->maxRecords) * sizeof(Record));
                memcpy(&leafNode->leaf[(leafNode->numKeys) ]->records[0], &record, sizeof(Record));

            }
            else if (leafNode->leaf[(leafNode->numKeys) ]->numOfRecords < bplus_info->maxRecordsPerBlock)
            {
                insertIntoLeaf(leafNode->leaf[(leafNode->numKeys)], record);
            }
            else
            {
                printf("we have to split this data node, key: %d\n", record.id);
                splitLeafNode(leafNode, leafNode->leaf[(leafNode->numKeys)], record, bplus_info);
            }
        }
    }
    else
    {
        // we have to split the parent node
        printf("we have to split this index node, key: %d\n", record.id);
        int idx = 0;
        while (idx < leafNode->numKeys && leafNode->keys[idx] < record.id)
        {
            idx++;
        }
        splitLeafNode(leafNode, leafNode->leaf[idx], record, bplus_info);

    }


    BF_Block_SetDirty(root);
    CALL_BF(BF_UnpinBlock(root));
    BF_Block_Destroy(&root);
    return 0;
}

BP_IndexNode *findLeafNode(BP_IndexNode *root, int key)
{
    // if the root is leaf return it
    // because we will insert the record into it

    if (root->isLeaf)
    { 
        for (int idx = 0; idx <= root->numKeys; idx++)
        {
            if(root->leaf[idx] != NULL)
            {
                for(int i = 0; i < root->leaf[idx]->numOfRecords; i++)
                {
                    if(key == root->leaf[idx]->records[i].id)
                    {
                        return NULL;
                    }
                }
            }

            if (key == root->keys[idx])
            {
                // we have already inserted that key in our tree
                return NULL;
            }

        }
        
        return root;
    }

    // if the root is not a leaf node we have to traverse the tree
    for (int idx = 0; idx < root->numKeys; idx++)
    {
        if (key == root->keys[idx])
        {
            // we have already inserted that key in our tree
            return NULL;
        }

        if (key < root->keys[idx])
        {
            // we have to go the right child
            return findLeafNode(root->child[idx], key);
        }
    }

    // if we reach here we have to go to the last child
    return findLeafNode(root->child[root->numKeys], key);
}

void insertIntoLeaf(BP_DataNode *leaf, Record record)
{
    // we have to find the correct position to insert the record
    int idx = 0;
    while (idx < leaf->numOfRecords && leaf->records[idx].id < record.id)
    {
        idx++;
    }

    // we have to move the records to the right
    for (int i = leaf->numOfRecords; i > idx; i--)
    {
        leaf->records[i] = leaf->records[i - 1];
    }

    // we insert the record
    memcpy(&leaf->records[idx], &record, sizeof(Record));
    leaf->numOfRecords++;
}

void splitLeafNode(BP_IndexNode* parentNode, BP_DataNode *leaf, Record record, BPLUS_INFO *bplus_info)
{
    //we need to equally slpite the datanode in two
    //then we need to insert the record in the correct leaf
    //then we need to insert the new leaf in the parent node, keepping the keys sorted
    //then we need to split the parent node if it is full , call the function recursively

    // BF_Block* OldDataNode = BF_GetBlock(file_desc, bplus_info->lastBlockID, OldDataNode);


    int middle = leaf->numOfRecords / 2;

    //we have to create a new leaf node
    BP_DataNode *newLeaf = malloc(sizeof(BP_DataNode));
    newLeaf->numOfRecords = leaf->numOfRecords - middle;
    newLeaf->maxRecords = bplus_info->maxRecordsPerBlock;
    newLeaf->records = malloc(newLeaf->maxRecords * sizeof(Record));
    newLeaf->nextPtr = leaf->nextPtr;
    leaf->nextPtr = newLeaf;

    //we have to copy the records to the new leaf
    for (int i = 0; i < newLeaf->numOfRecords; i++)
    {
        memcpy(&newLeaf->records[i], &leaf->records[middle + i], sizeof(Record));
    }

    //we have to update the old leaf
    leaf->numOfRecords = middle;
    for(int idx = middle; idx < leaf->maxRecords; idx++)
    {
        memset(&leaf->records[idx], 0, sizeof(Record));
    }

    //we have to insert the record in the correct leaf
    if(record.id < newLeaf->records[0].id)
    {
        insertIntoLeaf(leaf, record);
    }
    else
    {
        insertIntoLeaf(newLeaf, record);
    }

    //we have to insert the new leaf in the parent node and keep the keys sorted
    //if parent node is full we have to split it -> splitIndexNode deals with it
    if(parentNode->numKeys == parentNode->maxKeys)
    {
        splitIndexNode(parentNode, newLeaf->records[0].id, newLeaf, NULL, bplus_info);
        return;
    }
    //maybe insertIntoIndexNode will be used here

    //we are sure that there is space in our parent node
    //we made it sure from the InsertEntry function
    //we have to find the correct position to insert the new leaf to the parent node
    int idx = 0;
    while(idx < parentNode->numKeys && parentNode->keys[idx] < newLeaf->records[0].id)
    {
        idx++;
    }

    //we have to move the keys to the right
    for(int i = parentNode->numKeys; i > idx; i--)
    {
        parentNode->keys[i] = parentNode->keys[i-1];
        parentNode->leaf[i+1] = parentNode->leaf[i];
        parentNode->leaf[i] = NULL;

    }

    //we insert the new key
    parentNode->keys[idx] = newLeaf->records[0].id;

    //we insert the new leaf
    parentNode->leaf[idx + 1] = newLeaf;
    parentNode->numKeys++;

}

void splitIndexNode(BP_IndexNode *oldNode, int key, BP_DataNode* leafNode, BP_IndexNode* childNode, BPLUS_INFO *bplus_info)
{
    //now we have to split the index node
    //we have to find the middle key
    //we have to create a new index node
    //we have to copy the keys to the new index node
    //we have to insert the new key in the correct index node
    //we have to insert the new index node in the parent node, keeping the keys sorted

    int middle = oldNode->numKeys / 2;
    //we have to create a new index node

    BP_IndexNode *newNode = malloc(sizeof(BP_IndexNode));
    if(oldNode->isLeaf)
    {
        newNode->isLeaf = true;
    }
    else
    {
        newNode->isLeaf = false;
    }
    newNode->numKeys = oldNode->numKeys - middle;
    newNode->maxKeys = oldNode->maxKeys;
    newNode->keys = malloc(newNode->maxKeys * sizeof(int));
    memset(newNode->keys, 0, newNode->maxKeys * sizeof(int));
    newNode->child = malloc((newNode->maxKeys + 1) * sizeof(BP_IndexNode*));
    memset(newNode->child, 0, (newNode->maxKeys + 1) * sizeof(BP_IndexNode*));
    newNode->leaf = malloc((newNode->maxKeys + 1) * sizeof(BP_DataNode *));
    memset(newNode->leaf, 0, (newNode->maxKeys + 1) * sizeof(BP_DataNode *));
    newNode->parentNode = oldNode->parentNode;

    int middlekey;
    if (leafNode != NULL)
    {
        //print the records inside of oldNode
        // for(int i = 0; i <= oldNode->numKeys; i++)
        // {
        //     printf("oldNode key: %d\n", oldNode->leaf[i]->records[0].id);
        // }


        int order[bplus_info->sizeOfEurethrio + 1];

        BP_DataNode *orderLeaf[bplus_info->sizeOfEurethrio + 2];
        int i = 0;
        int j = 0;
        orderLeaf[0] = oldNode->leaf[0];
        while (i < oldNode->numKeys)
        {
            if (key > oldNode->keys[i])
            {
                order[j] = oldNode->keys[i];
                orderLeaf[j + 1] = oldNode->leaf[i + 1];
                // printf("orderLeaf %d %d: %d\n",i, j, orderLeaf[j]->records[0].id);
                i++;
                j++;
            }
            else
            {
                order[j] = key;
                orderLeaf[j + 1] = leafNode;
                j++;
                break;
            }
        }


        if(i == oldNode->numKeys)
        {
            order[j] = key;
            orderLeaf[j + 1] = leafNode;
            j++;
        }

        while (i < oldNode->numKeys)
        {
            order[j] = oldNode->keys[i];
            orderLeaf[j+1] = oldNode->leaf[i+1];
            i++;
            j++;
        }
        // orderLeaf[i] = oldNode->leaf[i];

        // for(int idx = 0; idx < (bplus_info->sizeOfEurethrio + 1); idx++)
        // {
        //     printf("orderLeaf %d: %d\n", idx, orderLeaf[idx]->records[0].id);
        // }

        // for(int idx = 0; idx < (bplus_info->sizeOfEurethrio + 2); idx++)
        // {
        //     printf("order keys %d: %d\n", idx, order[idx]);
        // }

        middlekey = order[middle];

        // we have to copy the keys to the new index node
        for (int i = 0; i < newNode->numKeys; i++)
        {
            newNode->keys[i] = order[middle + 1 + i];
            newNode->leaf[i] = orderLeaf[middle + 1 + i];
        }
        newNode->leaf[newNode->numKeys] = orderLeaf[middle + 1 + newNode->numKeys];

        for (int i = 0; i < oldNode->maxKeys; i++)
        {
            if (i < middle)
            {
                oldNode->keys[i] = order[i];
                oldNode->leaf[i] = orderLeaf[i];
            }
            else
            {
                oldNode->keys[i] = 0;
                if (i == middle)
                {
                    oldNode->leaf[i] = orderLeaf[i];
                }
                else
                {
                    oldNode->leaf[i] = NULL;
                }
            }
        }
        oldNode->numKeys = middle;
    }
    else
    {
       //print the records inside of oldNode
        // for(int i = 0; i <= oldNode->numKeys; i++)
        // {
        //     printf("oldNode child key: %d\n", oldNode->child[i]->keys[0]);
        // }

        // for(int idx = 0; idx < childNode->numKeys; idx++)
        // {
        //     printf("childNode keys %d: %d\n", idx, childNode->keys[idx]);
        // }


        int order[bplus_info->sizeOfEurethrio + 1];
        BP_IndexNode *orderChild[bplus_info->sizeOfEurethrio + 2];
        int i = 0;
        int j = 0;
        orderChild[0] = oldNode->child[0];
        while (i < oldNode->numKeys)
        {
            if (key > oldNode->keys[i])
            {
                order[j] = oldNode->keys[i];
                orderChild[j + 1] = oldNode->child[i + 1];
                // printf("orderLeaf %d %d: %d\n",i, j, orderLeaf[j]->records[0].id);
                i++;
                j++;
                // printf("oooooooooooooooooooorderChild %d %d: %d\n",i, j, orderChild[j]->keys[0]);
            }
            else
            {
                order[j] = key;
                orderChild[j + 1] = childNode;
                j++;
                // printf("oooooooooooooooooooorderChild %d %d: %d\n",i, j, orderChild[j]->keys[0]);
                break;
            }
        }

        if(i == oldNode->numKeys)
        {
            order[j] = key;
            orderChild[j + 1] = childNode;
            j++;
            // printf("oooooooooooooooooooorderChild %d %d: %d\n",i, j, orderChild[j]->keys[0]);

        }

        while (i < oldNode->numKeys)
        {
            order[j] = oldNode->keys[i];
            orderChild[j+1] = oldNode->child[i+1];
            i++;
            j++;
            // printf("oooooooooooooooooooorderChild %d %d: %d\n",i, j, orderChild[j]->keys[0]);
        }
        // orderLeaf[i] = oldNode->leaf[i];

        // for(int idx = 0; idx < (bplus_info->sizeOfEurethrio + 1); idx++)
        // {
        //     printf("order keys %d: %d\n", idx, order[idx]);
        // }

        // for(int idx = 0; idx < (bplus_info->sizeOfEurethrio + 2); idx++)
        // {
        //     printf("orderChild %d: %d\n", idx, orderChild[idx]->keys[0]);
        // }


        middlekey = order[middle];

        // we have to copy the keys to the new index node
        for (int i = 0; i < newNode->numKeys; i++)
        {
            newNode->keys[i] = order[middle + 1 + i];
            newNode->child[i] = orderChild[middle + 1 + i];
        }
        newNode->child[newNode->numKeys] = orderChild[middle + 1 + newNode->numKeys];

        for (int i = 0; i < oldNode->maxKeys; i++)
        {
            if (i < middle)
            {
                oldNode->keys[i] = order[i];
                oldNode->child[i] = orderChild[i];
            }
            else
            {
                oldNode->keys[i] = 0;
                if (i == middle)
                {
                    oldNode->child[i] = orderChild[i];
                }
                else
                {
                    oldNode->child[i] = NULL;
                }
            }
        }
        oldNode->numKeys = middle;
    }

    //we have to fix the parent node of the two index nodes
    if(oldNode->parentNode == NULL)
    {
        BF_Block* block;

        // allocate the first block for metadata
        BF_Block_Init(&block);

        // store it inside the file
        //allocate a new block in memory
        BF_AllocateBlock(bplus_info->fd, block);
        BP_IndexNode *newParent = (BP_IndexNode *)BF_Block_GetData(block);

        //then we have to create the new parent
        // BP_IndexNode *newParent = malloc(sizeof(BP_IndexNode));
        newParent->isLeaf = false;
        newParent->numKeys = 1;
        newParent->maxKeys = oldNode->maxKeys;
        newParent->keys = malloc(newParent->maxKeys * sizeof(int));
        memset(newParent->keys, 0, newParent->maxKeys * sizeof(int));
        newParent->child = malloc((newParent->maxKeys + 1) * sizeof(BP_IndexNode*));
        memset(newParent->child, 0, (newParent->maxKeys + 1) * sizeof(BP_IndexNode*));
        newParent->leaf = malloc((newParent->maxKeys + 1) * sizeof(BP_DataNode *));
        memset(newParent->leaf, 0, (newParent->maxKeys + 1) * sizeof(BP_DataNode *));
        newParent->parentNode = NULL;

        newParent->keys[0] = middlekey;
        newParent->child[0] = oldNode;
        newParent->child[1] = newNode;   

        oldNode->parentNode = newParent;
        newNode->parentNode = newParent;

        bplus_info->rootID++;
            
        BF_Block_SetDirty(block);
        BF_UnpinBlock(block);

        // destroy the root block
        BF_Block_Destroy(&block);

    }
    else
    {
        if(oldNode->parentNode->numKeys == oldNode->parentNode->maxKeys)
        {
            printf("we have to split the parent!\n");
            splitIndexNode(oldNode->parentNode, middlekey, NULL, newNode, bplus_info);
            return;
        }
        else
        {
            printf("we have to insert into parent, middlekey %d, key %d!\n", middlekey, key);
            insertIntoIndexNode(oldNode->parentNode, middlekey, newNode, bplus_info);
        }
    }


}


void insertIntoIndexNode(BP_IndexNode *parentNode, int key, BP_IndexNode *newNode, BPLUS_INFO *bplus_info)
{

    int idx = 0;
    while(idx < parentNode->numKeys && parentNode->keys[idx] < key)
    {
        idx++;
    }

    //we have to move the keys to the right
    for(int i = parentNode->numKeys; i > idx; i--)
    {
        parentNode->keys[i] = parentNode->keys[i-1];
        parentNode->child[i+1] = parentNode->child[i];
        parentNode->child[i] = NULL;

    }

    //we insert the new key
    parentNode->keys[idx] = key;

    //we insert the new leaf
    parentNode->child[idx + 1] = newNode;
    parentNode->numKeys++;


}

int BP_GetEntry(int file_desc, BPLUS_INFO *bplus_info, int value, Record **record)
{
    *record = NULL;
    return 0;
}
