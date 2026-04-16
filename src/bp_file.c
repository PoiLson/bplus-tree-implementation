#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"

int SIZEOFEURETHRIO = 0;

#define CALL_BF(call)         \
{                             \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
        BF_PrintError(code);  \
        return BP_ERROR;      \
    }                         \
}

//blovk that contains the metadata of the file
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

    // prepare the metadata (BP_info) for the first block of the bplus file
    BPLUS_INFO bpInfo;
    strcpy(bpInfo.kindOfFile, "B+ Tree");
    bpInfo.totalBlocks = 1;
    bpInfo.maxRecordsPerBlock = (BF_BLOCK_SIZE - sizeof(BP_DataNode)) / sizeof(Record);
    bpInfo.totalRecords = 0;
    bpInfo.lastBlockID = 1;
    bpInfo.rootID = 1; //the next block (0 is the metadata node)
    bpInfo.fd = fileDesc;

    //size of eurethrio declares the maximum number of keys an index node can have
    SIZEOFEURETHRIO = (BF_BLOCK_SIZE - sizeof(BP_IndexNode) - sizeof(int)) / (sizeof(int) + sizeof(int));
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
    rootNode->blockID = 1; //the root is the second block in our file
    rootNode->parentID = 0;

    rootNode->keys = (int *)malloc(rootNode->maxKeys * sizeof(int));
    memset(rootNode->keys, 0, rootNode->maxKeys * sizeof(int));
    
    rootNode->children = (int *)malloc((rootNode->maxKeys + 1) * sizeof(int));
    memset(rootNode->children, 0, (rootNode->maxKeys + 1) * sizeof(int));

    //update the bplus info
    bpInfo.totalBlocks++;

    BF_Block_SetDirty(root);
    CALL_BF(BF_UnpinBlock(root));

    //destroy the root block
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

    // we return the info we got from the first block
    return bpInfo;
}

int BP_CloseFile(int file_desc, BPLUS_INFO *info)
{
    CALL_BF(BF_UnpinBlock(infoblock)); // we need to unpin the metadata block since we no longer need it
    BF_Block_Destroy(&infoblock);

    CALL_BF(BF_CloseFile(file_desc));
    infodata = NULL;
    info = NULL;

    return 0;
}

int BP_InsertEntry(int file_desc, BPLUS_INFO *bplus_info, Record record)
{
    // we are finding the leaf node of the to insert the key
    int leafNodeID = findLeafNode(bplus_info->rootID, record.id, file_desc);

    if(leafNodeID == 0)
    {
        // we have already inserted that key in our tree
        return -1;
    }
    
    //now we are accessing the block that we know it exists
    BF_Block *block;
    BF_Block_Init(&block);
    BF_GetBlock(file_desc, leafNodeID, block);

    BP_IndexNode *leafNode = (BP_IndexNode *)BF_Block_GetData(block);
    
    // so now we are inserting the key into the leaf node
    // but first we check if it is full

    int flag = 0;

    if(leafNode->numKeys == 0)
    {
        leafNode->keys[0] = record.id;
        leafNode->numKeys++;
    }

    //work for all left children
    for (int idx = 0; idx < leafNode->numKeys; idx++)
    {
        // we can insert the record in the leaf node
        // if the key is smaller than the current key
        // and the leaf node has space for the record
        if (record.id < leafNode->keys[idx])
        {
            // if the leaf node does not exist we have to initialize it
            // we insert the record immediately
            if (leafNode->children[idx] == 0)
            {
                //We have to initialize a new block in our file
                BF_Block* block;
                BF_Block_Init(&block);
                CALL_BF(BF_AllocateBlock(file_desc, block));

                //initialize dataChild
                BP_DataNode* dataChild = (BP_DataNode*)BF_Block_GetData(block);

                dataChild->blockID = ++bplus_info->lastBlockID;
                bplus_info->totalBlocks++;

                leafNode->children[idx] = dataChild->blockID;
                dataChild->maxRecords = bplus_info->maxRecordsPerBlock;
                dataChild->numOfRecords = 1;

                //find the id of the data blobk on its right to give to the nextBlockID
                if(leafNode->children[idx + 1] == 0)
                {
                    dataChild->nextBlockID = 0;
                }
                else
                {
                    BF_Block* bl;
                    BF_Block_Init(&bl);
                    BF_GetBlock(file_desc, leafNode->children[idx + 1], bl);
                    BP_DataNode* nextChild = (BP_DataNode*)BF_Block_GetData(bl);
                
                    dataChild->nextBlockID = nextChild->blockID;

                    CALL_BF(BF_UnpinBlock(bl));
                    BF_Block_Destroy(&bl);
                }
                
                dataChild->records = malloc((dataChild->maxRecords) * sizeof(Record));
                memset(dataChild->records, 0, sizeof(Record)*dataChild->maxRecords);
                memcpy(&dataChild->records[0], &record, sizeof(Record));
                flag = 1;

                BF_Block_SetDirty(block);
                CALL_BF(BF_UnpinBlock(block));
                BF_Block_Destroy(&block);
                break;
            }

            // else it exists and we have to check if it has space in it
            //and we have to get it from the file
            BF_Block* block;
            BF_Block_Init(&block);
            BF_GetBlock(file_desc, leafNode->children[idx], block);
            BP_DataNode* dataChild = (BP_DataNode*)BF_Block_GetData(block);

            if (dataChild->numOfRecords < bplus_info->maxRecordsPerBlock)
            {
                insertIntoLeaf(dataChild, record);

                BF_Block_SetDirty(block);
                CALL_BF(BF_UnpinBlock(block));
                BF_Block_Destroy(&block);
    
                flag = 1;
                break;
            }
            else
            {
                // we have to split the leaf node
                splitLeafNode(leafNode, dataChild, record, bplus_info);

                BF_Block_SetDirty(block);
                CALL_BF(BF_UnpinBlock(block));
                BF_Block_Destroy(&block);
                
                flag = 1;
                break;
            }
        }
    }
    
    //right child
    if(flag == 0)
    {
        BF_Block* block;
        BP_DataNode* dataChild;
        if (leafNode->children[leafNode->numKeys] != 0)
        {
            BF_Block_Init(&block);
            BF_GetBlock(file_desc, leafNode->children[leafNode->numKeys], block);
            dataChild = (BP_DataNode*)BF_Block_GetData(block);
        }
        
        // if it does not exist, initialize it
        if (leafNode->children[(leafNode->numKeys)] == 0)
        {
            //We have to initialize a new block in our file
            BF_Block_Init(&block);
            CALL_BF(BF_AllocateBlock(file_desc, block));

            //initialize dataChild
            dataChild = (BP_DataNode*)BF_Block_GetData(block);

            dataChild->blockID = ++bplus_info->lastBlockID;
            bplus_info->totalBlocks++;

            leafNode->children[(leafNode->numKeys)] = dataChild->blockID;
            dataChild->numOfRecords = 1;
            dataChild->maxRecords = bplus_info->maxRecordsPerBlock;
            //find the id of the data blobk on its right to give to the nextBlockID
            if( leafNode->numKeys == leafNode->maxKeys)
            {
                //if we are in the last data node of this leaf initialize next ptr to 0, will change when splitted
                dataChild->nextBlockID = 0;
            }
            else if (leafNode->children[(leafNode->numKeys) + 1] == 0)
            {
                dataChild->nextBlockID = 0;
            }
            else
            {
                BF_Block* bl;
                BF_Block_Init(&bl);
                BF_GetBlock(file_desc, leafNode->children[(leafNode->numKeys) + 1], bl);
                BP_DataNode* nextChild = (BP_DataNode*)BF_Block_GetData(bl);
            
                dataChild->nextBlockID = nextChild->blockID;

                CALL_BF(BF_UnpinBlock(bl));
                BF_Block_Destroy(&bl);
            }

            
            dataChild->records = malloc((dataChild->maxRecords) * sizeof(Record));
            memset(dataChild->records, 0, sizeof(Record)*(dataChild->maxRecords));
            memcpy(&dataChild->records[0], &record, sizeof(Record));
        }
        else if (dataChild->numOfRecords < bplus_info->maxRecordsPerBlock)
        {
            insertIntoLeaf(dataChild, record);
        }
        else
        {
            splitLeafNode(leafNode, dataChild, record, bplus_info); 
        }
       
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
    }

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return 0;
}

int findLeafNode(int rootID, int key, int fd)
{
    // if the root is leaf return it
    // because we will insert the record into it
    BF_Block *block;
    BF_Block_Init(&block);
    BF_GetBlock(fd, rootID, block);

    BP_IndexNode *root = (BP_IndexNode *)BF_Block_GetData(block);

    if (root->isLeaf)
    { 
        for (int idx = 0; idx <= root->numKeys; idx++)
        {

            if(root->children[idx] != 0)
            {
                //We have to get a new block in our file
                BF_Block* child;
                BF_Block_Init(&child);
                BF_GetBlock(fd, root->children[idx], child);

                BP_DataNode* dataChild = (BP_DataNode*)BF_Block_GetData(child);

                for(int i = 0; i < dataChild->numOfRecords; i++)
                {
                    if(key == dataChild->records[i].id)
                    {
                        //unpin the child
                        CALL_BF(BF_UnpinBlock(child));
                        BF_Block_Destroy(&child);

                        //unpin the current block that it is our root
                        CALL_BF(BF_UnpinBlock(block));
                        BF_Block_Destroy(&block);

                        return 0;
                    }
                }

                CALL_BF(BF_UnpinBlock(child));
                BF_Block_Destroy(&child);
            }
        }
        
        //unpin the current block that it is our root
        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);

        return rootID;
    }

    // if the root is not a leaf node we have to traverse the tree
    for (int idx = 0; idx < root->numKeys; idx++)
    {
        
        if (key == root->keys[idx])
        {
            // we have already inserted that key in our tree
            //unpin the current block that it is our root
            CALL_BF(BF_UnpinBlock(block));
            BF_Block_Destroy(&block);
            
            return 0;
        }

        if (key < root->keys[idx])
        {
            // we have to go the left child
            //we have to get the get the block
            
            int nextID = root->children[idx];

            //unpin the current block that it is our root
            CALL_BF(BF_UnpinBlock(block));
            BF_Block_Destroy(&block);

            return findLeafNode(nextID, key, fd);
        }
    }

    // if we reach here we have to go to the last child
    //we have to get the get the block
    int lastID = root->children[root->numKeys];

    //unpin the current block that it is our root
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return findLeafNode(lastID, key, fd);
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
    int middle = leaf->numOfRecords / 2;

    //see which key is going to be the middle one
    //depended on where the new key goes
    Record arrayOfRecords[leaf->numOfRecords + 1];
    memset(arrayOfRecords, 0, sizeof(Record)*(leaf->numOfRecords+1));

    int counter = 0;
    for(int idx = 0; idx < leaf->numOfRecords + 1; idx++)
    {
        if(counter == leaf->numOfRecords)
        {
            //if we end here the new key is the larger one
            memcpy(&arrayOfRecords[counter], &record, sizeof(Record));
            break;
        }


        if(record.id > leaf->records[counter].id)
            memcpy(&arrayOfRecords[idx], &leaf->records[counter], sizeof(Record));

        if(record.id < leaf->records[counter].id)
        {
            if(idx == counter)
            {
                memcpy(&arrayOfRecords[idx], &record, sizeof(Record));
                continue;
            }
            else
                memcpy(&arrayOfRecords[idx], &leaf->records[counter], sizeof(Record));

        }

        counter++;
    }

    //we have to create a new leaf node
    //so that means that we have to allocate a new block
    BF_Block *newBlock;
    BF_Block_Init(&newBlock);
    BF_AllocateBlock(bplus_info->fd, newBlock); // Allocate a new block for the new leaf
    BP_DataNode *newLeaf = (BP_DataNode *)BF_Block_GetData(newBlock);

    //+1 because now we are adding the new key from the start into the new leaf (split the old leaf)
    newLeaf->numOfRecords = leaf->numOfRecords - middle + 1;
    newLeaf->maxRecords = bplus_info->maxRecordsPerBlock;
    newLeaf->blockID = ++bplus_info->lastBlockID;
    bplus_info->totalBlocks++;

    newLeaf->nextBlockID = leaf->nextBlockID;
    leaf->nextBlockID = newLeaf->blockID;

    newLeaf->records = malloc((newLeaf->maxRecords) * sizeof(Record));
    memset(newLeaf->records, 0, sizeof(Record)*(newLeaf->maxRecords));

    //we have to copy the records to the new leaf
    for (int i = 0; i < newLeaf->numOfRecords; i++)
    {
        memcpy(&newLeaf->records[i], &arrayOfRecords[middle + i], sizeof(Record));
    }

    //we have to update the old leaf
    leaf->numOfRecords = middle;

    for(int idx = 0; idx < leaf->maxRecords; idx++)
    {
        if(idx <= middle)
            memcpy(&leaf->records[idx], &arrayOfRecords[idx], sizeof(Record));
        else
            memset(&leaf->records[idx], 0, sizeof(Record));
    }

    //we have to insert the new leaf in the parent node and keep the keys sorted
    //if parent node is full we have to split it -> splitIndexNode deals with it
    if(parentNode->numKeys == parentNode->maxKeys)
    {
        splitIndexNode(parentNode, newLeaf->records[0].id, newLeaf->blockID, bplus_info);

        BF_Block_SetDirty(newBlock);  // Mark the new leaf node block as dirty
        BF_UnpinBlock(newBlock);
        BF_Block_Destroy(&newBlock);
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
        parentNode->children[i+1] = parentNode->children[i];
        parentNode->children[i] = 0;

    }

    //we insert the new key
    parentNode->keys[idx] = newLeaf->records[0].id;

    //we insert the new leaf
    parentNode->children[idx + 1] = newLeaf->blockID;
    parentNode->numKeys++;

    BF_Block_SetDirty(newBlock);  // Mark the new leaf node block as dirty
    BF_UnpinBlock(newBlock);
    BF_Block_Destroy(&newBlock);
}

void splitIndexNode(BP_IndexNode *oldNode, int key, int BlockId, BPLUS_INFO *bplus_info)
{
    //now we have to split the index node
    //we have to find the middle key
    //we have to create a new index node
    //we have to copy the keys to the new index node
    //we have to insert the new key in the correct index node
    //we have to insert the new index node in the parent node, keeping the keys sorted

    int middle = oldNode->numKeys / 2;

    //we have to create a new index node
    BF_Block* block;
    BF_Block_Init(&block);
    BF_AllocateBlock(bplus_info->fd, block);
    BP_IndexNode *newNode = (BP_IndexNode*) BF_Block_GetData(block);

    //initialize new index nodes values
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
    newNode->children = malloc((newNode->maxKeys + 1) * sizeof(int));
    memset(newNode->children, 0, (newNode->maxKeys + 1) * sizeof(int));
    newNode->parentID = oldNode->parentID;
    newNode->blockID = ++bplus_info->lastBlockID;
    bplus_info->totalBlocks++;

    int middlekey;

    int order[bplus_info->sizeOfEurethrio + 1];
    int orderChild[bplus_info->sizeOfEurethrio + 2];
    int i = 0;
    int j = 0;
    orderChild[0] = oldNode->children[0];

    //figures out the position the new key, block pair belong to
    while (i < oldNode->numKeys)
    {
        if (key > oldNode->keys[i])
        {
            order[j] = oldNode->keys[i];
            orderChild[j + 1] = oldNode->children[i + 1];
            i++;
            j++;
        }
        else
        {
            order[j] = key;
            orderChild[j + 1] = BlockId;
            j++;
            break;
        }
    }

    if(i == oldNode->numKeys)
    {
        order[j] = key;
        orderChild[j + 1] = BlockId;
        j++;

    }

    while (i < oldNode->numKeys)
    {
        order[j] = oldNode->keys[i];
        orderChild[j+1] = oldNode->children[i+1];
        i++;
        j++;
    }
    middlekey = order[middle];

    // we have to copy the keys to the new index node
    for (int i = 0; i < newNode->numKeys; i++)
    {
        newNode->keys[i] = order[middle + 1 + i];
        newNode->children[i] = orderChild[middle + 1 + i];
    }
    newNode->children[newNode->numKeys] = orderChild[middle + 1 + newNode->numKeys];

    //we have to fix the old node
    for (int i = 0; i < oldNode->maxKeys; i++)
    {
        if (i < middle)
        {
            oldNode->keys[i] = order[i];
            oldNode->children[i] = orderChild[i];
        }
        else
        {
            oldNode->keys[i] = 0;
            if (i == middle)
            {
                oldNode->children[i] = orderChild[i];
            }
            else
            {
                oldNode->children[i] = 0;
            }
        }
    }
    oldNode->numKeys = middle;

    for (int i= 0; i <= newNode->numKeys; i++){
        BF_Block* bl;
        BF_Block_Init(&bl);
        BF_GetBlock(bplus_info->fd, newNode->children[i], bl);
        BP_IndexNode* child = (BP_IndexNode*)BF_Block_GetData(bl);
        child->parentID = newNode->blockID;

        BF_Block_SetDirty(bl);
        BF_UnpinBlock(bl);
        BF_Block_Destroy(&bl);
    }

    //we have to fix the parent node of the two index nodes
    if(oldNode->parentID == 0)
    {
        //if the parent doesn't exist -> the case where we hane to split the root
        BF_Block* bl;
        BF_Block_Init(&bl);
        BF_AllocateBlock(bplus_info->fd, bl);
        BP_IndexNode *newParent = (BP_IndexNode *)BF_Block_GetData(bl);

        //then we have to initialise the new parent
        newParent->isLeaf = false;
        newParent->numKeys = 1;
        newParent->maxKeys = oldNode->maxKeys;
        newParent->keys = malloc(newParent->maxKeys * sizeof(int));
        memset(newParent->keys, 0, newParent->maxKeys * sizeof(int));
        newParent->children = malloc((newParent->maxKeys + 1) * sizeof(int*));
        memset(newParent->children, 0, (newParent->maxKeys + 1) * sizeof(int*));
        newParent->parentID = 0;
        newParent->blockID = ++bplus_info->lastBlockID;
        bplus_info->totalBlocks++;

        newParent->keys[0] = middlekey;
        newParent->children[0] = oldNode->blockID;
        newParent->children[1] = newNode->blockID;

        oldNode->parentID = newParent->blockID;
        newNode->parentID = newParent->blockID;

        bplus_info->rootID= newParent->blockID;
            
        BF_Block_SetDirty(bl);
        BF_UnpinBlock(bl);
        BF_Block_Destroy(&bl);
    }
    else
    {
        //we get the parent node
        BF_Block* bl;
        BF_Block_Init(&bl);
        BF_GetBlock(bplus_info->fd, oldNode->parentID, bl);
        BP_IndexNode *Parent = (BP_IndexNode *)BF_Block_GetData(bl);
        if(Parent->numKeys == Parent->maxKeys)
        {
            splitIndexNode(Parent, middlekey, newNode->blockID, bplus_info);
            
        }
        else
        {
            insertIntoIndexNode(Parent, middlekey, newNode->blockID, bplus_info);
        }

        BF_Block_SetDirty(bl);
        BF_UnpinBlock(bl);
        BF_Block_Destroy(&bl);
    }

    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
    return;
}

void insertIntoIndexNode(BP_IndexNode *parentNode, int key, int BlockId, BPLUS_INFO *bplus_info)
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
        parentNode->children[i+1] = parentNode->children[i];
        parentNode->children[i] = 0;
    }

    //we insert the new key
    parentNode->keys[idx] = key;

    //we insert the new leaf
    parentNode->children[idx + 1] = BlockId;
    parentNode->numKeys++;
}

BP_IndexNode *SearchForLeaf(int rootID, int key, int fd)
{
    BF_Block *block;
    BF_Block_Init(&block);
    BF_GetBlock(fd, rootID, block);

    BP_IndexNode *root = (BP_IndexNode *)BF_Block_GetData(block);

    // if the root is leaf return it
    // because we will insert the record into it
    if (root->isLeaf)
    {   
        BF_UnpinBlock(block);
        BF_Block_Destroy(&block);
        return root;
    }

    // if the root is not a leaf node we have to traverse the tree
    for (int idx = 0; idx < root->numKeys; idx++)
    {
        if (key < root->keys[idx])
        {
            // we have to go the right child
            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
            return SearchForLeaf(root->children[idx], key, fd);
        }
    }

    // if we reach here we have to go to the last child
    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
    return SearchForLeaf(root->children[root->numKeys], key, fd);
}

int BP_GetEntry(int file_desc, BPLUS_INFO *bplus_info, int value, Record **record)
{

    BP_IndexNode *leafNode = SearchForLeaf(bplus_info->rootID, value, file_desc);
    //we have found the leafnode that the record may be

    for(int idx = 0; idx <= leafNode->numKeys; idx++)
    {
        if(leafNode->children[idx] != 0)
        {
            BF_Block* block;
            BF_Block_Init(&block);
            BF_GetBlock(file_desc, leafNode->children[idx], block);
            BP_DataNode* dataChild = (BP_DataNode*)BF_Block_GetData(block);
            for(int i = 0; i < dataChild->numOfRecords; i++)
            {
                if(value == dataChild->records[i].id)
                {
                    *record = &dataChild->records[i];

                
                    BF_UnpinBlock(block);
                    BF_Block_Destroy(&block);
                    return 0;
                }
            }
            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
        }
        else
            break;
    }

    //if not found
    *record = NULL;
    return -1;
}