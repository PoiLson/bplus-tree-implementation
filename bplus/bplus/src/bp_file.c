#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include <bp_datanode.h>
#include <stdbool.h>

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return BP_ERROR;     \
    }                         \
  }

BF_Block* curBlock = NULL;
void* curdata = NULL;
BPLUS_INFO* curBlockInfo = NULL;
int curBlockpos = 0;

BF_Block* infoblock = NULL;
void* infodata = NULL;

int BP_CreateFile(char *fileName)
{
  int fileDesc;
  BF_Block* block;

  //we create the file
  CALL_BF(BF_CreateFile(fileName));
  //we open it
  CALL_BF(BF_OpenFile(fileName, &fileDesc));

  //allocate the first block for metadata
  BF_Block_Init(&block);

  //store it inside the file
  CALL_BF(BF_AllocateBlock(fileDesc, block));

  //prepare the metadata (HP_info) for the first block of the heap file
  BPLUS_INFO bpInfo;
  strcpy(bpInfo.kindOfFile, "B+ Tree");
  bpInfo.totalBlocks = 0;
  bpInfo.maxRecordsPerBlock = (BF_BLOCK_SIZE - sizeof(BPLUS_INFO)) / sizeof(Record);
  bpInfo.totalRecords = 0;
  bpInfo.lastBlockID = 0;

  //memcpy the metadata into the first block
  char *data = BF_Block_GetData(block);
  memcpy(data, &bpInfo, sizeof(BPLUS_INFO));
  BF_Block_SetDirty(block);

  //Finally, we unpin the block and close the file
  CALL_BF(BF_UnpinBlock(block));

  //destroy the block
  BF_Block_Destroy(&block);
  CALL_BF(BF_CloseFile(fileDesc));

  return 0;
}

BPLUS_INFO* BP_OpenFile(char *fileName, int *file_desc)
{
  BPLUS_INFO* bpInfo;    
  BF_OpenFile(fileName, file_desc);

  BF_Block_Init(&infoblock);

  BF_GetBlock(*file_desc, 0, infoblock); //it pins the block in the memory
  infodata = BF_Block_GetData(infoblock); //get the data fo the first block

  bpInfo = (BPLUS_INFO*) infodata;
  // checking that the data are ok!
  printf("Kind: %s\n", bpInfo->kindOfFile);
  printf("totalBlocks: %d\n", bpInfo->totalBlocks);
  printf("maxRecordsPerBlock: %d\n", bpInfo->maxRecordsPerBlock);
  printf("totalRecords: %d\n", bpInfo->totalRecords);
  printf("lastBlockID: %d\n", bpInfo->lastBlockID);

  //we return the info we got from the first block
  return bpInfo;
}

int BP_CloseFile(int file_desc, BPLUS_INFO* info)
{  
  if(curBlock != NULL)
  {
    CALL_BF(BF_UnpinBlock(curBlock));
    BF_Block_Destroy(&curBlock);
  }

  CALL_BF(BF_UnpinBlock(infoblock)); //we need to unpin the block since we no longer need it
  BF_Block_Destroy(&infoblock);

  CALL_BF(BF_CloseFile(file_desc));
  infodata = NULL;
  info = NULL;
  curdata = NULL;

  return 0;
}

int BP_InsertEntry(int file_desc, BPLUS_INFO *bplus_info, Record record)
{
    // Initialize variables for traversal
    int currentBlockId = bplus_info->lastBlockID;
    BF_Block *block;
    char *data;
    BF_Block_Init(&block);

    int flag = 0;

    // Traverse to the correct leaf node
    while (1) {
        CALL_BF(BF_GetBlock(file_desc, currentBlockId, block));
        data = BF_Block_GetData(block);

        // Check if it's a leaf node
        bool isLeaf;
        memcpy(&isLeaf, data, sizeof(bool));

        if (isLeaf) {
            break; // We've reached the leaf node
        }

        // If it's an index node, find the appropriate child
        BPLUS_INDEX_NODE *indexNode = (BPLUS_INDEX_NODE *)data;
        flag = 0;
        for (int i = 0; i < indexNode->numOfKeys; i++) {
            if (record.id < indexNode->keys[i]) {
                currentBlockId = indexNode->children[i];
                BF_UnpinBlock(block);
                flag = 1;
                break;
            }
        }

        if(flag == 0)
        {
          // If no key matched, take the last child
          currentBlockId = indexNode->children[indexNode->numOfKeys];
          BF_UnpinBlock(block);
        }

    }

    // Insert the record into the data node
    BPLUS_DATA_NODE *dataNode = (BPLUS_DATA_NODE *)data;

    // Check for duplicate records
    for (int i = 0; i < dataNode->numOfRecords; i++) {
        if (dataNode->records[i].id == record.id) {
            printf("Error: Duplicate key detected.\n");
            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
            return BP_ERROR;
        }
    }

    // Insert if there's space
    if (dataNode->numOfRecords < bplus_info->maxRecordsPerBlock) {
        // Find the correct position to insert in sorted order
        int i = dataNode->numOfRecords - 1;
        while (i >= 0 && dataNode->records[i].id > record.id) {
            dataNode->records[i + 1] = dataNode->records[i];
            i--;
        }
        dataNode->records[i + 1] = record;
        dataNode->numOfRecords++;

        BF_Block_SetDirty(block);
        BF_UnpinBlock(block);
        BF_Block_Destroy(&block);
        return currentBlockId;
    }
    // Handle node splitting if the data node is full
    printf("Splitting node...\n");

    // Allocate a new data node for the split
    BF_Block *newBlock;
    BF_Block_Init(&newBlock);
    CALL_BF(BF_AllocateBlock(file_desc, newBlock));
    char *newData = BF_Block_GetData(newBlock);

    BPLUS_DATA_NODE *newDataNode = (BPLUS_DATA_NODE *)newData;
    newDataNode->isLeaf = true;
    newDataNode->numOfRecords = 0;

    // Split the records between the original and new nodes
    int mid = dataNode->numOfRecords / 2;
    for (int i = mid; i < dataNode->numOfRecords; i++) {
        newDataNode->records[newDataNode->numOfRecords++] = dataNode->records[i];
    }
    dataNode->numOfRecords = mid;

    // Update the linked list of data nodes
    newDataNode->nextPtr = dataNode->nextPtr;
    dataNode->nextPtr = newDataNode;

    // Promote the middle key to the parent
    int middleKey = newDataNode->records[0].id; // The middle key to promote

    // Check if there is a parent (handle split for parent)
    int parentBlockId = currentBlockId; // Initially the current block is the parent
    BPLUS_INDEX_NODE *parentNode = NULL;
    bool parentExists = false;

    // Check if there's a parent node (if not, create a new root)
    if (parentBlockId != bplus_info->lastBlockID) {
        // We have a parent node; find where to insert the middle key
        CALL_BF(BF_GetBlock(file_desc, parentBlockId, block));
        parentNode = (BPLUS_INDEX_NODE *)BF_Block_GetData(block);
        parentExists = true;
    }

    // If parent exists, insert the middle key into the parent
    if (parentExists) {
        // Shift keys in parent to make room for the new key
        int i = parentNode->numOfKeys - 1;
        while (i >= 0 && parentNode->keys[i] > middleKey) {
            parentNode->keys[i + 1] = parentNode->keys[i];
            parentNode->children[i + 2] = parentNode->children[i + 1];
            i--;
        }
        parentNode->keys[i + 1] = middleKey;
        parentNode->children[i + 2] = newBlock->blockId;
        parentNode->numOfKeys++;

        // Mark parent as dirty and unpin
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
    } else {
        // Create a new root (if there is no parent)
        printf("Creating a new root...\n");

        // Create a new block for the root
        BF_Block *newRootBlock;
        BF_Block_Init(&newRootBlock);
        CALL_BF(BF_AllocateBlock(file_desc, newRootBlock));
        BPLUS_INDEX_NODE *newRootNode = (BPLUS_INDEX_NODE *)BF_Block_GetData(newRootBlock);

        // Initialize the new root
        newRootNode->numOfKeys = 1;
        newRootNode->keys[0] = middleKey;
        newRootNode->children[0] = parentBlockId;
        newRootNode->children[1] = newBlock->blockId;

        // Set the new root block as the root of the tree
        bplus_info->lastBlockID = newRootBlock->blockId;

        // Mark the new root as dirty and unpin
        BF_Block_SetDirty(newRootBlock);
        CALL_BF(BF_UnpinBlock(newRootBlock));
        BF_Block_Destroy(&newRootBlock);
    }

    // Mark both blocks as dirty and unpin them
    BF_Block_SetDirty(block);
    BF_Block_SetDirty(newBlock);
    CALL_BF(BF_UnpinBlock(block));
    CALL_BF(BF_UnpinBlock(newBlock));
    BF_Block_Destroy(&block);
    BF_Block_Destroy(&newBlock);

    return BP_OK;
}

int BP_GetEntry(int file_desc,BPLUS_INFO *bplus_info, int value,Record** record)
{  
  *record=NULL;
  return 0;
}
