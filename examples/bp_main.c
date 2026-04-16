#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "bp_file.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"

#define RECORDS_NUM 800 // you can change it if you want
#define FILE_NAME "data.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

void insertEntries();
void findEntries();
int our_main();

void print_bfs_Tree(BPLUS_INFO* bp_info, BP_IndexNode* root)
{
  // printf("blockID: %d\n", root->blockID);
  if(root->isLeaf){
    // printf("number of keys in leafs: %d\n", root->numKeys);
    for(int j = 0; j < root->numKeys; j++){
      BF_Block* block;
      BF_Block_Init(&block);
      BF_GetBlock(bp_info->fd, root->children[j], block);  // Fetch the block using leaf[i] (which is a block ID)
      BP_DataNode* dataChild = (BP_DataNode*)BF_Block_GetData(block);  // Get the data from the block
      // printf("Leaf blockID: %d\n", dataChild->blockID);
      // Print the records in the leaf node
      for (int k = 0; k < dataChild->numOfRecords; k++)
      {
        printf("Record id: %d\n", dataChild->records[k].id);
      }
      BF_UnpinBlock(block);
      BF_Block_Destroy(&block);
      printf("Leaf key: %d\n", root->keys[j]);
    }
      // printf("leaf blockID: %d\n", root->children[root->numKeys]);
      BF_Block* block;
      BF_Block_Init(&block);
      BF_GetBlock(bp_info->fd, root->children[root->numKeys], block);  // Fetch the block using leaf[i] (which is a block ID)
      BP_DataNode* dataChild = (BP_DataNode*)BF_Block_GetData(block);  // Get the data from the block
      // Print the records in the leaf node
      for (int j = 0; j < dataChild->numOfRecords; j++)
      {
        printf("Record id: %d\n", dataChild->records[j].id);
      }
      BF_UnpinBlock(block);
      BF_Block_Destroy(&block);
  }
  else{
    // printf("number of keys: %d\n", root->numKeys);
    for(int j = 0; j < root->numKeys; j++){
      BF_Block* block;
      BF_Block_Init(&block);
      BF_GetBlock(bp_info->fd, root->children[j], block);  // Fetch the block using leaf[i] (which is a block ID)
      BP_IndexNode* indexChild = (BP_IndexNode*)BF_Block_GetData(block);  // Get the data from the block
      print_bfs_Tree(bp_info, indexChild);
      BF_UnpinBlock(block);
      BF_Block_Destroy(&block);
      if(root->blockID == bp_info->rootID){
        printf("Root key: %d\n", root->keys[j]);
      }
      else{
        printf("Index key: %d\n", root->keys[j]);
      }
    }
      BF_Block* block;
      BF_Block_Init(&block);
      BF_GetBlock(bp_info->fd, root->children[root->numKeys], block);  // Fetch the block using leaf[i] (which is a block ID)
      BP_IndexNode* indexChild = (BP_IndexNode*)BF_Block_GetData(block);  // Get the data from the block
      print_bfs_Tree(bp_info, indexChild);
      BF_UnpinBlock(block);
      BF_Block_Destroy(&block); 
  }
}

int main()
{

  // insertEntries();

  // findEntries();
  
  our_main();

  return 0;
}

int our_main()
{
  BF_Init(LRU);
  BP_CreateFile(FILE_NAME);

  int file_desc;

  BPLUS_INFO* bp_info = BP_OpenFile(FILE_NAME, &file_desc);
  
  Record record;

  for(int idx = 0; idx < 2000; idx++)
  {
    record = randomRecord();

    // printf("inserting %d\n", record.id);
    BP_InsertEntry(file_desc, bp_info, record);
  }

  BF_Block* block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(file_desc, bp_info->rootID, block));

  BP_IndexNode* root = (BP_IndexNode*)BF_Block_GetData(block);

  //FUNCTION TO PRINT BPLUS TREE IF YOU WANT, UNCOMMENT IT
  print_bfs_Tree(bp_info, root);

  Record tmpRec;
  Record* result=&tmpRec;
  
  //searching for a value we know it exists
  int id=5; 
  printf("Searching for: %d\n",id);
  BP_GetEntry( file_desc, bp_info, id, &result);

  if(result!=NULL)
    printRecord(*result);
  else
    printf("Record not found in bplus tree!\n");

  //searching for a value we know it does not exist
  id=1500; 
  printf("Searching for: %d\n",id);
  BP_GetEntry( file_desc, bp_info, id, &result);

  if(result!=NULL)
    printRecord(*result);
  else
    printf("Record not found in bplus tree!\n");

  BP_CloseFile(file_desc, bp_info);
  BF_Close();

  return 0;
}

void insertEntries()
{
  BF_Init(LRU);
  BP_CreateFile(FILE_NAME);

  int file_desc;
  BPLUS_INFO* info = BP_OpenFile(FILE_NAME, &file_desc);

  Record record;
  for (int i = 0; i < RECORDS_NUM; i++)
  {
    record = randomRecord();
    BP_InsertEntry(file_desc,info, record);
  }

  BP_CloseFile(file_desc,info);
  BF_Close();
}

void findEntries()
{
  int file_desc;
  BPLUS_INFO* info;

  BF_Init(LRU);
  info=BP_OpenFile(FILE_NAME, &file_desc);

  Record tmpRec;  //Αντί για malloc
  Record* result=&tmpRec;

  int id = 340; 
  printf("Searching for: %d\n",id);
  BP_GetEntry( file_desc,info, id,&result);

  if(result!=NULL)
    printRecord(*result);

  BP_CloseFile(file_desc,info);
  BF_Close();
}