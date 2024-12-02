#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "bp_file.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"

#define RECORDS_NUM 200 // you can change it if you want
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

int main()
{
  
  BF_Init(LRU);
  BP_CreateFile(FILE_NAME);

  int file_desc;

  BPLUS_INFO* bp_info = BP_OpenFile(FILE_NAME, &file_desc);
  
  Record record;

  for(int idx = 0; idx < 46; idx++)
  {
    record = randomRecord();
    BP_InsertEntry(file_desc, bp_info, record);
  }

  BF_Block* block;
  BF_Block_Init(&block);
  CALL_OR_DIE(BF_GetBlock(file_desc, 1, block));

  BP_IndexNode* root = (BP_IndexNode*)BF_Block_GetData(block);
  
  // print children
  for(int idx = 0; idx < root->numKeys; idx++)
  {
    printf("%d, keys: %d \n", root->keys[idx], root->numKeys);

    for(int i = 0; i < root->leaf[idx]->numOfRecords; i++)
    {
      printf("Record: %d\n", root->leaf[idx]->records[i].id);
    }
    puts("-------------------------\n");
  }

  if(root->leaf[root->numKeys] != NULL)
  {

    for(int idx = 0; idx < root->leaf[root->numKeys]->numOfRecords; idx++)
    {
        printf("Record2: %d\n", root->leaf[root->numKeys]->records[idx].id);

    }

  }




  // insertEntries();
  // findEntries();

  ////////////////////////////////////////////////
  

  BP_CloseFile(file_desc, bp_info);
  BF_Close();
}

void insertEntries(){
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

void findEntries(){
  int file_desc;
  BPLUS_INFO* info;

  BF_Init(LRU);
  info=BP_OpenFile(FILE_NAME, &file_desc);

  Record tmpRec;  //Αντί για malloc
  Record* result=&tmpRec;
  
  int id=159; 
  printf("Searching for: %d\n",id);
  BP_GetEntry( file_desc,info, id,&result);
  if(result!=NULL)
    printRecord(*result);

  BP_CloseFile(file_desc,info);
  BF_Close();
}