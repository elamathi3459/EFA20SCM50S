#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <math.h>

#include "storage_mgr.h"
#include "dberror.h"

FILE *fp;

extern void initStorageManager(void)
{
	// Initialising file pointer i.e. storage manager.
	printf("The Storage Manager is Initialized\n");
	fp = NULL;
}
RC createPageFile(char *fileName)
{
	char *pg;

	FILE *fp = fopen(fileName, "w+"); //Open File in write mode

	//this will be the page to store the data
	pg = (char *)calloc(PAGE_SIZE, sizeof(char));

	/* calloc() allocates the memory and also initializes the allocated memory block to zero.
      calloc() takes two arguments, 1. Number of blocks to be allocated 2. Size of each block.
   */

	fwrite(pg, PAGE_SIZE, 1, fp);

	//memset will allocate memory block by \0 if the file exists
	memset(pg, '\0', PAGE_SIZE);

	//free the memory to avoid leaks
	//yes free(pgInfo);
	free(pg);

	// closing the file
	fclose(fp);

	return RC_OK;
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
	// Opening file stream in read mode. 'r' mode creates an empty file for reading only.
	fp = fopen(fileName, "r");

	// Checking if file was successfully opened.
	if (fp == NULL)
	{
		return RC_FILE_NOT_FOUND;
	}
	else
	{
		// Updating file handle's filename and set the current position to the start of the page.
		fHandle->fileName = fileName;
		fHandle->curPagePos = 0;

		struct stat fileInfo;
		if (fstat(fileno(fp), &fileInfo) < 0)
			return RC_ERROR;
		fHandle->totalNumPages = fileInfo.st_size / PAGE_SIZE;

		// Closing file stream so that all the buffers are flushed.
		fclose(fp);
		return RC_OK;
	}
}
RC closePageFile(SM_FileHandle *fHandle)
{
	int fileClosed;
	if (fHandle == NULL)
	{
		return RC_FILE_HANDLE_NOT_INIT;
	}

	if (fopen(fHandle->fileName, "r") == NULL)
	{
		return RC_FILE_NOT_FOUND;
	}

	fileClosed = fclose(fHandle->mgmtInfo);
	if (fileClosed == 0)
	{
		return RC_OK;
	}
}

//Destroying a page files
RC destroyPageFile(char *fileName)
{
	int del;
	//if the file exists
	if (access(fileName, F_OK) == 0)
	{
		//  printf("Debug: Removing File\n");
		del = remove(fileName);
		return RC_OK;
	}
	//if the file doesn't exists
	else
	{
		// printf("Debug: Cannot Remove FIle as not found");
		return RC_FILE_NOT_FOUND;
	}
}

RC readBlock(int blockNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	RC READ_SIZE;
	RC MSG_RETURN;

	// if totalNumPages less than blockNum or blockNum less than zero then throw an error
	if ((fHandle->totalNumPages - 1) < blockNum || blockNum < 0)
	{
		//  printf("Debug: Error in File Handle Condition\n");
		MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
	}
	else
	{

		FILE *fp = fopen(fHandle->fileName, "r"); //open the file in read mode
		fseek(fp, (blockNum)*PAGE_SIZE, SEEK_SET);

		READ_SIZE = fread(memPage, sizeof(char), PAGE_SIZE, fp); //read the page into memPage
		fHandle->curPagePos = blockNum;							 //update the curPagePos to the recently read page
		// if the read block is not within the Page size limit, throw an error
		if (READ_SIZE < PAGE_SIZE || READ_SIZE > PAGE_SIZE)
		{

			// printf("Debug: Error in page size limit for read Block\n");
			MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
		}
		else
			MSG_RETURN = RC_OK;

		fclose(fp); //close file ptr
	}

	return MSG_RETURN;
}

//   This method returns the curPagePos of the file
int getBlockPos(SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}

// This method reads the first block of the file
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	RC READ_SIZE;
	RC MSG_RETURN;

	// if file not found then return error msg
	if (fHandle == NULL)
	{
		MSG_RETURN = RC_FILE_NOT_FOUND;
	}
	else
	{
		// if totalNumPages less than zero then return error
		if (fHandle->totalNumPages - 1 < 0)
		{
			MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
		}
		else
		{
			FILE *fp = fopen(fHandle->fileName, "r");
			fseek(fp, 0, SEEK_SET);
			READ_SIZE = fread(memPage, sizeof(char), PAGE_SIZE, fp);
			fHandle->curPagePos = 0;
			//If the read block is less than 0 or exceeds the page size limit, throw an error
			if (READ_SIZE > PAGE_SIZE || READ_SIZE < 0)
			{
				MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
			}
			else
				MSG_RETURN = RC_OK;
			fclose(fp);
		}
	}
	return MSG_RETURN;
}

// This method reads the previous block of the file
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	RC READ_SIZE;
	RC MSG_RETURN;

	// if file not found then return error msg
	if (fHandle == NULL)
	{
		MSG_RETURN = RC_FILE_NOT_FOUND;
	}
	else
	{
		if ((fHandle->totalNumPages - 1) < fHandle->curPagePos || fHandle->curPagePos <= 0)
		{
			MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
		}
		else
		{
			FILE *fp = fopen(fHandle->fileName, "r");
			fseek(fp, (fHandle->curPagePos - 1) * PAGE_SIZE, SEEK_SET);
			READ_SIZE = fread(memPage, sizeof(char), PAGE_SIZE, fp);
			fHandle->curPagePos = fHandle->curPagePos - 1;
			if (READ_SIZE > PAGE_SIZE || READ_SIZE < 0)
			{
				MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
			}
			else
				MSG_RETURN = RC_OK;
			fclose(fp);
		}
	}
	return MSG_RETURN;
}

// This method reads the current block of the file
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	RC READ_SIZE;
	RC MSG_RETURN;

	// if file not found then return error msg
	if (fHandle == NULL)
	{
		MSG_RETURN = RC_FILE_NOT_FOUND;
	}
	else
	{
		if ((fHandle->totalNumPages - 1) < fHandle->curPagePos || fHandle->curPagePos < 0)
		{
			MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
		}
		else
		{
			FILE *fp = fopen(fHandle->fileName, "r");
			fseek(fp, (fHandle->curPagePos) * PAGE_SIZE, SEEK_SET);
			READ_SIZE = fread(memPage, sizeof(char), PAGE_SIZE, fp);
			if (READ_SIZE > PAGE_SIZE || READ_SIZE < 0)
			{
				MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
			}
			else
				MSG_RETURN = RC_OK;
			fclose(fp);
		}
	}
	return MSG_RETURN;
}

//This method is used to read the next block (i.e, the current block defined in the fHandle->curPagePos) into memPage
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	RC READ_SIZE;
	RC MSG_RETURN;

	// if file not found then return error msg
	if (fHandle == NULL)
	{
		MSG_RETURN = RC_FILE_NOT_FOUND;
	}
	else
	{
		if ((fHandle->totalNumPages - 2) < fHandle->curPagePos || fHandle->curPagePos < 0)
		{
			//printf("Debug: Error on fileHander");
			MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
		}
		else
		{
			FILE *fp = fopen(fHandle->fileName, "r");
			fseek(fp, (fHandle->curPagePos + 1) * PAGE_SIZE, SEEK_SET);
			READ_SIZE = fread(memPage, sizeof(char), PAGE_SIZE, fp);
			fHandle->curPagePos = fHandle->curPagePos + 1;
			if (READ_SIZE > PAGE_SIZE || READ_SIZE < 0)
			{
				MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
			}
			else
				MSG_RETURN = RC_OK;
			fclose(fp);
		}
	}
	return MSG_RETURN;
}

// This method reads the last block of the file
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	RC READ_SIZE;
	RC MSG_RETURN;

	// if file not found then return error msg
	if (fHandle == NULL)
	{
		MSG_RETURN = RC_FILE_NOT_FOUND;
	}
	else
	{
		FILE *fp = fopen(fHandle->fileName, "r");
		fseek(fp, (fHandle->totalNumPages - 1) * PAGE_SIZE, SEEK_SET);
		READ_SIZE = fread(memPage, sizeof(char), PAGE_SIZE, fp);
		fHandle->curPagePos = fHandle->totalNumPages - 1;
		if (READ_SIZE > PAGE_SIZE || READ_SIZE < 0)
		{
			MSG_RETURN = RC_READ_NON_EXISTING_PAGE;
		}
		else
			MSG_RETURN = RC_OK;
		fclose(fp);
	}
	return MSG_RETURN;
}

extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// Checking if the pageNumber parameter is less than Total number of pages and less than 0, then return respective error code
	if (pageNum > fHandle->totalNumPages || pageNum < 0)
		return RC_WRITE_FAILED;

	// Opening file stream in read & write mode. 'r+' mode opens the file for both reading and writing.
	fp = fopen(fHandle->fileName, "r+");

	// Checking if file was successfully opened.
	if (fp == NULL)
		return RC_FILE_NOT_FOUND;

	int startPosition = pageNum * PAGE_SIZE;

	if (pageNum == 0)
	{
		//Writing data to non-first page
		fseek(fp, startPosition, SEEK_SET);
		int i;
		for (i = 0; i < PAGE_SIZE; i++)
		{
			// Checking if it is end of file. If yes then append an enpty block.
			if (feof(fp)) // check file is ending in between writing
				appendEmptyBlock(fHandle);
			// Writing a character from memPage to page file
			fputc(memPage[i], fp);
		}

		// Setting the current page position to the cursor(pointer) position of the file stream
		fHandle->curPagePos = ftell(fp);

		// Closing file stream so that all the buffers are flushed.
		fclose(fp);
	}
	else
	{
		// Writing data to the first page.
		fHandle->curPagePos = startPosition;
		fclose(fp);
		writeCurrentBlock(fHandle, memPage);
	}
	return RC_OK;
}

extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// Opening file stream in read & write mode. 'r+' mode opens the file for both reading and writing.
	fp = fopen(fHandle->fileName, "r+");

	// Checking if file was successfully opened.
	if (fp == NULL)
		return RC_FILE_NOT_FOUND;

	// Appending an empty block to make some space for the new content.
	appendEmptyBlock(fHandle);

	// Initiliazing file pointer
	fseek(fp, fHandle->curPagePos, SEEK_SET);

	// Writing memPage contents to the file.
	fwrite(memPage, sizeof(char), strlen(memPage), fp);

	// Setting the current page position to the cursor(pointer) position of the file stream
	fHandle->curPagePos = ftell(fp);

	// Closing file stream so that all the buffers are flushed.
	fclose(fp);
	return RC_OK;
}

extern RC appendEmptyBlock(SM_FileHandle *fHandle)
{
	// Creating an empty page of size PAGE_SIZE bytes
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));

	// Moving the cursor (pointer) position to the begining of the file stream.
	// And the seek is success if fseek() return 0
	int isSeekSuccess = fseek(fp, 0, SEEK_END);

	if (isSeekSuccess == 0)
	{
		// Writing an empty page to the file
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, fp);
	}
	else
	{
		free(emptyBlock);
		return RC_WRITE_FAILED;
	}

	// De-allocating the memory previously allocated to 'emptyPage'.
	// This is optional but always better to do for proper memory management.
	free(emptyBlock);

	// Incrementing the total number of pages since we added an empty black.
	fHandle->totalNumPages++;
	return RC_OK;
}

extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
	// Opening file stream in append mode. 'a' mode opens the file to append the data at the end of file.
	fp = fopen(fHandle->fileName, "a");

	if (fp == NULL)
		return RC_FILE_NOT_FOUND;

	// Checking if numberOfPages is greater than totalNumPages.
	// If that is the case, then add empty pages till numberofPages = totalNumPages
	while (numberOfPages > fHandle->totalNumPages)
		appendEmptyBlock(fHandle);

	// Closing file stream so that all the buffers are flushed.
	fclose(fp);
	return RC_OK;
}