#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

SM_FileHandle fh;
typedef struct Page
{
	SM_PageHandle data; // Actual data of the page
	PageNumber pageNum; // An identification integer given to each page
	int dirtyBit; // Used to indicate whether the contents of the page has been modified by the client
	int fixCount; // Used to indicate the number of clients using that page at a given instance
	int hitNum;   // Used by LRU algorithm to get the least recently used page
	int refNum;
} PF;
int bSize=0,writecnt=0,clockPtr = 0,lfuPtr = 0,hit = 0,r=0;
//bSize means buffer size gives the max no.of page frame placed in the bufferPool
//r means rear Index which counts the no of pages from the disk

extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy,
		  void *stratData)
{
    int i=0;
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	// Reserver memory space = number of pages x space required for one page
	PF *pf = malloc(sizeof(PF) * numPages);
	// Buffersize is the total number of pages in memory or the buffer pool.
	bSize = numPages;
	// Intilalizing all pages in buffer pool. The values of fields (variables) in the page is either NULL or 0
	while(i < bSize)
	{
		pf[i].fixCount = 0;
		pf[i].hitNum = 0;
		pf[i].dirtyBit = 0;
		pf[i].pageNum = -1;
		pf[i].refNum = 0;
		pf[i].data = NULL;
		i++;
	}
	
	bm->mgmtData = pf;
	clockPtr=writecnt=lfuPtr = 0;
	
	return RC_OK;
	

}


RC FIFO(BM_BufferPool *const bm, PF *page)
{
    int currentPage;
	//printf("FIFO Started");
	PF *pf = (PF *) bm->mgmtData;

	int i=0, f;//f represents front index in queue
	f = r% bSize;//r represents rear index in queue

	// Interating through all the page frames in the buffer pool
	while(i<bSize)//for(i = 0; i < bSize; i++)
	{
	    i++;
		if(pf[f].fixCount == 0)
		{
			// If page in memory has been modified (dirtyBit = 1), then write page to disk
			if(pf[f].dirtyBit == 1)
			{
				openPageFile(bm->pageFile, &fh);
				writeBlock(pf[f].pageNum, &fh, pf[f].data);

				// Increase the writeCount which records the number of writes done by the buffer manager.
				writecnt++;
			}

			// Setting page frame's content to new page's content
			pf[f].dirtyBit = page->dirtyBit;
			pf[f].data = page->data;
			pf[f].fixCount = page->fixCount;
			pf[f].pageNum = page->pageNum;
			break;
		}
		else
		{
			// If the current page frame is being used by some client, we move on to the next location
			f++;
			currentPage=f%bSize;
			if(currentPage==0)
                f=0;
		}
	}
}
RC LFU(BM_BufferPool *const bm, PF *page)
{
	//printf("LFU Started");
	PF *pf = (PF *) bm->mgmtData;

	int i=0, temp, j=0, lfIndex, lfRef;//lfIndex returns the index which is used least and lfRef for it's reference
	lfIndex= lfuPtr;

	// Interating through all the page frames in the buffer pool
	while(i < bSize)
	{
	    temp=i;
	    i++;
		if(pf[lfIndex].fixCount == 0)
		{
			lfIndex = (lfIndex + temp) % bSize;
			lfRef = pf[lfIndex].refNum;
			break;
		}
	}

	i = (lfIndex + 1) % bSize;

	// Finding the page frame having minimum refNum (i.e. it is used the least frequent) page frame
	while(j < bSize)
	{
	    j++;
		if(pf[i].refNum < lfRef)
		{
			lfRef = pf[i].refNum;
			lfIndex = i;
		}
		i = (i + 1) % bSize;
	}

	// (dirtyBit = 1),If page in memory has been modified then write page to disk
	if(pf[lfIndex].dirtyBit == 1)
	{
		openPageFile(bm->pageFile, &fh);
		writeBlock(pf[lfIndex].pageNum, &fh, pf[lfIndex].data);

		//writeCount is incremented, which records the number of writes done by the buffer manager.
		writecnt++;
	}

	// Setting page frame's content to new page's content
	pf[lfIndex].dirtyBit = page->dirtyBit;
	pf[lfIndex].data = page->data;
	pf[lfIndex].fixCount = page->fixCount;
	pf[lfIndex].pageNum = page->pageNum;
	lfuPtr = lfIndex + 1;
}

// Defining LRU (Least Recently Used) function
RC LRU(BM_BufferPool *const bm, PF *page)
{
	PF *pf = (PF *) bm->mgmtData;
	int i=0, temp, lhIndex, lhNum;

	// Interating through all the page frames in the buffer pool.
	while(i < bSize)
	{
	    temp=i;
	    i++;
		// Finding page frame whose fixCount = 0 i.e. no client is using that page frame.
		if(pf[temp].fixCount == 0)
		{
			lhIndex = temp;
			lhNum = pf[temp].hitNum;
			break;
		}
	}

	// Finding the page frame having minimum hitNum (i.e. it is the least recently used) page frame
	i = lhIndex + 1;
	while(i < bSize)
	{
		if(pf[i].hitNum < lhNum)
		{
			lhIndex = i;
			lhNum = pf[i].hitNum;
		}
		i++;
	}

	//  (dirtyBit = 1) If page in memory has been modified, then write page to disk
	if(pf[lhIndex].dirtyBit == 1)
	{
		openPageFile(bm->pageFile, &fh);
		writeBlock(pf[lhIndex].pageNum, &fh, pf[lhIndex].data);

		//writeCount is incremented which records the number of writes done by the buffer manager.
		writecnt++;
	}

	// Setting page frame's content to new page's content
	pf[lhIndex].fixCount = page->fixCount;
	pf[lhIndex].data = page->data;
	pf[lhIndex].dirtyBit = page->dirtyBit;
	pf[lhIndex].hitNum = page->hitNum;
	pf[lhIndex].pageNum = page->pageNum;
}

//close the buffer pool
//removing all the pages from the memory and freeing up all resources.
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PF *pf = (PF *)bm->mgmtData;
	int i=0;
	// Write all dirty pages (modified pages) back to disk
	forceFlushPool(bm);
	while(i < bSize)
	{
		if(pf[i].fixCount != 0)
		{
			return RC_PINNED_PAGES_IN_BUFFER;
		}
		i++;
	}
	// Releasing space occupied by the page
	free(pf);
	bm->mgmtData = NULL;
	return RC_OK;
	
}
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	int i=0;
	PF *pf = (PF *)bm->mgmtData;
	// Store all dirty pages (modified pages) in memory to page file on disk
	while(i < bSize)
	{
		if(pf[i].fixCount == 0 && pf[i].dirtyBit == 1)
		{
			openPageFile(bm->pageFile, &fh);// Opening page file available on disk
			writeBlock(pf[i].pageNum, &fh, pf[i].data);// Writing block of data to the page file on disk
			pf[i].dirtyBit = 0;// Mark the page not dirty.
			writecnt++;
		}
		i++;
	}
	free(pf);
	return RC_OK;
	
}
extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PF *pf = (PF *)bm->mgmtData;
	
	int i;
	// In buffer pool, Iterating through all the page.
	for(i = 0; i < bSize; i++)
	{
		if(pf[i].pageNum == page->pageNum)
		{
			pf[i].dirtyBit = 1;
			return RC_OK;		
		}			
	}		
	free(pf);
	return RC_ERROR;
}
// This function removes a page from the memory
extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int i=0,temp;
	PF *pf = (PF *)bm->mgmtData;
	// In the buffer pool, Iterating through all the pages 
	while(i < bSize)
	{
	    temp=i;
	    i++;

		if(page->pageNum==pf[temp].pageNum )
		{
			pf[temp].fixCount=pf[temp].fixCount-1;
			break;
		}
	}
	return RC_OK;
	free(pf);
}
// modified pagescontents are written back to the page file on disk
extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int i=0;
	PF *pf = (PF *)bm->mgmtData;


	//In the buffer pool, Iterating through all the pages 
	while(i < bSize)
	{
		if(page->pageNum==pf[i].pageNum)
		{
			openPageFile(bm->pageFile, &fh);
			writeBlock(pf[i].pageNum, &fh, pf[i].data);
			pf[i].dirtyBit = 0;// Mark page as undirty because the modified page has been written to disk
			writecnt++;
		}
		i++;
	}
	return RC_OK;
}

extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
	    const PageNumber pageNum)
{
	int t=0;
	PF *pf = (PF *)bm->mgmtData;
	// If buffer pool is empty and this is the first page to be pinned
	if(pf[t].pageNum == -1)
	{
		openPageFile(bm->pageFile, &fh);
		pf[t].data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, pf[t].data);
		pf[t].pageNum = pageNum;
		pf[t].fixCount++;
		r= hit = 0;
		pf[t].refNum = 0;
		pf[t].hitNum = hit;
		page->pageNum = pageNum;
		page->data = pf[t].data;
        return RC_OK;
	}
	else
	{
		int i=0,temp;
		bool isBFull = true;//if buffer full

		while(i < bSize)
		{
		    temp=i;
		    i++;
			if(pf[temp].pageNum != -1)
			{
				if(pageNum==pf[temp].pageNum)// Checking if page is in memory
				{
					pf[temp].fixCount=pf[temp].fixCount+1;// now there is one more client accessing this page
					isBFull = false;
					hit++; 
					if(bm->strategy == RS_LRU)
						pf[temp].hitNum = hit;
					else if(bm->strategy == RS_CLOCK)
						pf[temp].hitNum = 1;// indicates that this was the last page frame examined 
					else if(bm->strategy == RS_LFU)
						pf[temp].refNum++;// adds one more to the count of no. of times the page is used 
					page->pageNum = pageNum;
					page->data = pf[temp].data;
					clockPtr++;
					break;
				}
			} else {
				openPageFile(bm->pageFile, &fh);
				pf[temp].data = (SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, pf[temp].data);
				pf[temp].fixCount = 1;
				pf[temp].pageNum = pageNum;
				pf[temp].refNum = 0;
				r++;
				hit++; 

				if(bm->strategy == RS_LRU)
					pf[temp].hitNum = hit;
				else if(bm->strategy == RS_CLOCK)
					pf[temp].hitNum = 1;

				page->pageNum = pageNum;
				page->data = pf[temp].data;

				isBFull = false;
				break;
			}
		}

		// If buffer is full and we must replace an existing page using page replacement strategy
		if(isBFull == true)
		{
			// Create a new page to store data read from the file.
			PF *np = (PF *) malloc(sizeof(PF));//np for new page

			// Reading page from disk and initializing page frame's content in the buffer pool
			openPageFile(bm->pageFile, &fh);
			np->data = (SM_PageHandle) malloc(PAGE_SIZE);
			readBlock(pageNum, &fh, np->data);
			np->dirtyBit = 0;
			np->pageNum = pageNum;
			np->fixCount = 1;
			np->refNum = 0;
			r++;
			hit++;

			if(bm->strategy == RS_LRU)
				np->hitNum = hit;
			else if(bm->strategy == RS_CLOCK)
				np->hitNum = 1;

			page->pageNum = pageNum;
			page->data = np->data;

			// appropriate algorithm's function is called depending on the page replacement strategy selected.
			switch(bm->strategy)
			{
				case RS_FIFO: // Using FIFO algorithm
					FIFO(bm, np);
					break;

				case RS_LRU: // Using LRU algorithm
					LRU(bm, np);
					break;

				case RS_LFU: // Using LFU algorithm
					LFU(bm, np);
					break;

			}

		}
		return RC_OK;
		free(pf);
	}
}

//statistical functions
extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	int i = 0;
	PageNumber *fc = malloc(sizeof(PageNumber) * bSize);//fc for frame contents
	PF *pf = (PF *) bm->mgmtData;
	// Iterating through all the pages in the buffer pool and setting frameContents' value to pageNum of the page
	while(i < bSize) {
            if(pf[i].pageNum!=-1)
                 fc[i]=pf[i].pageNum;
            else
                fc[i] =NO_PAGE;
		i++;
	}
	return fc;
}

// This function returns an array of bools
extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	int i=0;
	bool *dflag = malloc(sizeof(bool) * bSize);
	PF *pf = (PF *)bm->mgmtData;
	// Iterating through all the pages in the buffer pool and setting dirtyFlags' value to TRUE if page is dirty else FALSE
	while(i < bSize)
	{
	    if(pf[i].dirtyBit == 1)
            dflag[i]=true;
        else
            dflag[i]=false;
		i++;
	}
	return dflag;
}

// Returns an array of ints where the ith element is the fix count of the page stored in the ith page frame.
extern int *getFixCounts (BM_BufferPool *const bm)
{
	int i = 0;
	int *fc = malloc(sizeof(int) * bSize);//fc for fix counts
	PF *pf= (PF *)bm->mgmtData;
    	while(i < bSize)// Iterating through all the pages in the buffer pool and setting fixCounts' value to page's fixCount
	{
		if(pf[i].fixCount != -1)
            		fc[i] =pf[i].fixCount;
        	else
            		fc[i] =0;
		i++;
	}
	return fc;
}

// Returns the no. of pages that have been read from disk since a buffer pool has been initialized.
extern int getNumReadIO (BM_BufferPool *const bm)
{
	// Adding one because with start rearIndex with 0.
	return (r + 1);
}

// Returns the no. of pages written to the page file since the buffer pool has been initialized.
extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return writecnt;
}

