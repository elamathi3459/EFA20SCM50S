Buffer Pool Functions


1. initBufferPool
* openPageFile() - opens the already existing page file. This method is present in storage_mgr.c
* Initially it sets all the status values and pointers to 0 and NULL respectively
* We must also initialize the Queue and the pageFrame with some values
2. shutdownBufferPool
* Deallocates all the memory allocated to Queue and frame and destroy bufferpool
* It calls forceFlushPool() method to write all the dirty pages to the pageFile before freeing the memory.
3. forceFlushpool
* This method writes all the dirty pages to the file on the disk
* The dirty pages are now reset to 0 to make it not dirty
* Then increment the writeCount by 1


      Page Management Functions 


1. pinPage    
* Calls the openPageFile() method
* We replace page using page replacement strategy if the buffer is full or contains maximum to process
*  If the buffer is not full then use the readBlock() and ensureCapacity() function to read and pin the pages
2.  unpinPage
* unpins the page 
* The pageNum field of the page should be used to figure out which page to unpin.
* fixCount is decremented which shows that the client is not using the page.
3. markDirty
* Make this file as dirty which is identified by the pageNum in the buffer pool
* It is set to dirty when the user changes the file
* Find the buffer pageNum and set the dirty flag to 1
4. forcePage
* Calls the openPageFile() method to open the page
* Then this method calls the writeBlock() to write back the current content of the page to the page file on the disk(i.e,  find the pageNum page and write it back to the disk)
Statistics Functions :
1. getFrameContents
*  Returns the array of page numbers(pageNum of size numPages) currently in the buffer
* If the empty page which is denoted as NO_PAGE is returned if there is no pages available currently in the buffer
2. getDirtyFlags
* This method returns an array of bools(pageNum of size numPages) representing the dirty status of the pages in the buffer
* Empty page frames present are considered clean
3. getFixCounts
* This method returns an array of the fixCounts(i.e, array of int(size numPages)) of all the pages in the buffer.
4. getNumReadIO
* This method returns the number of reads done by the buffer manager when it is initialized using the readCount variable.
5. getNumWriteIO
* This method returns the number of writes done by the buffer manager since the write count has been initialized.
Test case1
* The program verifies all the test cases that are mentioned in the test file i.e test_assign2_1 and ensures that there are no errors.
Test case2
* Along with the first one additional test cases are added and taken for testing.
* Check files which are marked dirty and the number of writes performed by the buffer manager in the LRU implementation
Memory Leak
* Memory Leaks were checked in both the test cases and it is not found


________________






Team Members(Team 14)
1. Elamathi Senthilkumar
2. Syed Abdullah Iqbal
3. Varun veerla