/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 *
 * File:             buffer.cpp
 * Purpose:          Implementation of BufMgr class
 * Semester:         Fall 2018
 * Name(Student ID): Sankarshan Bhat(9080144620), Sangeetha SampathKumar (9080029862), Kartik Anand(9079772514)
 * Lecturer's Name:  Paris Koutris
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/hash_already_present_exception.h"

namespace badgerdb {

/**
  * Constructor of BufMgr class
  */
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/**
  * Destructor of BufMgr class
  */
BufMgr::~BufMgr() {
  for(FrameId i = 0; i < numBufs; i++){
    if(bufDescTable[i].dirty){
      flushFile(bufDescTable[i].file);
    }
  }

  delete [] bufPool;
  delete [] bufDescTable;
  delete hashTable;
}

/**
  * Advance clock to next frame in the buffer pool
  */
void BufMgr::advanceClock()
{
  this->clockHand = (this->clockHand+1)%this->numBufs;
}

/**
 * Allocate a free frame using the clock policy
 *
 * @param frame Frame reference, frame ID of allocated frame returned via this variable
 * @throws BufferExceededException If no such buffer is found which can be allocated
 */
void BufMgr::allocBuf(FrameId& frame)
{
  /**
   * Algo:
   *
   *   We need to iterate until either empty frame is found or complete 1 cyle and find candidate in 2 cycle
   *   we need also keep track of pinnedPagecount and check if all the frames are already pinned
   *   while ( (initialClockHandPos !=clockHand) || noOfRounds <=2 )&& (pinnedPagesCount != numBufs))
   *    if the frame is not Valid
   *      then we got the frame so return
   *    else (we need to find a candidate):
   *      if refBit is set:
   *        clear refBit and continue
   *      if pincCount > 0:
   *        increment pinnedPagesCount and
   *        continue
   *      else:
   *         we have found the frame
   *          if dirtyBit is set
   *            flush page to disk
   *         remove the frame from hash table and clear the frame in the buffer
   *
   **/
  uint32_t pinnedPagesCount = 0;
  uint32_t initialClockHandPos = clockHand;
  int roundElapsed = 0;
  bool frameFound = false;
  do {
    /*initial check*/
    if(initialClockHandPos == clockHand) {
      /*increment round and reset pinnedPagedcount after each round */
      roundElapsed++;
      pinnedPagesCount = 0;
    }

    /* increment the clock pointer */
    advanceClock();

    // get buffer description table for frame being pointed to by clock hand
    BufDesc *currentBufDesc = &bufDescTable[clockHand];

    //if the frame is invalid it means its an empty frame, so we got the frame hence return
    if (!currentBufDesc -> valid) {
      currentBufDesc->Clear();
      frame = currentBufDesc->frameNo;
      frameFound = true;
      break;
    }

    /* if refBit is set clear and continue*/

    if(currentBufDesc -> refbit)
    {
      currentBufDesc -> refbit = false;
      // Also check for pincount and increment the pinnedPagesCount if it is > 0
      if(currentBufDesc->pinCnt > 0)
        pinnedPagesCount++;
      continue;
    }
    else
    {
      //check for pincount and increment the pinnedPagesCount if it is > 0 and continue
      if(currentBufDesc -> pinCnt > 0)
      {
        pinnedPagesCount++;
        continue;
      }
      else /* we have found a frame */
      {
        /* if the dirty bit is set then write the page to disk*/
        if(currentBufDesc -> dirty)
        {
          try{
          bufDescTable[clockHand].file->writePage(bufPool[currentBufDesc->frameNo]);
        } catch (InvalidPageException invalidExe) {

        }

        }
        // Make sure that if the buffer frame allocated has a valid page in it, then we should remove the appropriate
        // entry from the hash table and clear the frame in buffer
        try{
          hashTable->remove(currentBufDesc ->file, currentBufDesc ->pageNo);
        }catch (HashNotFoundException& hashNotExe) {

        }

        currentBufDesc->Clear();
        /*set the frame no */
        frame = currentBufDesc->frameNo;
        frameFound = true;
        /* break since frame is found */
        break;
      }
    }
  } while(((initialClockHandPos != clockHand) || roundElapsed <= 2) && (pinnedPagesCount < numBufs));

  /* Throw BufferExceededException if the valid frame is not found */
  if (!frameFound)
    throw BufferExceededException();
}


/**
 * Reads the given page from the file into a frame and returns the pointer to page.
 * If the requested page is already present in the buffer pool pointer to that frame is returned
 * otherwise a new frame is allocated from the buffer pool for reading the page.
 *
 * @param file    File object
 * @param PageNo  Page number in the file to be read
 * @param page    Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId frameNo;
  try {
    // check if page already exists
    this->hashTable->lookup(file, pageNo, frameNo);

    // increase the pin count of frame
    this->bufDescTable[frameNo].pinCnt += 1;

    // set refbit
    this->bufDescTable[frameNo].refbit = true;
  } catch (const HashNotFoundException& ex) {
    // allocate a new frame
    this->allocBuf(frameNo);

    // read the page to it
    this->bufPool[frameNo] = file->readPage(pageNo);

    // add to hash table
    this->hashTable->insert(file, pageNo, frameNo);

    // set appropriate parameters for the frame
    this->bufDescTable[frameNo].Set(file, pageNo);
  }

  // set page to designated frame no.
  page = &(this->bufPool[frameNo]);
}

/**
 * Unpin a page from memory since it is no longer required for it to remain in memory.
 *
 * @param file    File object
 * @param PageNo  Page number
 * @param dirty   True if the page to be unpinned needs to be marked dirty
 * @throws  PageNotPinnedException If the page is not already pinned
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
  try {
    FrameId frameNo;

    // page should already exists
    this->hashTable->lookup(file, pageNo, frameNo);

    // mark page as dirty if required
    if (dirty) {
      bufDescTable[frameNo].dirty = dirty;
    }

    // get current pin count and throw if count is already 0
    int pinCnt = bufDescTable[frameNo].pinCnt;
    if (pinCnt == 0) {
      throw PageNotPinnedException(file->filename(), pageNo, frameNo);
    }

    // reduce pin count
    bufDescTable[frameNo].pinCnt = pinCnt - 1;
  } catch (const HashNotFoundException& ex) {
    // do nothing if page not found in hash
  }
}

/**
 * Writes out all dirty pages of the file to disk.
 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
 * Otherwise Error returned.
 *
 * @param file File object
 * @throws PagePinnedException If any page of the file is pinned in the buffer pool
 * @throws BadBufferException If any frame allocated to the file is found to be invalid
 */
void BufMgr::flushFile(const File* file)
{
  for(FrameId i = 0; i < numBufs; i++){
    if(bufDescTable[i].file == file){

      /* first check if the page is already pinned, throw PagePinnedException*/
      if(bufDescTable[i].pinCnt != 0){
        throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
      }

      if(!bufDescTable[i].valid){
        /*if the page is not a valid page, throw an exception */
        throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      }

      if(bufDescTable[i].dirty){
        /*write to disk if the frame is dirty */
        bufDescTable[i].file->writePage(bufPool[i]);
        bufDescTable[i].dirty = false;
      }
      hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
      bufDescTable[i].Clear();
    }
  }
}

/**
 * Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file    File object
 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
 * @param page    Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
  try{
    /* Allocate an empty page in the specified file */
    /* page stores reference to newPage object */
    Page newPage = file->allocatePage();

    /* pageNo stores the page number of the newly allocated page */
    pageNo = newPage.page_number();
    /* assign pageNo as frameNo */
    FrameId frameNo;

    /* allocate a frame in buffer pool and sets the frame no */
    allocBuf(frameNo);

    // put the new page in buffer

    bufPool[frameNo] = newPage;


    /* Insert an entry to hashTable */
    hashTable->insert(file, pageNo, frameNo);

    /* set function on frame allocated for the page */
    bufDescTable[frameNo].Set(file, pageNo);

    /* Pointer to the buffer frame allocated for the page via the page parameter */
    page = &bufPool[frameNo];
  }


  /* hashTable->insert() throws HashAlreadyPresentException if a key is present already */
  catch(const HashAlreadyPresentException& hashPresent){
  }

  /* returns 1) Page No of Newly allocated page via pageNo parameter
             2) Pointer to the buffer frame allocated for the page via the page parameter */
}

/**
 * Delete page from file and also from buffer pool if present.
 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
 *
 * @param file    File object
 * @param PageNo  Page number
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
  /* This method Deletes a particular page from file */
  try{
    FrameId frameNo;

    /* Lookup hashTable to get the frame that is allocated for the page */
    hashTable->lookup(file, PageNo, frameNo);

    /* Free the frame */
    bufDescTable[frameNo].Clear();

    /* Remove hashTable entry */
    hashTable->remove(file, PageNo);

    /* Delete the page from the specified file */
    file->deletePage(PageNo);

  }

  /* hashtable throws exception for Entry not found in hashTable */
  catch(const HashNotFoundException& hashNotFound){
  }

}

/**
  * Print member variable values.
  */
void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
