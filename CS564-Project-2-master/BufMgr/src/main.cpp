#include <iostream>
#include <stdlib.h>
//#include <stdio.h>
#include <cstring>
#include <memory>
#include "page.h"
#include "buffer.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/bad_buffer_exception.h"

#define PRINT_ERROR(str) \
{ \
	std::cerr << "On Line No:" << __LINE__ << "\n"; \
	std::cerr << str << "\n"; \
	exit(1); \
}

using namespace badgerdb;

const PageId num = 100;
PageId pid[num], pageno1, pageno2, pageno3, i;
RecordId rid[num], rid2, rid3;
Page *page, *page2, *page3;
char tmpbuf[100];
BufMgr* bufMgr;
File *file1ptr, *file2ptr, *file3ptr, *file4ptr, *file5ptr, *file6ptr, *file7ptr;

void test1();
void test2();
void test3();
void test4();
void test5();
void test6();
void test7();
void test8();
void test9();
void test10();
void test11();
void test12();
void testBufMgr();

int main()
{
	//Following code shows how to you File and Page classes

  const std::string& filename = "test.db";
  // Clean up from any previous runs that crashed.
  try
	{
    File::remove(filename);
  }
	catch(FileNotFoundException)
	{
  }

  {
    // Create a new database file.
    File new_file = File::create(filename);

    // Allocate some pages and put data on them.
    PageId third_page_number;
    for (int i = 0; i < 5; ++i) {
      Page new_page = new_file.allocatePage();
      if (i == 3) {
        // Keep track of the identifier for the third page so we can read it
        // later.
        third_page_number = new_page.page_number();
      }
      new_page.insertRecord("hello!");
      // Write the page back to the file (with the new data).
      new_file.writePage(new_page);
    }

    // Iterate through all pages in the file.
    for (FileIterator iter = new_file.begin();
         iter != new_file.end();
         ++iter) {
      // Iterate through all records on the page.
      for (PageIterator page_iter = (*iter).begin();
           page_iter != (*iter).end();
           ++page_iter) {
        std::cout << "Found record: " << *page_iter
            << " on page " << (*iter).page_number() << "\n";
      }
    }

    // Retrieve the third page and add another record to it.
    Page third_page = new_file.readPage(third_page_number);
    const RecordId& rid = third_page.insertRecord("world!");
    new_file.writePage(third_page);

    // Retrieve the record we just added to the third page.
    std::cout << "Third page has a new record: "
        << third_page.getRecord(rid) << "\n\n";
  }
  // new_file goes out of scope here, so file is automatically closed.

  // Delete the file since we're done with it.
  File::remove(filename);

	//This function tests buffer manager, comment this line if you don't wish to test buffer manager
	testBufMgr();
}

void testBufMgr()
{
	// create buffer manager
	bufMgr = new BufMgr(num);

	// create dummy files
  const std::string& filename1 = "test.1";
  const std::string& filename2 = "test.2";
  const std::string& filename3 = "test.3";
  const std::string& filename4 = "test.4";
  const std::string& filename5 = "test.5";
  const std::string& filename6 = "test.6";
  const std::string& filename7 = "test.7";

  try
	{
    File::remove(filename1);
    File::remove(filename2);
    File::remove(filename3);
    File::remove(filename4);
    File::remove(filename5);
    File::remove(filename6);
    File::remove(filename7);
  }
	catch(FileNotFoundException e)
	{
  }

	File file1 = File::create(filename1);
	File file2 = File::create(filename2);
	File file3 = File::create(filename3);
	File file4 = File::create(filename4);
    File file5 = File::create(filename5);
    File file6 = File::create(filename6);
	File file7 = File::create(filename7);

	file1ptr = &file1;
	file2ptr = &file2;
	file3ptr = &file3;
	file4ptr = &file4;
    file5ptr = &file5;
    file6ptr = &file6;
	file7ptr = &file7;

	//Test buffer manager
	//Comment tests which you do not wish to run now. Tests are dependent on their preceding tests. So, they have to be run in the following order.
	//Commenting  a particular test requires commenting all tests that follow it else those tests would fail.
	test1();
	test2();
	test3();
	test4();
	test5();
	test6();
	test7();
    test8();
    test9();
	test10();
	test11();
	test12();

    delete bufMgr;

	//Close files before deleting them
	file1.~File();
	file2.~File();
	file3.~File();
	file4.~File();
    file5.~File();
    file6.~File();
	file7.~File();

	//Delete files
	File::remove(filename1);
	File::remove(filename2);
	File::remove(filename3);
	File::remove(filename4);
    File::remove(filename5);
    File::remove(filename6);
	File::remove(filename7);

	std::cout << "\n" << "Passed all tests." << "\n";
}

void test1()
{
	//Allocating pages in a file...
	for (i = 0; i < num; i++)
	{
		bufMgr->allocPage(file1ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
		bufMgr->unPinPage(file1ptr, pid[i], true);
	}

	//Reading pages back...
	for (i = 0; i < num; i++)
	{
		bufMgr->readPage(file1ptr, pid[i], page);
		sprintf((char*)&tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		if(strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}
		bufMgr->unPinPage(file1ptr, pid[i], false);
	}
	std::cout<< "Test 1 passed" << "\n";
}

void test2()
{
	//Writing and reading back multiple files
	//The page number and the value should match

	for (i = 0; i < num/3; i++)
	{
		bufMgr->allocPage(file2ptr, pageno2, page2);
		sprintf((char*)tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
		rid2 = page2->insertRecord(tmpbuf);

		int index = random() % num;
        pageno1 = pid[index];
		bufMgr->readPage(file1ptr, pageno1, page);
		sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pageno1, (float)pageno1);
		if(strncmp(page->getRecord(rid[index]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->allocPage(file3ptr, pageno3, page3);
		sprintf((char*)tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
		rid3 = page3->insertRecord(tmpbuf);

		bufMgr->readPage(file2ptr, pageno2, page2);
		sprintf((char*)&tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
		if(strncmp(page2->getRecord(rid2).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->readPage(file3ptr, pageno3, page3);
		sprintf((char*)&tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
		if(strncmp(page3->getRecord(rid3).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->unPinPage(file1ptr, pageno1, false);
	}

	for (i = 0; i < num/3; i++) {
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
	}

	std::cout << "Test 2 passed" << "\n";
}

void test3()
{
	try
	{
		bufMgr->readPage(file4ptr, 1, page);
		PRINT_ERROR("ERROR :: File4 should not exist. Exception should have been thrown before execution reaches this point.");
	}
	catch(InvalidPageException e)
	{
	}

	std::cout << "Test 3 passed" << "\n";
}

void test4()
{
	bufMgr->allocPage(file4ptr, i, page);
	bufMgr->unPinPage(file4ptr, i, true);
	try
	{
		bufMgr->unPinPage(file4ptr, i, false);
		PRINT_ERROR("ERROR :: Page is already unpinned. Exception should have been thrown before execution reaches this point.");
	}
	catch(PageNotPinnedException e)
	{
	}

	std::cout << "Test 4 passed" << "\n";
}

void test5()
{
	for (i = 0; i < num; i++) {
		bufMgr->allocPage(file5ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.5 Page %d %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
	}

	PageId tmp;
	try
	{
		bufMgr->allocPage(file5ptr, tmp, page);
		PRINT_ERROR("ERROR :: No more frames left for allocation. Exception should have been thrown before execution reaches this point.");
	}
	catch(BufferExceededException e)
	{
	}

	std::cout << "Test 5 passed" << "\n";

	for (i = 1; i <= num; i++)
		bufMgr->unPinPage(file5ptr, i, true);
}

void test6()
{
	//flushing file with pages still pinned. Should generate an error
	for (i = 1; i <= num; i++) {
		bufMgr->readPage(file1ptr, i, page);
	}

	try
	{
		bufMgr->flushFile(file1ptr);
		PRINT_ERROR("ERROR :: Pages pinned for file being flushed. Exception should have been thrown before execution reaches this point.");
	}
	catch(PagePinnedException e)
	{
	}

	std::cout << "Test 6 passed" << "\n";

	for (i = 1; i <= num; i++)
		bufMgr->unPinPage(file1ptr, i, true);

	bufMgr->flushFile(file1ptr);
}


// test case to check the clock policy
void test7()
{
        // Allocate pages that are more than available space in buffer pool and read back

        // allocate too many pages
        try {
                for (i = 0; i < num+1; i++) {
                        bufMgr->allocPage(file1ptr, pid[i], page);
                        sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
                        rid[i] = page->insertRecord(tmpbuf);
                }
                PRINT_ERROR("ERROR :: No more frames left for allocation. Exception should have been thrown before execution reaches this point.");
        }
        catch (BufferExceededException e)
        {
                std::cout << "exception caught which is right" << "\n";
        }

        for (i = 0; i < num; i++) {
                bufMgr->readPage(file1ptr, pid[i], page);
                sprintf((char*)&tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
                if (strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
                {
                        PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
                }
        }

        //randomly unpin the page to make the frame free
        // unpin twice to  release it
        bufMgr->unPinPage(file1ptr, pid[55], false);
        bufMgr->unPinPage(file1ptr, pid[55], false);

        try {
        bufMgr->allocPage(file1ptr, pid[101], page);
        sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
        rid[i] = page->insertRecord(tmpbuf);
        } catch (BufferExceededException e)
        {
                PRINT_ERROR("exceptions should not be have been thrown from allocBuf");
        }

        for (i = 0; i <= num; i++) {
        	if (i!=55){
			  bufMgr->unPinPage(file1ptr, pid[i], false);
			  bufMgr->unPinPage(file1ptr, pid[i], false);
			}
		}

		bufMgr->unPinPage(file1ptr, pid[101], true);


		bufMgr->flushFile(file1ptr);



        std::cout << "Test 7 passed" << "\n";
}

//This test case to check whether after flushing file to disk if we read back whether content is read correctly or not
void test8() {

	for (i = 0; i < num; i++) {
		bufMgr->allocPage(file1ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		//store the content so that we can check later
		rid[i] = page->insertRecord(tmpbuf);
		bufMgr->unPinPage(file1ptr, pid[i], true);
	}

	// flush the file
	bufMgr->flushFile(file1ptr);

	// Now read  the pages back and check the content for the correctness
	for (i = 0; i < num; i++) {
		bufMgr->readPage(file1ptr, pid[i], page);
		sprintf((char*)&tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		if(strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}
		bufMgr->unPinPage(file1ptr, pid[i], false);
	}

	std::cout << "Test 8 passed" << "\n";
}

// stress test BufMgr::readPage method
void test9() {
    // write something to each page
    for (i = 0; i < num; i++) {
        // first allocate the page and write to it
        bufMgr->allocPage(file6ptr, pid[i], page);
        sprintf((char*)tmpbuf, "test.6 Page %d %7.1f", pid[i], (float)pid[i]);

        // store something
        rid[i] = page->insertRecord(tmpbuf);
        bufMgr->unPinPage(file6ptr, pid[i], true);
    }

    // change the content of each page
    for (i = 0; i < num; i++) {
        bufMgr->readPage(file6ptr, pid[i], page);
        sprintf((char*)tmpbuf, "test.6 something else %d %7.1f", pid[i], (float)pid[i]);

        // update the content so that we can check later
        page->updateRecord(rid[i], tmpbuf);

        bufMgr->unPinPage(file6ptr, pid[i], true);
    }

    // flush the file
    bufMgr->flushFile(file6ptr);

    // Now read  the pages back and check the content for the correctness
    for (i = 0; i < num; i++) {
        bufMgr->readPage(file6ptr, pid[i], page);
        sprintf((char*)&tmpbuf, "test.6 something else %d %7.1f", pid[i], (float)pid[i]);
        if(strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
        {
            PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
        }
        bufMgr->unPinPage(file6ptr, pid[i], false);
    }

    std::cout << "Test 9 passed" << "\n";
}

// stess test BufMgr::unPinPage method
void test10() {

    int readCount = 10;

    // write something to each page
    for (i = 0; i < num; i++) {
        // first allocate the page and write to it
        bufMgr->allocPage(file7ptr, pid[i], page);
        sprintf((char*)tmpbuf, "test.7 Page %d %7.1f", pid[i], (float)pid[i]);

        // store something
        rid[i] = page->insertRecord(tmpbuf);
        bufMgr->unPinPage(file7ptr, pid[i], true);
    }

    // effective pin count of every page is 0 before this loop
    for (int rc = 0; rc < readCount; rc++) {
        for (i = 0; i < num; i++) {
            bufMgr->readPage(file7ptr, pid[i], page);
        }
    }

    // effective pin count of every page is equal to read count before this loop
    for (int rc = 0; rc < readCount - 1; rc++) {
        for (i = 0; i < num; i++) {
            bufMgr->unPinPage(file7ptr, pid[i], false);
        }
    }

    // flush the file
    try {
        bufMgr->flushFile(file7ptr);
        PRINT_ERROR("ERROR :: Pages pinned for file being flushed. Exception should have been thrown before execution reaches this point.");
    }
    catch (const PagePinnedException& ex) {
        // we should get an exception since we've unpinned only readCount -1 times
        for (i = 0; i < num; i++) {
            bufMgr->unPinPage(file7ptr, pid[i], false);
        }
    }

    // effective pin count of every page is equal to 0 before this loop
    for (i = 0; i < num; i++) {
        try {
            bufMgr->unPinPage(file7ptr, pid[i], false);
            PRINT_ERROR("ERROR :: Page is already unpinned. Exception should have been thrown before execution reaches this point.");
        } catch (const PageNotPinnedException& ex) {
            // we should get an exception since pin count is 0 now
        }
    }

    // now it should work
    bufMgr->flushFile(file7ptr);

    std::cout << "Test 10 passed" << "\n";
}
/* Test case that tests allocation and disposal of the page */
void test11()
{
    try
    {
        //allocating a page
	bufMgr->allocPage(file5ptr, pid[99], page);
        sprintf((char*)tmpbuf, "test.7 Page %d %7.1f", pid[99], (float)pid[99]);

        /* write something to page */
        rid2 = page->insertRecord(tmpbuf);

	//disposing a page
	bufMgr->disposePage(file5ptr, pid[99]);
        
    }
	
    catch(BufferExceededException e){
    }
    std::cout << "Test 11 passed" << "\n";

    bufMgr->unPinPage(file5ptr, 0, true);
}
/* Test case that tests disposal of the page when page is invalid */
void test12()
{
    try
    {
        //allocating a page
	bufMgr->allocPage(file5ptr, pid[99], page);
        sprintf((char*)tmpbuf, "test.7 Page %d %7.1f", pid[99], (float)pid[99]);

        /* write something to page */
        rid2 = page->insertRecord(tmpbuf);

        //disposing a page
	bufMgr->disposePage(file5ptr, pid[99]);
	//trying to read into the disposed page that will throw exception
	bufMgr->readPage(file5ptr, pid[99], page);
	PRINT_ERROR("ERROR :: Exception will be thrown before this point");
        
    }
    catch(InvalidPageException ex){
    }
    catch(BufferExceededException e){
    }
    std::cout << "Test 12 passed" << "\n";

    bufMgr->unPinPage(file5ptr, 0, true);
}

