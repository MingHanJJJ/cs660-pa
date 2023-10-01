#include <db/HeapFile.h>
#include <db/TupleDesc.h>
#include <db/Page.h>
#include <db/PageId.h>
#include <db/HeapPage.h>
#include <stdexcept>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <cstdlib>

using namespace db;

//
// HeapFile
//

// TODO pa1.5: implement
HeapFile::HeapFile(const char *fname, const TupleDesc &td) : fname(fname), td(td) {
    // open fd
    fd = open(fname, O_RDWR | O_CREAT, 0644);
    if (fd == -1) {
        perror("open");
        return;
    }

    struct stat file_info;
    if (fstat(fd, &file_info) != 0) {
        std::cerr << "Error getting file size." << std::endl;
        close(fd);
        return;
    }
    // File size in bytes
    file_size = file_info.st_size;
    page_size = Database::getBufferPool().getPageSize();
    num_pages = file_size/page_size;

}

int HeapFile::getId() const {
    // TODO pa1.5: implement
    return std::hash<std::string>{}(fname);
}

const TupleDesc &HeapFile::getTupleDesc() const {
    // TODO pa1.5: implement
    return td;
}

Page *HeapFile::readPage(const PageId &pid) {
    // TODO pa1.5: implement
    int pgNo = pid.pageNumber();
    if(pgNo < num_pages){
        uint8_t data[page_size];
        off_t offset = pgNo*page_size;
        pread(fd, data, 4096, offset);
        return new HeapPage(HeapPageId(pid.getTableId(), pid.pageNumber()), data);
    }
    throw std::invalid_argument("page num is not right");
}

int HeapFile::getNumPages() {
    // TODO pa1.5: implement
    return num_pages;
}

HeapFileIterator HeapFile::begin() const {
    // TODO pa1.5: implement
}

HeapFileIterator HeapFile::end() const {
    // TODO pa1.5: implement
}

//
// HeapFileIterator
//

// TODO pa1.5: implement
HeapFileIterator::HeapFileIterator(HeapPageId *id, int num_pages, HeapPageIterator pageIterator) : pageId(id), num_pages(num_pages), pageIterator(pageIterator) {
    page = dynamic_cast<HeapPage *>(Database::getBufferPool().getPage(TransactionId(), pageId));
}

bool HeapFileIterator::operator!=(const HeapFileIterator &other) const {
    // TODO pa1.5: implement
    return *pageId != *other.pageId || pageIterator != other.pageIterator;
}

Tuple &HeapFileIterator::operator*() const {
    // TODO pa1.5: implement
    return *pageIterator;
}

HeapFileIterator &HeapFileIterator::operator++() {
    // TODO pa1.5: implement
    ++pageIterator;
    if(pageIterator == page->end() && pageId->pageNumber() < num_pages) {
        // new to change page

    }
    return *this;
}
