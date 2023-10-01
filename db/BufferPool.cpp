#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

// TODO pa1.3: implement
BufferPool::BufferPool(int numPages) {
    page_buffer = std::vector<Page*>(numPages);
}

Page *BufferPool::getPage(const TransactionId &tid, PageId *pid) {
    // TODO pa1.3: implement
    for(auto &page: page_buffer){
        if(page->getId() == *pid){
            return page;
        }
    }
    throw std::invalid_argument("Can't found page by the given id");
}

void BufferPool::evictPage() {
    // do nothing for now
}
