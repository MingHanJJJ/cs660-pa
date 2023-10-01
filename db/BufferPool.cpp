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
    // read from file and add it to the buffer
    page_buffer.push_back(Database::getCatalog().getDatabaseFile(pid->getTableId())->readPage(*pid));
    return page_buffer.back();

}

void BufferPool::evictPage() {
    // do nothing for now
}
