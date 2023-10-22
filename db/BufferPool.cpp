#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

void BufferPool::evictPage() {
    if(!pages.empty()){
        // evict first page
        auto it = pages.begin();
        flushPage(it->first);
        pages.erase(it);
    }
}

void BufferPool::flushAllPages() {
    for (auto it : pages){
        if(it.second->isDirty() != std::nullopt){
            flushPage(it.first);
        }
    }
}

void BufferPool::discardPage(const PageId *pid) {
    auto it = pages.find(pid);
    if (it != pages.end()) {
        pages.erase(it);
    }
}

void BufferPool::flushPage(const PageId *pid) {
    Page *page = pages[pid];
    if(page->isDirty() != std::nullopt){   // if the page is dirty
        page->markDirty(std::nullopt); // unmark dirty
        Database::getCatalog().getDatabaseFile(pid->getTableId())->writePage(page); // write page
    }

}

void BufferPool::flushPages(const TransactionId &tid) {
    for (auto it : pages){
        if(tid == it.second->isDirty()){
            flushPage(it.first);
        }
    }
}

void BufferPool::insertTuple(const TransactionId &tid, int tableId, Tuple *t) {
    // TODO pa2.3: implement
    std::vector<Page *> dirty_pages =  Database::getCatalog().getDatabaseFile(tableId)->insertTuple(tid, *t);
    for(auto p : dirty_pages){
        p->markDirty(tid);
    }
}

void BufferPool::deleteTuple(const TransactionId &tid, Tuple *t) {
    // TODO pa2.3: implement
}
