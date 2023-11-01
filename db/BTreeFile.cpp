#include <db/BTreeFile.h>
#include <db/Database.h>
#include <cassert>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace db;

BTreeLeafPage *BTreeFile::findLeafPage(TransactionId tid, PagesMap &dirtypages, BTreePageId *pid, Permissions perm,
                                       const Field *f) {
    if(pid->getType() == BTreePageType::LEAF){
        return dynamic_cast<BTreeLeafPage *>(getPage(tid, dirtypages, pid, Permissions::READ_ONLY));
    }
    if(pid->getType() != BTreePageType::INTERNAL) return NULL;
    BTreeInternalPage *internalPage = dynamic_cast<BTreeInternalPage *>(getPage(tid, dirtypages, pid, perm));
    // if the key is not provided
    if(f == nullptr){
        BTreeEntry leftest = *internalPage->begin();
        return findLeafPage(tid, dirtypages, leftest.getLeftChild(), Permissions::READ_ONLY, f);
    }
    // iterate through internal page
    for(auto childEntry : *internalPage){
        // if entry >= key
        if(childEntry.getKey()->compare(Op::GREATER_THAN_OR_EQ, f)){
            // find from left child
            return findLeafPage(tid, dirtypages, childEntry.getLeftChild(), Permissions::READ_ONLY, f);
        }
    }
    // last element
    BTreeEntry rightest = *internalPage->rbegin();
    return findLeafPage(tid, dirtypages, rightest.getLeftChild(), Permissions::READ_ONLY, f);
}

BTreeLeafPage *BTreeFile::splitLeafPage(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *page, const Field *field) {
    BTreeLeafPage *new_page = dynamic_cast<BTreeLeafPage *>(getEmptyPage(tid, dirtypages, BTreePageType::LEAF));
    // set left and right siblings
    BTreePageId *right_sibling = page->getRightSiblingId();
    page->setRightSiblingId(const_cast<BTreePageId *>(&new_page->getId()));
    new_page->setLeftSiblingId(const_cast<BTreePageId *>(&page->getId()));
    new_page->setRightSiblingId(right_sibling);

    std::vector<Tuple*> tuples_to_be_inserted;
    int max_tuples = page->getMaxTuples();
    //Predicate p = Predicate(0, Op::GREATER_THAN_OR_EQ, field);
    // iterate through page
    auto it = page->begin();
    auto end = page->end();
    Tuple *t = nullptr;
    int tuple_counter = 0;
    // store and delete second half of tuples
    while(it != end){
        t = &*it;
        if(tuple_counter < max_tuples/2){ // filter
            tuples_to_be_inserted.push_back(t);
            page->deleteTuple(t);
        }
        ++it;
        tuple_counter++;
    }
    for(auto t : tuples_to_be_inserted){
        new_page->insertTuple(t);
    }
    // insert parent entry to internal page
    BTreeInternalPage *parent = getParentWithEmptySlots(tid, dirtypages, page->getParentId(), const_cast<Field *>(field));
    BTreeEntry new_entry = BTreeEntry(const_cast<Field *>(field), const_cast<BTreePageId *>(&page->getId()), const_cast<BTreePageId *>(&new_page->getId()));
    parent->insertEntry(new_entry);
    // update parent pointers
    updateParentPointers(tid, dirtypages, parent);

    return new_page;
}

BTreeInternalPage *BTreeFile::splitInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                                Field *field) {
    BTreeInternalPage *new_page = dynamic_cast<BTreeInternalPage *>(getEmptyPage(tid, dirtypages, BTreePageType::INTERNAL));
    BTreeInternalPage *parent = getParentWithEmptySlots(tid, dirtypages, page->getParentId(), field);

    int max_entries = page->getMaxEntries();
    int entry_counter = 0;
    std::vector<BTreeEntry> entries_to_be_inserted;

    // iterate through page
    auto it = page->begin();
    auto end = page->end();
    BTreeEntry *e = nullptr;
    while (it != end) {
        e = &*it;
        if(entry_counter > max_entries/2){
            // store entries
            entries_to_be_inserted.push_back(BTreeEntry(e->getKey(), e->getLeftChild(), e->getRightChild()));
            // delete it
            page->deleteKeyAndRightChild(e);
        }
        entry_counter++;
        ++it;
    }
    // the first element of entries_to_be_inserted, should insert to parent node, set pointer
    BTreeEntry entry_to_be_pushed = entries_to_be_inserted.front();
    entry_to_be_pushed.setLeftChild(const_cast<BTreePageId *>(&page->getId()));
    entry_to_be_pushed.setRightChild(const_cast<BTreePageId *>(&new_page->getId()));
    // push to parent page and update parent pointer
    parent->insertEntry(entry_to_be_pushed);
    updateParentPointers(tid, dirtypages, parent);

    // insert entries to new page
    int size = entries_to_be_inserted.size();
    for(int i = 1; i < size; i++){
        new_page->insertEntry(entries_to_be_inserted[i]);
    }
    // update parent pointers
    updateParentPointers(tid, dirtypages, new_page);

    return new_page;
}

void BTreeFile::stealFromLeafPage(BTreeLeafPage *page, BTreeLeafPage *sibling, BTreeInternalPage *parent,
                                  BTreeEntry *entry, bool isRightSibling) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromLeftInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                          BTreeInternalPage *leftSibling, BTreeInternalPage *parent,
                                          BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromRightInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                           BTreeInternalPage *rightSibling, BTreeInternalPage *parent,
                                           BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeLeafPages(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *leftPage,
                               BTreeLeafPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeInternalPages(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *leftPage,
                                   BTreeInternalPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

BTreeFile::BTreeFile(const char *fname, int key, const TupleDesc &td) : keyField(key), td(td) {
    fd = open(fname, O_RDWR | O_CREAT | O_APPEND | O_TRUNC, 0644);
    if (fd == -1) {
        throw std::runtime_error("open");
    }
    tableid = std::hash<std::string>{}(fname);
}