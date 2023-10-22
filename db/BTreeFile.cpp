#include <db/BTreeFile.h>

using namespace db;

BTreeLeafPage *BTreeFile::findLeafPage(TransactionId tid, PagesMap &dirtypages, BTreePageId *pid, Permissions perm,
                                       const Field *f) {
    if(pid->getType() == BTreePageType::LEAF){
        return dynamic_cast<BTreeLeafPage *>(getPage(tid, dirtypages, pid, Permissions::READ_ONLY));
    }

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
    return findLeafPage(tid, dirtypages, rightest.getRightChild(), Permissions::READ_ONLY, f);;
}

BTreeLeafPage *BTreeFile::splitLeafPage(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *page, const Field *field) {
    // TODO pa2.3: implement
    return nullptr;
}

BTreeInternalPage *BTreeFile::splitInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                                Field *field) {
    // TODO pa2.3: implement
    return nullptr;
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
