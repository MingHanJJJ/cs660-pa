#include <db/RecordId.h>
#include <stdexcept>

using namespace db;

//
// RecordId
//

// TODO pa1.4: implement
RecordId::RecordId(const PageId *pid, int tupleno): pageId(pid), tupleno(tupleno) {
}

bool RecordId::operator==(const RecordId &other) const {
    // TODO pa1.4: implement
    return *other.pageId == *pageId && other.tupleno == tupleno;
}

const PageId *RecordId::getPageId() const {
    // TODO pa1.4: implement
    return pageId;
}

int RecordId::getTupleno() const {
    // TODO pa1.4: implement
    return tupleno;
}

//
// RecordId hash function
//

std::size_t std::hash<RecordId>::operator()(const RecordId &r) const {
    // TODO pa1.4: implement
    return hash<int>{}(r.getPageId()->pageNumber()) ^ hash<int>{}(r.getPageId()->getTableId()) ^ hash<int>{}(r.getTupleno());
}
