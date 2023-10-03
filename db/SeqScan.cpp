#include <db/SeqScan.h>


using namespace db;

// TODO pa1.6: implement
SeqScan::SeqScan(TransactionId *tid, int tableid, const std::string &tableAlias) : tableid(tableid), tableAlias(tableAlias) {
}

std::string SeqScan::getTableName() const {
    // TODO pa1.6: implement
    return Database::getCatalog().getTableName(tableid);
}

std::string SeqScan::getAlias() const {
    // TODO pa1.6: implement
    return tableAlias;
}

void SeqScan::reset(int tabid, const std::string &tableAlias) {
    // TODO pa1.6: implement
    this->tableid = tableid;
    this->tableAlias = tableAlias;
}

const TupleDesc &SeqScan::getTupleDesc() const {
    // TODO pa1.6: implement
    TupleDesc td = Database::getCatalog().getTupleDesc(tableid);
    std::vector<std::string> str_arr;
    std::vector<Types::Type> type_arr;
    for(auto& td_item : td){
       str_arr.push_back(tableAlias + '.' + td_item.fieldName);
       type_arr.push_back(td_item.fieldType);
    }
    return TupleDesc(type_arr, str_arr);
}

SeqScan::iterator SeqScan::begin() const {
    // TODO pa1.6: implement
    HeapFile *f = dynamic_cast<HeapFile *>(Database::getCatalog().getDatabaseFile(tableid));
    return SeqScanIterator(f->begin());
}

SeqScan::iterator SeqScan::end() const {
    // TODO pa1.6: implement
    HeapFile *f = dynamic_cast<HeapFile *>(Database::getCatalog().getDatabaseFile(tableid));
    return SeqScanIterator(f->end());
}

//
// SeqScanIterator
//

// TODO pa1.6: implement
SeqScanIterator::SeqScanIterator(HeapFileIterator f_iter) : f_iter(f_iter) {
}

bool SeqScanIterator::operator!=(const SeqScanIterator &other) const {
    // TODO pa1.6: implement
    return f_iter != other.f_iter;
}

SeqScanIterator &SeqScanIterator::operator++() {
    // TODO pa1.6: implement
   ++f_iter;
}

const Tuple &SeqScanIterator::operator*() const {
    // TODO pa1.6: implement
    return *f_iter;
}
