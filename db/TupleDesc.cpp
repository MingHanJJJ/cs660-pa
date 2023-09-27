#include <db/TupleDesc.h>

using namespace db;

//
// TDItem
//

bool TDItem::operator==(const TDItem &other) const {
    // TODO pa1.1: implement
}

std::size_t std::hash<TDItem>::operator()(const TDItem &r) const {
    // TODO pa1.1: implement
}

//
// TupleDesc
//

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types) {
    if(types.size() < 0) return;
    this->types = types;
}

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names) {
    if(types.size() < 0) return;
    this->types = types;
    this->names = names;
}

size_t TupleDesc::numFields() const {
    // TODO pa1.1: implement
    return  types.size();
}

std::string TupleDesc::getFieldName(size_t i) const {
    // TODO pa1.1: implement
    return names[i];
}

Types::Type TupleDesc::getFieldType(size_t i) const {
    // TODO pa1.1: implement
    return types[i];
}

int TupleDesc::fieldNameToIndex(const std::string &fieldName) const {
    // TODO pa1.1: implement
    int size = names.size();
    for(int i = 0; i < size; i++){
        if(names[i] == fieldName){
            return i;
        }
    }
    return -1; // if not found
}

size_t TupleDesc::getSize() const {
    // TODO pa1.1: implement
    size_t size = 0;
    for(auto &type : types){
        size += getLen(type);
    }
}

TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // TODO pa1.1: implement
    std::vector<Types::Type> td3_types;
    std::vector<std::string> td3_names;
    td3_types.insert(td3_types.end(), td1.types.begin(), td1.types.end());
    td3_types.insert(td3_types.end(), td2.types.begin(), td2.types.end());
    td3_names.insert()
}

std::string TupleDesc::to_string() const {
    // TODO pa1.1: implement
}

bool TupleDesc::operator==(const TupleDesc &other) const {
    // TODO pa1.1: implement
}

TupleDesc::iterator TupleDesc::begin() const {
    // TODO pa1.1: implement
}

TupleDesc::iterator TupleDesc::end() const {
    // TODO pa1.1: implement
}

std::size_t std::hash<db::TupleDesc>::operator()(const db::TupleDesc &td) const {
    // TODO pa1.1: implement
}
