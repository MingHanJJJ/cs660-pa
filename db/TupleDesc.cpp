#include <db/TupleDesc.h>

using namespace db;

//
// TDItem
//

bool TDItem::operator==(const TDItem &other) const {
    // TODO pa1.1: implement
    if(fieldName == other.fieldName && fieldType == other.fieldType){
        return true;
    } else{
        return false;
    }
}

std::size_t std::hash<TDItem>::operator()(const TDItem &r) const {
    // TODO pa1.1: implement
    return hash<std::string>{}(r.fieldName) ^ hash<Types::Type>{}(r.fieldType);
}

//
// TupleDesc
//

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types) {
    td_size_byte = 0;
    size_t size = types.size();
    td_items.reserve(size);
    for(int i = 0; i < size; i++){
        td_items.push_back((TDItem(types[i], std::to_string(i))));
        td_size_byte += getLen(types[i]); // calculate size in byte
    }
}

// TODO pa1.1: implement
TupleDesc::TupleDesc(const std::vector<Types::Type> &types, const std::vector<std::string> &names) {
    td_size_byte = 0;
    size_t size = types.size();
    td_items.reserve(size);
    for(int i = 0; i < size; i++){
        td_items.push_back((TDItem(types[i], names[i])));
        td_size_byte += getLen(types[i]); // calculate size in byte
    }
}

size_t TupleDesc::numFields() const {
    // TODO pa1.1: implement
    return  td_items.size();
}

std::string TupleDesc::getFieldName(size_t i) const {
    // TODO pa1.1: implement
    return td_items[i].fieldName;
}

Types::Type TupleDesc::getFieldType(size_t i) const {
    // TODO pa1.1: implement
    return td_items[i].fieldType;
}

int TupleDesc::fieldNameToIndex(const std::string &fieldName) const {
    // TODO pa1.1: implement
    std::size_t size = td_items.size();
    for(int i = 0; i < size; i++){
        if(td_items[i].fieldName == fieldName){
            return i;
        }
    }
    throw std::invalid_argument("Input FieldName is invalid");
}

size_t TupleDesc::getSize() const {
    // TODO pa1.1: implement
    return td_size_byte;
}

TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // TODO pa1.1: implement
    TupleDesc td3;
    td3.td_items.insert(td3.end(), td1.begin(), td1.end());
    td3.td_items.insert(td3.end(), td2.begin(), td2.end());
    // build private variable td_size_bytes
    td3.td_size_byte = 0;
    for(auto &item : td3.td_items){
        td3.td_size_byte += Types::getLen(item.fieldType);
    }
    return td3;
}

std::string TupleDesc::to_string() const {
    // TODO pa1.1: implement
    std::string out;
    size_t size = td_items.size();
    for(int i = 0; i < size; i++){
        std::string str_i = "[" + std::to_string(i) + "]";
        out += Types::to_string(td_items[i].fieldType) + str_i;
        out += "(" + td_items[i].fieldName + str_i +"),";
    }
    return out;
}

bool TupleDesc::operator==(const TupleDesc &other) const {
    // TODO pa1.1: implement
    size_t size = this->td_items.size();
    if(td_size_byte != other.getSize() || size != other.td_items.size()){
        return false;
    }
    for(int i = 0; i < size; i++){
        if(this->td_items[i].fieldType != other.td_items[i].fieldType){
            return false;
        }
    }
    return true;
}

TupleDesc::iterator TupleDesc::begin() const {
    // TODO pa1.1: implement
    return td_items.begin();

}

TupleDesc::iterator TupleDesc::end() const {
    // TODO pa1.1: implement
    return td_items.end();
}

std::size_t std::hash<db::TupleDesc>::operator()(const db::TupleDesc &td) const {
    // TODO pa1.1: implement
    std::size_t hash_t = 0;
    for(auto &tditem : td){
        hash_t ^= hash<db::TDItem>{}(tditem);
    }
    return hash_t;
}
