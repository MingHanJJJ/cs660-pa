#include <db/Tuple.h>

using namespace db;

//
// Tuple
//

// TODO pa1.1: implement
Tuple::Tuple(const TupleDesc &td, RecordId *rid) {
    std::size_t fields_len = td.numFields();
    fields = std::vector<Field*>(fields_len);
    this->td = td;
    this->rid = rid;
}

const TupleDesc &Tuple::getTupleDesc() const {
    // TODO pa1.1: implement
    return td;
}

const RecordId *Tuple::getRecordId() const {
    // TODO pa1.1: implement
    return rid;
}

void Tuple::setRecordId(const RecordId *id) {
    // TODO pa1.1: implement
    const RecordId *rid_const = id;
    rid = rid_const;
}

const Field &Tuple::getField(int i) const {
    // TODO pa1.1: implement
    return *fields[i];
}

void Tuple::setField(int i, const Field *f) {
    // TODO pa1.1: implement
    fields[i] = const_cast<Field*>(f);
}

Tuple::iterator Tuple::begin() const {
    // TODO pa1.1: implement
    return fields.begin();
}

Tuple::iterator Tuple::end() const {
    // TODO pa1.1: implement
    return fields.end();
}

std::string Tuple::to_string() const {
    // TODO pa1.1: implement
    std::string str;
    for(auto &field : fields){
        str += field->to_string() +", ";
    }
    return str;
}
