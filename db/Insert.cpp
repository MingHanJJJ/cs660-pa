#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
    // TODO pa3.3: some code goes here
    if(inserted) return std::nullopt;

    int count = 0;
    std::vector<Tuple> tuple_to_inserted;
    child->open();
    while(child->hasNext()){
        //Tuple* tup_ptr = new Tuple();
        Tuple tup_ptr = child->next();
        count++;
        tuple_to_inserted.push_back(tup_ptr);
        //db::Database::getBufferPool().insertTuple(t, tableId, &tup_ptr);
    }
    for(auto &tuple_insert : tuple_to_inserted){
        db::Database::getBufferPool().insertTuple(t, tableId, &tuple_insert);
    }
    child->close();
    Tuple tup = Tuple(td);
    tup.setField(0, new IntField(count));
    inserted = true;
    return tup;
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId): t(t), child(child), tableId(tableId) {
    // TODO pa3.3: some code goes here
    inserted = false;
    std::vector<Types::Type> types;
    std::vector<std::string> names;
    types.push_back(Types::INT_TYPE);
    names.push_back("inserted records");
    td = TupleDesc(types, names);

    children.push_back(child);

}

const TupleDesc &Insert::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return td;
}

void Insert::open() {
    // TODO pa3.3: some code goes here
    Operator::open();
    child->open();
}

void Insert::close() {
    // TODO pa3.3: some code goes here
    Operator::close();
    child->close();
}

void Insert::rewind() {
    // TODO pa3.3: some code goes here
    Operator::rewind();
    child->rewind();
}

std::vector<DbIterator *> Insert::getChildren() {
    // TODO pa3.3: some code goes here
    children[0] = child;
    return children;
}

void Insert::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    this->child = children[0];
    this->children[0] = children[0];
}
