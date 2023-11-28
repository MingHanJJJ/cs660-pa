#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child): t(t), child(child) {
    // TODO pa3.3: some code goes here
    std::vector<Types::Type> types;
    std::vector<std::string> names;
    types.push_back(Types::INT_TYPE);
    names.push_back("deleted records");
    td = TupleDesc(types, names);

    deleted = false;
    children.push_back(child);

}

const TupleDesc &Delete::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return td;
}

void Delete::open() {
    // TODO pa3.3: some code goes here
    Operator::open();
    child->open();
}

void Delete::close() {
    // TODO pa3.3: some code goes here
    Operator::close();
    child->close();
}

void Delete::rewind() {
    // TODO pa3.3: some code goes here
    Operator::rewind();
    child->rewind();
}

std::vector<DbIterator *> Delete::getChildren() {
    // TODO pa3.3: some code goes here
    children[0] = child;
    return children;
}


void Delete::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    this->child = children[0];
    this->children[0] = children[0];
}

std::optional<Tuple> Delete::fetchNext() {
    // TODO pa3.3: some code goes here
    if(deleted) return std::nullopt;

    int count = 0;
    std::vector<Tuple*> tuple_to_deleted;
    child->open();
    while(child->hasNext()){
        Tuple tup = child->next();
        Tuple* tup_ptr = &tup;
        count++;
        tuple_to_deleted.push_back(tup_ptr);
    }
    for(auto &tuple_delete : tuple_to_deleted){
        //db::Database::getBufferPool().deleteTuple(t, tuple_delete);
    }
    child->close();
    Tuple tup = Tuple(td);
    tup.setField(0, new IntField(count));
    deleted = true;
    return tup;
}
