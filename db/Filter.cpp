#include <db/Filter.h>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child) : p(p), child(child) {
    // TODO pa3.1: some code goes here
}

Predicate *Filter::getPredicate() {
    // TODO pa3.1: some code goes here
    Predicate* p_ptr = &p;
    return p_ptr;
}

const TupleDesc &Filter::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return child->getTupleDesc();
}

void Filter::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    child->open();
}

void Filter::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    child->close();
}

void Filter::rewind() {
    // TODO pa3.1: some code goes here
    Operator::rewind();
    child->rewind();
}

std::vector<DbIterator *> Filter::getChildren() {
    // TODO pa3.1: some code goes here
    return children;
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    this->children = children;
}

std::optional<Tuple> Filter::fetchNext() {
    // TODO pa3.1: some code goes here
    if(!child->hasNext()) return std::nullopt;
    db::Tuple tup = child->next();
    while(!p.filter(tup)) {
        if(child->hasNext()){
            tup = child->next();
        } else{
            return std::nullopt;
        }
    }
    return tup;
}
