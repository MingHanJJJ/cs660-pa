#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
    // TODO pa3.2: some code goes here
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop)
                                        : child(child), afield(afield), gfield(gfield){
    // TODO pa3.2: some code goes here
    std::vector<Types::Type> types;
    std::vector<std::string> names;
    if(gfield != Aggregator::NO_GROUPING){
        types.push_back(child->getTupleDesc().getFieldType(gfield));
        types.push_back(child->getTupleDesc().getFieldType(afield));
        names.push_back(child->getTupleDesc().getFieldName(gfield));
        names.push_back(child->getTupleDesc().getFieldName(afield));
    }else{
        types.push_back(child->getTupleDesc().getFieldType(afield));
        names.push_back(child->getTupleDesc().getFieldName(afield));
    }
    td = TupleDesc(types, names);
}

int Aggregate::groupField() {
    // TODO pa3.2: some code goes here
    return gfield;
}

std::string Aggregate::groupFieldName() {
    // TODO pa3.2: some code goes here
    if(gfield != Aggregator::NO_GROUPING){
        return child->getTupleDesc().getFieldName(gfield);
    }else{
        return nullptr;
    }
}

int Aggregate::aggregateField() {
    // TODO pa3.2: some code goes here
    return afield;
}

std::string Aggregate::aggregateFieldName() {
    // TODO pa3.2: some code goes here
    return child->getTupleDesc().getFieldName(afield);
}

Aggregator::Op Aggregate::aggregateOp() {
    // TODO pa3.2: some code goes here
    return aop;
}

void Aggregate::open() {
    // TODO pa3.2: some code goes here
    Operator::open();
    child->open();
}

void Aggregate::rewind() {
    // TODO pa3.2: some code goes here
    Operator::rewind();
    child->rewind();
}

const TupleDesc &Aggregate::getTupleDesc() const {
    // TODO pa3.2: some code goes here
    return td;
}

void Aggregate::close() {
    // TODO pa3.2: some code goes here
    Operator::close();
    child->close();
}

std::vector<DbIterator *> Aggregate::getChildren() {
    // TODO pa3.2: some code goes here
}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.2: some code goes here
}
