#include <db/Join.h>

using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2) : p(p), child1(child1), child2(child2){
    // TODO pa3.1: some code goes here
    const db::TupleDesc &td1 = child1->getTupleDesc();
    const db::TupleDesc &td2 = child2->getTupleDesc();
    td = db::TupleDesc::merge(td1, td2);
    childern.push_back(child1);
    childern.push_back(child2);
}

JoinPredicate *Join::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return p;
}

std::string Join::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return child1->getTupleDesc().getFieldName(p->getField1());
}

std::string Join::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return child2->getTupleDesc().getFieldName(p->getField2());
}

const TupleDesc &Join::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return td;
}

void Join::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    child1->open();
    child2->open();
}

void Join::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    child1->close();
    child2->close();
}

void Join::rewind() {
    // TODO pa3.1: some code goes here
    Operator::rewind();
    close();
    open();
}

std::vector<DbIterator *> Join::getChildren() {
    // TODO pa3.1: some code goes here
    childern[0] = child1;
    childern[1] = child2;
    return childern;
}

void Join::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    child1 = children[0];
    child2 = children[1];
    this->childern[0] = child1;
    this->childern[1] = child2;
}

std::optional<Tuple> Join::fetchNext() {
    // TODO pa3.1: some code goes here
    if(tup1 == std::nullopt) {
        tup1 = child1->next();
    }
    if(!child2->hasNext()) {
        tup1 = child1->next();
        child2->rewind();
    }

    db::Tuple tup2 = child2->next();;
    while(!p->filter(reinterpret_cast<Tuple *>(&tup1), &tup2)) {
        if(child2->hasNext()){
            // child 2 is not in the end
            tup2 = child2->next();
        } else{
            // child to is the end, move child1 to next
            if(child1->hasNext()){
                tup1 = child1->next();

                child2->rewind();
                tup2 = child2->next();
            } else {
                // both child1 and 2 are at the end
                return std::nullopt;
            }
        }
    }
    db::Tuple out_tuple = Tuple(td);
    int index = 0;
    // combine tup1 and tup2
    for(int i = 0; i < tup1->getTupleDesc().numFields(); i++) {
        out_tuple.setField(index, &tup1->getField(i));
        index++;
    }
    for(int i = 0; i < tup2.getTupleDesc().numFields(); i++) {
        out_tuple.setField(index, &tup2.getField(i));
        index++;
    }
    return out_tuple;
}

