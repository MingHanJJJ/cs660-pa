#include <db/IntegerAggregator.h>
#include <db/IntField.h>

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    // TODO pa3.2: some code goes here
    int gbfield;
    TupleDesc td;
    std::unordered_map<Field *, int> count;
    std::unordered_map<Field *, int>::iterator it;
    bool isOpen = false;
public:
    IntegerAggregatorIterator(int gbfield,
                              const TupleDesc &td,
                              const std::unordered_map<Field *, int> &count) : gbfield(gbfield), td(td), count(count){
        // TODO pa3.2: some code goes here
    }

    void open() override {
        // TODO pa3.2: some code goes here
        isOpen = true;
        it = count.begin();
    }

    bool hasNext() override {
        // TODO pa3.2: some code goes here
        return it != count.end();
    }

    Tuple next() override {
        // TODO pa3.2: some code goes here
        Tuple tup = Tuple(td);
        if(gbfield != -1){
            tup.setField(0, it->first);
            tup.setField(1, new IntField(it->second));
        } else{
            tup.setField(0, new IntField(it->second));
        }
        it++;
        return tup;

    }

    void rewind() override {
        // TODO pa3.2: some code goes here
        close();
        open();
    }

    const TupleDesc &getTupleDesc() const override {
        // TODO pa3.2: some code goes here
        return td;
    }

    void close() override {
        // TODO pa3.2: some code goes here
        isOpen = false;
    }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Aggregator::Op what) : gbfield(gbfield), gbfieldtype(gbfieldtype), afield(afield), what(what) {
    // TODO pa3.2: some code goes here
    std::vector<Types::Type> types;
    std::vector<std::string> names;
    if(gbfield != -1){
        types.push_back(gbfieldtype.value());
        types.push_back(Types::INT_TYPE);
        names.push_back("");
        names.push_back("");
    }else{
        types.push_back(Types::INT_TYPE);
        names.push_back("");
    }
    td = TupleDesc(types, names);

}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    // TODO pa3.2: some code goes here
    Field *f;
    if(gbfield != -1){
        Field &field = const_cast<Field &>(tup->getField(gbfield));
        Field *field_ptr = &field;
        bool find = false;
        for(auto &f_map : field_ptr_map){
            if(f_map->compare(Predicate::Op::EQUALS, field_ptr)){
                f = f_map;
                find = true;
                break;
            }
        }
        if(!find){
            field_ptr_map.push_back(field_ptr);
            f = field_ptr;
        }
    }else{
        f = nullptr;
    }




    if(count.find(f) != count.end()){ //find
        IntField intField = (IntField &&) tup->getField(afield);
        switch (what) {
            case Aggregator::Op::AVG:
                avg_counter[f].first += intField.getValue();
                avg_counter[f].second++;
                count[f] = avg_counter[f].first/avg_counter[f].second;
                break;
            case Aggregator::Op::COUNT:
                count[f]++;
                break;
            case Aggregator::Op::MAX:
                count[f] = std::max(count[f], intField.getValue());
                break;
            case Aggregator::Op::MIN:
                count[f] = std::min(count[f], intField.getValue());
                break;
            case Aggregator::Op::SUM:
                count[f] += intField.getValue();
                break;
            default:
                break;
        }
    } else{
        IntField intField = (IntField &&) tup->getField(afield);
        switch (what) {
            case Aggregator::Op::AVG:
                avg_counter[f].first = intField.getValue();
                avg_counter[f].second = 1;
                count[f] = avg_counter[f].first;
                break;
            case Aggregator::Op::COUNT:
                count[f] = 1;
                break;
            case Aggregator::Op::MAX:
                count[f] = intField.getValue();
                break;
            case Aggregator::Op::MIN:
                count[f] = intField.getValue();
                break;
            case Aggregator::Op::SUM:
                count[f] = intField.getValue();
                break;
            default:
                break;
        }
    }

}


DbIterator *IntegerAggregator::iterator() const {
    // TODO pa3.2: some code goes here
    return new IntegerAggregatorIterator(gbfield, td, count);
}
