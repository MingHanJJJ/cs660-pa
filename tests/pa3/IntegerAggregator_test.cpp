#include <gtest/gtest.h>
#include <db/Database.h>
#include <db/HeapFile.h>
#include <db/SeqScan.h>
#include <db/IntField.h>
#include <db/IntegerAggregator.h>
#include <db/Aggregate.h>
#include <db/Utility.h>
#include <db/Type.h>

TEST(IntegerAggregatorTest, test) {
    db::TupleDesc td = db::Utility::getTupleDesc(3);
    db::HeapFile table("table.dat", td);
    db::Database::getCatalog().addTable(&table, "t1");

    db::SeqScan ss1(table.getId(), "s1");
    db::IntegerAggregator agg(db::Aggregator::NO_GROUPING, std::nullopt, 1, db::Aggregator::Op::SUM);

    ss1.open();
    while (ss1.hasNext()) {
        auto tup = ss1.next();
        std::cout << tup.to_string() << std::endl;
        agg.mergeTupleIntoGroup(&tup);
    }
    ss1.close();
    auto agg_it = agg.iterator();

    int i = 0;
    agg_it->open();
    while (agg_it->hasNext()) {
        auto tup = agg_it->next();
        ++i;
        std::cout << tup.to_string() << std::endl;
        int count = ((db::IntField &) tup.getField(0)).getValue();
        EXPECT_EQ(count, 12075);
    }
    agg_it->close();

    EXPECT_EQ(i, 1);
}

TEST(IntegerAggregatorTest2, test) {
    db::TupleDesc td = db::Utility::getTupleDesc(3);
    db::HeapFile table("table.dat", td);
    db::Database::getCatalog().addTable(&table, "t1");

    db::SeqScan ss1(table.getId(), "s1");
    db::IntegerAggregator agg(1, db::Types::INT_TYPE, 2, db::Aggregator::Op::COUNT);

    ss1.open();
    while (ss1.hasNext()) {
        auto tup = ss1.next();
        //std::cout << tup.to_string() << std::endl;
        agg.mergeTupleIntoGroup(&tup);
    }
    ss1.close();
    auto agg_it = agg.iterator();

    int i = 0;
    agg_it->open();
    while (agg_it->hasNext()) {
        auto tup = agg_it->next();
        ++i;
        std::cout << tup.to_string() << std::endl;
        int count = ((db::IntField &) tup.getField(0)).getValue();

    }
    agg_it->close();


}

TEST(IntegerAggregatorTest3, test) {
    db::TupleDesc td = db::Utility::getTupleDesc(3);
    db::HeapFile table("table.dat", td);
    db::Database::getCatalog().addTable(&table, "t1");

    db::SeqScan ss1(table.getId(), "s1");
    db::Aggregate agg(&ss1, 0 , 1, db::Aggregator::Op::COUNT);
    db::DbIterator *it = &agg;
    it->open();
    while (it->hasNext()) {
        auto tup = it->next();
        std::cout << ' ' << tup.to_string() << std::endl;
    }
    it->close();

}

