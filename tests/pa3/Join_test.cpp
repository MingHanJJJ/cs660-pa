#include <gtest/gtest.h>
#include <db/Database.h>
#include <db/Utility.h>
#include <db/HeapFile.h>
#include <db/SeqScan.h>
#include <db/IntField.h>
#include <db/Join.h>

static int count(db::DbIterator *it) {
    int i = 0;
    it->open();
    while (it->hasNext()) {
        auto tup = it->next();
        ++i;
        std::cout << i << ' ' << tup.to_string() << std::endl;
    }
    it->close();
    return i;
}

TEST(JoinTest, test) {
    db::TupleDesc td = db::Utility::getTupleDesc(3);

    db::HeapFile table("table.dat", td);
    db::Database::getCatalog().addTable(&table, "t1");
    db::SeqScan ss1(table.getId(), "s1");
    db::SeqScan ss2(table.getId(), "s2");

    db::JoinPredicate pred(0, db::Predicate::Op::GREATER_THAN_OR_EQ, 1);
    db::Join join(&pred, &ss1, &ss2);
    join.getChildren();
    EXPECT_EQ(count(&join), 5250);
}

TEST(JoinPredicateTest, test) {
    db::TupleDesc td = db::Utility::getTupleDesc(3);

    db::HeapFile table("table.dat", td);
    db::Database::getCatalog().addTable(&table, "t1");
    db::JoinPredicate pred(0, db::Predicate::Op::GREATER_THAN_OR_EQ, 1);

    db::IntField tup1_1(7);
    db::IntField tup1_2(2);
    db::IntField tup1_3(3);
    db::IntField tup2_1(4);
    db::IntField tup2_2(5);
    db::IntField tup2_3(6);

    db::Tuple tup1(td);
    db::Tuple tup2(td);

    tup1.setField(0, &tup1_1);
    tup1.setField(1, &tup1_2);
    tup1.setField(2, &tup1_3);

    tup2.setField(0, &tup2_1);
    tup2.setField(1, &tup2_2);
    tup2.setField(2, &tup2_3);

    EXPECT_EQ(pred.filter(&tup1, &tup2), true);

}
