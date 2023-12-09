#include <gtest/gtest.h>
#include <db/HeapFile.h>
#include <db/IntField.h>
#include <db/IntHistogram.h>
#include <db/Utility.h>

void random_data(db::IntHistogram &hist) {
    std::vector values = {148, 171, 193, 135, 100, 124, 110, 132, 182, 165, 108, 189, 189, 153, 172, 191, 181, 122, 131,
                          146, 124, 176, 107, 108, 120, 166, 147, 170, 146, 146, 174, 179, 189, 167, 102, 110, 170, 195,
                          137, 190, 132, 166, 105, 161, 115, 104, 184, 121, 163, 122, 116, 147, 179, 191, 175, 165, 103,
                          127, 175, 151, 125, 121, 109, 188, 176, 125, 152, 174, 185, 152, 197, 141, 169, 176, 141, 110,
                          122, 166, 153, 150, 171, 173, 103, 132, 139, 127, 119, 134, 105, 173, 150, 123, 114, 121, 152,
                          185, 111, 121, 185, 107, 173, 181, 187, 127, 175, 148, 113, 182, 124, 141, 158, 194, 193, 104,
                          117, 142, 102, 120, 167, 186, 153, 115, 109, 146, 165, 140, 118, 151, 194, 118, 185, 113, 165,
                          162, 180, 106, 120, 167, 120, 151, 102, 126, 128, 172, 198, 111, 116, 151, 160, 157, 128, 108,
                          144, 159, 142, 166, 114, 194, 166, 125, 120, 128, 137, 141, 175, 152, 199, 146, 161, 122, 190,
                          111, 179, 192, 148, 189, 148, 117, 134, 108, 113, 190, 146, 121, 178, 126, 120, 136, 124, 185,
                          134, 169, 117, 160, 140, 157, 174, 135, 196, 119, 131, 167, 193, 145, 195, 151, 140, 100, 170,
                          188, 107, 141, 125, 130, 182, 184, 176, 137, 127, 119, 170, 160, 185, 191, 106, 147, 156, 166,
                          178, 158, 173, 132, 119, 170, 192, 185, 191, 198, 153, 133, 108, 131, 159, 129, 190, 154, 115,
                          165, 168, 189, 108, 178, 118, 144, 193, 175, 169, 120, 192, 128, 121, 186, 173, 172, 114, 195,
                          110, 179, 196, 144, 110, 193, 191, 134, 143, 139, 161, 158, 188, 191, 196, 121, 168, 122, 182,
                          190, 167, 112, 184, 168, 176, 166, 154, 103, 197, 196, 157, 151, 146, 189, 108, 173, 154, 187,
                          173, 128, 135, 100, 144, 183, 186, 120, 112, 107, 175, 154, 191, 173, 179, 129, 118, 139, 150,
                          198, 151, 178, 144, 145, 120, 184, 142, 113, 190, 162, 170, 171, 129, 165, 144, 187, 108, 133,
                          185, 182, 108, 105, 161, 195, 137, 152, 137, 184, 136, 179, 142, 171, 143, 122, 107, 145, 132,
                          166, 123, 136, 157, 156, 148, 113, 198, 190, 189, 169, 151, 111, 122, 121, 175, 155, 168, 172,
                          113, 109, 143, 111, 150, 114, 154, 163, 187, 179, 149, 175, 195, 119, 143, 189, 160, 176, 136,
                          169, 154, 190, 110, 171, 176, 124, 115, 154, 110, 123, 179, 115, 111, 166, 192, 197, 141, 160,
                          185, 114, 164, 190, 163, 168, 104, 109, 135, 137, 115, 174, 122, 183, 125, 149, 129, 180, 168,
                          189, 111, 148, 112, 147, 131, 186, 121, 195, 127, 130, 112, 187, 185, 150, 158, 148, 176, 155,
                          153, 195, 147, 195, 107, 166, 182, 194, 166, 188, 164, 155, 165, 186, 167, 193, 111, 142, 179,
                          113, 144, 128, 164, 189, 121, 191, 177, 140, 191, 135, 117, 194, 199, 161, 186, 137, 132, 118,
                          190, 164, 181, 162, 140, 167, 163, 198, 171, 194, 173, 148, 179, 168, 180, 168, 133, 166, 135,
                          154, 102, 102, 128, 125, 132, 108, 152, 163, 108, 152, 158, 147, 124, 138, 128, 102, 193, 137,
                          141, 191, 150, 137, 170, 180, 172, 142, 188, 198, 156, 108, 162, 180, 125, 135, 120, 111, 175,
                          136, 188, 195, 170, 194, 180, 111, 192, 142, 114, 103, 190, 125, 199, 151, 192, 114, 140, 105,
                          199, 170, 142, 182, 197, 139, 115, 188, 159, 150, 153, 195, 127, 173, 111, 103, 131, 154, 144,
                          161, 125, 167, 196, 137, 144, 148, 157, 130, 171, 158, 182, 177, 166, 150, 130, 177, 164, 133,
                          161, 153, 128, 146, 110, 185, 100, 161, 181, 157, 165, 115, 178, 108, 141, 114, 123, 196, 195,
                          193, 102, 120, 182, 132, 182, 131, 174, 194, 124, 159, 187, 150, 100, 125, 163, 164, 136, 186,
                          125, 119, 179, 118, 164, 164, 141, 176, 115, 134, 171, 141, 167, 194, 194, 186, 151, 102, 177,
                          163, 188, 141, 122, 138, 103, 184, 191, 173, 141, 171, 156, 111, 146, 127, 172, 137, 171, 124,
                          117, 159, 188, 198, 135, 158, 129, 173, 198, 118, 171, 167, 176, 190, 116, 109, 107, 141, 108,
                          161, 160, 175, 102, 124, 126, 173, 126, 128, 163, 188, 150, 167, 192, 143, 167, 174, 119, 164,
                          191, 149, 153, 146, 193, 187, 111, 188, 124, 181, 130, 186, 136, 145, 109, 150, 144, 103, 182,
                          102, 189, 146, 193, 141, 108, 179, 164, 101, 184, 107, 100, 156, 180, 198, 191, 122, 184, 122,
                          114, 162, 198, 103, 107, 194, 105, 128, 136, 137, 101, 108, 189, 177, 144, 153, 184, 155, 167,
                          124, 129, 101, 189, 105, 155, 113, 185, 126, 168, 120, 135, 156, 114, 189, 111, 137, 133, 121,
                          138, 179, 114, 194, 180, 188, 183, 139, 130, 104, 177, 184, 168, 183, 190, 185, 184, 183, 132,
                          187, 126, 129, 108, 153, 198, 164, 137, 175, 195, 195, 159, 129, 128, 117, 138, 131, 124, 186,
                          120, 133, 135, 159, 140, 122, 188, 196, 192, 143, 196, 111, 175, 162, 182, 181, 172, 198, 158,
                          188, 154, 191, 158, 144, 107, 188, 138, 144, 192, 100, 125, 198, 144, 125, 136, 173, 185, 115,
                          141, 130, 128, 147, 170, 119, 108, 160, 158, 103, 184, 180, 196, 113, 180, 124, 178, 105, 155,
                          141, 176, 196, 128, 188, 172, 104, 122, 181, 179, 183, 154, 183, 180, 115, 174, 150, 178, 173,
                          117, 124, 105, 143, 173, 145, 181, 194, 191, 189, 115, 128, 106, 142, 134, 163, 128, 155, 110,
                          183, 137, 112, 157, 172, 148, 165, 106, 116, 122, 188, 116, 148, 188, 170, 188, 135, 180, 123,
                          155, 141, 152, 155, 125, 151, 157, 137, 104, 153, 191, 143, 145, 191, 162, 175, 115, 163, 136,
                          100, 186, 136, 177, 197, 147, 192, 166, 175, 189, 158, 121, 195, 187, 193, 148, 180, 196, 157,
                          143, 124, 111, 121, 177, 119, 137, 104, 127, 121, 185, 146};
    for (auto v: values) {
        hist.addValue(v);
    }
}

TEST(IntHistogramTest, equals) {
    db::IntHistogram hist(10, 50, 249);
    random_data(hist);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::EQUALS, 110), 204 / 20. / 1000, 0.001);
}

TEST(IntHistogramTest, not_equals) {
    db::IntHistogram hist(10, 50, 249);
    random_data(hist);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::NOT_EQUALS, 209), (1000 - 111 / 20.) / 1000, 0.001);
}

TEST(IntHistogramTest, less_than) {
    db::IntHistogram hist(10, 50, 249);
    random_data(hist);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::LESS_THAN, 160), (190 / 2 + 181 + 204 + 83) / 1000., 0.001);
}

TEST(IntHistogramTest, less_than_or_equals) {
    db::IntHistogram hist(10, 50, 249);
    random_data(hist);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::LESS_THAN_OR_EQ, 160), (190 * 11 / 20. + 181 + 204 + 83) / 1000., 0.001);
}

TEST(IntHistogramTest, greater_than) {
    db::IntHistogram hist(10, 50, 249);
    random_data(hist);
    auto x = hist.estimateSelectivity(db::Predicate::Op::GREATER_THAN, 160);
    EXPECT_TRUE(std::abs(x - (190 / 2 + 231 + 111) / 1000.) < 0.001 or
                std::abs(x - (190 * 9 / 20. + 231 + 111) / 1000.) < 0.001);
}

TEST(IntHistogramTest, greater_than_or_equals) {
    db::IntHistogram hist(10, 50, 249);
    random_data(hist);

    auto x = hist.estimateSelectivity(db::Predicate::Op::GREATER_THAN_OR_EQ, 120);
    EXPECT_TRUE(std::abs(x - (204 * 10 / 20. + 181 + 190 + 231 + 111) / 1000.) < 0.001 or
                std::abs(x - (204 * 11 / 20. + 181 + 190 + 231 + 111) / 1000.) < 0.001);
}

TEST(IntHistogramTest, empty) {
    db::IntHistogram hist(10, 50, 249);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::EQUALS, 110), 0, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::NOT_EQUALS, 110), 0, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::LESS_THAN, 110), 0, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::LESS_THAN_OR_EQ, 110), 0, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::GREATER_THAN_OR_EQ, 110), 0, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::GREATER_THAN, 110), 0, 0.001);
}

TEST(IntHistogramTest, small) {
    db::IntHistogram hist(10, 50, 249);
    random_data(hist);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::GREATER_THAN_OR_EQ, 20), 1, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::LESS_THAN, 20), 0, 0.001);
}

TEST(IntHistogramTest, big) {
    db::IntHistogram hist(10, 50, 249);
    random_data(hist);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::GREATER_THAN_OR_EQ, 300), 0, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::LESS_THAN, 300), 1, 0.001);
}

TEST(IntHistogramTest, sparse) {
    db::IntHistogram hist(1000, 50, 249);
    random_data(hist);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::EQUALS, 160), 7. / 1000, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::NOT_EQUALS, 209), 1, 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::LESS_THAN, 160), 563 / 1000., 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::LESS_THAN_OR_EQ, 160), 570 / 1000., 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::GREATER_THAN, 160), 430 / 1000., 0.001);
    EXPECT_NEAR(hist.estimateSelectivity(db::Predicate::Op::GREATER_THAN_OR_EQ, 160), 437 / 1000., 0.001);
}
