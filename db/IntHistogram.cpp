#include <db/IntHistogram.h>


using namespace db;

IntHistogram::IntHistogram(int buckets, int min, int max) : buckets(buckets), min(min), max(max) {
    // TODO pa4.1: some code goes here
    int width;
    if((max - min + 1) % buckets != 0){
        width = (max - min + 1)/buckets + 1;
    } else{
        width = (max - min + 1)/buckets;
    }
    // from bucket 1 to n-1
    int num = min;
    for(int b = 0; b < buckets-1; b++){
        histog.push_back(Bucket(num, num + width - 1));
        num += width;
    }
    // last bucket
    histog.push_back(Bucket(num, max));
    totalHeight = 0;
}

void IntHistogram::addValue(int v) {
    // TODO pa4.1: some code goes here
    totalHeight++;
    for(int i = 0; i < buckets; i++){
        if(histog[i].min <= v && v <= histog[i].max){
            histog[i].height++;
            return;
        }
    }
}

double IntHistogram::estimateSelectivity(Predicate::Op op, int v) const {
    // TODO pa4.1: some code goes here
    if(op == Predicate::Op::GREATER_THAN || op == Predicate::Op::GREATER_THAN_OR_EQ){
        int b;
        // find b
        for(int i = 0; i < buckets; i++){
            if(histog[i].min <= v && v <= histog[i].max){
                b = i;
                break;
            }
        }
        double b_f = (double) histog[b].height / totalHeight;
        double b_part = (double)(histog[b].max - v) / (histog[b].max - histog[b].min + 1);
        double selectivity = b_f * b_part;

        // rest buckets larger than b
        for(int i = b+1; i < buckets; i++){
            selectivity += (double) histog[i].height / totalHeight;
        }
        return selectivity;

    }else if(op == Predicate::Op::LESS_THAN || op == Predicate::Op::LESS_THAN_OR_EQ){
        int b;
        // find b
        for(int i = 0; i < buckets; i++){
            if(histog[i].min <= v && v <= histog[i].max){
                b = i;
                break;
            }
        }
        double b_f = (double) histog[b].height / totalHeight;
        double b_part = (double)(v - histog[b].min) / (histog[b].max - histog[b].min + 1);
        double selectivity = b_f * b_part;

        // rest buckets larger than b
        for(int i = b-1; i >= 0; i--){
            selectivity += (double) histog[i].height / totalHeight;
        }
        return selectivity;

    }else if(op == Predicate::Op::EQUALS){
        int b;
        // find b
        for(int i = 0; i < buckets; i++){
            if(histog[i].min <= v && v <= histog[i].max){
                b = i;
                break;
            }
        }

        double selectivity = ((double) histog[b].height/(histog[b].max - histog[b].min + 1)) / totalHeight;
        return selectivity;
    }

}

double IntHistogram::avgSelectivity() const {
    // TODO pa4.1: some code goes here
}

std::string IntHistogram::to_string() const {
    // TODO pa4.1: some code goes here
    for(int i = 0; i < buckets; i++){
        std::cout << "bucket" << i <<"   min: " << histog[i].min <<" max: " << histog[i].max << " height: " << histog[i].height<<std::endl;
    }
}

