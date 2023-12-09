#include <db/JoinOptimizer.h>
#include <db/PlanCache.h>
#include <cfloat>

using namespace db;

double JoinOptimizer::estimateJoinCost(int card1, int card2, double cost1, double cost2) {
    // TODO pa4.2: some code goes here
    //(1229, 1381, 1523, 1663), 1523 + 1229 * 1663
    double totalCost = cost1 + card1*cost2;// + card1*card2; // CPU
    return totalCost;
}

int JoinOptimizer::estimateTableJoinCardinality(Predicate::Op joinOp,
                                                const std::string &table1Alias, const std::string &table2Alias,
                                                const std::string &field1PureName, const std::string &field2PureName,
                                                int card1, int card2, bool t1pkey, bool t2pkey,
                                                const std::unordered_map<std::string, TableStats> &stats,
                                                const std::unordered_map<std::string, int> &tableAliasToId) {
    // TODO pa4.2: some code goes here
    if(joinOp == Predicate::Op::EQUALS){
        if(card1 > card2){
            return t1pkey ? card2 : card1;
        } else if(card1 < card2){
            return t2pkey ? card1 : card2;
        } else{
            return card1;
        }
    }if(joinOp == Predicate::Op::GREATER_THAN || joinOp == Predicate::Op::GREATER_THAN_OR_EQ ||
            joinOp == Predicate::Op::LESS_THAN || joinOp == Predicate::Op::LESS_THAN_OR_EQ) {
        return card1*card2*0.3;
    }
}

std::vector<LogicalJoinNode> JoinOptimizer::orderJoins(std::unordered_map<std::string, TableStats> stats,
                                                       std::unordered_map<std::string, double> filterSelectivities) {
    // TODO pa4.3: some code goes here
}
