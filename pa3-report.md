# Programming Assignment 3 report

#### 1. Design of filter:

For filter, we simply call the previous filter function on every `fetchnect()` call.

#### 2. Design of join:

There is 2 way of join supported in this assignment, NLJ and hash join. For NLJ, we simply 
iterate through every combination. As for hash join, we have a hash table to find join relations.

#### 3. Design of aggregation:

There are several operation supported such as avg, min, max, count. For every operation, I used switch to 
do different type of calculation. 

#### 4. Design of insert and delete:

I iterate through the given DBiterator, and store the tuple pointer in a vector. Then, insert/ delete all the tuples in
the stored vector.