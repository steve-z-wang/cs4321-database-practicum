# Project Phase 2 Submission

The top level class of our code is Compiler.java which is in the compiler package. The file is located at 
src/main/java/compiler/Compiler.java.

## How SMJ works 

### Create the SML operator 
The creation of the Sort-Merge Join (SMJ) logic occurs during the construction of the physical plan. To implement the 
SMJ operator, we first use the SMJConditionExtractor (which follows the visitor pattern) to traverse the join condition. 
Based on the equality conditions identified, we define the sort order for both the left and right tables. Subsequently, 
sort operators are created for both tables to ensure they are ordered by the join key. Finally, the SMJ operator is 
built on top of these sort operators, which handles merging the two sorted tables to produce the join result.

### Partition Reset
Partition resetting is implemented using an abstract reset(index) method in the PhysicalOperator class. During the 
merging process, we maintain and update the tuple index to keep track of progress. When a partition reset is required, 
the operator’s index is reset using this method. Additionally, a corresponding reset(index) method is provided for 
TupleReader and TupleWriter to ensure that all components involved in reading and writing tuples can properly handle 
index resets when needed.

## How DISTINCT works 

The creation of the DISTINCT operator is implemented in the LogicalPlanBuilder class. 

The DISTINCT logic in the LogicalPlanBuilder is implemented by combining a LogicalSort operator followed by a 
LogicalDistinct operator. The DISTINCT operation ensures that duplicate rows are removed from the query result, 
adhering to SQL semantics. Here’s how it works:

When the applyDistinct method is called, it first determines whether there is an existing ORDER BY clause in the query. 
If so, the sort operator (LogicalSort) is adjusted to account for all the columns needed for DISTINCT while preserving 
the existing sort order. If there is no ORDER BY clause, a new sort order is created based on all columns in the 
projection or the full schema if projection is not used. This ensures that all rows are sorted consistently, enabling 
efficient duplicate elimination. After sorting, the LogicalDistinct operator is added to eliminate duplicates by 
processing sorted rows sequentially, keeping only unique rows in the output. By relying on sorting and sequential 
processing, this approach avoids unbounded memory usage, as rows are processed in sorted order, allowing duplicates to 
be identified and discarded without maintaining large in-memory data structures.


## How to run the code
To run the code, you can use the following command:
```
java -jar ./build/libs/db_practicum_steve_haorun-2.jar inputDir outputDir tempDir
```