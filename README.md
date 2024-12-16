# Project Phase 3 Submission

The top level class of our code is compiler.java which is in the compiler package. The file is located at 
src/main/java/compiler/compiler.java.

## How Index Scan works 

### where the lowkey and highkey are set 

We first would extract the lowkey and highkey from the selection condition in the query when creating the physical plan. 
The logic of this part is located in IndexConditionExtractor.java. The file is located at physicaloperator/scan/IndexConditionExtractor.java.
I uses a visitor pattern to extract the lowkey and highkey from the selection condition.

lowkey and highkey are set in the IndexScanOperator, the location of the code is in the physicaloperator/scan/IndexScanOperator.java file.


### where in your code the grader can see different handling of clustered vs. unclustered indexes 

When we build the index file with the IndexBuilder, we would set the isClustered flag to true or false. And if the index is clustered, 
We would sort and replace the original data file with the sorted data file. The location of the code is in the Index/IndexBuilder.java file.

In the IndexScanOperator we have seperated reset and getNextTuple for clustered and unclustered indexes. The location of the code is in the physicaloperator/scan/IndexScanOperator.java file.

### an explanation on how you perform the root-to-leaf tree descent and which nodes are deserialized

The root-to-leaf tree descent in the IndexScanOperator involves traversing the B+ Tree structure to locate the relevant 
leaf node that may contain the desired records. Starting at the root node, the algorithm evaluates key ranges at each level
of the tree, using the provided lowKey value (or a default starting point if lowKey is null) to determine the appropriate 
child node to follow. This process continues iteratively until reaching the corresponding leaf node. During this traversal,
only the internal nodes necessary to locate the leaf node and the relevant leaf nodes are deserialized from disk. 
Deserialization is performed using the file channel and buffer (indexChannel and indexPageBuffer), 
ensuring efficient access to nodes. Once the correct leaf node is reached, its entries are scanned to find matching 
records within the specified key range. If no valid starting point exists, traversal stops, and no further nodes are deserialized.

## How to run the code
To run the code, you can use the following command:
```
java -jar ./build/libs/db_practicum_steve_haorun-3.jar ./src/test/integration/resources/p3_b_plus_tree_test_samples/interpreter_config_file.txt
```
