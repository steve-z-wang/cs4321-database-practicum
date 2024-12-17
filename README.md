# Project Phase 4 Submission

The top level class of our code is compiler.java which is in the compiler package. The file is located at 
src/main/java/compiler/compiler.java.

---

## **Major Implementations**

### **1. Selection Pushing**
**Location**:
- `LogicalPlanBuilder`
- `LogicalOperatorVisitor`

**Logic**:
- **Union-Find Data Structure**: Implemented to propagate selection conditions across equality constraints in the `WHERE` clause.
- **Selection Pushing**:
    - A visitor pattern (`LogicalOperatorVisitor`) traverses the `WHERE` clause and constructs a union-find structure.
    - Constraints of the form `att1 OP value` and `att1 = att2` are processed.
    - Derived constraints are pushed to their respective children in the logical plan tree.

---

### **2. Implementation Choice for Logical Selections**
**Location**:
- `PhysicalPlanBuilder`
- `SelectOperator`

**Logic**:
- For each selection operator:
    - The available access paths (index scan or table scan) are evaluated using cost estimates based on **I/O cost**.
    - **Index Scan Cost**: Computed using reduction factors derived from `stats.txt`.
    - **Table Scan Cost**: Based on the total pages required to read the relation.
- The best access path with the lowest estimated cost is chosen for execution.

---

### **3. Join Order Optimization**
**Location**:
- `PhysicalPlanBuilder`

**Logic**:
- **Dynamic Programming Algorithm**:
    - A bottom-up approach is used to generate optimal left-deep join trees.
    - Subsets of relations are evaluated in increasing size. For each subset, the algorithm computes:
        - The cost of the optimal plan.
        - The corresponding join order.
    - The cost function is based on intermediate relation sizes, approximated using **V-values** (number of distinct values for attributes).

---

### **4. Join Implementation Choice**
**Location**:
- `PhysicalPlanBuilder`
- `physicaloperator/join`

**Logic**:
- **BNLJ (Block Nested Loop Join)** and **SMJ (Sort Merge Join)** are implemented.
- **Selection Criteria**:
    - **SMJ** is preferred when the join condition involves equality, and the input relations are already sorted or small enough to sort efficiently.
    - **BNLJ** is chosen when the join condition involves cross-products or non-equality conditions.
- **Strategy Rationale**:
    - SMJ performs better for sorted inputs and equality joins.
    - BNLJ is a fallback for other scenarios, ensuring correctness.

---



## **Notes**
- The `stats.txt` file is generated correctly and used for cost estimation.
- Index information and multiple access paths are handled properly.
- Dynamic programming ensures optimal join order selection based on intermediate relation sizes.

---


## How to run the code
To run the code, you can use the following command:
```
java -jar ./build/libs/db_practicum_steve_haorun-4.jar ./src/test/integration/resources/p4_optimizer_samples/interpreter_config_file.txt
```
