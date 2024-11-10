# INSTRUCTIONS.md

## Top-level Class

The top-level class for running the program is `Compiler`, located in the `src/main/java/compiler` directory. This class acts as the entry point to the program, reading input queries and producing the required output.

## Join Condition Extraction Logic

The logic for extracting join conditions from the `WHERE` clause is implemented in the `WhereClauseProcessor` class, which can be found in the `src/main/java/io/QueryPlanBuilder.java` file. Specifically, the `classifyExpression` method within `WhereClauseProcessor` is responsible for determining whether an expression is a join condition or a filter condition and processes them accordingly. The comments in the `WhereClauseProcessor` class detail how join conditions are separated from filter conditions and added to the query plan.

## Known Bugs

There are no known bugs at the time of submission. The code assumes that all input operators provide non-null tuples and does not handle null values explicitly.
