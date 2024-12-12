package queryplan;

import config.IndexConfigManager;
import config.IndexDefinition;
import config.PhysicalPlanConfig;
import java.util.List;
import logicaloperator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import physicaloperator.DuplicateEliminationOperator;
import physicaloperator.PhysicalOperator;
import physicaloperator.ProjectOperator;
import physicaloperator.SelectOperator;
import physicaloperator.join.BlockNestedLoopJoinOperator;
import physicaloperator.join.JoinTupleComparator;
import physicaloperator.join.SortMergeJoinOperator;
import physicaloperator.join.TupleNestedLoopJoinOperator;
import physicaloperator.scan.IndexConditionExtractor;
import physicaloperator.scan.IndexScanOperator;
import physicaloperator.scan.ScanOperator;
import physicaloperator.sort.ExternalSortOperator;
import physicaloperator.sort.InMemorySortOperator;

public class PhysicalPlanBuilder implements LogicalOperatorVisitor {

  private PhysicalOperator physicalPlan;
  private final PhysicalPlanConfig config;

  public PhysicalPlanBuilder() {
    this.config = PhysicalPlanConfig.getInstance();
  }

  // Getter for physical plan
  public PhysicalOperator getPhysicalPlan() {
    return physicalPlan;
  }

  // Visitor methods
  @Override
  public void visit(LogicalScan logicalScan) {
    this.physicalPlan = new ScanOperator(logicalScan.getTable());
  }

  @Override
  public void visit(LogicalSelect logicalSelect) {
    Expression condition = logicalSelect.getCondition();

    if (config.getScanMethod() == PhysicalPlanConfig.ScanMethod.INDEX_SCAN) {
      IndexConfigManager indexConfigManager = IndexConfigManager.getInstance();

      // get the table name
      LogicalScan logicalScan = (LogicalScan) logicalSelect.getChildOperator();
      Table table = logicalScan.getTable();
      String tableName = logicalScan.getTable().getName();

      // get the index definition for the table
      IndexDefinition index = indexConfigManager.getIndexForRelation(tableName);

      if (index != null) {

        // Try to split the condition into index-usable part and remainder
        IndexConditionExtractor conditionExtractor = new IndexConditionExtractor(index);
        condition.accept(conditionExtractor);

        PhysicalOperator plan;

        // if we have a valid index condition, use it
        Integer lowerBound = conditionExtractor.getLowerBound();
        Integer upperBound = conditionExtractor.getUpperBound();
        if (lowerBound != null || upperBound != null) {

          this.physicalPlan = new IndexScanOperator(table, index, lowerBound, upperBound);

          // if we have remainder conditions, wrap the index scan in a select operator
          Expression remainderConditions = conditionExtractor.getRemainderConditions();
          if (remainderConditions != null) {
            this.physicalPlan = new SelectOperator(this.physicalPlan, remainderConditions);
          }
          return;
        }
        // if we have no valid index condition, fall back to a regular scan
      }
      // if we have no index for the table, fall back to a regular scan
    }
    // if we are not using index scan, fall back to a regular scan

    logicalSelect.getChildOperator().accept(this);
    this.physicalPlan = new SelectOperator(this.physicalPlan, condition);
  }

  @Override
  public void visit(LogicalJoin logicalJoin) {
    logicalJoin.getLeftChild().accept(this);
    PhysicalOperator leftChild = this.physicalPlan;
    logicalJoin.getRightChild().accept(this);
    PhysicalOperator rightChild = this.physicalPlan;

    if (config.getJoinMethod() == PhysicalPlanConfig.JoinMethod.BNLJ) {
      this.physicalPlan =
          new BlockNestedLoopJoinOperator(
              leftChild, rightChild, logicalJoin.getCondition(), config.getJoinBufferPages());
    } else if (config.getJoinMethod() == PhysicalPlanConfig.JoinMethod.SMJ) {
      this.physicalPlan = buildSMJ(leftChild, rightChild, logicalJoin.getCondition());
    } else {
      this.physicalPlan =
          new TupleNestedLoopJoinOperator(leftChild, rightChild, logicalJoin.getCondition());
    }
  }

  @Override
  public void visit(LogicalSort logicalSort) {
    logicalSort.getChildOperator().accept(this);
    PhysicalOperator child = this.physicalPlan;

    if (config.getSortMethod() == PhysicalPlanConfig.SortMethod.EXTERNAL) {
      this.physicalPlan =
          new ExternalSortOperator(
              child, logicalSort.getOrderByElements(), config.getSortBufferPages());
    } else {
      this.physicalPlan = new InMemorySortOperator(child, logicalSort.getOrderByElements());
    }
  }

  @Override
  public void visit(LogicalDistinct logicalDistinct) {
    logicalDistinct.getChildOperator().accept(this);
    this.physicalPlan = new DuplicateEliminationOperator(this.physicalPlan);
  }

  @Override
  public void visit(LogicalProject logicalProject) {
    logicalProject.getChildOperator().accept(this);
    this.physicalPlan = new ProjectOperator(this.physicalPlan, logicalProject.getOutputSchema());
  }

  /** Helper method to build a SortMergeJoin operator */
  private PhysicalOperator buildSMJ(
      PhysicalOperator leftChild, PhysicalOperator rightChild, Expression joinCondition) {

    // If join condition is null, fall back to TNLJ
    if (joinCondition == null) {
      return new TupleNestedLoopJoinOperator(leftChild, rightChild, null);
    }

    // Extract the sort order for both left and right child
    SMJConditionExtractor smjConditionExtractor = new SMJConditionExtractor(leftChild, rightChild);
    joinCondition.accept(smjConditionExtractor);

    // If the join condition is not valid for SMJ, fall back to TNLJ
    if (!smjConditionExtractor.isValidSortMergeJoin()) {
      return new TupleNestedLoopJoinOperator(leftChild, rightChild, joinCondition);
    }

    // prepare left and right sort operators
    PhysicalOperator leftSort;
    List<OrderByElement> orderByElements1 = smjConditionExtractor.getLeftChildSortOrder();
    if (config.getSortMethod() == PhysicalPlanConfig.SortMethod.EXTERNAL) {
      leftSort = new ExternalSortOperator(leftChild, orderByElements1, config.getSortBufferPages());
    } else {
      leftSort = new InMemorySortOperator(leftChild, orderByElements1);
    }
    PhysicalOperator rightSort;
    List<OrderByElement> orderByElements = smjConditionExtractor.getRightChildSortOrder();
    if (config.getSortMethod() == PhysicalPlanConfig.SortMethod.EXTERNAL) {
      rightSort =
          new ExternalSortOperator(rightChild, orderByElements, config.getSortBufferPages());
    } else {
      rightSort = new InMemorySortOperator(rightChild, orderByElements);
    }

    // Create a join tuple comparator
    JoinTupleComparator joinTupleComparator =
        new JoinTupleComparator(
            leftSort.getOutputSchema(),
            smjConditionExtractor.getLeftChildSortOrder(),
            rightSort.getOutputSchema(),
            smjConditionExtractor.getRightChildSortOrder());

    return new SortMergeJoinOperator(leftSort, rightSort, joinTupleComparator);
  }
}
