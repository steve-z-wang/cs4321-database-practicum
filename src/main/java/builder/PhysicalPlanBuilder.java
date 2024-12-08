package builder;

import config.PhysicalPlanConfig;
import java.util.List;
import logicaloperator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import physicaloperator.DuplicateEliminationOperator;
import physicaloperator.PhysicalOperator;
import physicaloperator.ProjectOperator;
import physicaloperator.ScanOperator;
import physicaloperator.SelectOperator;
import physicaloperator.join.BlockNestedLoopJoinOperator;
import physicaloperator.join.JoinTupleComparator;
import physicaloperator.join.SortMergeJoinOperator;
import physicaloperator.join.TupleNestedLoopJoinOperator;
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

  private PhysicalOperator getJoinOperator(
      PhysicalOperator leftChild, PhysicalOperator rightChild, Expression joinCondition) {
    switch (config.getJoinMethod()) {
      case BNLJ:
        return new BlockNestedLoopJoinOperator(leftChild, rightChild, joinCondition);
      case SMJ:

        // if the join condition is null, fall back to BNLJ
        if (joinCondition == null) {
          return new BlockNestedLoopJoinOperator(
              leftChild, rightChild, null, PhysicalPlanConfig.DEFAULT_BNLJ_BUFFER_PAGES);
        }

        // Extract the sort order for both left and right child
        SMJConditionExtractor smjConditionExtractor =
            new SMJConditionExtractor(leftChild, rightChild);
        joinCondition.accept(smjConditionExtractor);

        // If the join condition is not valid for SMJ, fall back to BNLJ
        if (!smjConditionExtractor.isValidSortMergeJoin()) {
          return new BlockNestedLoopJoinOperator(
              leftChild, rightChild, joinCondition, PhysicalPlanConfig.DEFAULT_BNLJ_BUFFER_PAGES);
        }

        // Create left and right sort operators
        PhysicalOperator leftSort =
            getSortOperator(leftChild, smjConditionExtractor.getLeftChildSortOrder());
        PhysicalOperator rightSort =
            getSortOperator(rightChild, smjConditionExtractor.getRightChildSortOrder());

        // Create a join tuple comparator
        JoinTupleComparator joinTupleComparator =
            new JoinTupleComparator(
                leftSort.getOutputSchema(),
                smjConditionExtractor.getLeftChildSortOrder(),
                rightSort.getOutputSchema(),
                smjConditionExtractor.getRightChildSortOrder());

        return new SortMergeJoinOperator(leftSort, rightSort, joinTupleComparator);

      default: // TNLJ
        return new TupleNestedLoopJoinOperator(leftChild, rightChild, joinCondition);
    }
  }

  private PhysicalOperator getSortOperator(
      PhysicalOperator child, List<OrderByElement> OrderByElements) {
    if (config.getSortMethod() == PhysicalPlanConfig.SortMethod.IN_MEMORY) {
      return new InMemorySortOperator(child, OrderByElements);
    } else {
      return new ExternalSortOperator(child, OrderByElements);
    }
  }

  // Visitor methods
  @Override
  public void visit(LogicalScan logicalScan) {
    this.physicalPlan = new ScanOperator(logicalScan.getTable());
  }

  @Override
  public void visit(LogicalSelect logicalSelect) {
    logicalSelect.getChildOperator().accept(this);
    PhysicalOperator child = this.physicalPlan;
    this.physicalPlan = new SelectOperator(child, logicalSelect.getCondition());
  }

  @Override
  public void visit(LogicalJoin logicalJoin) {
    logicalJoin.getLeftChild().accept(this);
    PhysicalOperator leftChild = this.physicalPlan;
    logicalJoin.getRightChild().accept(this);
    PhysicalOperator rightChild = this.physicalPlan;

    this.physicalPlan = getJoinOperator(leftChild, rightChild, logicalJoin.getCondition());
  }

  @Override
  public void visit(LogicalSort logicalSort) {
    logicalSort.getChildOperator().accept(this);
    PhysicalOperator child = this.physicalPlan;

    this.physicalPlan = getSortOperator(child, logicalSort.getOrderByElements());
  }

  @Override
  public void visit(LogicalDistinct logicalDistinct) {
    logicalDistinct.getChildOperator().accept(this);
    PhysicalOperator child = this.physicalPlan;
    this.physicalPlan = new DuplicateEliminationOperator(child);
  }

  @Override
  public void visit(LogicalProject logicalProject) {
    logicalProject.getChildOperator().accept(this);
    PhysicalOperator child = this.physicalPlan;
    this.physicalPlan = new ProjectOperator(child, logicalProject.getOutputSchema());
  }
}
