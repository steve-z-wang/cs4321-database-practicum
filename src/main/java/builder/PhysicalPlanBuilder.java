package builder;

import config.PhysicalPlanConfig;
import logicaloperator.*;
import physicaloperator.base.Operator;
import physicaloperator.join.BlockNestedLoopJoinOperator;
import physicaloperator.join.SortMergeJoinOperator;
import physicaloperator.join.TupleNestedLoopJoinOperator;
import physicaloperator.other.DuplicateEliminationOperator;
import physicaloperator.other.ProjectOperator;
import physicaloperator.other.ScanOperator;
import physicaloperator.other.SelectOperator;
import physicaloperator.sort.ExternalSortOperator;
import physicaloperator.sort.InMemorySortOperator;

public class PhysicalPlanBuilder implements LogicalOperatorVisitor {

  private Operator physicalPlan;
  private final PhysicalPlanConfig config;

  public PhysicalPlanBuilder() {
    this.config = PhysicalPlanConfig.getInstance();
  }

  // Getter for physical plan
  public Operator getPhysicalPlan() {
    return physicalPlan;
  }

  // Visitor methods
  @Override
  public void visit(LogicalScan logicalScan) {
    this.physicalPlan = new ScanOperator(logicalScan.getTable());
  }

  @Override
  public void visit(LogicalSelect logicalSelect) {
    logicalSelect.getChildOperator().accept(this);
    Operator child = this.physicalPlan;
    this.physicalPlan = new SelectOperator(child, logicalSelect.getCondition());
  }

  @Override
  public void visit(LogicalJoin logicalJoin) {
    logicalJoin.getLeftChild().accept(this);
    Operator leftChild = this.physicalPlan;
    logicalJoin.getRightChild().accept(this);
    Operator rightChild = this.physicalPlan;

    switch (config.getJoinMethod()) {
      case TNLJ:
        this.physicalPlan =
            new TupleNestedLoopJoinOperator(leftChild, rightChild, logicalJoin.getCondition());
        break;
      case BNLJ:
        this.physicalPlan =
            new BlockNestedLoopJoinOperator(
                leftChild, rightChild, logicalJoin.getCondition(), config.getJoinBufferPages());
        break;
      case SMJ:
        this.physicalPlan =
            new SortMergeJoinOperator(leftChild, rightChild, logicalJoin.getCondition());
    }
  }

  @Override
  public void visit(LogicalSort logicalSort) {
    logicalSort.getChildOperator().accept(this);
    Operator child = this.physicalPlan;

    if (config.getSortMethod() == PhysicalPlanConfig.SortMethod.IN_MEMORY) {
      this.physicalPlan = new InMemorySortOperator(child, logicalSort.getOrderByElements());
    } else {
      this.physicalPlan =
          new ExternalSortOperator(
              child, logicalSort.getOrderByElements(), config.getSortBufferPages());
    }
  }

  @Override
  public void visit(LogicalDistinct logicalDistinct) {
    logicalDistinct.getChildOperator().accept(this);
    Operator child = this.physicalPlan;
    this.physicalPlan = new DuplicateEliminationOperator(child);
  }

  @Override
  public void visit(LogicalProject logicalProject) {
    logicalProject.getChildOperator().accept(this);
    Operator child = this.physicalPlan;
    this.physicalPlan = new ProjectOperator(child, logicalProject.getOutputSchema());
  }
}
