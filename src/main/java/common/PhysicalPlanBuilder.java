package common;

import logicaloperator.*;
import physicaloperator.*;

public class PhysicalPlanBuilder implements LogicalOperatorVisitor {

  private Operator root;

  public PhysicalPlanBuilder() {
    root = null;
    // read the config file
  }

  public Operator getPhysicalPlan() {
    return root;
  }

  @Override
  public void visit(LogicalScan logicalScan) {
    this.root = new ScanOperator(logicalScan.getTable());
  }

  @Override
  public void visit(LogicalSelect logicalSelect) {
    logicalSelect.getChildOperator().accept(this);
    Operator child = this.root;
    this.root = new SelectOperator(child, logicalSelect.getCondition());
  }

  @Override
  public void visit(LogicalJoin logicalJoin) {
    logicalJoin.getLeftChild().accept(this);
    Operator leftChild = this.root;
    logicalJoin.getRightChild().accept(this);
    Operator rightChild = this.root;
    this.root = new JoinOperator(leftChild, rightChild, logicalJoin.getCondition());
  }

  @Override
  public void visit(LogicalSort logicalSort) {
    logicalSort.getChildOperator().accept(this);
    Operator child = this.root;
    this.root = new SortOperator(child, logicalSort.getOrderByElements());
  }

  @Override
  public void visit(LogicalDistinct logicalDistinct) {
    logicalDistinct.getChildOperator().accept(this);
    Operator child = this.root;
    this.root = new DuplicateEliminationOperator(child);
  }

  @Override
  public void visit(LogicalProject logicalProject) {
    logicalProject.getChildOperator().accept(this);
    Operator child = this.root;
    this.root = new ProjectOperator(child, logicalProject.getOutputSchema());
  }
}
