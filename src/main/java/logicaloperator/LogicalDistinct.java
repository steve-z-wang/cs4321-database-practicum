package logicaloperator;

import common.LogicalOperatorVisitor;

public class LogicalDistinct extends LogicalOperator {

  private final LogicalOperator childOperator;

  public LogicalDistinct(LogicalOperator child) {
    this.childOperator = child;
  }

  public LogicalOperator getChildOperator() {
    return childOperator;
  }

  @Override
  public String toString() {
    return "Distinct[" + childOperator.toString() + "]";
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }
}
