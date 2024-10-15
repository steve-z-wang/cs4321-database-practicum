package logicaloperator;

import common.LogicalOperatorVisitor;

public abstract class LogicalOperator {
  public abstract String toString();

  public abstract void accept(LogicalOperatorVisitor visitor);




}
