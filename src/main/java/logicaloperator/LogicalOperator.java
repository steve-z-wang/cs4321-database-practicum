package logicaloperator;

import java.util.List;

public abstract class LogicalOperator {
  public abstract String toString();

  public abstract void accept(LogicalOperatorVisitor visitor);

  public abstract List<LogicalOperator> getChildren();
}
