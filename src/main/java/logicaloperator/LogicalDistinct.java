package logicaloperator;

public class LogicalDistinct {

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
}
