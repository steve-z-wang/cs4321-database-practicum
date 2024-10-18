package logicaloperator;

import common.LogicalOperatorVisitor;
import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class LogicalSort extends LogicalOperator {

  private final LogicalOperator childOperator;
  private final List<OrderByElement> OrderByElements;

  public LogicalSort(LogicalOperator child, List<OrderByElement> OrderByElements) {
    this.childOperator = child;
    this.OrderByElements = OrderByElements;
  }

  public LogicalOperator getChildOperator() {
    return childOperator;
  }

  public List<OrderByElement> getOrderByElements() {
    return OrderByElements;
  }

  @Override
  public String toString() {
    return "Sort[" + childOperator.toString() + " by " + OrderByElements + "]";
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }
}
