package logicaloperator;

import common.LogicalOperatorVisitor;
import net.sf.jsqlparser.expression.Expression;

public class LogicalSelect extends LogicalOperator{
  private final LogicalOperator childOperator;
  private final Expression condition;

  public LogicalSelect(LogicalOperator child, Expression condition) {
    this.childOperator = child;
    this.condition = condition;
  }

  public LogicalOperator getChildOperator() {
    return childOperator;
  }

  public Expression getCondition() {
    return condition;
  }

  @Override
  public String toString() {
    return "Select[" + childOperator.toString() + " on " + condition + "]";
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }
}
