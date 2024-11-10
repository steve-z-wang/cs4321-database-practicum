package logicaloperator;

import net.sf.jsqlparser.expression.Expression;

public class LogicalJoin extends LogicalOperator {

  private final LogicalOperator leftChild;
  private final LogicalOperator rightChild;
  private final Expression condition;

  public LogicalJoin(LogicalOperator leftChild, LogicalOperator rightChild, Expression condition) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.condition = condition;
  }

  public LogicalOperator getLeftChild() {
    return leftChild;
  }

  public LogicalOperator getRightChild() {
    return rightChild;
  }

  public Expression getCondition() {
    return condition;
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return "Join["
        + leftChild.toString()
        + " and "
        + rightChild.toString()
        + " on "
        + condition
        + "]";
  }
}
