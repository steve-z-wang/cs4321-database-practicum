package physicaloperator.scan;

import config.IndexDefinition;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

public class IndexConditionExtractor extends ExpressionVisitorAdapter<Void> {

  private final IndexDefinition indexDefinition;

  // extraction results
  private Integer lowerBound;
  private Integer upperBound;
  private Expression remainderConditions;

  public IndexConditionExtractor(IndexDefinition indexDefinition) {
    this.indexDefinition = indexDefinition;
    this.lowerBound = null;
    this.upperBound = null;
    this.remainderConditions = null;
  }

  public Integer getLowerBound() {
    return lowerBound;
  }

  public Integer getUpperBound() {
    return upperBound;
  }

  public Expression getRemainderConditions() {
    return remainderConditions;
  }

  @Override
  public <S> Void visit(AndExpression op, S context) {
    op.getLeftExpression().accept(this);
    op.getRightExpression().accept(this);
    return null;
  }

  @Override
  public <S> Void visit(EqualsTo op, S context) {
    addRemainderCondition(op);
    return null;
  }

  @Override
  public <S> Void visit(NotEqualsTo op, S context) {
    addRemainderCondition(op);
    return null;
  }

  /** for >= operator */
  @Override
  public <S> Void visit(GreaterThanEquals op, S context) {
    Expression left = op.getLeftExpression();
    Expression right = op.getRightExpression();
    handleGreaterThan(op, left, right, 0);
    return null;
  }

  /** for > operator */
  @Override
  public <S> Void visit(GreaterThan op, S context) {
    Expression left = op.getLeftExpression();
    Expression right = op.getRightExpression();
    handleGreaterThan(op, left, right, 1);
    return null;
  }

  @Override
  public <S> Void visit(MinorThanEquals op, S context) {
    Expression left = op.getLeftExpression();
    Expression right = op.getRightExpression();
    handleMinorThan(op, left, right, 0);
    return null;
  }

  @Override
  public <S> Void visit(MinorThan op, S context) {
    Expression left = op.getLeftExpression();
    Expression right = op.getRightExpression();
    handleMinorThan(op, left, right, 1);
    return null;
  }

  /** Helper methods */
  private void handleGreaterThan(Expression op, Expression left, Expression right, int minusRange) {
    if (left instanceof Column leftColumn && right instanceof LongValue rightNum) {
      if (isValidIndexColumn(leftColumn)) {
        addLowerBound((int) rightNum.getValue() + minusRange);
      } else {
        addRemainderCondition(op);
      }
    } else if (left instanceof LongValue leftNum && right instanceof Column rightColumn) {
      if (isValidIndexColumn(rightColumn)) {
        addUpperBound((int) leftNum.getValue() - 1);
      } else {
        addRemainderCondition(op);
      }
    } else {
      throw new IllegalArgumentException("Invalid expression: " + op);
    }
  }

  private void handleMinorThan(Expression op, Expression left, Expression right, int minusRange) {
    if (left instanceof Column leftColumn && right instanceof LongValue rightNum) {
      if (isValidIndexColumn(leftColumn)) {
        addUpperBound((int) rightNum.getValue() - minusRange);
      } else {
        addRemainderCondition(op);
      }
    } else if (left instanceof LongValue leftNum && right instanceof Column rightColumn) {
      if (isValidIndexColumn(rightColumn)) {
        addLowerBound((int) leftNum.getValue() + minusRange);
      } else {
        addRemainderCondition(op);
      }
    } else {
      throw new IllegalArgumentException("Invalid expression: " + op);
    }
  }

  private boolean isValidIndexColumn(Column column) {
    return column.getColumnName().equals(indexDefinition.getAttribute());
  }

  private void addLowerBound(int value) {
    if (lowerBound == null || value > lowerBound) {
      lowerBound = value;
    }
  }

  private void addUpperBound(int value) {
    if (upperBound == null || value < upperBound) {
      upperBound = value;
    }
  }

  private void addRemainderCondition(Expression condition) {
    if (remainderConditions == null) {
      remainderConditions = condition;
    } else {
      remainderConditions = new AndExpression(condition, remainderConditions);
    }
  }
}
