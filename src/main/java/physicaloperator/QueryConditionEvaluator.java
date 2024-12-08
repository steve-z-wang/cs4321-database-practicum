package physicaloperator;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

/**
 * Evaluates boolean expressions.
 *
 * <p>Supports AndExpression, EqualsTo, NotEqualsTo, GreaterThan, GreaterThanEquals, MinorThan and
 * MinorThanEquals.
 */
public class QueryConditionEvaluator extends ExpressionVisitorAdapter<Boolean> {
  private final NumericExpressionEvaluator numericExpressionEvaluator =
      new NumericExpressionEvaluator();

  @Override
  public <S> Boolean visit(AndExpression andExpression, S context) {

    Boolean leftResult = andExpression.getLeftExpression().accept(this, context);
    if (!leftResult) {
      return false; // Short-circuit evaluation: if left is false, no need to evaluate right
    }

    Boolean rightResult = andExpression.getRightExpression().accept(this, context);
    return rightResult;
  }

  @Override
  public <S> Boolean visit(EqualsTo equalsTo, S context) {
    Integer leftValue = equalsTo.getLeftExpression().accept(numericExpressionEvaluator, context);
    Integer rightValue = equalsTo.getRightExpression().accept(numericExpressionEvaluator, context);

    return leftValue.equals(rightValue);
  }

  @Override
  public <S> Boolean visit(NotEqualsTo notEqualsTo, S context) {
    Integer leftValue = notEqualsTo.getLeftExpression().accept(numericExpressionEvaluator, context);
    Integer rightValue =
        notEqualsTo.getRightExpression().accept(numericExpressionEvaluator, context);

    return !leftValue.equals(rightValue);
  }

  @Override
  public <S> Boolean visit(GreaterThan greaterThan, S context) {
    Integer leftValue = greaterThan.getLeftExpression().accept(numericExpressionEvaluator, context);
    Integer rightValue =
        greaterThan.getRightExpression().accept(numericExpressionEvaluator, context);

    return leftValue > rightValue;
  }

  @Override
  public <S> Boolean visit(GreaterThanEquals greaterThanEquals, S context) {
    Integer leftValue =
        greaterThanEquals.getLeftExpression().accept(numericExpressionEvaluator, context);
    Integer rightValue =
        greaterThanEquals.getRightExpression().accept(numericExpressionEvaluator, context);

    return leftValue >= rightValue;
  }

  @Override
  public <S> Boolean visit(MinorThan minorThan, S context) {
    Integer leftValue = minorThan.getLeftExpression().accept(numericExpressionEvaluator, context);
    Integer rightValue = minorThan.getRightExpression().accept(numericExpressionEvaluator, context);

    return leftValue < rightValue;
  }

  @Override
  public <S> Boolean visit(MinorThanEquals minorThanEquals, S context) {
    Integer leftValue =
        minorThanEquals.getLeftExpression().accept(numericExpressionEvaluator, context);
    Integer rightValue =
        minorThanEquals.getRightExpression().accept(numericExpressionEvaluator, context);

    return leftValue <= rightValue;
  }

  static class NumericExpressionEvaluator extends ExpressionVisitorAdapter<Integer> {

    @Override
    public <S> Integer visit(LongValue longValue, S context) {
      return (int) longValue.getValue();
    }

    @Override
    public <S> Integer visit(Column column, S context) {
      QueryConditionContext queryConditionContext = (QueryConditionContext) context;
      return queryConditionContext.getColumnValue(column);
    }
  }
}
