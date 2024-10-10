package physicaloperator;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

/**
 * Evaluates boolean expressions.
 *
 * <p>Supports AndExpression, EqualsTo, NotEqualsTo, GreaterThan, GreaterThanEquals, MinorThan and
 * MinorThanEquals.
 */
public class BooleanEvaluator extends ExpressionVisitorAdapter<Boolean> {
  private final NumericEvaluator numericEvaluator = new NumericEvaluator();

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
    Integer leftValue = equalsTo.getLeftExpression().accept(numericEvaluator, context);
    Integer rightValue = equalsTo.getRightExpression().accept(numericEvaluator, context);

    return leftValue.equals(rightValue);
  }

  @Override
  public <S> Boolean visit(NotEqualsTo notEqualsTo, S context) {
    Integer leftValue = notEqualsTo.getLeftExpression().accept(numericEvaluator, context);
    Integer rightValue = notEqualsTo.getRightExpression().accept(numericEvaluator, context);

    return !leftValue.equals(rightValue);
  }

  @Override
  public <S> Boolean visit(GreaterThan greaterThan, S context) {
    Integer leftValue = greaterThan.getLeftExpression().accept(numericEvaluator, context);
    Integer rightValue = greaterThan.getRightExpression().accept(numericEvaluator, context);

    return leftValue > rightValue;
  }

  @Override
  public <S> Boolean visit(GreaterThanEquals greaterThanEquals, S context) {
    Integer leftValue = greaterThanEquals.getLeftExpression().accept(numericEvaluator, context);
    Integer rightValue = greaterThanEquals.getRightExpression().accept(numericEvaluator, context);

    return leftValue >= rightValue;
  }

  @Override
  public <S> Boolean visit(MinorThan minorThan, S context) {
    Integer leftValue = minorThan.getLeftExpression().accept(numericEvaluator, context);
    Integer rightValue = minorThan.getRightExpression().accept(numericEvaluator, context);

    return leftValue < rightValue;
  }

  @Override
  public <S> Boolean visit(MinorThanEquals minorThanEquals, S context) {
    Integer leftValue = minorThanEquals.getLeftExpression().accept(numericEvaluator, context);
    Integer rightValue = minorThanEquals.getRightExpression().accept(numericEvaluator, context);

    return leftValue <= rightValue;
  }
}
